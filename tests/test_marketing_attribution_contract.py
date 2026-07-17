"""Cross-layer contracts for deterministic marketing attribution."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STREAM_TEMPLATE = ROOT / "utility" / "notebooks" / "templates" / "driver-05-stream.py"
KQL_TABLES = ROOT / "fabric" / "kql_database" / "01-create-tables.kql"
KQL_MAPPINGS = ROOT / "fabric" / "kql_database" / "02-create-ingestion-mappings.kql"
KQL_FUNCTIONS = ROOT / "fabric" / "kql_database" / "03-create-functions.kql"
SILVER_NOTEBOOK = ROOT / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb"
GOLD_NOTEBOOK = ROOT / "fabric" / "lakehouse" / "04-streaming-to-gold.ipynb"
SCHEMAS = ROOT / "utility" / "src" / "retail_setup" / "generation" / "schemas.py"
GOLD = ROOT / "utility" / "src" / "retail_setup" / "generation" / "gold.py"

PURCHASE_FIELDS = {
    "campaign_id",
    "impression_id",
    "gross_subtotal_cents",
    "discount_cents",
    "subtotal_cents",
    "tax_cents",
    "total_cents",
}


def _assignment(source: str, name: str):
    tree = ast.parse(source)
    return next(
        node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        )
    )


def _event_payloads() -> dict[str, list[tuple[str, str, str]]]:
    source = STREAM_TEMPLATE.read_text(encoding="utf-8")
    return ast.literal_eval(_assignment(source, "EVENT_PAYLOADS"))


def _envelope_columns() -> list[str]:
    source = STREAM_TEMPLATE.read_text(encoding="utf-8")
    return [name for name, _type in ast.literal_eval(_assignment(source, "ENVELOPE"))]


def _kql_columns(table: str) -> list[str]:
    source = KQL_TABLES.read_text(encoding="utf-8")
    match = re.search(
        rf"^\.create-merge table {re.escape(table)} \(\n(?P<body>.*?)^\)",
        source,
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"missing KQL table {table}"
    return [
        line.strip().split(":", 1)[0]
        for line in match.group("body").splitlines()
        if ":" in line
    ]


def _mapping_block(table: str) -> str:
    source = KQL_MAPPINGS.read_text(encoding="utf-8")
    match = re.search(
        rf"^\.create-or-alter table {re.escape(table)} ingestion json mapping "
        rf"'EventMapping'\n```\n(?P<body>\[.*?\])\n```",
        source,
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"missing KQL mapping {table}"
    return match.group("body")


def _notebook_code(path: Path) -> str:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )


def test_live_payloads_match_kql_tables_and_json_mappings() -> None:
    payloads = _event_payloads()
    expected = {
        "receipt_created": PURCHASE_FIELDS,
        "online_order_created": PURCHASE_FIELDS,
        "ad_impression": {"customer_id"},
    }

    for event_type, required_fields in expected.items():
        payload_fields = {column for column, _path, _type in payloads[event_type]}
        kql_fields = set(_kql_columns(event_type))
        mapping = _mapping_block(event_type)
        assert required_fields <= payload_fields
        assert required_fields <= kql_fields
        for field in required_fields:
            assert f'"column":"{field}"' in mapping
            assert f'"path":"$.payload.{field}"' in mapping


def test_typed_spark_projection_matches_every_kql_table_order() -> None:
    envelope = _envelope_columns()
    for event_type, fields in _event_payloads().items():
        payload_columns = [column for column, _path, _type in fields]
        assert _kql_columns(event_type) == payload_columns + envelope


def test_attribution_uses_existing_envelope_without_new_event_type() -> None:
    payloads = _event_payloads()
    source = STREAM_TEMPLATE.read_text(encoding="utf-8")

    assert len(payloads) == 18
    assert "marketing_attribution" not in payloads
    assert '_str(correlation).alias("correlation_id")' in source
    assert "attribution_journey_id" not in {
        field for fields in payloads.values() for field, _path, _type in fields
    }


def test_kql_functions_enforce_last_touch_and_reconciliation() -> None:
    source = KQL_FUNCTIONS.read_text(encoding="utf-8")

    marketing_pos = source.index("fn_marketing_attribution()")
    wrapper_pos = source.index("fn_attribution_window(")
    performance_pos = source.index("fn_campaign_performance(")
    assert marketing_pos < wrapper_pos < performance_pos

    for expected in (
        "touch_rank = strcat(",
        "arg_max(touch_rank, *)",
        "lag_seconds between (0 .. 604800)",
        'payment_status == "APPROVED"',
        "payment_cents == total_cents",
        "promotion_discount_cents == discount_cents",
        "gross_subtotal_cents - discount_cents == subtotal_cents",
        "subtotal_cents + tax_cents == total_cents",
        'attribution_model = "LAST_TOUCH_7D"',
        "attributed_revenue_cents",
        "or impression_id in (converted_impressions)",
    ):
        assert expected in source


def test_querysets_use_reconciled_attribution_functions() -> None:
    funnel = ROOT / "fabric" / "querysets" / "q_campaign_conversion_funnel.kql"
    cost = ROOT / "fabric" / "querysets" / "q_marketing_cost_24h.kql"
    reconciliation = (
        ROOT / "fabric" / "querysets" / "q_attribution_reconciliation_24h.kql"
    )

    assert "fn_campaign_performance(7d)" in funnel.read_text(encoding="utf-8")
    assert "fn_campaign_performance(24h)" in cost.read_text(encoding="utf-8")
    reconciliation_text = reconciliation.read_text(encoding="utf-8")
    assert "fn_marketing_attribution()" in reconciliation_text
    assert "payment_total_delta" in reconciliation_text


def test_silver_persists_only_complete_reconciled_journeys() -> None:
    source = _notebook_code(SILVER_NOTEBOOK)
    compile(source, "<03-streaming-to-silver>", "exec")

    for expected in (
        '"marketing_attribution": ["attribution_id"]',
        "def process_marketing_attribution():",
        'F.col("event_ts").desc(), F.col("impression_id_ext").desc()',
        'F.col("payment_status") == "APPROVED"',
        'F.col("payment_cents") == F.col("total_cents")',
        'F.col("promotion_discount_cents") == F.col("discount_cents")',
        'F.lit("UNATTRIBUTED_NO_JOURNEY").alias("attribution_status")',
        "attribution = attribution.unionByName(unattributed)",
        '"fact_marketing_attribution"',
        "total += process_marketing_attribution()",
    ):
        assert expected in source


def test_gold_contract_exists_in_batch_and_streaming_paths() -> None:
    schema_source = SCHEMAS.read_text(encoding="utf-8")
    gold_source = GOLD.read_text(encoding="utf-8")
    notebook_source = _notebook_code(GOLD_NOTEBOOK)
    compile(notebook_source, "<04-streaming-to-gold>", "exec")

    for source in (schema_source, gold_source, notebook_source):
        assert "campaign_performance_daily" in source
    for expected in (
        "spend_cents",
        "conversions",
        "attributed_revenue_cents",
        "conversion_rate",
        "roas",
    ):
        assert expected in schema_source
        assert expected in gold_source
        assert expected in notebook_source
