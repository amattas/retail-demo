"""Producers of the Eventhouse anomaly and pricing-approval tables match the real DDL."""

import json
import re
from datetime import UTC, datetime
from pathlib import Path

from deploy.scripts._ontology_model import parse_kql_tables

ROOT = Path(__file__).resolve().parents[2]


def kql_table_schemas(path: Path) -> dict[str, tuple[str, ...]]:
    return {
        table: tuple(name for name, _type in columns)
        for table, columns in parse_kql_tables(path.read_text(encoding="utf-8")).items()
    }

KQL = ROOT / "fabric" / "kql_database"
LAKEHOUSE = ROOT / "fabric" / "lakehouse"


def _cells(name: str) -> list[str]:
    notebook = json.loads((LAKEHOUSE / name).read_text(encoding="utf-8"))
    return ["".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"]


def _cell_defining(name: str, marker: str) -> str:
    return next(source for source in _cells(name) if marker in source)


def _anomaly_helpers() -> dict:
    namespace: dict = {"ANOMALY_LOOKBACK_HOURS": 168, "ANOMALY_PERSIST_HOURS": 24}
    exec(  # noqa: S102 - executes the repository notebook's pure helper cell
        "import hashlib\nfrom datetime import timezone\n"
        + _cell_defining("16-persist-anomaly-alerts.ipynb", "ALERT_COLUMNS = ("),
        namespace,
    )
    return namespace


def test_anomaly_producer_writes_the_anomaly_alerts_columns_in_table_order():
    tables = kql_table_schemas(KQL / "06-ml-anomaly-detection.kql")
    helpers = _anomaly_helpers()
    assert helpers["ALERT_COLUMNS"] == tuple(tables["anomaly_alerts"])
    run_columns = tuple(
        part.split()[0] for part in helpers["RUN_SCHEMA"].split(",")
    )
    assert run_columns == tuple(tables["anomaly_detection_runs"])


def test_persistence_function_projects_anomaly_alerts_and_skips_persisted_ids():
    text = (KQL / "06-ml-anomaly-detection.kql").read_text(encoding="utf-8")
    body = text.split(") fn_new_anomaly_alerts(", 1)[1].split("\n}\n", 1)[0]
    assert "join kind=leftanti (anomaly_alerts | project alert_id) on alert_id" in body
    assert "time_window < window_end" in body
    projected = re.search(r"\n    \| project (.*)$", body, re.DOTALL)
    assert projected is not None
    columns = tuple(column.strip() for column in projected.group(1).split(","))
    assert columns == tuple(kql_table_schemas(KQL / "06-ml-anomaly-detection.kql")["anomaly_alerts"])


def test_anomaly_run_identity_is_stable_and_excludes_the_open_hour():
    helpers = _anomaly_helpers()
    now = datetime(2026, 9, 7, 12, 41, 5, tzinfo=UTC)
    assert helpers["closed_bin_end"](now) == datetime(2026, 9, 7, 12, tzinfo=UTC)
    assert helpers["detection_query"](helpers["closed_bin_end"](now), 168, 24) == (
        "fn_new_anomaly_alerts(168, 24, datetime(2026-09-07T12:00:00Z))"
    )
    first = helpers["alert_batch_identity"](["b", "a"])
    assert first == helpers["alert_batch_identity"](["a", "b"])
    assert first != helpers["alert_batch_identity"](["a"])


def test_anomaly_producer_runs_in_the_streaming_pipeline():
    content = json.loads(
        (ROOT / "fabric" / "pipelines" / "streaming-data-load.DataPipeline" / "pipeline-content.json")
        .read_text(encoding="utf-8")
    )
    names = [activity["name"] for activity in content["properties"]["activities"]]
    assert "16-persist-anomaly-alerts" in names


def test_pricing_notebook_publishes_pending_events_in_ddl_order_from_gold_columns():
    created = kql_table_schemas(KQL / "07-pricing-approval-tables.kql")[
        "pricing_recommendation_created"
    ]
    source = _cell_defining("14-ml-dynamic-pricing.ipynb", "PRICING_EVENT_COLUMNS = (")
    declared = re.search(r"PRICING_EVENT_COLUMNS = \((.*?)\)\n", source, re.DOTALL)
    assert declared is not None
    columns = tuple(re.findall(r'"([a-z_]+)"', declared.group(1)))
    assert columns == tuple(created)
    mapping = source.split("def pricing_approval_events", 1)[1].split("\ndef ", 1)[0]
    aliases = re.findall(r'\.alias\("([a-z_]+)"\)', mapping)
    assert [alias for alias in aliases if alias in created] == [
        column for column in columns if column != "model_type"
    ]
    manifest = json.loads((ROOT / "contracts" / "retail-demo.json").read_text(encoding="utf-8"))
    gold = next(
        contract["output"]["fields"]
        for contract in manifest["ml_contracts"]
        if contract["output"]["table"] == "pricing_recommendations"
    )
    gold_columns = {field["name"] for field in gold}
    read = set(re.findall(r'F\.col\("([A-Za-z_]+)"\)', mapping))
    read |= set(re.findall(r'"(hit_min_margin|hit_max_price|violates_max_change|'
                           r'experimental_status|reason_codes)"', mapping))
    assert read <= gold_columns
    assert 'F.lit("PENDING").alias("status")' in mapping
