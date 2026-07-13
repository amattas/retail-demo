"""Cross-layer contracts for the truck arrival/departure lifecycle."""

import ast
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
STREAM_TEMPLATE = ROOT / "utility" / "notebooks" / "templates" / "driver-05-stream.py"
STREAM_NOTEBOOK = ROOT / "utility" / "notebooks" / "stream-events.ipynb"
SILVER_NOTEBOOK = ROOT / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb"
GOLD_NOTEBOOK = ROOT / "fabric" / "lakehouse" / "04-streaming-to-gold.ipynb"
KQL_TABLES = ROOT / "fabric" / "kql_database" / "01-create-tables.kql"
KQL_FUNCTIONS = ROOT / "fabric" / "kql_database" / "03-create-functions.kql"
QUERYSET = ROOT / "fabric" / "querysets" / "q_truck_dwell_by_site.kql"
RULES = ROOT / "fabric" / "rules" / "definitions.kql"
DASHBOARD = ROOT / "fabric" / "dashboards" / "retail-ops.template.json"
SCHEMAS = ROOT / "utility" / "src" / "retail_setup" / "generation" / "schemas.py"

LIFECYCLE_KEYS = ["truck_id", "dc_id", "store_id", "shipment_id"]


def _notebook_source(path: Path) -> str:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )


def _assignment(tree: ast.Module, name: str) -> Any:
    node = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        )
    )
    return ast.literal_eval(node.value)


def _kql_columns(table: str) -> list[str]:
    text = KQL_TABLES.read_text(encoding="utf-8")
    match = re.search(
        rf"\.create-merge table {table} \((.*?)\n\)",
        text,
        re.DOTALL,
    )
    assert match is not None
    return re.findall(r"^\s*([a-z_]+):", match.group(1), re.MULTILINE)


def test_generator_emits_shared_keys_and_positive_late_dwell() -> None:
    source = STREAM_TEMPLATE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    payloads = _assignment(tree, "EVENT_PAYLOADS")
    threshold = _assignment(tree, "TRUCK_DWELL_THRESHOLD_MINUTES")
    normal_min = _assignment(tree, "TRUCK_NORMAL_DWELL_MINUTES_MIN")
    normal_max = _assignment(tree, "TRUCK_NORMAL_DWELL_MINUTES_MAX")
    late_minutes = _assignment(tree, "TRUCK_LATE_DWELL_MINUTES")

    for event_type in ("truck_arrived", "truck_departed"):
        assert [column for column, _field, _type in payloads[event_type]][
            :4
        ] == LIFECYCLE_KEYS
        assert [
            column for column, _field, _type in payloads[event_type]
        ] == _kql_columns(event_type)[: len(payloads[event_type])]

    arrival = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    departure = arrival + timedelta(minutes=late_minutes)
    assert 0 < normal_min <= normal_max < threshold
    assert late_minutes > threshold == 90
    assert departure > arrival
    assert (departure - arrival).total_seconds() > 0

    assert '_iso(truck_arrival_ts).alias("arrival_time")' in source
    assert '_iso(truck_departure_ts).alias("departure_time")' in source
    assert (
        "F.unix_timestamp(truck_arrival_ts) + truck_dwell_minutes * F.lit(60)" in source
    )
    assert 'truck_dwell_minutes.alias("actual_unload_duration")' in source
    assert source.count('session=F.col("shipment_id")') == 2
    assert 'F.pmod(F.col("v"), F.lit(TRUCK_LATE_BUCKET_MODULUS))' in source


def test_generated_stream_notebook_contains_lifecycle_timing_contract() -> None:
    source = _notebook_source(STREAM_NOTEBOOK)
    assert "TRUCK_LATE_DWELL_MINUTES = 120" in source
    assert '_iso(truck_departure_ts).alias("departure_time")' in source
    assert "), truck_departure_ts," in source


def test_silver_joins_one_completed_lifecycle_on_authoritative_keys() -> None:
    source = _notebook_source(SILVER_NOTEBOOK)
    schema_source = SCHEMAS.read_text(encoding="utf-8")

    assert f"TRUCK_LIFECYCLE_KEYS = {LIFECYCLE_KEYS!r}".replace("'", '"') in source
    assert "arrivals.groupBy(*TRUCK_LIFECYCLE_KEYS)" in source
    assert 'F.min("arrival_time").alias("arrival_time")' in source
    assert "departures.groupBy(*TRUCK_LIFECYCLE_KEYS)" in source
    assert 'F.max("departure_time").alias("departure_time")' in source
    assert 'arrived.join(departed, join_condition, "inner")' in source
    assert 'paired.join(existing, TRUCK_LIFECYCLE_KEYS, "left_anti")' in source
    assert 'F.col("departed.departure_time").alias("etd")' in source
    assert 'F.col("departed.departure_time").alias("departure_time")' in source
    assert '.filter(F.col("etd") > F.col("eta"))' in source
    assert "total += process_truck_lifecycles()" in source
    assert 'process_events("truck_arrived"' not in source
    assert 'process_events("truck_departed"' not in source
    assert 'regexp_extract(F.col("arrived.truck_id"), r"(\\d+)$", 1)' in source

    schema_block = re.search(
        r'"fact_truck_moves": \[(.*?)\n\s*\],',
        schema_source,
        re.DOTALL,
    )
    assert schema_block is not None
    for column in (*LIFECYCLE_KEYS, "eta", "etd", "departure_time"):
        assert f'("{column}",' in schema_block.group(1)


def test_gold_aggregates_one_lifecycle_in_minutes_with_canonical_sites() -> None:
    source = _notebook_source(GOLD_NOTEBOOK)

    assert f"lifecycle_keys = {LIFECYCLE_KEYS!r}".replace("'", '"') in source
    assert ".groupBy(*lifecycle_keys)" in source
    assert 'F.min("eta").alias("arrival_time")' in source
    assert 'F.col("departure_time") > F.col("arrival_time")' in source
    assert 'F.lit("STORE_")' in source
    assert 'F.lit("DC_")' in source
    assert "/ 60.0" in source
    assert 'F.avg("dwell_min").alias("avg_dwell_min")' in source
    assert 'F.count("*").alias("trucks")' in source


def test_eventhouse_queryset_dashboard_and_rule_share_units_and_threshold() -> None:
    function = KQL_FUNCTIONS.read_text(encoding="utf-8")
    queryset = QUERYSET.read_text(encoding="utf-8")
    rules = RULES.read_text(encoding="utf-8")
    dashboard = DASHBOARD.read_text(encoding="utf-8")

    assert ") on truck_id, dc_id, store_id, shipment_id" in function
    assert (
        "arg_min(arrival_time, *) by truck_id, dc_id, store_id, shipment_id" in function
    )
    assert (
        "arg_max(departure_time, *) by truck_id, dc_id, store_id, shipment_id"
        in function
    )
    assert "where dwell_seconds > 0" in function
    assert "dwell_minutes = toreal(dwell_seconds) / 60.0" in function
    assert "strcat('STORE_', tostring(store_id))" in function
    assert "strcat('DC_', tostring(dc_id))" in function

    assert "fn_truck_sla()" in queryset
    assert "mv_truck_sla" not in queryset
    assert "avg(dwell_minutes)" in queryset
    assert "fn_truck_sla()" in dashboard
    assert "avg(dwell_minutes)" in dashboard

    threshold_match = re.search(r"let threshold_minutes = ([0-9.]+);", rules)
    assert threshold_match is not None
    assert float(threshold_match.group(1)) == 90.0
    assert "fn_truck_sla()" in rules
    assert "arrival_ingest_timestamp > ago(5m)" in rules
    assert "where dwell_minutes > threshold_minutes" in rules
    assert "site" in rules
