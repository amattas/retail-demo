"""Exercise the deployed notebook's inventory expressions with real Spark."""

import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.generation.schemas import spark_schema

pytestmark = pytest.mark.spark_no_filesystem

NOTEBOOK = (
    Path(__file__).resolve().parents[3] / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb"
)


def _notebook_functions(spark, existing, name="transform_inventory_updated", exists=True):
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    source = "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )
    names = {
        "deduplicate_candidates",
        "_event_time_expr",
        "_legacy_index",
        "_inventory_before_boundary",
        "_inventory_recompute_boundaries",
        "_prior_inventory_balances",
        "transform_inventory_updated",
        "transform_dc_inventory_updated",
    }
    nodes = [
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    namespace = {
        "F": F,
        "Window": Window,
        "LAKEHOUSE_NAME": "lh",
        "SILVER_DB": "ag",
        "EVENT_TIME_FIELDS": {"inventory_updated": ("ingest_timestamp",)},
        "INVENTORY_ORDER_FIELDS": ("event_ts", "trace_id"),
        "STREAM_DEDUPE_KEYS": {"inventory_updated": ["trace_id"]},
        "target_table_exists": lambda _: exists,
        "spark": SimpleNamespace(table=lambda _: existing),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    return namespace[name]


def test_real_inventory_windows_repair_legacy_nulls_and_replay_idempotently(spark):
    # Use JVM-created rows so this non-filesystem smoke also runs without winutils.
    existing = spark.sql("""
        SELECT cast('2026-09-01 08:00:00' AS timestamp) AS event_ts,
               'seed' AS trace_id, 1L AS store_id, 1L AS product_id,
               100L AS quantity, 'INITIAL' AS txn_type, 'seed' AS source,
               100L AS balance, date'2026-09-01' AS event_date,
               1L AS __index_level_0__, 0L AS requested_quantity, 0L AS unfulfilled_quantity
        UNION ALL
        SELECT timestamp'2026-09-01 09:00:00', 'legacy', 1L, 1L,
               -10L, 'SALE', 'live', cast(NULL AS bigint), date'2026-09-01', 2L, 10L, 0L
    """).select(*spark_schema("fact_store_inventory_txn").fieldNames())
    incoming = spark.sql("""
        SELECT timestamp'2026-09-01 10:00:00' AS ingest_timestamp,
               'b' AS trace_id, 1L AS store_id, 1L AS product_id,
               -5L AS quantity_delta, 'SALE' AS reason, 'live' AS payload_source
        UNION ALL
        SELECT timestamp'2026-09-01 10:00:00', 'a', 1L, 1L, 2L, 'RESTOCK', 'live'
    """)
    result = _notebook_functions(spark, existing)(incoming).persist()
    try:
        balances = {row.trace_id: row.balance for row in result.collect()}
        assert balances == {"legacy": 90, "a": 92, "b": 87}
        assert result.filter("source <> 'live' OR balance IS NULL").isEmpty()
        new_state = existing.filter("trace_id = 'seed'").unionByName(result)
        replay = _notebook_functions(spark, new_state)(incoming)
        assert {row.trace_id: row.balance for row in replay.collect()} == {"a": 92, "b": 87}
        assert replay.select(F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss")).first()[0] == (
            "2026-09-01 10:00:00"
        )
    finally:
        result.unpersist()


def test_live_replay_preserves_historical_unfulfilled_demand(spark):
    existing = spark.sql("""
        SELECT timestamp'2026-09-01 08:00:00' AS event_ts, 'seed' AS trace_id,
               1L AS store_id, 1L AS product_id, 10L AS quantity, 10L AS balance,
               'INITIAL' AS txn_type, 'seed' AS source, date'2026-09-01' AS event_date,
               1L AS __index_level_0__, 0L AS requested_quantity, 0L AS unfulfilled_quantity
        UNION ALL
        SELECT timestamp'2026-09-01 11:00:00', 'lost', 1L, 1L, 0L, 10L,
               'LOST_SALE', 'attempt', date'2026-09-01', 2L, 15L, 15L
    """)
    incoming = spark.sql("""
        SELECT timestamp'2026-09-01 10:00:00' AS ingest_timestamp, 'sale' AS trace_id,
               1L AS store_id, 1L AS product_id, -2L AS quantity_delta,
               'SALE' AS reason, 'live' AS payload_source
    """)
    rows = {row.trace_id: row for row in _notebook_functions(spark, existing)(incoming).collect()}
    assert rows["lost"].balance == 8
    assert rows["lost"].quantity == 0
    assert rows["lost"].requested_quantity == 15
    assert rows["lost"].unfulfilled_quantity == 15
    assert rows["sale"].requested_quantity == 2
    assert rows["sale"].unfulfilled_quantity == 0


def test_live_lost_sales_carry_their_requested_quantity(spark):
    existing = spark.sql("""
        SELECT timestamp'2026-09-01 08:00:00' AS event_ts, 'seed' AS trace_id,
               1L AS store_id, 1L AS product_id, 2L AS quantity, 2L AS balance,
               'INITIAL' AS txn_type, 'seed' AS source, date'2026-09-01' AS event_date,
               1L AS __index_level_0__, 0L AS requested_quantity, 0L AS unfulfilled_quantity
    """)
    incoming = spark.sql("""
        SELECT timestamp'2026-09-01 10:00:00' AS ingest_timestamp, 'lost' AS trace_id,
               1L AS store_id, 1L AS product_id, 0L AS quantity_delta,
               'LOST_SALE' AS reason, 'CUSTOMER_PURCHASE' AS payload_source,
               3L AS requested_quantity
        UNION ALL
        SELECT timestamp'2026-09-01 11:00:00', 'sale', 1L, 1L, -2L, 'SALE',
               'ONLINE_ORDER', 2L
        UNION ALL
        SELECT timestamp'2026-09-01 12:00:00', 'unload', 1L, 1L, 24L, 'INBOUND_SHIPMENT',
               'SHP-1', 0L
    """)
    rows = {row.trace_id: row for row in _notebook_functions(spark, existing)(incoming).collect()}
    assert (rows["lost"].quantity, rows["lost"].balance) == (0, 2)
    assert (rows["lost"].requested_quantity, rows["lost"].unfulfilled_quantity) == (3, 3)
    assert (rows["sale"].requested_quantity, rows["sale"].unfulfilled_quantity) == (2, 0)
    assert (rows["sale"].balance, rows["unload"].balance) == (0, 24)
    assert rows["unload"].requested_quantity == 0


def test_dc_stock_moves_route_to_the_dc_ledger_with_running_balances(spark):
    events = spark.sql("""
        SELECT timestamp'2026-09-01 04:00:00' AS ingest_timestamp, 'sup' AS trace_id,
               CAST(NULL AS BIGINT) AS store_id, 2L AS dc_id, 7L AS product_id,
               120L AS quantity_delta, 'INBOUND_SHIPMENT' AS reason,
               'SUPPLIER-2-7-1' AS payload_source, 120L AS requested_quantity
        UNION ALL SELECT timestamp'2026-09-01 10:00:00', 'load', NULL, 2L, 7L, -24L,
               'OUTBOUND_SHIPMENT', 'SHP-ns-5-1', 36L
        UNION ALL SELECT timestamp'2026-09-01 11:00:00', 'sale', 5L, 2L, 7L, -1L,
               'SALE', 'CUSTOMER_PURCHASE', 1L
    """)
    dc = _notebook_functions(spark, None, "transform_dc_inventory_updated", exists=False)
    rows = [
        (row.trace_id, row.dc_id, row.txn_type, row.quantity, row.balance, row.Source)
        for row in dc(events).orderBy("event_ts").collect()
    ]
    assert rows == [
        ("sup", 2, "INBOUND_SHIPMENT", 120, 120, "SUPPLIER-2-7-1"),
        ("load", 2, "OUTBOUND_SHIPMENT", -24, 96, "SHP-ns-5-1"),
    ]
    store = _notebook_functions(spark, None, exists=False)
    assert [row.trace_id for row in store(events).collect()] == ["sale"]
