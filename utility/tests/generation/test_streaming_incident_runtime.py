"""Execute the incident and online-line Silver transforms from notebook 03."""

import ast
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark_no_filesystem

NOTEBOOK = (
    Path(__file__).resolve().parents[3] / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb"
)


def _namespace() -> dict:
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    source = "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )
    tree = ast.parse(source)
    functions = {
        "transform_logistics_incidents",
        "transform_logistics_incident_stores",
        "transform_online_order_line_added",
        "transform_online_order_delivered",
        "_event_time_expr",
        "_event_time_columns",
        "_legacy_index",
    }
    constants = {"LOGISTICS_INCIDENT_KEYS", "EVENT_TIME_FIELDS"}
    nodes = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in functions
        or isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id in constants for target in node.targets)
    ]
    namespace: dict = {"F": F}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    return namespace


def test_incidents_open_on_start_and_close_on_resolution(spark):
    transform = _namespace()["transform_logistics_incidents"]
    started = spark.sql("""
        SELECT 'INC-1' AS incident_id, 'DC_OUTAGE' AS incident_type, 2L AS dc_id,
               CAST(NULL AS BIGINT) AS store_id,
               timestamp'2026-07-21 08:00:00' AS started_at, 'HIGH' AS severity,
               5L AS affected_store_count, 'Power outage' AS description,
               timestamp'2026-07-21 08:00:05' AS ingest_timestamp, 't1' AS trace_id
        UNION ALL SELECT 'INC-2', 'LANE_DELAY', 1L, 7L, timestamp'2026-07-21 09:00:00',
               'MEDIUM', 1L, 'Closed lane', timestamp'2026-07-21 09:00:05', 't2'
    """)
    resolved = spark.sql("""
        SELECT 'INC-1' AS incident_id, 'DC_OUTAGE' AS incident_type, 2L AS dc_id,
               CAST(NULL AS BIGINT) AS store_id,
               timestamp'2026-07-21 11:30:00' AS resolved_at,
               timestamp'2026-07-21 11:30:05' AS ingest_timestamp
        UNION ALL SELECT 'INC-0', 'WEATHER', 3L, NULL, timestamp'2026-07-20 23:00:00',
               timestamp'2026-07-20 23:00:05'
    """)
    rows = {
        row.incident_id: row
        for row in transform(started.unionByName(started), resolved)
        .select(
            "incident_id",
            "dc_id",
            "store_id",
            F.date_format("started_at", "HH:mm").alias("started"),
            F.date_format("resolved_at", "HH:mm").alias("resolved"),
            F.col("event_date").cast("string").alias("day"),
        )
        .collect()
    }
    assert (rows["INC-1"].started, rows["INC-1"].resolved, rows["INC-1"].store_id) == (
        "08:00",
        "11:30",
        None,
    )
    assert (rows["INC-2"].resolved, rows["INC-2"].store_id, rows["INC-2"].day) == (
        None,
        7,
        "2026-07-21",
    )
    # A resolution whose start aged out of Bronze carries no start; the MERGE
    # only updates an existing incident with it and never inserts it.
    assert rows["INC-0"].started is None and rows["INC-0"].resolved == "23:00"


def test_incident_starts_list_every_affected_store_in_the_bridge(spark):
    transform = _namespace()["transform_logistics_incident_stores"]
    started = spark.sql("""
        SELECT 'INC-1' AS incident_id, 'DC_OUTAGE' AS incident_type, 2L AS dc_id,
               timestamp'2026-07-21 08:00:00' AS started_at,
               array(3L, 4L, 5L) AS affected_store_ids,
               timestamp'2026-07-21 08:00:05' AS ingest_timestamp
    """)
    resolved = spark.sql("""
        SELECT 'INC-1' AS incident_id, timestamp'2026-07-21 11:30:00' AS resolved_at,
               timestamp'2026-07-21 11:30:05' AS ingest_timestamp
    """)
    rows = sorted(
        (row.store_id, row.dc_id, row.incident_type, row.resolved, row.trace_id)
        for row in transform(started.unionByName(started), resolved)
        .withColumn("resolved", F.date_format("resolved_at", "HH:mm"))
        .collect()
    )
    assert rows == [
        (store, 2, "DC_OUTAGE", "11:30", f"TRC-INCS-INC-1-{store}") for store in (3, 4, 5)
    ]


def test_online_lines_and_delivery_map_to_silver_columns(spark):
    namespace = _namespace()
    lines = spark.sql("""
        SELECT 'ONL-1' AS order_id, 2L AS line_number, 501L AS product_id, 3L AS quantity,
               9.99 AS unit_price, 29.97 AS extended_price, CAST(NULL AS STRING) AS promo_code,
               'BOPIS' AS fulfillment_mode, 'STORE' AS node_type, 4L AS node_id,
               timestamp'2026-07-21 09:00:00' AS ingest_timestamp, 'trc' AS trace_id
    """)
    line = namespace["transform_online_order_line_added"](lines).collect()[0]
    assert (line.order_id, line.line_num, line.unit_cents, line.ext_cents) == (
        "ONL-1",
        2,
        999,
        2997,
    )
    assert (line.unit_price, line.ext_price, line.fulfillment_status) == (
        "9.99",
        "29.97",
        "PENDING",
    )

    delivered = spark.sql("""
        SELECT 'ONL-1' AS order_id, 'STORE' AS node_type, 4L AS node_id,
               'BOPIS' AS fulfillment_mode,
               timestamp'2026-07-22 15:00:00' AS delivered_time,
               timestamp'2026-07-22 15:00:04' AS ingest_timestamp
    """)
    status = (
        namespace["transform_online_order_delivered"](delivered)
        .select("order_id_ext", "status", F.date_format("event_ts", "HH:mm:ss").alias("at"))
        .collect()[0]
    )
    assert tuple(status) == ("ONL-1", "DELIVERED", "15:00:00")
