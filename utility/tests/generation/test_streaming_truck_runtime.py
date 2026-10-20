"""Execute truck lifecycle joins, rather than checking only their source text."""

import ast
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark_no_filesystem


def test_truck_lifecycle_pairs_only_complete_authoritative_keys(spark):
    path = (
        Path(__file__).resolve().parents[3]
        / "fabric"
        / "lakehouse"
        / "03-streaming-to-silver.ipynb"
    )
    notebook = json.loads(path.read_text(encoding="utf-8"))
    source = "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )
    tree = ast.parse(source)
    functions = {"transform_truck_lifecycle", "_event_time_expr", "_legacy_index"}
    nodes = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in functions
        or isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name)
            and target.id in {"TRUCK_LIFECYCLE_KEYS", "EVENT_TIME_FIELDS"}
            for target in node.targets
        )
    ]
    namespace = {"F": F}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    arrivals = spark.sql("""
        SELECT 'TRUCK_7' AS truck_id, 1L AS dc_id, 2L AS store_id,
               'S1' AS shipment_id, timestamp'2026-09-01 10:00:00' AS arrival_time,
               timestamp'2026-09-01 15:00:00' AS ingest_timestamp, 'a1' AS trace_id
        UNION ALL SELECT 'TRUCK_7', 1L, 3L, 'S1',
               timestamp'2026-09-01 11:00:00', timestamp'2026-09-01 16:00:00', 'a2'
        UNION ALL SELECT 'TRUCK_7', 1L, 2L, 'UNFINISHED',
               timestamp'2026-09-01 11:00:00', timestamp'2026-09-01 16:00:00', 'a3'
    """)
    departures = spark.sql("""
        SELECT 'TRUCK_7' AS truck_id, 1L AS dc_id, 2L AS store_id,
               'S1' AS shipment_id, timestamp'2026-09-01 10:30:00' AS departure_time,
               timestamp'2026-09-01 17:00:00' AS ingest_timestamp,
               'd1' AS trace_id, 30.0 AS actual_unload_duration
        UNION ALL SELECT 'TRUCK_7', 1L, 3L, 'S1',
               timestamp'2026-09-01 13:00:00', timestamp'2026-09-01 18:00:00', 'd2', 120.0
        UNION ALL SELECT 'TRUCK_8', 1L, 2L, 'S1',
               timestamp'2026-09-01 10:30:00', timestamp'2026-09-01 17:00:00', 'bad-truck', 30.0
        UNION ALL SELECT 'TRUCK_7', 9L, 2L, 'S1',
               timestamp'2026-09-01 10:30:00', timestamp'2026-09-01 17:00:00', 'bad-dc', 30.0
    """)
    result = namespace["transform_truck_lifecycle"](
        arrivals.unionByName(arrivals), departures.unionByName(departures)
    ).select(
        "truck_id",
        "dc_id",
        "store_id",
        "shipment_id",
        ((F.col("etd").cast("long") - F.col("eta").cast("long")) / 60).alias("dwell"),
    )
    rows = sorted(tuple(row) for row in result.collect())
    assert rows == [(7, 1, 2, "S1", 30.0), (7, 1, 3, "S1", 120.0)]
