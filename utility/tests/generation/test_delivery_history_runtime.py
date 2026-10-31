"""Real Spark execution of the delivery notebook's keyed as-of features."""

import ast
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

pytestmark = pytest.mark.spark_no_filesystem

NOTEBOOK_PATH = (
    Path(__file__).resolve().parents[3] / "fabric" / "lakehouse" / "13-ml-delivery-prediction.ipynb"
)


def _code_cells():
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    return ["".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"]


@pytest.mark.parametrize("live_state", ["empty", "open_only", "completed_and_open"])
def test_delivery_combines_completed_history_with_live_lifecycle(spark, live_state):
    history = spark.sql("""
        SELECT 'history-1' AS shipment_id, 1 AS truck_id, 1 AS store_id, 1 AS dc_id,
               'COMPLETED' AS status, timestamp'2026-09-01 08:00:00' AS eta,
               timestamp'2026-09-01 08:30:00' AS etd,
               timestamp'2026-09-01 08:30:00' AS departure_time,
               timestamp'2026-09-01 08:30:00' AS event_ts
        UNION ALL SELECT 'history-2', 2, 1, 1, 'COMPLETED',
               timestamp'2026-09-01 09:00:00', timestamp'2026-09-01 10:00:00',
               timestamp'2026-09-01 10:00:00', timestamp'2026-09-01 10:00:00'
        UNION ALL SELECT 'history-open', 3, 1, 1, 'ARRIVED',
               timestamp'2026-09-01 10:00:00', timestamp'2026-09-01 10:40:00',
               CAST(NULL AS TIMESTAMP), timestamp'2026-09-01 10:00:00'
    """)
    arrivals = spark.sql("""
        SELECT 'history-1' AS shipment_id, 'TRUCK-1' AS truck_id,
               1 AS store_id, 1 AS dc_id,
               timestamp'2026-09-01 08:00:00' AS arrival_time,
               timestamp'2026-09-01 08:00:01' AS ingest_timestamp
        UNION ALL SELECT 'live-open', 'TRUCK-4', 1, 1,
               timestamp'2026-09-01 11:00:00', timestamp'2026-09-01 11:00:01'
    """)
    departures = spark.sql("""
        SELECT 'history-1' AS shipment_id,
               timestamp'2026-09-01 08:30:00' AS departure_time
    """)
    if live_state == "empty":
        arrivals, departures = arrivals.limit(0), departures.limit(0)
    elif live_state == "open_only":
        arrivals = arrivals.filter(F.col("shipment_id") == "live-open")
        departures = departures.limit(0)
    bronze = {"truck_arrived": arrivals, "truck_departed": departures}
    namespace = {
        "F": F,
        "Window": Window,
        "TRUCK_ARRIVED_TABLE": "truck_arrived",
        "TRUCK_DEPARTED_TABLE": "truck_departed",
        "TRUCK_MOVES_TABLE": "fact_truck_moves",
        "MAX_VALID_DWELL_MINUTES": 480,
        "bronze_exists": lambda _name: True,
        "silver_exists": lambda _name: True,
        "read_bronze": bronze.__getitem__,
        "read_silver": lambda _name: history,
        "normalize_timestamp": lambda name: F.to_timestamp(F.col(name).cast("string")),
    }
    cells = _code_cells()
    preparation = next(cell for cell in cells if "PREPARING DWELL TIME DATA" in cell)
    lifecycle = next(cell for cell in cells if "arrival_rank_window" in cell)
    try:
        exec(compile(preparation, str(NOTEBOOK_PATH), "exec"), namespace)
        exec(compile(lifecycle, str(NOTEBOOK_PATH), "exec"), namespace)

        completed = namespace["df_dwell"].select("shipment_id", "dwell_minutes").collect()
        assert sorted((row.shipment_id, row.dwell_minutes) for row in completed) == [
            ("history-1", 30.0),
            ("history-2", 60.0),
        ]
        open_rows = (
            namespace["df_arrived"]
            .join(
                namespace["df_departed"].select("shipment_id"),
                on="shipment_id",
                how="left_anti",
            )
            .select("shipment_id")
            .collect()
        )
        assert {row.shipment_id for row in open_rows} == (
            set() if live_state == "empty" else {"live-open"}
        )
    finally:
        for name in ("df_arrived", "df_departed", "df_dwell"):
            if name in namespace:
                namespace[name].unpersist()


def test_delivery_history_excludes_same_timestamp_and_future_labels(spark):
    source = "\n".join(_code_cells())
    node = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == "build_asof_history_stats"
    )
    namespace = {"F": F, "Window": Window, "target_col": "dwell_minutes"}
    exec(
        compile(ast.Module(body=[node], type_ignores=[]), str(NOTEBOOK_PATH), "exec"),
        namespace,
    )
    targets = spark.sql("""
        SELECT 'A' AS shipment_id, timestamp'2026-09-01 10:00:00' AS arrived_ts, 1 AS site
        UNION ALL SELECT 'B', timestamp'2026-09-01 10:00:00', 2
        UNION ALL SELECT 'C', timestamp'2026-09-01 12:00:00', 1
    """)
    history = spark.sql("""
        SELECT timestamp'2026-09-01 09:00:00' AS label_available_ts,
               1 AS site, 10.0 AS dwell_minutes
        UNION ALL SELECT timestamp'2026-09-01 10:00:00', 1, 20.0
        UNION ALL SELECT timestamp'2026-09-01 11:00:00', 1, 30.0
        UNION ALL SELECT timestamp'2026-09-01 09:00:00', 2, 40.0
        UNION ALL SELECT timestamp'2026-09-01 13:00:00', 1, 999.0
    """)
    frame = namespace["build_asof_history_stats"](targets, history, ["site"], "average", "count")
    rows = {row.shipment_id: (row.average, row["count"]) for row in frame.collect()}
    assert rows == {"A": (10.0, 1.0), "B": (40.0, 1.0), "C": (20.0, 3.0)}
