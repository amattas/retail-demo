"""Real Spark execution of the delivery notebook's keyed as-of features."""

import ast
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

pytestmark = pytest.mark.spark_no_filesystem


def test_delivery_history_excludes_same_timestamp_and_future_labels(spark):
    path = (
        Path(__file__).resolve().parents[3]
        / "fabric"
        / "lakehouse"
        / "13-ml-delivery-prediction.ipynb"
    )
    notebook = json.loads(path.read_text(encoding="utf-8"))
    source = "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )
    node = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == "build_asof_history_stats"
    )
    namespace = {"F": F, "Window": Window, "target_col": "dwell_minutes"}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
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
