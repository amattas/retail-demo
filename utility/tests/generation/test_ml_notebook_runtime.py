"""Real Spark execution of the ML notebooks' data-preparation helpers."""

import ast
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

pytestmark = pytest.mark.spark_no_filesystem

LAKEHOUSE = Path(__file__).resolve().parents[3] / "fabric" / "lakehouse"


def _load(notebook: str, names: set[str], **namespace):
    """Exec the named functions and assignments of a notebook's code cells."""
    cells = json.loads((LAKEHOUSE / notebook).read_text(encoding="utf-8"))["cells"]
    source = "\n".join("".join(cell["source"]) for cell in cells if cell["cell_type"] == "code")
    nodes = [
        node
        for node in ast.parse(source).body
        if (isinstance(node, ast.FunctionDef) and node.name in names)
        or (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id in names for t in node.targets)
        )
    ]
    found = {n.name if isinstance(n, ast.FunctionDef) else n.targets[0].id for n in nodes}
    assert found == names, f"missing from {notebook}: {names - found}"
    scope = {
        "F": F,
        "Window": Window,
        "timedelta": timedelta,
        "timezone": timezone,
        "datetime": datetime,
        **namespace,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), notebook, "exec"), scope)
    return scope


def test_demand_counts_sales_only_through_the_last_complete_day(spark):
    nb06 = _load("06-ml-demand-forecast.ipynb", {"last_complete_day", "build_daily_sales"})
    receipts = spark.createDataFrame(
        [
            ("S1", 1, "SALE", datetime(2026, 7, 19, 10)),
            ("S2", 1, "SALE", datetime(2026, 7, 20, 11)),
            ("R1", 1, "RETURN", datetime(2026, 7, 20, 12)),
            ("S3", 1, "SALE", datetime(2026, 7, 21, 9)),
        ],
        "receipt_id_ext string, store_id long, receipt_type string, event_ts timestamp",
    )
    lines = spark.createDataFrame(
        [("S1", 7, 2, "4.00"), ("S2", 7, 3, "6.00"), ("R1", 7, -2, "-4.00"), ("S3", 7, 5, "10.00")],
        "receipt_id_ext string, product_id long, quantity int, ext_price string",
    )
    anchor = nb06["last_complete_day"](datetime(2026, 7, 21, 9))
    assert anchor == date(2026, 7, 20)

    rows = {
        row.sale_date: row.units_sold
        for row in nb06["build_daily_sales"](lines, receipts, anchor).collect()
    }
    # The return never nets units; the partial as-of day (21st) is excluded.
    assert rows == {date(2026, 7, 19): 2.0, date(2026, 7, 20): 3.0}


def test_stockout_current_positions_roll_forward_to_the_as_of_time(spark):
    nb12 = _load(
        "12-ml-stockout-prediction.ipynb",
        {"build_end_of_day_inventory", "roll_forward_current_positions"},
    )
    as_of = datetime(2026, 7, 21, 9)
    ledger = spark.createDataFrame(
        [
            # A slow mover last counted in March, and a busy SKU.
            (1, 10, datetime(2026, 3, 2, 8), "t1", 4.0),
            (1, 11, datetime(2026, 7, 20, 8), "t2", 9.0),
            (1, 11, datetime(2026, 7, 21, 7), "t3", 7.0),
            # A delivery scheduled after the as-of time isn't true yet.
            (1, 11, datetime(2026, 7, 22, 6), "t4", 40.0),
        ],
        "store_id long, product_id long, event_ts timestamp, trace_id string, balance double",
    ).withColumn("event_date", F.to_date("event_ts"))
    eod = nb12["build_end_of_day_inventory"](ledger.filter(F.col("event_ts") <= F.lit(as_of)))
    current = {
        row.product_id: row for row in nb12["roll_forward_current_positions"](eod, as_of).collect()
    }

    assert set(current) == {10, 11}
    slow = current[10]
    assert slow.current_inventory == 4.0
    assert slow.inventory_as_of == datetime(2026, 3, 2, 8)
    # Demand windows end now, not at the stale ledger date.
    assert slow.snapshot_date == date(2026, 7, 21)
    assert slow.features_as_of == as_of
    assert current[11].current_inventory == 7.0
