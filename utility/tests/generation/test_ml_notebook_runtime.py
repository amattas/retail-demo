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


def _customer_sources(spark):
    receipts = spark.createDataFrame(
        [
            # Store customer 1: two sales and a return.
            (1, 10, "SALE", datetime(2026, 1, 5, 12), "12.00", 1200, "CARD"),
            (1, 11, "SALE", datetime(2026, 2, 9, 12), "8.00", 800, "CASH"),
            (1, 10, "RETURN", datetime(2026, 2, 10, 12), "-8.00", -800, "CASH"),
            # Anonymous shopper.
            (None, 10, "SALE", datetime(2026, 2, 9, 12), "50.00", 5000, "CARD"),
            # After the as-of time.
            (1, 10, "SALE", datetime(2026, 6, 1, 12), "9.00", 900, "CARD"),
        ],
        "customer_id long, store_id long, receipt_type string, event_ts timestamp, "
        "total_amount string, total_cents long, payment_method string",
    )
    online = spark.createDataFrame(
        [
            # Online-only customer 2.
            (2, datetime(2026, 1, 20, 12), "30.00", 3000, "CARD"),
            (2, datetime(2026, 3, 2, 12), "20.00", 2000, "CARD"),
            (None, datetime(2026, 3, 2, 12), "5.00", 500, "CARD"),
        ],
        "customer_id long, event_ts timestamp, total_amount string, total_cents long, "
        "payment_method string",
    )
    return receipts, online


def test_segmentation_counts_online_and_skips_returns_and_anonymous(spark):
    nb08 = _load(
        "08-ml-customer-segmentation.ipynb",
        {"purchase_amount", "customer_purchases", "build_rfm"},
    )
    receipts, online = _customer_sources(spark)
    as_of = datetime(2026, 3, 10, 12)
    purchases = nb08["customer_purchases"](receipts, online, as_of)
    rows = {row.customer_id: row for row in nb08["build_rfm"](purchases, as_of.date()).collect()}

    assert set(rows) == {1, 2}
    assert (rows[1].frequency, rows[1].monetary_value) == (2, 20.0)
    assert (rows[2].frequency, rows[2].monetary_value) == (2, 50.0)
    assert rows[2].recency_days == 8


def test_churn_snapshots_pair_only_active_customers_and_see_online_activity(spark):
    nb09 = _load(
        "09-ml-churn-prediction.ipynb",
        {
            "customer_activity",
            "daily_customer_activity",
            "pair_with_snapshots",
            "build_feature_snapshots",
            "attach_forward_churn_labels",
        },
        FEATURE_WINDOW_DAYS=60,
        CHURN_WINDOW_DAYS=30,
    )
    receipts, online = _customer_sources(spark)
    daily = nb09["daily_customer_activity"](
        nb09["customer_activity"](receipts, online, datetime(2026, 3, 10, 12))
    )
    customers = spark.createDataFrame(
        [(1, 5.0), (2, 6.0), (3, 7.0)], "customer_id long, geography_id double"
    )
    snapshots = [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)]
    features = nb09["build_feature_snapshots"](daily, customers, snapshots)
    labeled = {
        (row.customer_id, row.snapshot_date): row
        for row in nb09["attach_forward_churn_labels"](features, daily, snapshots).collect()
    }

    # Customer 3 never purchased and nobody is scored before their first purchase.
    assert set(labeled) == {
        (1, date(2026, 2, 1)),
        (1, date(2026, 3, 1)),
        (2, date(2026, 2, 1)),
        (2, date(2026, 3, 1)),
    }
    first = labeled[(1, date(2026, 3, 1))]
    assert (first.purchase_count, first.total_spend, first.unique_stores) == (2, 20.0, 2)
    assert first.basket_std == pytest.approx(2.8284, abs=1e-4)
    # Customer 2's online order on 2 March keeps them active after 1 February.
    online_only = labeled[(2, date(2026, 2, 1))]
    assert (online_only.online_purchase_count, online_only.is_churned) == (1, 0.0)
    assert online_only.unique_stores == 0
    assert labeled[(1, date(2026, 2, 1))].is_churned == 0.0
    assert labeled[(1, date(2026, 3, 1))].is_churned == 1.0
