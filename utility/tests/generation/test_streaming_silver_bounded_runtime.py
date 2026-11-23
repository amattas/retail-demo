"""Notebook 03 reads only new data: bounded attribution and truck snapshots (Spark)."""

import ast
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

pytestmark = pytest.mark.spark_no_filesystem

NOTEBOOK = (
    Path(__file__).resolve().parents[3] / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb"
)
FUNCTIONS = {
    "_normalize_checkpoint_timestamp",
    "_normalized_attribution_journey_id",
    "attribution_lower_bound",
    "_key_frame",
    "select_attribution_candidates",
    "build_marketing_attribution",
    "truck_lifecycle_snapshots",
    "record_late_attribution_keys",
}
CONSTANTS = {
    "ATTRIBUTION_WINDOW_DAYS",
    "ATTRIBUTION_LATE_KEY_LIMIT",
    "ATTRIBUTION_LATE_KEY_COLUMNS",
    "ATTRIBUTION_STATE",
    "TRUCK_LIFECYCLE_KEYS",
    "TRUCK_ARRIVAL_LOOKBACK",
}
WATERMARK = datetime(2026, 3, 10, 12, 0)


@pytest.fixture(scope="module")
def nb03():
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    source = "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )
    nodes = [
        node
        for node in ast.parse(source).body
        if (isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS)
        or (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id in CONSTANTS for t in node.targets)
        )
    ]
    namespace = {
        "F": F,
        "Window": Window,
        "timedelta": timedelta,
        "timezone": __import__("datetime").timezone,
        "REPLAY_OVERLAP_SECONDS": 10800,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    return namespace


def _tables(spark, history: int) -> dict:
    receipts, touches, payments = [], [], []

    def purchase(receipt_id, journey, ts, touch_ts=None):
        receipts.append((receipt_id, "SALE", journey, ts, ts.date(), 1, 9, 1000, 0, 1000, 80, 1080))
        payments.append((receipt_id, None, ts, f"T-{receipt_id}", "APPROVED", 1080, 1))
        if touch_ts is not None:
            touches.append((journey, touch_ts, f"I-{receipt_id}", "C1", "CR1", "EMAIL", "AD9", 9.0))

    for index in range(history):
        ts = WATERMARK - timedelta(days=30, hours=index)
        purchase(f"H{index}", f"JH{index}", ts, ts - timedelta(hours=1))
    purchase("N1", "JN1", WATERMARK + timedelta(hours=1), WATERMARK - timedelta(days=2))
    purchase("N2", None, WATERMARK + timedelta(hours=2))
    old = WATERMARK - timedelta(days=20)
    purchase("L1", "JL1", old, old - timedelta(hours=1))
    return {
        "fact_receipts": spark.createDataFrame(
            receipts,
            "receipt_id_ext string, receipt_type string, attribution_journey_id string, "
            "event_ts timestamp, event_date date, store_id long, customer_id long, "
            "gross_subtotal_cents long, discount_cents long, subtotal_cents long, "
            "tax_cents long, total_cents long",
        ),
        "fact_online_order_headers": spark.createDataFrame(
            [],
            "order_id_ext string, attribution_journey_id string, event_ts timestamp, "
            "event_date date, customer_id long, gross_subtotal_cents long, "
            "discount_cents long, subtotal_cents long, tax_cents long, total_cents long",
        ),
        "fact_marketing": spark.createDataFrame(
            touches,
            "attribution_journey_id string, event_ts timestamp, impression_id_ext string, "
            "campaign_id string, creative_id string, channel string, customer_ad_id string, "
            "customer_id double",
        ),
        "fact_payments": spark.createDataFrame(
            payments,
            "receipt_id_ext string, order_id_ext string, event_ts timestamp, "
            "transaction_id string, status string, amount_cents long, store_id long",
        ),
        "fact_promotions": spark.createDataFrame(
            [],
            "attribution_journey_id string, receipt_id_ext string, event_ts timestamp, "
            "discount_cents long",
        ),
    }


def _attribute(nb03, tables, bound, late_keys=()):
    attribution, candidates = nb03["build_marketing_attribution"](tables, bound, late_keys, ())
    try:
        rows = {row.attribution_id: row.attribution_status for row in attribution.collect()}
        return rows, candidates.count()
    finally:
        candidates.unpersist()


def test_attribution_work_scales_with_new_purchases_not_history(spark, nb03):
    bound = nb03["attribution_lower_bound"](WATERMARK)
    assert bound == WATERMARK - timedelta(days=7, hours=3)
    expected = {
        "ATTR-STORE-N1": "ATTRIBUTED",
        "ATTR-STORE-N2": "UNATTRIBUTED_NO_JOURNEY",
        "ATTR-STORE-L1": "ATTRIBUTED",
    }

    for history in (5, 60):
        rows, candidates = _attribute(nb03, _tables(spark, history), bound, {"L1"})
        # Two new purchases plus the late key, however long the history is.
        assert candidates == 3
        assert rows == expected

    rows, candidates = _attribute(nb03, _tables(spark, 5), bound)
    assert candidates == 2
    assert set(rows) == {"ATTR-STORE-N1", "ATTR-STORE-N2"}


def test_first_attribution_run_reads_full_history(spark, nb03):
    rows, candidates = _attribute(nb03, _tables(spark, 5), None)
    assert candidates == 8
    assert rows["ATTR-STORE-H0"] == "ATTRIBUTED"
    assert len(rows) == 8


def test_truck_snapshots_keep_new_departures_and_their_arrivals(spark, nb03):
    start = datetime(2026, 3, 10, 9, 0)

    def frame(rows, time_column):
        return spark.createDataFrame(
            rows,
            f"truck_id string, dc_id long, store_id long, shipment_id string, "
            f"{time_column} timestamp, ingest_timestamp timestamp",
        )

    arrivals = frame(
        [
            ("T1", 1, 5, "S-new", start - timedelta(hours=2), start - timedelta(hours=2)),
            ("T2", 1, 5, "S-old", start - timedelta(days=3), start - timedelta(days=3)),
            ("T3", 1, 5, "S-open", start + timedelta(hours=1), start + timedelta(hours=1)),
        ],
        "arrival_time",
    )
    departures = frame(
        [
            ("T1", 1, 5, "S-new", start + timedelta(minutes=30), start + timedelta(minutes=30)),
            ("T2", 1, 5, "S-old", start - timedelta(days=2), start - timedelta(days=2)),
        ],
        "departure_time",
    )
    snapshots = nb03["truck_lifecycle_snapshots"]

    recent_arrivals, recent_departures = snapshots(arrivals, departures, start)
    assert [row.shipment_id for row in recent_departures.collect()] == ["S-new"]
    assert [row.shipment_id for row in recent_arrivals.collect()] == ["S-new"]

    all_arrivals, all_departures = snapshots(arrivals, departures, None)
    assert (all_arrivals.count(), all_departures.count()) == (3, 2)


def test_late_merged_rows_feed_the_next_attribution_candidates(spark, nb03):
    bound = nb03["attribution_lower_bound"](WATERMARK)
    state = nb03["ATTRIBUTION_STATE"]
    state.update(bound=bound, purchase_keys=set(), journeys=set(), overflow=False)
    record = nb03["record_late_attribution_keys"]
    old = WATERMARK - timedelta(days=20)
    receipts = spark.createDataFrame(
        [("L1", " JL1 ", old), ("N1", "JN1", WATERMARK + timedelta(hours=1))],
        "receipt_id_ext string, attribution_journey_id string, event_ts timestamp",
    )
    payments = spark.createDataFrame(
        [(None, "O7", old), ("N1", None, WATERMARK)],
        "receipt_id_ext string, order_id_ext string, event_ts timestamp",
    )
    record(receipts, "receipt_created")
    record(payments, "payment_processed")
    record(receipts, "receipt_line_added")  # not an attribution source
    # Only rows older than the bound are late; journeys are trimmed.
    assert state["purchase_keys"] == {"L1", "O7"}
    assert state["journeys"] == {"JL1"}
    assert state["overflow"] is False

    rows, candidates = _attribute(nb03, _tables(spark, 5), bound, state["purchase_keys"])
    assert rows["ATTR-STORE-L1"] == "ATTRIBUTED"

    nb03["ATTRIBUTION_LATE_KEY_LIMIT"] = 1
    try:
        record(payments, "payment_processed")
        record(receipts, "receipt_created")
        # Too many late keys: the run falls back to full history.
        assert state["overflow"] is True
    finally:
        nb03["ATTRIBUTION_LATE_KEY_LIMIT"] = 100000
