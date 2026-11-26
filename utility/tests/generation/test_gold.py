"""Tests for the 10 Gold aggregates shared by setup-11 and notebook 04.

Runs the full generation engine for a tiny config, then validates the gold
aggregates against the in-memory fact tables, and checks the Gold rules
(approved SALE receipts, lifecycle-grain truck dwell, the history cutoff) on
small hand-built frames.
"""

from datetime import date, datetime, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.engine import generate_all
from retail_setup.generation.gold import GOLD_TABLES, generate_gold
from retail_setup.generation.gold_rules import (
    gold_approved_sales,
    gold_campaign_performance_daily,
    gold_cutoff,
    gold_latest_inventory_position,
    gold_online_sales_daily,
    gold_sales_minute_store,
    gold_truck_dwell_daily,
)
from retail_setup.generation.schemas import column_names

FAR_CUTOFF = datetime(2100, 1, 1)


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 10, 6),
        end_date=date(2025, 10, 8),
        store_count=2,
        dc_count=1,
        customer_count=150,
        seed=77,
        transactions_per_store_day=25,
        online_orders_per_day=15,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    result = generate_all(spark, dicts, cfg)
    cutoff = gold_cutoff(cfg.end_date, False, datetime.now(timezone.utc))
    tables = dict(result.tables, _cutoff=cutoff)
    return tables, generate_gold(spark, result.tables, cutoff)


def _approved_sales(tables):
    return gold_approved_sales(tables["fact_receipts"], tables["fact_payments"], tables["_cutoff"])


def test_all_gold_tables(setup):
    tables, gold = setup
    assert set(gold) == set(GOLD_TABLES)
    for name, df in gold.items():
        assert df.columns == column_names(name), name
        if name == "truck_dwell_daily":
            # Three days of opening stock rarely reach a reorder point, and
            # the end-of-history outage holds any load until after the
            # cutoff, so the window may have no completed delivery; dwell is
            # covered by a focused test.
            cutoff = F.lit(tables["_cutoff"].isoformat(sep=" ")).cast("timestamp")
            delivered = (
                tables["fact_truck_moves"]
                .filter(F.col("status") == "COMPLETED")
                .filter(F.coalesce("departure_time", "etd") < cutoff)
            )
            assert (df.count() > 0) == (delivered.count() > 0)
        else:
            assert df.count() > 0, name


def test_truck_dwell_daily_counts_completed_lifecycles_by_arrival_day(spark):
    # Timestamps are SQL literals in the UTC session zone (not driver-local).
    moves = spark.sql("""
        SELECT * FROM VALUES
          -- History: status rows; only COMPLETED carries the actual times.
          ('S1', 7, 1, 5, 'SCHEDULED', timestamp'2025-10-06 22:00:00',
           timestamp'2025-10-06 22:30:00', CAST(NULL AS TIMESTAMP)),
          ('S1', 7, 1, 5, 'ARRIVED', timestamp'2025-10-06 23:40:00',
           timestamp'2025-10-07 00:20:00', CAST(NULL AS TIMESTAMP)),
          ('S1', 7, 1, 5, 'COMPLETED', timestamp'2025-10-06 23:40:00',
           timestamp'2025-10-07 00:20:00', timestamp'2025-10-07 00:20:00'),
          -- Live: one COMPLETED row per lifecycle.
          ('S2', 7, 1, 5, 'COMPLETED', timestamp'2025-10-06 08:00:00',
           timestamp'2025-10-06 09:00:00', timestamp'2025-10-06 09:00:00'),
          -- A DC-site lifecycle, and one that departs after the cutoff.
          ('S3', 7, 9, CAST(NULL AS BIGINT), 'COMPLETED', timestamp'2025-10-06 08:00:00',
           timestamp'2025-10-06 08:20:00', timestamp'2025-10-06 08:20:00'),
          ('S4', 7, 1, 5, 'COMPLETED', timestamp'2025-10-07 23:00:00',
           timestamp'2025-10-08 01:00:00', timestamp'2025-10-08 01:00:00')
        AS moves(shipment_id, truck_id, dc_id, store_id, status, eta, etd, departure_time)
    """)

    rows = {
        (r.site, r.day): r for r in gold_truck_dwell_daily(moves, datetime(2025, 10, 8)).collect()
    }

    assert set(rows) == {("STORE_5", date(2025, 10, 6)), ("DC_9", date(2025, 10, 6))}
    store = rows[("STORE_5", date(2025, 10, 6))]
    # S1 crosses midnight but counts once, on its arrival day; dwell 40 and 60.
    assert store.trucks == 2
    assert store.avg_dwell_min == 50
    assert rows[("DC_9", date(2025, 10, 6))].avg_dwell_min == 20


def test_sales_aggregates_count_only_approved_sales_before_the_cutoff(spark):
    ts = datetime(2025, 10, 6, 12)
    receipts = spark.sql("""
        SELECT * FROM VALUES
          ('R1', 'SALE', 1L, timestamp'2025-10-06 12:00:00', '10.00', 'CARD'),
          ('R2', 'RETURN', 1L, timestamp'2025-10-06 12:00:00', '-4.00', 'CARD'),
          -- only a declined payment
          ('R3', 'SALE', 1L, timestamp'2025-10-06 12:00:00', '7.00', 'CASH'),
          -- after the cutoff
          ('R4', 'SALE', 1L, timestamp'2025-10-09 12:30:00', '5.00', 'CARD'),
          -- declined, then approved
          ('R5', 'SALE', 1L, timestamp'2025-10-06 12:00:00', '3.00', 'CARD')
        AS receipts(receipt_id_ext, receipt_type, store_id, event_ts, total_amount,
                    payment_method)
    """)
    payments = spark.createDataFrame(
        [
            ("R1", None, "APPROVED"),
            ("R2", None, "APPROVED"),
            ("R3", None, "DECLINED"),
            ("R4", None, "APPROVED"),
            ("R5", None, "DECLINED"),
            ("R5", None, "APPROVED"),
            (None, "O1", "APPROVED"),
            (None, "O2", "DECLINED"),
        ],
        "receipt_id_ext string, order_id_ext string, status string",
    )
    cutoff = gold_cutoff(date(2025, 10, 8), False, datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert cutoff == datetime(2025, 10, 9, 12)
    assert gold_cutoff(date(2025, 10, 8), True, datetime(2026, 1, 1, tzinfo=timezone.utc)) == (
        datetime(2026, 1, 1)
    )

    sales = gold_sales_minute_store(receipts, payments, cutoff).collect()
    assert [(row.receipts, row.total_sales) for row in sales] == [(2, 13.0)]

    headers = spark.createDataFrame(
        [("O1", ts, "9.00", "1.00", "10.00"), ("O2", ts, "4.00", "0.50", "4.50")],
        "order_id_ext string, event_ts timestamp, subtotal_amount string, "
        "tax_amount string, total_amount string",
    )
    online = gold_online_sales_daily(headers, payments, cutoff).first()
    assert (online.orders, online.total) == (1, 10.0)


def test_sales_minute_store_totals(setup):
    tables, gold = setup
    expected = _approved_sales(tables).agg(F.sum(F.col("total_amount").cast("double"))).first()[0]
    actual = gold["sales_minute_store"].agg(F.sum("total_sales")).first()[0]
    assert abs(expected - actual) < 0.01


def test_inventory_position_is_latest(setup):
    tables, gold = setup
    txn = tables["fact_store_inventory_txn"].filter(
        F.col("event_ts") < F.lit(str(tables["_cutoff"])).cast("timestamp")
    )
    latest = txn.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("store_id", "product_id").orderBy(
                F.desc("event_ts"), F.desc("trace_id")
            )
        ),
    ).filter("rn = 1")
    pos = gold["inventory_position_current"]
    assert pos.count() == latest.count()
    j = pos.join(
        latest.select("store_id", "product_id", F.col("balance").alias("b")),
        ["store_id", "product_id"],
    )
    assert j.filter(F.col("on_hand") != F.col("b")).count() == 0


@pytest.mark.parametrize("node_column", ["store_id", "dc_id"])
def test_inventory_position_ties_use_trace_id_desc(spark, node_column):
    event_ts = datetime(2025, 10, 6, 12)
    transactions = spark.createDataFrame(
        [
            (1, 10, 100, event_ts, "trace-a"),
            (1, 10, 200, event_ts, "trace-z"),
        ],
        f"{node_column} long, product_id long, balance long, event_ts timestamp, trace_id string",
    )

    row = gold_latest_inventory_position(transactions, node_column, FAR_CUTOFF).first()

    assert row.on_hand == 200
    assert row.as_of == event_ts


def test_tender_mix_partitions_receipts(setup):
    tables, gold = setup
    assert (
        gold["tender_mix_daily"].agg(F.sum("transactions")).first()[0]
        == _approved_sales(tables).count()
    )


def test_online_sales_daily(setup):
    tables, gold = setup
    approved = gold_online_sales_daily(
        tables["fact_online_order_headers"], tables["fact_payments"], tables["_cutoff"]
    )
    assert (
        gold["online_sales_daily"].agg(F.sum("orders")).first()[0]
        == approved.agg(F.sum("orders")).first()[0]
    )


def test_campaign_performance_reconciles_spend_and_conversions(spark):
    marketing = spark.createDataFrame(
        [
            ("CMP-1", "EMAIL", datetime(2025, 10, 6, 10), "IMP-1", 10),
            ("CMP-1", "EMAIL", datetime(2025, 10, 6, 11), "IMP-2", 20),
        ],
        "campaign_id string, channel string, event_ts timestamp, "
        "impression_id_ext string, cost_cents long",
    )
    attribution = spark.createDataFrame(
        [
            (
                "ATTR-1",
                "ATTRIBUTED",
                "CMP-1",
                "EMAIL",
                datetime(2025, 10, 6, 11),
                1000,
                100,
                80,
                1080,
            )
        ],
        "attribution_id string, attribution_status string, campaign_id string, "
        "channel string, touch_ts timestamp, attributed_revenue_cents long, "
        "discount_cents long, tax_cents long, payment_cents long",
    ).withColumn("purchase_ts", F.col("touch_ts"))

    row = gold_campaign_performance_daily(marketing, attribution, FAR_CUTOFF).first()

    assert row.impressions == 2
    assert row.spend_cents == 30
    assert row.conversions == 1
    assert row.attributed_revenue_cents == 1000
    assert row.discount_cents == 100
    assert row.tax_cents == 80
    assert row.payment_cents == 1080
    assert row.conversion_rate == pytest.approx(0.5)
    assert row.roas == pytest.approx(1000 / 30)
