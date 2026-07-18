"""Tests for the 10 Gold aggregates (port of 02-historical-data-load Part 3).

Runs the full generation engine for a tiny config, then validates the gold
aggregates against the in-memory fact tables.

Deviation from plan: the plan's test inlined
``__import__("pyspark.sql.window", fromlist=["Window"]).Window``; we use a
normal ``from pyspark.sql.window import Window`` import (same semantics).
"""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.engine import generate_all
from retail_setup.generation.gold import (
    GOLD_TABLES,
    generate_campaign_performance,
    generate_gold,
)
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 10, 6),
                           end_date=date(2025, 10, 8), store_count=2, dc_count=1,
                           customer_count=150, seed=77, transactions_per_store_day=25,
                           online_orders_per_day=15)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    result = generate_all(spark, dicts, cfg)
    return result.tables, generate_gold(spark, result.tables)


def test_all_gold_tables(setup):
    _, gold = setup
    assert set(gold) == set(GOLD_TABLES)
    for name, df in gold.items():
        assert df.columns == column_names(name), name
        assert df.count() > 0, name


def test_sales_minute_store_totals(setup):
    tables, gold = setup
    expected = tables["fact_receipts"].agg(
        F.sum(F.col("total_amount").cast("double"))).first()[0]
    actual = gold["sales_minute_store"].agg(F.sum("total_sales")).first()[0]
    assert abs(expected - actual) < 0.01


def test_inventory_position_is_latest(setup):
    tables, gold = setup
    txn = tables["fact_store_inventory_txn"]
    latest = (txn.withColumn("rn", F.row_number().over(
        Window.partitionBy("store_id", "product_id").orderBy(F.desc("event_ts"))))
        .filter("rn = 1"))
    pos = gold["inventory_position_current"]
    assert pos.count() == latest.count()
    j = pos.join(latest.select("store_id", "product_id",
                               F.col("balance").alias("b")),
                 ["store_id", "product_id"])
    assert j.filter(F.col("on_hand") != F.col("b")).count() == 0


def test_tender_mix_partitions_receipts(setup):
    tables, gold = setup
    assert gold["tender_mix_daily"].agg(F.sum("transactions")).first()[0] == \
        tables["fact_receipts"].count()


def test_online_sales_daily(setup):
    tables, gold = setup
    assert gold["online_sales_daily"].agg(F.sum("orders")).first()[0] == \
        tables["fact_online_order_headers"].count()


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
    )

    row = generate_campaign_performance(marketing, attribution).first()

    assert row.impressions == 2
    assert row.spend_cents == 30
    assert row.conversions == 1
    assert row.attributed_revenue_cents == 1000
    assert row.discount_cents == 100
    assert row.tax_cents == 80
    assert row.payment_cents == 1080
    assert row.conversion_rate == pytest.approx(0.5)
    assert row.roas == pytest.approx(1000 / 30)
