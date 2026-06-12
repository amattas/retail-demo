"""Tests for the 9 Gold aggregates (port of 02-historical-data-load Part 3).

Runs the full generation engine for a tiny config, then validates the gold
aggregates against the in-memory fact tables.

Deviation from plan: the plan's test inlined
``__import__("pyspark.sql.window", fromlist=["Window"]).Window``; we use a
normal ``from pyspark.sql.window import Window`` import (same semantics).
"""

from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.engine import generate_all
from retail_setup.generation.gold import GOLD_TABLES, generate_gold
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


def test_all_nine_tables(setup):
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
