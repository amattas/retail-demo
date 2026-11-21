from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import load_dictionaries
from retail_setup.generation.baskets import basket_products
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.stream_baskets import attach_stream_baskets
from retail_setup.generation.traffic import generate_hourly_visits

pytestmark = pytest.mark.spark_no_filesystem


def test_supercenter_ticket_calibration_and_live_basket_continuity(spark):
    cfg = GenerationConfig(
        start_date=date(2025, 7, 1),
        end_date=date(2025, 7, 3),
        store_count=2,
        seed=42,
    )
    dictionaries = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type)
    dims = generate_dimensions(spark, dictionaries, cfg)
    headers = generate_receipts_group(spark, dims, dictionaries.profile, cfg)["fact_receipts"]
    average = headers.agg(F.avg("total_cents")).first()[0] / 100
    assert 55 < average < 115, average
    products = basket_products(dims["dim_products"], cfg.seed)
    purchases = spark.range(3000).select(
        F.concat(F.lit("LIVE-"), "id").alias("receipt_id"),
        (F.col("id") % 1000 + 1).alias("customer_id"),
        F.lit(800).alias("store_bps"),
    )
    live = attach_stream_baskets(
        spark, purchases, products, dictionaries.profile, cfg.seed, cfg.end_date, cfg.anchor_date
    ).cache()
    try:
        row = live.agg(F.min("basket_n"), F.max("basket_n"), F.countDistinct("basket_n")).first()
        assert row[0] == 1 and row[1] > 4 and row[2] > 5
        gross = (
            live.select(F.explode("basket_lines").alias("line"))
            .agg(F.sum("line.gross_cents"))
            .first()[0]
            / 3000
            / 100
        )
        assert 50 < gross < 125, gross
    finally:
        live.unpersist()


def test_live_baskets_replay_from_the_recorded_catalog_date(spark):
    cfg = GenerationConfig(
        start_date=date(2025, 7, 1),
        end_date=date(2025, 7, 3),
        store_count=2,
        seed=42,
    )
    dictionaries = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type)
    products = basket_products(
        generate_dimensions(spark, dictionaries, cfg)["dim_products"], cfg.seed
    )
    purchases = spark.range(500).select(
        F.concat(F.lit("RCP-stream-a-0a1b2c3d-"), "id").alias("receipt_id"),
        (F.col("id") % 97 + 1).alias("customer_id"),
        F.lit(800).alias("store_bps"),
    )

    def lines(catalog_date):
        live = attach_stream_baskets(
            spark,
            purchases,
            products,
            dictionaries.profile,
            cfg.seed,
            catalog_date,
            cfg.anchor_date,
        )
        return sorted(
            (row.receipt_id, row.line.line_num, row.line.product_id, row.line.quantity)
            for row in live.select("receipt_id", F.explode("basket_lines").alias("line")).collect()
        )

    # A replay on a later day rebuilds the batch from the recorded date, so it
    # regenerates exactly the same lines.
    first = lines(cfg.end_date)
    assert first == lines(cfg.end_date)
    launched = {
        row.product_id
        for row in products.where(F.col("launch_date") <= F.lit(cfg.end_date)).collect()
    }
    assert {product_id for _receipt, _line, product_id, _qty in first} <= launched


@pytest.mark.parametrize("seed", [7, 42, 101])
def test_conversion_is_visit_first_not_four_fixed_bands(spark, seed):
    cfg = GenerationConfig(
        start_date=date(2025, 3, 1),
        end_date=date(2025, 3, 7),
        store_count=2,
        seed=seed,
    )
    dictionaries = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type)
    dims = generate_dimensions(spark, dictionaries, cfg)
    visits = generate_hourly_visits(spark, dims["dim_stores"], dictionaries.profile, cfg)
    row = visits.agg(
        F.sum((F.col("n_receipts") > F.col("visitors")).cast("int")).alias("oversold"),
        F.countDistinct(
            F.round(F.when(F.col("visitors") > 0, F.col("n_receipts") / F.col("visitors")), 2)
        ).alias("rates"),
    ).first()
    assert row.oversold == 0
    assert row.rates > 15
