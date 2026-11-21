from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import load_dictionaries
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
    products = dims["dim_products"].select(
        F.col("ID").alias("product_id"),
        "SalePrice",
        "taxability",
        F.col("Department").alias("department"),
        F.to_date("LaunchDate").alias("launch_date"),
    )
    purchases = spark.range(3000).select(
        F.concat(F.lit("LIVE-"), "id").alias("receipt_id"),
        (F.col("id") % 1000 + 1).alias("customer_id"),
        F.lit(800).alias("store_bps"),
    )
    live = attach_stream_baskets(
        spark, purchases, products, dictionaries.profile, cfg.seed, cfg.end_date
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
    products = generate_dimensions(spark, dictionaries, cfg)["dim_products"].select(
        F.col("ID").alias("product_id"),
        "SalePrice",
        "taxability",
        F.col("Department").alias("department"),
        F.to_date("LaunchDate").alias("launch_date"),
    )
    purchases = spark.range(500).select(
        F.concat(F.lit("RCP-stream-a-0a1b2c3d-"), "id").alias("receipt_id"),
        (F.col("id") % 97 + 1).alias("customer_id"),
        F.lit(800).alias("store_bps"),
    )

    def lines(catalog_date):
        live = attach_stream_baskets(
            spark, purchases, products, dictionaries.profile, cfg.seed, catalog_date
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


def test_variable_live_lines_reconcile_discount_and_tax(spark):
    from retail_setup.generation.runtime import seeded_draws
    from retail_setup.generation.stream_events import price_line

    frame = spark.sql("""
        SELECT true AS _offer, 'RCP-x-1' AS receipt_id, 6 AS month, 800 AS store_bps,
               array(named_struct('line_num', 1, 'quantity', 3, 'unit_cents', 199L,
                                  'gross_cents', 597L, 'taxability', 'TAXABLE'),
                     named_struct('line_num', 2, 'quantity', 1, 'unit_cents', 299L,
                                  'gross_cents', 299L, 'taxability', 'NON_TAXABLE'),
                     named_struct('line_num', 3, 'quantity', 2, 'unit_cents', 500L,
                                  'gross_cents', 1000L, 'taxability', 'REDUCED_RATE')) AS lines
    """)
    priced = (
        frame.select(
            F.transform(
                "lines",
                lambda line: price_line(
                    seeded_draws(7),
                    line,
                    F.col("receipt_id"),
                    F.col("month"),
                    F.col("_offer"),
                    F.col("store_bps"),
                ),
            ).alias("lines")
        )
        .first()
        .lines
    )
    # Every line of an offered receipt gets its own code; the discount comes off
    # before tax (taxable 100%, non-taxable 0%, reduced rate 50%).
    assert all(line.promo_code for line in priced)
    assert all(line.discount_cents > 0 for line in priced)
    for line in priced:
        assert line.net_cents == line.gross_cents - line.discount_cents
    share = {"TAXABLE": 1.0, "NON_TAXABLE": 0.0, "REDUCED_RATE": 0.5}
    assert [line.tax_cents for line in priced] == [
        int(line.net_cents * share[line.taxability] * 800 / 10000 + 0.5) for line in priced
    ]
