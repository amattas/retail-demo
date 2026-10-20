import ast
from datetime import date
from pathlib import Path

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
        start_date=date(2025, 7, 1), end_date=date(2025, 7, 3),
        store_count=2, seed=42,
    )
    dictionaries = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type)
    dims = generate_dimensions(spark, dictionaries, cfg)
    headers = generate_receipts_group(spark, dims, dictionaries.profile, cfg)["fact_receipts"]
    average = headers.agg(F.avg("total_cents")).first()[0] / 100
    assert 55 < average < 115, average
    products = dims["dim_products"].select(
        F.col("ID").alias("product_id"), "SalePrice", "taxability",
        F.col("Department").alias("department"), F.to_date("LaunchDate").alias("launch_date"),
    )
    purchases = spark.range(3000).select(
        F.concat(F.lit("LIVE-"), "id").alias("receipt_id"),
        (F.col("id") % 1000 + 1).alias("customer_id"),
        F.lit(800).alias("store_bps"),
    )
    live = attach_stream_baskets(spark, purchases, products, dictionaries.profile, cfg.seed).cache()
    try:
        row = live.agg(F.min("basket_n"), F.max("basket_n"), F.countDistinct("basket_n")).first()
        assert row[0] == 1 and row[1] > 4 and row[2] > 5
        gross = live.select(F.explode("basket_lines").alias("line")).agg(
            F.sum("line.gross_cents")
        ).first()[0] / 3000 / 100
        assert 50 < gross < 125, gross
    finally:
        live.unpersist()


@pytest.mark.parametrize("seed", [7, 42, 101])
def test_conversion_is_visit_first_not_four_fixed_bands(spark, seed):
    cfg = GenerationConfig(
        start_date=date(2025, 3, 1), end_date=date(2025, 3, 7), store_count=2, seed=seed,
    )
    dictionaries = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type)
    dims = generate_dimensions(spark, dictionaries, cfg)
    visits = generate_hourly_visits(spark, dims["dim_stores"], dictionaries.profile, cfg)
    row = visits.agg(
        F.sum((F.col("n_receipts") > F.col("visitors")).cast("int")).alias("oversold"),
        F.countDistinct(F.round(F.when(
            F.col("visitors") > 0, F.col("n_receipts") / F.col("visitors")
        ), 2)).alias("rates"),
    ).first()
    assert row.oversold == 0
    assert row.rates > 15


def test_variable_live_lines_reconcile_discount_and_tax(spark):
    path = Path(__file__).resolve().parents[2] / "notebooks" / "templates" / "driver-05-stream.py"
    nodes = [
        node for node in ast.parse(path.read_text(encoding="utf-8")).body
        if isinstance(node, ast.FunctionDef) and node.name in {"_taxmult", "_price_live_line"}
    ]
    namespace = {"F": F}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    frame = spark.sql("""
        SELECT true AS _offer, 'BOGO' AS promo_type, 50 AS _ppct, 800 AS store_bps,
               array(named_struct('line_num', 1, 'quantity', 3, 'unit_cents', 199L,
                                  'gross_cents', 597L, 'taxability', 'TAXABLE'),
                     named_struct('line_num', 2, 'quantity', 1, 'unit_cents', 299L,
                                  'gross_cents', 299L, 'taxability', 'NON_TAXABLE'),
                     named_struct('line_num', 3, 'quantity', 2, 'unit_cents', 500L,
                                  'gross_cents', 1000L, 'taxability', 'REDUCED_RATE')) AS lines
    """)
    priced = frame.select(F.transform("lines", namespace["_price_live_line"]).alias("lines")).first().lines
    assert [line.discount_cents for line in priced] == [100, 0, 250]
    assert [line.net_cents for line in priced] == [497, 299, 750]
    assert [line.tax_cents for line in priced] == [40, 0, 30]
