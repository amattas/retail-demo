from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.promotions import generate_promotions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 5, 5),
                           end_date=date(2025, 5, 11), store_count=2, dc_count=1,
                           customer_count=200, seed=21, transactions_per_store_day=50)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    promos, promo_lines = generate_promotions(spark, sales)
    return sales, promos, promo_lines


def test_contract_columns(setup):
    _, promos, promo_lines = setup
    assert promos.columns == column_names("fact_promotions")
    assert promo_lines.columns == column_names("fact_promo_lines")


def test_promo_lines_match_discounted_receipt_lines(setup):
    sales, _, promo_lines = setup
    discounted = sales["fact_receipt_lines"].filter(
        F.col("promo_code").isNotNull()
        & (F.col("unit_cents") * F.col("quantity") - F.col("ext_cents") > 0))
    assert promo_lines.count() == discounted.count()
    assert promo_lines.filter(F.col("discount_cents") <= 0).count() == 0


def test_promotions_aggregate(setup):
    _, promos, promo_lines = setup
    agg = promo_lines.groupBy("receipt_id_ext", "promo_code").agg(
        F.sum("discount_cents").alias("d"),
        F.countDistinct("product_id").alias("pc"))
    j = promos.join(agg, ["receipt_id_ext", "promo_code"])
    assert j.count() == promos.count()
    assert j.filter(F.col("discount_cents") != F.col("d")).count() == 0
    assert j.filter(F.col("product_count") != F.col("pc")).count() == 0


def test_promotions_link_to_receipts(setup):
    sales, promos, _ = setup
    assert promos.join(sales["fact_receipts"], "receipt_id_ext", "left_anti").count() == 0
    assert promos.filter(~F.col("discount_type").isin("PERCENTAGE", "BOGO")).count() == 0
    assert promos.filter(
        F.col("promo_code").startswith("BOGO") & (F.col("discount_type") != "BOGO")
    ).count() == 0
