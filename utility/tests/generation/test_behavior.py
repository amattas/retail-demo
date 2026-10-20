from pyspark.sql import functions as F

from retail_setup.generation.behavior import (
    customer_visit_tickets,
    pick_retail_product,
    retail_basket_size,
    retail_quantity,
)
from retail_setup.generation.runtime import seeded_draws


def test_gaussian_tails_and_negative_binomial_moments(spark):
    draws = seeded_draws(42)
    frame = spark.range(40000).select(
        draws.gauss(["id"], "tail").alias("z"),
        draws.negative_binomial(["id"], "count", F.lit(12.0), 4).alias("n"),
    )
    row = frame.agg(
        F.sum((F.abs("z") > 3).cast("int")).alias("tail"),
        F.avg("n").alias("mean"), F.var_pop("n").alias("variance"),
    ).first()
    assert row.tail > 40
    assert 11.7 < row.mean < 12.3
    assert 45 < row.variance < 51
    zero = spark.range(1).select(
        draws.negative_binomial(["id"], "zero", F.lit(0.0), 4).alias("count")
    ).first()
    assert zero["count"] == 0


def test_category_quantities_favor_single_units_without_four_unit_cap(spark):
    draws = seeded_draws(7)
    frame = spark.range(20000).select(
        retail_quantity(draws, ["id"], F.lit("Grocery")).alias("food"),
        retail_quantity(draws, ["id"], F.lit("Electronics")).alias("durable"),
    )
    row = frame.agg(
        F.avg((F.col("food") == 1).cast("double")).alias("food_single"),
        F.avg((F.col("durable") == 1).cast("double")).alias("durable_single"),
        F.max("food").alias("max_food"),
    ).first()
    assert 0.75 < row.food_single < 0.81
    assert 0.94 < row.durable_single < 0.98
    assert row.max_food > 4


def test_offer_causes_basket_lift_with_same_seed_and_customers(spark):
    draws = seeded_draws(17)
    frame = spark.range(10000).select(
        retail_basket_size(draws, ["id"], F.col("id"), F.lit(8.0), F.lit(False), 0.2).alias("base"),
        retail_basket_size(draws, ["id"], F.col("id"), F.lit(8.0), F.lit(True), 0.2).alias("offer"),
    )
    row = frame.agg(F.avg("base"), F.avg("offer")).first()
    assert row[1] > row[0] * 1.12
    assert frame.filter("offer < base OR base < 1").count() == 0


def test_customer_frequency_weights_are_persistent_and_heterogeneous(spark):
    draws = seeded_draws(42)
    frame = spark.range(10000).withColumn(
        "tickets", customer_visit_tickets(draws, F.col("id"))
    )
    row = frame.agg(F.avg("tickets"), F.stddev_pop("tickets"), F.min("tickets")).first()
    assert row[2] >= 1
    assert row[1] / row[0] > 0.8
    repeated = frame.repartition(3).withColumn(
        "again", customer_visit_tickets(draws, F.col("id"))
    )
    assert repeated.filter("tickets != again").count() == 0


def test_weighted_catalog_binary_search_handles_boundaries(spark):
    frame = spark.sql("""
        SELECT explode(array(0.0D, 0.199D, 0.2D, 0.699D, 0.7D, 0.999D)) AS u,
               array(named_struct('_cdf', 0.2D, 'product_id', 1),
                     named_struct('_cdf', 0.7D, 'product_id', 2),
                     named_struct('_cdf', 1.0D, 'product_id', 3)) AS catalog
    """)
    rows = frame.select(
        "u", pick_retail_product(F.col("catalog"), F.col("u"))["product_id"].alias("product_id")
    ).orderBy("u").collect()
    assert [row.product_id for row in rows] == [1, 1, 2, 2, 3, 3]
