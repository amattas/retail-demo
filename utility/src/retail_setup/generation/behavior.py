"""Shared historical/live shopping behavior, expressed as deterministic Spark columns."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.generation.runtime import seeded_draws


def customer_visit_tickets(draws: seeded_draws, customer: Column) -> Column:
    """Persistent dimension-sized weights for heterogeneous shopping frequency."""
    return F.ceil(4.0 * F.exp(0.9 * draws.gauss([customer], "visit-propensity") - 0.405)).cast(
        "int"
    )


def repeat_purchase_probability(department: Column) -> Column:
    """Consumables permit multi-unit purchases more often than durable goods."""
    consumable = F.lower(department).rlike(
        "grocery|fresh|food|household|personal|health|beauty|pet|baby"
    )
    return F.when(consumable, F.lit(0.22)).otherwise(F.lit(0.04))


def retail_quantity(draws: seeded_draws, keys: list, department: Column) -> Column:
    return (
        F.lit(1)
        + F.floor(
            F.log(F.lit(1.0) - draws.u(keys, "retail-quantity"))
            / F.log(repeat_purchase_probability(department))
        )
    ).cast("int")


def retail_basket_size(
    draws: seeded_draws,
    purchase_keys: list,
    customer: Column,
    mean: Column,
    offer: Column,
    promo_rate: float,
) -> Column:
    """Mission mixtures with a calibrated mean and persistent customer budgets."""
    trip = draws.u(purchase_keys, "retail-mission")
    mission = (
        F.when(trip < 0.30, 0.30).when(trip < 0.70, 0.90).when(trip < 0.90, 1.60).otherwise(2.50)
    ) / F.lit(1.02)
    budget = F.exp(0.35 * draws.gauss([customer], "retail-budget") - F.lit(0.35**2 / 2))
    # Exposure is decided before demand: coupons increase additional-item demand.
    lift = (F.lit(1.0) + offer.cast("double") * 0.20) / (1.0 + promo_rate * 0.20)
    extra = F.greatest(mean - 1.0, F.lit(0.0)) * mission * budget * lift
    return (1 + draws.negative_binomial(purchase_keys, "retail-basket", extra, 3)).cast("int")


def retail_catalog(products: DataFrame, dates: DataFrame, draws: seeded_draws) -> DataFrame:
    """One launch-eligible weighted product CDF per day and department.

    Popularity is persistent per SKU, with price sensitivity and a long tail.
    Arrays keep selection a bounded lookup rather than a receipt x catalog join.
    """
    eligible = (
        dates.select("event_date")
        .distinct()
        .join(F.broadcast(products), F.col("launch_date") <= F.col("event_date"))
    )
    eligible = eligible.withColumn(
        "_popularity",
        F.exp(0.8 * draws.gauss(["product_id"], "retail-popularity") - 0.32)
        / F.pow(F.col("SalePrice"), 1.15),
    )
    group = Window.partitionBy("event_date", "department")
    ordered = group.orderBy("product_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    eligible = (
        eligible.withColumn("_total_weight", F.sum("_popularity").over(group))
        .withColumn("_probability", F.col("_popularity") / F.col("_total_weight"))
        .withColumn("_cdf", F.sum("_probability").over(ordered))
        .withColumn(
            "_unit_value",
            F.col("SalePrice") / (1.0 - repeat_purchase_probability(F.col("department"))),
        )
    )
    return eligible.groupBy("event_date", "department").agg(
        F.sort_array(
            F.collect_list(F.struct("_cdf", "product_id", "SalePrice", "taxability"))
        ).alias("_catalog"),
        F.sum(F.col("_probability") * F.col("_unit_value")).alias("_expected_line_value"),
        F.sum(
            F.col("_probability")
            * F.col("_unit_value")
            * F.when(F.col("taxability") == "TAXABLE", 1.0)
            .when(F.col("taxability") == "REDUCED_RATE", 0.5)
            .otherwise(0.0)
        ).alias("_expected_taxable_value"),
    )


def pick_retail_product(catalog: Column, uniform: Column) -> Column:
    """Binary CDF search avoids scanning the catalog for every basket line."""
    size = F.size(catalog)
    initial = F.struct(F.lit(1).alias("lo"), size.alias("hi"))

    def narrow(bounds: Column, _step: Column) -> Column:
        middle = F.floor((bounds["lo"] + bounds["hi"]) / 2).cast("int")
        left = F.element_at(catalog, middle)["_cdf"] > uniform
        return F.struct(
            F.when(left, bounds["lo"]).otherwise(F.least(middle + 1, size)).alias("lo"),
            F.when(left, middle).otherwise(bounds["hi"]).alias("hi"),
        )

    steps = F.sequence(F.lit(1), (F.ceil(F.log2(size)) + 1).cast("int"))
    selected = F.aggregate(steps, initial, narrow)["lo"]
    # The final entry absorbs floating-point CDF accumulation error.
    return F.when(size > 0, F.element_at(catalog, selected)).otherwise(
        F.raise_error("Cannot select a product from an empty catalog")
    )


def calibrated_basket_mean(
    expected_line: Column,
    expected_taxable: Column,
    tax_rate: Column,
    target: float,
    promo_rate: float,
    nominal_lines: float,
) -> Column:
    affordable_lines = F.lit(target) / (
        (expected_line + expected_taxable * tax_rate) * (1.0 - promo_rate * 0.20)
    )
    return F.when(
        (expected_line > 0)
        & (expected_line < float("inf"))
        & (expected_taxable >= 0)
        & (tax_rate >= 0),
        F.least(affordable_lines, F.lit(nominal_lines)),
    ).otherwise(F.raise_error("Basket calibration requires valid catalog prices and tax"))
