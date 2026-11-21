"""Shared historical/live shopping behavior, expressed as deterministic Spark columns."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.generation.pricing import PriceSchedule, demand_response, shelf_price_struct
from retail_setup.generation.runtime import seeded_draws


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
    shopper: Column,
    mean: Column,
    offer: Column,
    promo_rate: float,
) -> Column:
    """Mission mixtures with a calibrated mean and persistent customer budgets.

    ``shopper`` keys the budget: the customer (``customers.shopper_key``), so
    a customer's baskets share one budget, or the purchase when anonymous.
    """
    trip = draws.u(purchase_keys, "retail-mission")
    mission = (
        F.when(trip < 0.30, 0.30).when(trip < 0.70, 0.90).when(trip < 0.90, 1.60).otherwise(2.50)
    ) / F.lit(1.02)
    budget = F.exp(0.35 * draws.gauss([shopper], "retail-budget") - F.lit(0.35**2 / 2))
    # Exposure is decided before demand: coupons increase additional-item demand.
    lift = (F.lit(1.0) + offer.cast("double") * 0.20) / (1.0 + promo_rate * 0.20)
    extra = F.greatest(mean - 1.0, F.lit(0.0)) * mission * budget * lift
    return (1 + draws.negative_binomial(purchase_keys, "retail-basket", extra, 3)).cast("int")


def retail_catalog(
    products: DataFrame, dates: DataFrame, draws: seeded_draws, schedule: PriceSchedule
) -> DataFrame:
    """One launch-eligible weighted product CDF per day and department.

    Popularity is persistent per SKU, with cross-sectional price sensitivity,
    a long tail, and a demand response to the SKU's shelf price that day
    (markdowns and price reviews, ``pricing.demand_response``). Each entry
    carries the day's shelf price (``unit_cents``) and the SKU's complementary
    partner priced the same day (``baskets.basket_products``).
    Arrays keep selection a bounded lookup rather than a receipt x catalog join.
    """
    eligible = (
        dates.select("event_date")
        .distinct()
        .join(F.broadcast(products), F.col("launch_date") <= F.col("event_date"))
    )
    day = F.col("event_date")
    eligible = eligible.withColumn(
        "_price",
        shelf_price_struct(
            schedule, F.col("product_id"), F.col("SalePrice"), F.col("inflation"), day
        ),
    ).withColumn(
        "_partner_price",
        shelf_price_struct(
            schedule, F.col("partner_id"), F.col("partner_price"), F.col("inflation"), day
        ),
    )
    eligible = eligible.withColumn(
        "_popularity",
        F.exp(0.8 * draws.gauss(["product_id"], "retail-popularity") - 0.32)
        / F.pow(F.col("SalePrice"), 1.15)
        * demand_response(
            F.col("_price.price_cents"), F.col("_price.trend_cents"), F.col("elasticity")
        ),
    ).withColumn("unit_cents", F.col("_price.price_cents"))
    # Floating-point sums run in product_id order over the whole department, so
    # they are bit-identical whatever order the shuffle delivers rows in.
    by_product = Window.partitionBy("event_date", "department").orderBy("product_id")
    ordered = by_product.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    whole = by_product.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    taxable_share = (
        F.when(F.col("taxability") == "TAXABLE", 1.0)
        .when(F.col("taxability") == "REDUCED_RATE", 0.5)
        .otherwise(0.0)
    )
    eligible = (
        eligible.withColumn("_total_weight", F.sum("_popularity").over(whole))
        .withColumn("_probability", F.col("_popularity") / F.col("_total_weight"))
        .withColumn("_cdf", F.sum("_probability").over(ordered))
        .withColumn(
            "_unit_value",
            F.col("unit_cents") / 100.0 / (1.0 - repeat_purchase_probability(F.col("department"))),
        )
        .withColumn(
            "_expected_line_value",
            F.sum(F.col("_probability") * F.col("_unit_value")).over(whole),
        )
        .withColumn(
            "_expected_taxable_value",
            F.sum(F.col("_probability") * F.col("_unit_value") * taxable_share).over(whole),
        )
        .withColumn(
            "partner_ok",
            F.col("partner_id").isNotNull() & (F.col("partner_launch") <= F.col("event_date")),
        )
        .withColumn("partner_cents", F.col("_partner_price.price_cents"))
    )
    return eligible.groupBy("event_date", "department").agg(
        F.sort_array(
            F.collect_list(
                F.struct(
                    "_cdf",
                    "product_id",
                    "SalePrice",
                    "taxability",
                    "unit_cents",
                    "partner_id",
                    "partner_cents",
                    "partner_taxability",
                    "partner_ok",
                )
            )
        ).alias("_catalog"),
        F.first("_expected_line_value").alias("_expected_line_value"),
        F.first("_expected_taxable_value").alias("_expected_taxable_value"),
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
    complement_rate: float = 0.0,
) -> Column:
    """Drawn basket lines so the average ticket meets ``target``.

    ``complement_rate`` is the share of drawn lines that bring a complementary
    product along (``baskets.with_complements``); those extra lines count
    towards the ticket too.
    """
    affordable_lines = F.lit(target) / (
        (expected_line + expected_taxable * tax_rate)
        * (1.0 - promo_rate * 0.20)
        * (1.0 + complement_rate)
    )
    return F.when(
        (expected_line > 0)
        & (expected_line < float("inf"))
        & (expected_taxable >= 0)
        & (tax_rate >= 0),
        F.least(affordable_lines, F.lit(nominal_lines)),
    ).otherwise(F.raise_error("Basket calibration requires valid catalog prices and tax"))
