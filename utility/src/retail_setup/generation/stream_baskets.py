"""Variable live baskets using the same catalog, mission, quantity and budget model."""

from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.behavior import (
    calibrated_basket_mean,
    pick_retail_product,
    retail_basket_size,
    retail_catalog,
    retail_quantity,
)
from retail_setup.generation.baskets import receipt_offer
from retail_setup.generation.customers import shopper_key
from retail_setup.generation.pricing import PriceSchedule
from retail_setup.generation.receipts import _seasonal_factor
from retail_setup.generation.runtime import seeded_draws


@dataclass(frozen=True)
class BasketCatalog:
    """Department lookup and basket calibration for one catalog date."""

    lookup: DataFrame
    line_value: float
    taxable_value: float


def prepare_basket_catalog(
    spark: SparkSession,
    products: DataFrame,
    profile: StoreTypeProfile,
    seed: int,
    catalog_date: date,
    anchor_date: date,
) -> BasketCatalog:
    """Build the live catalog as of ``catalog_date`` once per stream run.

    ``products`` is ``baskets.basket_products`` of ``dim_products``. Launch
    eligibility, shelf prices (``pricing``, anchored on ``anchor_date`` as in
    history) and the seasonal department weights all come from
    ``catalog_date``, not the clock, so a replayed micro-batch rebuilds the
    same baskets. The stream keeps the date in its checkpoint incarnation
    record and moves it forward on a restart that has no batch to replay.
    """
    draws = seeded_draws(seed)
    catalog = retail_catalog(
        products,
        spark.range(1).select(F.lit(catalog_date).alias("event_date")),
        draws,
        PriceSchedule(seed, anchor_date),
    )
    weight = sum(
        (
            F.when(
                F.col("department") == name,
                F.lit(value) * _seasonal_factor(name, F.month("event_date")),
            ).otherwise(0.0)
            for name, value in profile.department_weights.items()
        ),
        F.lit(0.0),
    )
    # Department arrays are bounded profile metadata, not a collected customer pool.
    entries = catalog.withColumn("_weight", weight).orderBy("department").collect()
    if not entries:
        raise ValueError("No launched products available for live baskets")
    total = sum(row["_weight"] for row in entries)
    missing = set(profile.department_weights) - {row["department"] for row in entries}
    if missing:
        raise ValueError(
            f"Live catalog has no launched products for departments: {sorted(missing)}"
        )
    cumulative = 0.0
    metadata = []
    for row in entries:
        cumulative += row["_weight"] / total
        metadata.append((row["department"], cumulative, row["_catalog"]))
    schema = (
        "department string, _cdf double, _catalog array<struct<_cdf:double,"
        "product_id:bigint,SalePrice:double,taxability:string,unit_cents:bigint,"
        "partner_id:bigint,partner_cents:bigint,partner_taxability:string,partner_ok:boolean>>"
    )
    lookup = spark.createDataFrame(metadata, schema).agg(
        F.sort_array(F.collect_list(F.struct("_cdf", "department", "_catalog"))).alias(
            "_departments"
        )
    )
    line_value = sum(row["_weight"] * row["_expected_line_value"] for row in entries) / total
    taxable_value = sum(row["_weight"] * row["_expected_taxable_value"] for row in entries) / total
    return BasketCatalog(lookup, line_value, taxable_value)


def attach_baskets(
    purchases: DataFrame,
    catalog: BasketCatalog,
    profile: StoreTypeProfile,
    seed: int,
    complement_rate: float = 0.0,
) -> DataFrame:
    """Attach ``_offer``, ``basket_n`` and ``basket_lines`` to purchase rows.

    Lines carry the catalog date's shelf price and the SKU's complementary
    partner; ``complement_rate`` sizes baskets that will bring complements
    along (``baskets.with_complements``, store purchases).

    Rows need ``receipt_id``, ``customer_id`` (NULL for anonymous shoppers)
    and ``store_bps``. Budgets and favorite departments are the customer's,
    or the purchase's own for anonymous shoppers, as in history.
    """
    draws = seeded_draws(seed)
    purchases = (
        purchases.join(F.broadcast(catalog.lookup))
        .withColumn("_shopper", shopper_key(F.col("customer_id"), F.col("receipt_id")))
        .withColumn("_offer", receipt_offer(draws, F.col("receipt_id"), profile.promo_rate))
        .withColumn(
            "basket_n",
            retail_basket_size(
                draws,
                ["receipt_id"],
                F.col("_shopper"),
                calibrated_basket_mean(
                    F.lit(catalog.line_value),
                    F.lit(catalog.taxable_value),
                    F.col("store_bps") / 10000.0,
                    profile.avg_ticket_target,
                    profile.promo_rate,
                    profile.basket_lambda,
                    complement_rate,
                ),
                F.col("_offer"),
                profile.promo_rate,
            ),
        )
    )

    def line(index):
        keys = [F.col("receipt_id"), index]
        department_draw = F.when(
            draws.u(keys, "mission-coherence") < 0.60,
            F.when(
                draws.u(["receipt_id"], "favorite-mission") < 0.50,
                draws.u(["_shopper"], "favorite-department"),
            ).otherwise(draws.u(["receipt_id"], "mission-department")),
        ).otherwise(draws.u(keys, "dept"))
        department = pick_retail_product(F.col("_departments"), department_draw)
        product = pick_retail_product(department["_catalog"], draws.u(keys, "prod"))
        quantity = retail_quantity(draws, keys, department["department"])
        unit = product["unit_cents"]
        return F.struct(
            index.alias("line_num"),
            product["product_id"].alias("product_id"),
            quantity.alias("quantity"),
            unit.alias("unit_cents"),
            (unit * quantity).alias("gross_cents"),
            product["taxability"].alias("taxability"),
            department["department"].alias("department"),
            product["partner_id"].alias("partner_id"),
            product["partner_cents"].alias("partner_cents"),
            product["partner_taxability"].alias("partner_taxability"),
            product["partner_ok"].alias("partner_ok"),
        )

    return purchases.withColumn(
        "basket_lines", F.transform(F.sequence(F.lit(1), F.col("basket_n")), line)
    ).drop("_departments", "_shopper")


def attach_stream_baskets(
    spark: SparkSession,
    purchases: DataFrame,
    products: DataFrame,
    profile: StoreTypeProfile,
    seed: int,
    catalog_date: date,
    anchor_date: date,
) -> DataFrame:
    """Attach baskets drawn from the catalog as of ``catalog_date`` (one-off form)."""
    catalog = prepare_basket_catalog(spark, products, profile, seed, catalog_date, anchor_date)
    return attach_baskets(purchases, catalog, profile, seed)
