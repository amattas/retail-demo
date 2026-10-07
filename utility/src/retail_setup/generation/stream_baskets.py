"""Variable live baskets using the same catalog, mission, quantity and budget model."""

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
from retail_setup.generation.receipts import _seasonal_factor
from retail_setup.generation.runtime import seeded_draws


def attach_stream_baskets(
    spark: SparkSession, purchases: DataFrame, products: DataFrame,
    profile: StoreTypeProfile, seed: int,
) -> DataFrame:
    """Snapshot launch eligibility at stream start; restart to include new launches."""
    draws = seeded_draws(seed)
    catalog = retail_catalog(
        products, spark.range(1).select(F.current_date().alias("event_date")), draws
    )
    weight = sum(
        (F.when(
            F.col("department") == name,
            F.lit(value) * _seasonal_factor(name, F.month("event_date")),
        ).otherwise(0.0) for name, value in profile.department_weights.items()),
        F.lit(0.0),
    )
    # Department arrays are bounded profile metadata, not a collected customer pool.
    entries = catalog.withColumn("_weight", weight).orderBy("department").collect()
    if not entries:
        raise ValueError("No launched products available for live baskets")
    total = sum(row["_weight"] for row in entries)
    missing = set(profile.department_weights) - {row["department"] for row in entries}
    if missing:
        raise ValueError(f"Live catalog has no launched products for departments: {sorted(missing)}")
    cumulative = 0.0
    metadata = []
    for row in entries:
        cumulative += row["_weight"] / total
        metadata.append((row["department"], cumulative, row["_catalog"]))
    schema = "department string, _cdf double, _catalog array<struct<_cdf:double," \
             "product_id:bigint,SalePrice:double,taxability:string>>"
    lookup = spark.createDataFrame(metadata, schema).agg(
        F.sort_array(F.collect_list(F.struct("_cdf", "department", "_catalog"))).alias("_departments")
    )
    line_value = sum(row["_weight"] * row["_expected_line_value"] for row in entries) / total
    taxable_value = sum(row["_weight"] * row["_expected_taxable_value"] for row in entries) / total
    purchases = purchases.join(F.broadcast(lookup)).withColumn(
        "_offer", draws.u(["receipt_id"], "offer") < profile.promo_rate
    ).withColumn(
        "basket_n",
        retail_basket_size(
            draws, ["receipt_id"], F.col("customer_id"),
            calibrated_basket_mean(
                F.lit(line_value), F.lit(taxable_value), F.col("store_bps") / 10000.0,
                profile.avg_ticket_target, profile.promo_rate, profile.basket_lambda,
            ), F.col("_offer"), profile.promo_rate,
        ),
    )

    def line(index):
        keys = [F.col("receipt_id"), index]
        department_draw = F.when(
            draws.u(keys, "mission-coherence") < 0.60,
            F.when(
                draws.u(["receipt_id"], "favorite-mission") < 0.50,
                draws.u(["customer_id"], "favorite-department"),
            ).otherwise(draws.u(["receipt_id"], "mission-department")),
        ).otherwise(draws.u(keys, "dept"))
        department = pick_retail_product(F.col("_departments"), department_draw)
        product = pick_retail_product(department["_catalog"], draws.u(keys, "prod"))
        quantity = retail_quantity(draws, keys, department["department"])
        unit = F.round(product["SalePrice"] * 100).cast("long")
        return F.struct(
            index.alias("line_num"), product["product_id"].alias("product_id"),
            quantity.alias("quantity"), unit.alias("unit_cents"),
            (unit * quantity).alias("gross_cents"), product["taxability"].alias("taxability"),
        )

    return purchases.withColumn(
        "basket_lines", F.transform(F.sequence(F.lit(1), F.col("basket_n")), line)
    ).drop("_departments")
