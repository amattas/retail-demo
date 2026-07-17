"""Gold aggregates — exact port of 02-historical-data-load.ipynb Part 3.

Operates on the in-memory ``GenerationResult.tables`` dict (``tables[...]``
replaces the legacy ``read_silver(...)``). Definitions are identical in
02-historical-data-load and 04-streaming-to-gold (verified 2026-06-12).

Deliberate decisions (vs the legacy notebooks):
- Money sums: legacy summed the formatted STRING columns (total_amount,
  subtotal_amount, tax_amount, ext_price, cost) relying on Spark's implicit
  cast — here we cast explicitly to double. Identical results, honest types.
- ``computed_at`` (top_products_15m) and ``as_of`` (both inventory positions)
  are produced exactly as the legacy code does, even though the TMDL doesn't
  bind them — extra columns are allowed.
- Legacy quirks preserved: truck_dwell_daily derives ``site`` as
  ``STORE_<id>`` / ``DC_<id>`` and filters ``dwell_min > 0``; the two
  inventory positions take the latest balance per key via a row_number
  window over ``event_ts`` descending.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.generation.schemas import column_names

GOLD_TABLES: list[str] = [
    "sales_minute_store",
    "top_products_15m",
    "inventory_position_current",
    "dc_inventory_position_current",
    "truck_dwell_daily",
    "online_sales_daily",
    "zone_dwell_minute",
    "marketing_cost_daily",
    "campaign_performance_daily",
    "tender_mix_daily",
]


def _money(col: str):
    """Explicit double cast for the legacy formatted string money columns."""
    return F.col(col).cast("double")


def generate_campaign_performance(
    fact_marketing: DataFrame,
    fact_marketing_attribution: DataFrame,
) -> DataFrame:
    """Aggregate campaign spend and reconciled conversions by touch day."""

    campaign_spend = (
        fact_marketing.withColumn("day", F.to_date("event_ts"))
        .groupBy("campaign_id", "channel", "day")
        .agg(
            F.countDistinct("impression_id_ext").alias("impressions"),
            F.sum("cost_cents").cast("long").alias("spend_cents"),
        )
    )
    campaign_conversions = (
        fact_marketing_attribution.filter(
            F.col("attribution_status") == "ATTRIBUTED"
        )
        .withColumn("day", F.to_date("touch_ts"))
        .groupBy("campaign_id", "channel", "day")
        .agg(
            F.countDistinct("attribution_id").alias("conversions"),
            F.sum("attributed_revenue_cents")
            .cast("long")
            .alias("attributed_revenue_cents"),
            F.sum("discount_cents").cast("long").alias("discount_cents"),
            F.sum("tax_cents").cast("long").alias("tax_cents"),
            F.sum("payment_cents").cast("long").alias("payment_cents"),
        )
    )
    return (
        campaign_spend.join(
            campaign_conversions, ["campaign_id", "channel", "day"], "left"
        )
        .fillna(
            0,
            subset=[
                "conversions",
                "attributed_revenue_cents",
                "discount_cents",
                "tax_cents",
                "payment_cents",
            ],
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("impressions") == 0, F.lit(0.0)).otherwise(
                F.col("conversions") / F.col("impressions")
            ),
        )
        .withColumn(
            "roas",
            F.when(F.col("spend_cents") == 0, F.lit(0.0)).otherwise(
                F.col("attributed_revenue_cents") / F.col("spend_cents")
            ),
        )
        .select(*column_names("campaign_performance_daily"))
    )


def generate_gold(
    spark: SparkSession, tables: dict[str, DataFrame]
) -> dict[str, DataFrame]:
    """Build the 10 Gold aggregate frames from the generated fact tables."""
    gold: dict[str, DataFrame] = {}

    # Sales by minute per store
    gold["sales_minute_store"] = (
        tables["fact_receipts"]
        .withColumn("ts", F.date_trunc("minute", F.col("event_ts")))
        .groupBy("store_id", "ts")
        .agg(
            F.sum(_money("total_amount")).alias("total_sales"),
            F.count("*").alias("receipts"),
            F.avg(_money("total_amount")).alias("avg_basket"),
        )
        .select(*column_names("sales_minute_store"))
    )

    # Top products by revenue (15m windows)
    gold["top_products_15m"] = (
        tables["fact_receipt_lines"]
        .withColumn("window_15m", F.window(F.col("event_ts"), "15 minutes"))
        .groupBy("product_id", "window_15m")
        .agg(
            F.sum(_money("ext_price")).alias("revenue"),
            F.sum("quantity").alias("units"),
        )
        .withColumn("computed_at", F.col("window_15m.end"))
        .drop("window_15m")
        .select(*column_names("top_products_15m"))
    )

    # Current store inventory position (latest balance per store/product)
    store_pos_window = Window.partitionBy("store_id", "product_id").orderBy(
        F.desc("event_ts"))
    gold["inventory_position_current"] = (
        tables["fact_store_inventory_txn"]
        .withColumn("rn", F.row_number().over(store_pos_window))
        .filter(F.col("rn") == 1)
        .select(
            "store_id", "product_id",
            F.col("balance").alias("on_hand"),
            F.col("event_ts").alias("as_of"),
        )
        .select(*column_names("inventory_position_current"))
    )

    # DC inventory position (latest balance per dc/product)
    dc_pos_window = Window.partitionBy("dc_id", "product_id").orderBy(
        F.desc("event_ts"))
    gold["dc_inventory_position_current"] = (
        tables["fact_dc_inventory_txn"]
        .withColumn("rn", F.row_number().over(dc_pos_window))
        .filter(F.col("rn") == 1)
        .select(
            "dc_id", "product_id",
            F.col("balance").alias("on_hand"),
            F.col("event_ts").alias("as_of"),
        )
        .select(*column_names("dc_inventory_position_current"))
    )

    # Truck dwell time daily
    gold["truck_dwell_daily"] = (
        tables["fact_truck_moves"]
        .withColumn("day", F.to_date("event_ts"))
        .withColumn(
            "site",
            F.when(F.col("store_id").isNotNull(),
                   F.concat(F.lit("STORE_"), F.col("store_id")))
            .otherwise(F.concat(F.lit("DC_"), F.col("dc_id"))),
        )
        .withColumn(
            "dwell_min",
            (F.unix_timestamp("etd") - F.unix_timestamp("eta")) / 60,
        )
        .filter(F.col("dwell_min").isNotNull() & (F.col("dwell_min") > 0))
        .groupBy("site", "day")
        .agg(
            F.avg("dwell_min").alias("avg_dwell_min"),
            F.countDistinct("truck_id").alias("trucks"),
        )
        .select(*column_names("truck_dwell_daily"))
    )

    # Online sales daily
    gold["online_sales_daily"] = (
        tables["fact_online_order_headers"]
        .withColumn("day", F.to_date("event_ts"))
        .groupBy("day")
        .agg(
            F.count("*").alias("orders"),
            F.sum(_money("subtotal_amount")).alias("subtotal"),
            F.sum(_money("tax_amount")).alias("tax"),
            F.sum(_money("total_amount")).alias("total"),
            F.avg(_money("total_amount")).alias("avg_order_value"),
        )
        .select(*column_names("online_sales_daily"))
    )

    # Zone dwell per minute
    gold["zone_dwell_minute"] = (
        tables["fact_foot_traffic"]
        .withColumn("ts", F.date_trunc("minute", F.col("event_ts")))
        .groupBy("store_id", "zone", "ts")
        .agg(
            F.avg("dwell_seconds").alias("avg_dwell"),
            F.sum("count").alias("customers"),
        )
        .select(*column_names("zone_dwell_minute"))
    )

    # Marketing cost daily
    gold["marketing_cost_daily"] = (
        tables["fact_marketing"]
        .withColumn("day", F.to_date("event_ts"))
        .groupBy("campaign_id", "day")
        .agg(
            F.count("*").alias("impressions"),
            F.sum(_money("cost")).alias("cost"),
        )
        .select(*column_names("marketing_cost_daily"))
    )

    gold["campaign_performance_daily"] = generate_campaign_performance(
        tables["fact_marketing"], tables["fact_marketing_attribution"]
    )

    # Tender mix daily
    gold["tender_mix_daily"] = (
        tables["fact_receipts"]
        .withColumn("day", F.to_date("event_ts"))
        .groupBy("day", "payment_method")
        .agg(
            F.count("*").alias("transactions"),
            F.sum(_money("total_amount")).alias("total_amount"),
        )
        .select(*column_names("tender_mix_daily"))
    )

    return gold
