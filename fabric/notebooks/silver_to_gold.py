# Databricks/Fabric Notebook (PySpark) — Silver → Gold aggregates

from pyspark.sql import functions as F

# Sales by minute per store
df_sales_minute = (
    spark.read.format("delta").load("/Tables/silver/receipts")
    .groupBy(
        F.col("store_id"),
        F.window(F.col("event_ts"), "1 minute").alias("w")
    )
    .agg(F.sum("total").alias("total_sales"), F.count("*").alias("receipts"))
    .select(
        F.col("store_id"),
        F.col("w.start").alias("ts"),
        F.col("total_sales"),
        F.col("receipts"),
        (F.col("total_sales")/F.col("receipts")).alias("avg_basket")
    )
)
df_sales_minute.write.format("delta").mode("overwrite").save("/Tables/gold/sales_minute_store")

# Top products 15m
df_top_products = (
    spark.read.format("delta").load("/Tables/silver/receipt_lines")
    .where(F.col("event_ts") > F.current_timestamp() - F.expr("INTERVAL 15 MINUTES"))
    .groupBy("product_id")
    .agg(F.sum("ext_price").alias("revenue"), F.sum("qty").alias("units"))
    .withColumn("computed_at", F.current_timestamp())
)
df_top_products.write.format("delta").mode("overwrite").save("/Tables/gold/top_products_15m")

# Inventory position current (store-product) from store_inventory_txn
df_inv = spark.read.format("delta").load("/Tables/silver/store_inventory_txn")
df_inv_pos = (
    df_inv.groupBy("store_id", "product_id")
    .agg(F.sum("qty_delta").alias("on_hand"))
    .withColumn("as_of", F.current_timestamp())
)
df_inv_pos.write.format("delta").mode("overwrite").save("/Tables/gold/inventory_position_current")

# Truck dwell daily
df_truck = spark.read.format("delta").load("/Tables/silver/truck_moves")
df_truck_daily = (
    df_truck.groupBy(
        F.when(F.col("store_id").isNotNull(), F.concat(F.lit("Store-"), F.col("store_id").cast("string"))).otherwise(F.concat(F.lit("DC-"), F.col("dc_id").cast("string"))).alias("site"),
        F.to_date("event_ts").alias("day")
    )
    .agg((F.avg(F.unix_timestamp("departure_time") - F.unix_timestamp("arrival_time"))/60.0).alias("avg_dwell_min"),
         F.count("*").alias("trucks"))
)
df_truck_daily.write.format("delta").mode("overwrite").save("/Tables/gold/truck_dwell_daily")

# Campaign revenue daily (illustrative)
df_mkt = spark.read.format("delta").load("/Tables/silver/marketing")
df_rc = spark.read.format("delta").load("/Tables/silver/receipts")
# Note: joins require a common key. Here we assume trace_id linkage or later enrichment.
df_campaign_rev = (
    df_mkt.groupBy(F.col("campaign_id"), F.to_date("event_ts").alias("day"))
    .agg(F.count("*").alias("impressions"))
)
df_campaign_rev = df_campaign_rev.withColumn("conversions", F.lit(None).cast("bigint")).withColumn("revenue", F.lit(None).cast("double"))
df_campaign_rev.write.format("delta").mode("overwrite").save("/Tables/gold/campaign_revenue_daily")

print("Silver → Gold completed")

