# Databricks/Fabric Notebook (PySpark) — Silver → Gold aggregates

from pyspark.sql import functions as F

# Sales by minute per store
df_receipts = spark.read.format("delta").load("/Tables/silver/receipts")
df_sales_minute = (
    df_receipts
    .withColumn("total_num", F.col("total_cents")/F.lit(100.0))
    .groupBy(
        F.col("store_id"),
        F.window(F.col("event_ts"), "1 minute").alias("w")
    )
    .agg(F.sum("total_num").alias("total_sales"), F.count("*").alias("receipts"))
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
df_rl = spark.read.format("delta").load("/Tables/silver/receipt_lines")
df_top_products = (
    df_rl
    .where(F.col("event_ts") > F.current_timestamp() - F.expr("INTERVAL 15 MINUTES"))
    .withColumn("ext_num", F.col("ext_cents")/F.lit(100.0))
    .groupBy("product_id")
    .agg(F.sum("ext_num").alias("revenue"), F.sum("quantity").alias("units"))
    .withColumn("computed_at", F.current_timestamp())
)
df_top_products.write.format("delta").mode("overwrite").save("/Tables/gold/top_products_15m")

# Inventory position current (store-product) from store_inventory_txn
df_inv = spark.read.format("delta").load("/Tables/silver/store_inventory_txn")
from pyspark.sql.window import Window
w_sp = Window.partitionBy("store_id", "product_id").orderBy(F.col("event_ts").desc())
df_inv_latest = (
    df_inv
    .withColumn("rn", F.row_number().over(w_sp))
    .where(F.col("rn") == 1)
    .select("store_id", "product_id", F.col("balance").alias("on_hand"))
)
df_inv_fallback = (
    df_inv
    .groupBy("store_id", "product_id")
    .agg(F.sum(F.col("quantity").cast("bigint")).alias("on_hand"))
)
df_inv_pos = (
    df_inv_latest.unionByName(df_inv_fallback)
    .groupBy("store_id", "product_id")
    .agg(F.max("on_hand").alias("on_hand"))
    .withColumn("as_of", F.current_timestamp())
)
df_inv_pos.write.format("delta").mode("overwrite").save("/Tables/gold/inventory_position_current")

# Truck dwell daily
df_truck = spark.read.format("delta").load("/Tables/silver/truck_moves")
df_arr = df_truck.where(F.col("status") == F.lit("ARRIVED")).select(
    F.col("truck_id"), F.col("dc_id"), F.col("store_id"), F.col("shipment_id"), F.col("event_ts").alias("arrival_ts")
)
df_dep = df_truck.where(F.col("status") == F.lit("DEPARTED")).select(
    F.col("truck_id"), F.col("dc_id"), F.col("store_id"), F.col("shipment_id"), F.col("event_ts").alias("departure_ts")
)
df_truck_joined = df_arr.join(df_dep, ["truck_id", "dc_id", "store_id", "shipment_id"], "left")
df_truck_daily = (
    df_truck_joined
    .withColumn("site", F.when(F.col("store_id").isNotNull(), F.concat(F.lit("Store-"), F.col("store_id").cast("string"))).otherwise(F.concat(F.lit("DC-"), F.col("dc_id").cast("string"))))
    .withColumn("day", F.to_date(F.coalesce(F.col("departure_ts"), F.col("arrival_ts"))))
    .withColumn("dwell_min", (F.unix_timestamp("departure_ts") - F.unix_timestamp("arrival_ts")) / 60.0)
    .groupBy("site", "day")
    .agg(F.avg("dwell_min").alias("avg_dwell_min"), F.count("*").alias("trucks"))
)
df_truck_daily.write.format("delta").mode("overwrite").save("/Tables/gold/truck_dwell_daily")

# Campaign revenue daily (illustrative; conversions/revenue may be enriched later)
df_mkt = spark.read.format("delta").load("/Tables/silver/marketing")
df_campaign_rev = (
    df_mkt.groupBy(F.col("campaign_id"), F.to_date("event_ts").alias("day"))
    .agg(F.count("*").alias("impressions"))
)
df_campaign_rev = df_campaign_rev.withColumn("conversions", F.lit(None).cast("bigint")).withColumn("revenue", F.lit(None).cast("double"))
df_campaign_rev.write.format("delta").mode("overwrite").save("/Tables/gold/campaign_revenue_daily")

# Online sales daily (headers)
df_ooh = spark.read.format("delta").load("/Tables/silver/online_order_headers")
df_online_sales_daily = (
    df_ooh
    .groupBy(F.to_date(F.col("event_ts")).alias("day"))
    .agg(
        F.count("*").alias("orders"),
        (F.sum("subtotal_cents")/F.lit(100.0)).alias("subtotal"),
        (F.sum("tax_cents")/F.lit(100.0)).alias("tax"),
        (F.sum("total_cents")/F.lit(100.0)).alias("total")
    )
    .withColumn("avg_order_value", F.col("total")/F.col("orders"))
)
df_online_sales_daily.write.format("delta").mode("overwrite").save("/Tables/gold/online_sales_daily")

# Fulfillment performance daily (lines)
df_ool = spark.read.format("delta").load("/Tables/silver/online_order_lines")
df_fulfillment_daily = (
    df_ool
    .withColumn("ts", F.coalesce("delivered_ts", "shipped_ts", "picked_ts"))
    .groupBy(
        F.to_date(F.col("ts")).alias("day"),
        F.col("fulfillment_mode"),
        F.col("fulfillment_status")
    )
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units")
    )
)
df_fulfillment_daily.write.format("delta").mode("overwrite").save("/Tables/gold/fulfillment_daily")

# Zone dwell per minute (store, zone)
df_ft = spark.read.format("delta").load("/Tables/silver/foot_traffic")
df_zone_dwell_minute = (
    df_ft
    .groupBy(
        F.col("store_id"),
        F.col("zone"),
        F.window(F.col("event_ts"), "1 minute").alias("w")
    )
    .agg(
        F.avg(F.col("dwell_seconds")).alias("avg_dwell"),
        F.sum(F.col("count")).alias("customers")
    )
    .select("store_id", "zone", F.col("w.start").alias("ts"), "avg_dwell", "customers")
)
df_zone_dwell_minute.write.format("delta").mode("overwrite").save("/Tables/gold/zone_dwell_minute")

# BLE presence per minute (unique devices)
df_ble = spark.read.format("delta").load("/Tables/silver/ble_pings")
df_ble_presence_minute = (
    df_ble
    .groupBy(
        F.col("store_id"),
        F.window(F.col("event_ts"), "1 minute").alias("w")
    )
    .agg(F.countDistinct("customer_ble_id").alias("devices"))
    .select("store_id", F.col("w.start").alias("ts"), "devices")
)
df_ble_presence_minute.write.format("delta").mode("overwrite").save("/Tables/gold/ble_presence_minute")

# Marketing cost daily
df_mkt_cost = (
    df_mkt
    .groupBy(F.col("campaign_id"), F.to_date("event_ts").alias("day"))
    .agg(
        F.count("*").alias("impressions"),
        (F.sum("CostCents")/F.lit(100.0)).alias("cost")
    )
)
df_mkt_cost.write.format("delta").mode("overwrite").save("/Tables/gold/marketing_cost_daily")

print("Silver → Gold completed")
