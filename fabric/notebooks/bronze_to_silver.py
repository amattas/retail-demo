# Databricks/Fabric Notebook (PySpark) — Bronze JSON → Silver Delta
# Aligns with datagen schemas in datagen/src/retail_datagen/streaming/schemas.py

from pyspark.sql import functions as F
from pyspark.sql import types as T

bronze_root = "/Tables/bronze/events"

def read_bronze(event_type: str):
    path = f"{bronze_root}/event_type={event_type}"
    return spark.read.json(path, multiLine=False)

def write_delta(df, path, mode="append"):
    (df.write.format("delta").mode(mode).option("mergeSchema", "true").save(path))

# No legacy/envelope passthrough: Silver uses canonical columns only

# 1) Receipts (canonical columns only)
rc = read_bronze("receipt_created")
df_receipts = (
    rc.select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.receipt_id").alias("receipt_id_ext"),
        F.lit(None).cast("string").alias("payment_method"),
        F.lit(None).cast("string").alias("discount_amount"),
        F.round(F.col("payload.tax") * 100).cast("bigint").alias("tax_cents"),
        F.col("payload.subtotal").cast("string").alias("subtotal"),
        F.col("payload.total").cast("string").alias("total"),
        F.round(F.col("payload.total") * 100).cast("bigint").alias("total_cents"),
        F.lit(None).cast("string").alias("receipt_type"),
        F.round(F.col("payload.subtotal") * 100).cast("bigint").alias("subtotal_cents"),
        F.col("payload.tax").cast("string").alias("tax"),
        F.col("payload.customer_id").cast("bigint").alias("customer_id"),
        F.col("payload.store_id").cast("bigint").alias("store_id"),
        F.lit(None).cast("string").alias("return_for_receipt_id_ext"),
    )
)
write_delta(df_receipts, "/Tables/silver/receipts")

# 2) Receipt lines (canonical columns only)
rl = read_bronze("receipt_line_added")
df_receipt_lines = (
    rl.select(
        F.round(F.col("payload.unit_price") * 100).cast("bigint").alias("unit_cents"),
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.unit_price").cast("string").alias("unit_price"),
        F.col("payload.product_id").cast("bigint").alias("product_id"),
        F.col("payload.quantity").cast("bigint").alias("quantity"),
        F.col("payload.extended_price").cast("string").alias("ext_price"),
        F.col("payload.line_number").cast("bigint").alias("line_num"),
        F.col("payload.promo_code").alias("promo_code"),
        F.round(F.col("payload.extended_price") * 100).cast("bigint").alias("ext_cents"),
        F.col("payload.receipt_id").alias("receipt_id_ext"),
    )
)
write_delta(df_receipt_lines, "/Tables/silver/receipt_lines")

# 3) Store/DC inventory transactions (canonical)
inv = read_bronze("inventory_updated")
df_store_inv = (
    inv.where(F.col("payload.store_id").isNotNull())
    .select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.product_id").cast("bigint").alias("product_id"),
        F.col("payload.reason").alias("txn_type"),
        F.col("payload.quantity_delta").cast("bigint").alias("quantity"),
        F.col("payload.source").alias("source"),
        F.col("payload.store_id").cast("bigint").alias("store_id"),
        F.lit(None).cast("bigint").alias("balance"),
    )
)
write_delta(df_store_inv, "/Tables/silver/store_inventory_txn")

df_dc_inv = (
    inv.where(F.col("payload.dc_id").isNotNull())
    .select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.product_id").cast("bigint").alias("product_id"),
        F.col("payload.reason").alias("txn_type"),
        F.col("payload.quantity_delta").cast("bigint").alias("quantity"),
        F.col("payload.dc_id").cast("bigint").alias("dc_id"),
        F.lit(None).cast("bigint").alias("balance"),
        F.col("payload.source").alias("source"),
    )
)
write_delta(df_dc_inv, "/Tables/silver/dc_inventory_txn")

# 4) Foot traffic (canonical)
ft = read_bronze("customer_entered")
df_foot = (
    ft.select(
        F.col("payload.customer_count").cast("bigint").alias("count"),
        F.col("payload.zone").alias("zone"),
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.sensor_id").alias("sensor_id"),
        F.col("payload.dwell_time").cast("bigint").alias("dwell_seconds"),
        F.col("payload.store_id").cast("bigint").alias("store_id"),
    )
)
write_delta(df_foot, "/Tables/silver/foot_traffic")

# 5) BLE pings (canonical)
ble = read_bronze("ble_ping_detected")
df_ble = (
    ble.select(
        F.col("payload.zone").alias("zone"),
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.rssi").cast("bigint").alias("rssi"),
        F.col("payload.customer_ble_id").alias("customer_ble_id"),
        F.lit(None).cast("bigint").alias("customer_id"),
        F.col("payload.store_id").cast("bigint").alias("store_id"),
        F.col("payload.beacon_id").alias("beacon_id"),
    )
)
write_delta(df_ble, "/Tables/silver/ble_pings")

# 6) Marketing (ad impressions; canonical)
mk = read_bronze("ad_impression")
df_mkt = (
    mk.select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.campaign_id").alias("campaign_id"),
        F.col("payload.device_type").alias("device"),
        F.col("payload.creative_id").alias("creative_id"),
        F.col("payload.customer_ad_id").alias("customer_ad_id"),
        F.col("payload.impression_id").alias("impression_id_ext"),
        F.col("payload.cost").cast("string").alias("cost"),
        F.round(F.col("payload.cost") * 100).cast("bigint").alias("cost_cents"),
        F.lit(None).cast("bigint").alias("customer_id"),
        F.col("payload.channel").alias("channel"),
    )
)
write_delta(df_mkt, "/Tables/silver/marketing")

# 7) Online orders (created → headers only; canonical)
oo = read_bronze("online_order_created")
df_oo_hdr = (
    oo.select(
        F.lit(None).cast("timestamp").alias("completed_ts"),
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.order_id").alias("order_id_ext"),
        F.round(F.col("payload.tax") * 100).cast("bigint").alias("tax_cents"),
        F.col("payload.subtotal").cast("string").alias("subtotal"),
        F.col("payload.total").cast("string").alias("total"),
        F.round(F.col("payload.total") * 100).cast("bigint").alias("total_cents"),
        F.round(F.col("payload.subtotal") * 100).cast("bigint").alias("subtotal_cents"),
        F.col("payload.tax").cast("string").alias("tax"),
        F.col("payload.customer_id").cast("bigint").alias("customer_id"),
        F.lit(None).cast("string").alias("payment_method"),
    )
)
write_delta(df_oo_hdr, "/Tables/silver/online_order_headers")

# 8) Promotions applied (no legacy)
pr = read_bronze("promotion_applied")
df_promos = (
    pr.select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.receipt_id").alias("receipt_id"),
        F.col("payload.promo_code").alias("promo_code"),
        F.col("payload.discount_amount").cast("double").alias("discount_amount"),
        F.col("payload.discount_type").alias("discount_type"),
        F.to_json(F.col("payload.product_ids")).alias("product_ids_json"),
    )
)
write_delta(df_promos, "/Tables/silver/promotions")

# 8b) Promotion lines (explode product_ids)
df_promo_lines = (
    pr.select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.receipt_id").alias("receipt_id"),
        F.col("payload.promo_code").alias("promo_code"),
        F.explode_outer("payload.product_ids").alias("product_id"),
        F.col("payload.discount_amount").cast("double").alias("discount_amount"),
        F.col("payload.discount_type").alias("discount_type"),
    )
    .withColumn("product_id", F.col("product_id").cast("bigint"))
)
write_delta(df_promo_lines, "/Tables/silver/promo_lines")

# 9) Stockouts detected
so = read_bronze("stockout_detected")
df_stockouts = (
    so.select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.store_id").cast("bigint").alias("store_id"),
        F.col("payload.dc_id").cast("bigint").alias("dc_id"),
        F.col("payload.product_id").cast("bigint").alias("product_id"),
        F.col("payload.last_known_quantity").cast("int").alias("last_known_quantity"),
        F.col("payload.detection_time").cast("timestamp").alias("detection_time"),
    )
)
write_delta(df_stockouts, "/Tables/silver/stockouts")

# 10) Reorders triggered
ro = read_bronze("reorder_triggered")
df_reorders = (
    ro.select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.store_id").cast("bigint").alias("store_id"),
        F.col("payload.dc_id").cast("bigint").alias("dc_id"),
        F.col("payload.product_id").cast("bigint").alias("product_id"),
        F.col("payload.current_quantity").cast("int").alias("current_quantity"),
        F.col("payload.reorder_quantity").cast("int").alias("reorder_quantity"),
        F.col("payload.reorder_point").cast("int").alias("reorder_point"),
        F.col("payload.priority").alias("priority"),
    )
)
write_delta(df_reorders, "/Tables/silver/reorders")

# 11) Payments processed
pp = read_bronze("payment_processed")
df_payments = (
    pp.select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.receipt_id").alias("receipt_id"),
        F.col("payload.payment_method").alias("payment_method"),
        F.col("payload.amount").cast("double").alias("amount"),
        F.col("payload.transaction_id").alias("transaction_id"),
        F.col("payload.processing_time").cast("timestamp").alias("processing_time"),
        F.col("payload.status").alias("status"),
    )
)
write_delta(df_payments, "/Tables/silver/payments")

# 12) Store operations
so_open = read_bronze("store_opened")
so_closed = read_bronze("store_closed")
df_store_ops = (
    so_open.select(
        F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
        F.col("payload.store_id").cast("bigint").alias("store_id"),
        F.col("payload.operation_time").cast("timestamp").alias("operation_time"),
        F.col("payload.operation_type").alias("operation_type"),
    )
    .unionByName(
        so_closed.select(
            F.col("ingest_timestamp").cast("timestamp").alias("event_ts"),
            F.col("payload.store_id").cast("bigint").alias("store_id"),
            F.col("payload.operation_time").cast("timestamp").alias("operation_time"),
            F.col("payload.operation_type").alias("operation_type"),
        )
    )
)
write_delta(df_store_ops, "/Tables/silver/store_ops")

# 13) Truck moves (arrived/departed) — canonical columns
arr = read_bronze("truck_arrived").select(
    F.col("payload.arrival_time").cast("timestamp").alias("event_ts"),
    F.col("payload.truck_id").alias("truck_id"),
    F.col("payload.dc_id").cast("bigint").alias("dc_id"),
    F.col("payload.store_id").cast("bigint").alias("store_id"),
    F.col("payload.shipment_id").alias("shipment_id"),
    F.lit("ARRIVED").alias("status"),
    F.lit(None).cast("timestamp").alias("eta"),
    F.lit(None).cast("timestamp").alias("etd"),
)

dep = read_bronze("truck_departed").select(
    F.col("payload.departure_time").cast("timestamp").alias("event_ts"),
    F.col("payload.truck_id").alias("truck_id"),
    F.col("payload.dc_id").cast("bigint").alias("dc_id"),
    F.col("payload.store_id").cast("bigint").alias("store_id"),
    F.col("payload.shipment_id").alias("shipment_id"),
    F.lit("DEPARTED").alias("status"),
    F.lit(None).cast("timestamp").alias("eta"),
    F.lit(None).cast("timestamp").alias("etd"),
)

df_truck_moves = arr.unionByName(dep)
write_delta(df_truck_moves, "/Tables/silver/truck_moves")

print("Bronze → Silver completed")
