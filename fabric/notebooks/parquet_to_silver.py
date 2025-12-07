# Fabric Notebook (PySpark) - Load exported Parquet files directly to Silver Delta
# Use this for historical batch data from datagen Parquet exports
# (Bronze layer is for streaming JSON events only)

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Configure source path - update this to your Parquet export location
# If using Azure Storage: "abfss://container@account.dfs.core.windows.net/path"
# If using Lakehouse Files: "/lakehouse/default/Files/datagen-export"
PARQUET_SOURCE = "/lakehouse/default/Files/datagen-export"

def write_delta(df, table_path, mode="overwrite"):
    """Write DataFrame to Delta table with schema merge enabled."""
    (df.write
       .format("delta")
       .mode(mode)
       .option("mergeSchema", "true")
       .save(table_path))
    print(f"  Written to {table_path}")

# =============================================================================
# DIMENSION TABLES (Master Data)
# =============================================================================

print("Loading dimension tables...")

# Geographies
try:
    df_geo = spark.read.parquet(f"{PARQUET_SOURCE}/master/dim_geographies.parquet")
    write_delta(df_geo, "/Tables/silver/dim_geographies")
except Exception as e:
    print(f"  Skipping dim_geographies: {e}")

# Stores
try:
    df_stores = spark.read.parquet(f"{PARQUET_SOURCE}/master/dim_stores.parquet")
    write_delta(df_stores, "/Tables/silver/dim_stores")
except Exception as e:
    print(f"  Skipping dim_stores: {e}")

# Distribution Centers
try:
    df_dcs = spark.read.parquet(f"{PARQUET_SOURCE}/master/dim_distribution_centers.parquet")
    write_delta(df_dcs, "/Tables/silver/dim_distribution_centers")
except Exception as e:
    print(f"  Skipping dim_distribution_centers: {e}")

# Trucks
try:
    df_trucks = spark.read.parquet(f"{PARQUET_SOURCE}/master/dim_trucks.parquet")
    write_delta(df_trucks, "/Tables/silver/dim_trucks")
except Exception as e:
    print(f"  Skipping dim_trucks: {e}")

# Customers
try:
    df_customers = spark.read.parquet(f"{PARQUET_SOURCE}/master/dim_customers.parquet")
    write_delta(df_customers, "/Tables/silver/dim_customers")
except Exception as e:
    print(f"  Skipping dim_customers: {e}")

# Products
try:
    df_products = spark.read.parquet(f"{PARQUET_SOURCE}/master/dim_products.parquet")
    write_delta(df_products, "/Tables/silver/dim_products")
except Exception as e:
    print(f"  Skipping dim_products: {e}")

print("Dimension tables loaded.\n")

# =============================================================================
# FACT TABLES
# =============================================================================

print("Loading fact tables...")

# Helper to read all partitioned Parquet files for a fact table
def read_fact_parquet(table_name):
    """Read all Parquet files for a fact table (handles year/month partitions)."""
    path = f"{PARQUET_SOURCE}/facts/{table_name}"
    return spark.read.parquet(f"{path}/*/*/*.parquet")  # year/month/files

# 1) Receipts
try:
    df_receipts = read_fact_parquet("fact_receipts").select(
        F.col("event_ts"),
        F.col("receipt_id_ext"),
        F.col("payment_method"),
        F.col("discount_amount"),
        F.col("tax_cents"),
        F.col("subtotal"),
        F.col("total"),
        F.col("total_cents"),
        F.col("receipt_type"),
        F.col("subtotal_cents"),
        F.col("tax"),
        F.col("customer_id"),
        F.col("store_id"),
        F.col("return_for_receipt_id_ext"),
    )
    write_delta(df_receipts, "/Tables/silver/receipts")
except Exception as e:
    print(f"  Skipping receipts: {e}")

# 2) Receipt Lines
try:
    df_receipt_lines = read_fact_parquet("fact_receipt_lines").select(
        F.col("unit_cents"),
        F.col("unit_price"),
        F.col("event_ts"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("ext_price"),
        F.col("line_num"),
        F.col("promo_code"),
        F.col("ext_cents"),
        F.col("receipt_id_ext"),
    )
    write_delta(df_receipt_lines, "/Tables/silver/receipt_lines")
except Exception as e:
    print(f"  Skipping receipt_lines: {e}")

# 3) Store Inventory Transactions
try:
    df_store_inv = read_fact_parquet("fact_store_inventory_txn").select(
        F.col("event_ts"),
        F.col("product_id"),
        F.col("txn_type"),
        F.col("quantity"),
        F.col("source"),
        F.col("store_id"),
        F.col("balance"),
    )
    write_delta(df_store_inv, "/Tables/silver/store_inventory_txn")
except Exception as e:
    print(f"  Skipping store_inventory_txn: {e}")

# 4) DC Inventory Transactions
try:
    df_dc_inv = read_fact_parquet("fact_dc_inventory_txn").select(
        F.col("event_ts"),
        F.col("product_id"),
        F.col("txn_type"),
        F.col("quantity"),
        F.col("dc_id"),
        F.col("balance"),
        F.col("source"),
    )
    write_delta(df_dc_inv, "/Tables/silver/dc_inventory_txn")
except Exception as e:
    print(f"  Skipping dc_inventory_txn: {e}")

# 5) Foot Traffic
try:
    df_foot = read_fact_parquet("fact_foot_traffic").select(
        F.col("count"),
        F.col("zone"),
        F.col("event_ts"),
        F.col("sensor_id"),
        F.col("dwell_seconds"),
        F.col("store_id"),
    )
    write_delta(df_foot, "/Tables/silver/foot_traffic")
except Exception as e:
    print(f"  Skipping foot_traffic: {e}")

# 6) BLE Pings
try:
    df_ble = read_fact_parquet("fact_ble_pings").select(
        F.col("zone"),
        F.col("event_ts"),
        F.col("rssi"),
        F.col("customer_ble_id"),
        F.col("customer_id"),
        F.col("store_id"),
        F.col("beacon_id"),
    )
    write_delta(df_ble, "/Tables/silver/ble_pings")
except Exception as e:
    print(f"  Skipping ble_pings: {e}")

# 7) Marketing
try:
    df_mkt = read_fact_parquet("fact_marketing").select(
        F.col("event_ts"),
        F.col("campaign_id"),
        F.col("device"),
        F.col("creative_id"),
        F.col("customer_ad_id"),
        F.col("impression_id_ext"),
        F.col("cost"),
        F.col("cost_cents"),
        F.col("customer_id"),
        F.col("channel"),
    )
    write_delta(df_mkt, "/Tables/silver/marketing")
except Exception as e:
    print(f"  Skipping marketing: {e}")

# 8) Online Order Headers
try:
    df_oo_hdr = read_fact_parquet("fact_online_order_headers").select(
        F.col("completed_ts"),
        F.col("event_ts"),
        F.col("order_id_ext"),
        F.col("tax_cents"),
        F.col("subtotal"),
        F.col("total"),
        F.col("total_cents"),
        F.col("subtotal_cents"),
        F.col("tax"),
        F.col("customer_id"),
        F.col("payment_method"),
    )
    write_delta(df_oo_hdr, "/Tables/silver/online_order_headers")
except Exception as e:
    print(f"  Skipping online_order_headers: {e}")

# 9) Online Order Lines
try:
    df_oo_lines = read_fact_parquet("fact_online_order_lines").select(
        F.col("unit_cents"),
        F.col("shipped_ts"),
        F.col("unit_price"),
        F.col("fulfillment_status"),
        F.col("order_id"),
        F.col("delivered_ts"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("ext_price"),
        F.col("node_type"),
        F.col("fulfillment_mode"),
        F.col("picked_ts"),
        F.col("node_id"),
        F.col("line_num"),
        F.col("promo_code"),
        F.col("ext_cents"),
    )
    write_delta(df_oo_lines, "/Tables/silver/online_order_lines")
except Exception as e:
    print(f"  Skipping online_order_lines: {e}")

# 10) Truck Moves
try:
    df_trucks = read_fact_parquet("fact_truck_moves").select(
        F.col("event_ts"),
        F.col("truck_id"),
        F.col("dc_id"),
        F.col("store_id"),
        F.col("shipment_id"),
        F.col("status"),
        F.col("eta"),
        F.col("etd"),
    )
    write_delta(df_trucks, "/Tables/silver/truck_moves")
except Exception as e:
    print(f"  Skipping truck_moves: {e}")

print("\nFact tables loaded.")
print("Parquet -> Silver load complete!")
