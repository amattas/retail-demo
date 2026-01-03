# Fabric Notebook — Lakehouse maintenance (OPTIMIZE/VACUUM/Z-Order)

tables = [
    # Bronze
    "/Tables/bronze/events",
    # Silver
    "/Tables/silver/receipts",
    "/Tables/silver/receipt_lines",
    "/Tables/silver/store_inventory_txn",
    "/Tables/silver/dc_inventory_txn",
    "/Tables/silver/foot_traffic",
    "/Tables/silver/ble_pings",
    "/Tables/silver/marketing",
    "/Tables/silver/promotions",
    "/Tables/silver/promo_lines",
    "/Tables/silver/online_orders",
    "/Tables/silver/truck_moves",
    "/Tables/silver/stockouts",
    "/Tables/silver/reorders",
    "/Tables/silver/payments",
    "/Tables/silver/store_ops",
    # Gold
    "/Tables/gold/sales_minute_store",
    "/Tables/gold/top_products_15m",
    "/Tables/gold/inventory_position_current",
    "/Tables/gold/truck_dwell_daily",
    "/Tables/gold/campaign_revenue_daily",
    # Dims
    "/Tables/dim/stores",
    "/Tables/dim/products_master",
    "/Tables/dim/customers",
    "/Tables/dim/geographies_master",
    "/Tables/dim/distribution_centers",
    "/Tables/dim/trucks",
]

def optimize(path: str, zorder_cols=None):
    z = f" ZORDER BY ({', '.join(zorder_cols)})" if zorder_cols else ""
    spark.sql(f"OPTIMIZE delta.`{path}`{z}")

def vacuum(path: str, retain_hours=168):
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")

# Suggested Z-Order columns by table name
zorder_map = {
    "/Tables/silver/receipts": ["store_id", "event_ts"],
    "/Tables/silver/receipt_lines": ["product_id", "receipt_id"],
    "/Tables/silver/store_inventory_txn": ["store_id", "product_id"],
    "/Tables/silver/dc_inventory_txn": ["dc_id", "product_id"],
    "/Tables/silver/foot_traffic": ["store_id", "zone"],
    "/Tables/silver/ble_pings": ["store_id", "customer_ble_id"],
    "/Tables/silver/truck_moves": ["store_id", "dc_id", "shipment_id"],
    "/Tables/gold/sales_minute_store": ["store_id", "ts"],
    "/Tables/gold/top_products_15m": ["product_id"],
    "/Tables/gold/inventory_position_current": ["store_id", "product_id"],
}

for path in tables:
    try:
        optimize(path, zorder_map.get(path))
        vacuum(path)
        print(f"Optimized and vacuumed: {path}")
    except Exception as e:
        print(f"Maintenance skipped for {path}: {e}")

print("Maintenance complete")

