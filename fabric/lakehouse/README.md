# Lakehouse

Lakehouse for persistent storage and medallion flows. Receives raw events (Bronze), curates into Silver, and aggregates to Gold.

Bronze:
- Raw JSON from Eventstream, partitioned by `event_type` and `date=YYYY-MM-DD`

Silver (Delta tables aligned to datagen DuckDB facts):
- `dc_inventory_txn`, `store_inventory_txn` (now include `txn_type`, `quantity`, `balance`)
- `truck_moves` (includes `eta`, `etd`, `status`)
- `receipts` (now includes `receipt_id_ext`, `payment_method`, `_cents` fields)
- `receipt_lines` (now includes `line_num`, `_cents` fields, `receipt_id_ext`)
- `foot_traffic` (now includes `dwell_seconds`)
- `ble_pings` (now includes `CustomerId`)
- `marketing` (now includes `impression_id_ext`, `CostCents`, `CustomerId`)
- `online_order_headers`, `online_order_lines` (split headers/lines)
- Dimensions: `stores`, `distribution_centers`, `trucks`, `customers`, `products_master`, `geographies_master`

Gold:
- Sales by minute/hour/day (store, region, product)
- Inventory position current and days-of-supply
- Logistics dwell and SLA metrics
- Marketing attribution and promo lift
- Online sales daily, fulfillment performance, zone dwell, BLE presence, marketing cost

Artifacts
- Silver DDL: `silver/ddl.sql`
- Gold DDL: `gold/ddl.sql`
- Notebook Bronze→Silver: `../notebooks/bronze_to_silver.py`
- Notebook Silver→Gold: `../notebooks/silver_to_gold.py`
- Pipelines (templates): `../pipelines/pl_bronze_to_silver.template.json`, `../pipelines/pl_silver_to_gold.template.json`
