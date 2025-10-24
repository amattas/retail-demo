# Lakehouse

Lakehouse for persistent storage and medallion flows. Receives raw events (Bronze), curates into Silver, and aggregates to Gold.

Bronze:
- Raw JSON from Eventstream, partitioned by `event_type` and `date=YYYY-MM-DD`

Silver (Delta tables aligned to historical facts in datagen):
- `dc_inventory_txn`, `store_inventory_txn`, `truck_moves`
- `receipts`, `receipt_lines`, `foot_traffic`, `ble_pings`, `marketing`
- Dimensions: `stores`, `distribution_centers`, `trucks`, `customers`, `products_master`, `geographies_master`

Gold:
- Sales by minute/hour/day (store, region, product)
- Inventory position current and days-of-supply
- Logistics dwell and SLA metrics
- Marketing attribution and promo lift

