# Data Schema

Source-of-truth schemas derive from the data generator.

Event Envelope:
- See `datagen/src/retail_datagen/streaming/schemas.py` for `EventEnvelope` and payloads.
- Event types include: `receipt_created`, `receipt_line_added`, `payment_processed`, `inventory_updated`, `stockout_detected`, `reorder_triggered`, `customer_entered`, `customer_zone_changed`, `ble_ping_detected`, `truck_arrived`, `truck_departed`, `store_opened`, `store_closed`, `ad_impression`, `promotion_applied`.

Historical Facts and Dimensions:
- See `datagen/AGENTS.md` for master dimensions and facts: `stores`, `distribution_centers`, `trucks`, `customers`, `products_master`, plus `receipts`, `receipt_lines`, `dc_inventory_txn`, `store_inventory_txn`, `foot_traffic`, `ble_pings`, `marketing`.

Lakehouse Silver tables mirror these facts/dimensions; KQL tables mirror event payloads for hot queries.

