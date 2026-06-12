# Data Schema

Source-of-truth schemas derive from the data generator.

## Column Naming Convention

**Standard:** All column names use `snake_case` throughout the data pipeline.

**Rationale:**
- Aligns with Python (PEP 8) naming conventions used in datagen
- Consistent with KQL table names and event types
- Avoids case-sensitivity issues across platforms

**Scope:** DuckDB, KQL tables, Lakehouse Silver/Gold tables

**Examples:** `event_ts`, `receipt_id_ext`, `customer_id`, `store_id`

**Exception:** Semantic Model display names can use user-friendly formats for presentation.

---

Event Envelope:
- See `datagen/src/retail_datagen/streaming/schemas.py` for `EventEnvelope` and payloads.
- Event types (18 total): `receipt_created`, `receipt_line_added`, `payment_processed`, `inventory_updated`, `stockout_detected`, `reorder_triggered`, `customer_entered`, `customer_zone_changed`, `ble_ping_detected`, `truck_arrived`, `truck_departed`, `store_opened`, `store_closed`, `ad_impression`, `promotion_applied`, `online_order_created`, `online_order_picked`, `online_order_shipped`.

Historical Facts and Dimensions (current DuckDB):
- Dimensions (`dim_*`):
  - dim_geographies: ID, City, State, ZipCode, District, Region
  - dim_stores: ID, StoreNumber, Address, GeographyID, tax_rate, volume_class, store_format, operating_hours, daily_traffic_multiplier
  - dim_distribution_centers: ID, DCNumber, Address, GeographyID
  - dim_trucks: ID, LicensePlate, Refrigeration, DCID
  - dim_customers: ID, FirstName, LastName, Address, GeographyID, LoyaltyCard, Phone, BLEId, AdId
  - dim_products: ID, ProductName, Brand, Company, Department, Category, Subcategory, Cost, MSRP, SalePrice, RequiresRefrigeration, LaunchDate, taxability, Tags
- Facts (`fact_*`):
  - fact_receipts: event_ts, receipt_id_ext, payment_method, discount_amount, tax_cents, Subtotal, total_amount, total_cents, receipt_type, subtotal_cents, tax_amount, customer_id, store_id, return_for_receipt_id_ext
  - fact_receipt_lines: unit_cents, unit_price, event_ts, product_id, quantity, ext_price, line_num, promo_code, ext_cents, receipt_id_ext
  - fact_store_inventory_txn: event_ts, product_id, txn_type, quantity, source, store_id, balance
  - fact_dc_inventory_txn: event_ts, product_id, txn_type, quantity, dc_id, balance, Source
  - fact_foot_traffic: count, zone, event_ts, sensor_id, dwell_seconds, store_id
  - fact_ble_pings: zone, event_ts, rssi, customer_ble_id, CustomerId, store_id, beacon_id
  - fact_marketing: event_ts, campaign_id, device, creative_id, customer_ad_id, impression_id_ext, cost, CostCents, CustomerId, channel
  - fact_online_order_headers: completed_ts, event_ts, order_id_ext, tax_cents, subtotal_amount, total_amount, total_cents, subtotal_cents, tax_amount, customer_id, payment_method
  - fact_online_order_lines: unit_cents, shipped_ts, unit_price, fulfillment_status, order_id, delivered_ts, product_id, quantity, ext_price, node_type, fulfillment_mode, picked_ts, node_id, line_num, promo_code, ext_cents
  - fact_truck_moves: shipment_id, status, eta, event_ts, etd, truck_id, dc_id, store_id

---

## Lakehouse Tables

### Silver Layer (ag schema)

Silver tables keep their `dim_*`/`fact_*` names inside the `ag` schema (e.g., `ag.fact_receipts`). They are loaded by two paths:

- **Batch**: `02-historical-data-load.ipynb` loads all 6 dimensions and all 18 facts from `Files/` parquet shortcuts, and builds a `dim_date` calendar table.
- **Streaming**: `03-streaming-to-silver.ipynb` incrementally appends new Eventhouse events (`cusn` shortcuts) using watermarks stored in `ag._watermarks`.

**Dimensions (7):** `dim_geographies`, `dim_stores`, `dim_distribution_centers`, `dim_trucks`, `dim_customers`, `dim_products`, `dim_date` (generated)

**Facts (18, batch-loaded):** `fact_receipts`, `fact_receipt_lines`, `fact_payments`, `fact_store_inventory_txn`, `fact_dc_inventory_txn`, `fact_truck_moves`, `fact_truck_inventory`, `fact_foot_traffic`, `fact_ble_pings`, `fact_customer_zone_changes`, `fact_marketing`, `fact_online_order_headers`, `fact_online_order_lines`, `fact_store_ops`, `fact_stockouts`, `fact_promotions`, `fact_promo_lines`, `fact_reorders`

**Streaming event → Silver table mapping** (from `03-streaming-to-silver.ipynb`):

| Event | Silver target |
|-------|---------------|
| `receipt_created` | `ag.fact_receipts` |
| `receipt_line_added` | `ag.fact_receipt_lines` |
| `payment_processed` | `ag.fact_payments` |
| `inventory_updated` | `ag.fact_store_inventory_txn` |
| `customer_entered` | `ag.fact_foot_traffic` |
| `truck_arrived`, `truck_departed` | `ag.fact_truck_moves` |
| `stockout_detected` | `ag.fact_stockouts` |
| `reorder_triggered` | `ag.fact_reorders` |
| `store_opened`, `store_closed` | `ag.fact_store_ops` |
| `ad_impression` | `ag.fact_marketing` |
| `promotion_applied` | `ag.fact_promotions` |
| `customer_zone_changed` | `ag.fact_customer_zone_changes` |
| `ble_ping_detected` | `ag.fact_ble_pings` |
| `online_order_created` | `ag.fact_online_order_headers` |
| `online_order_picked`, `online_order_shipped` | `ag.fact_online_order_status` |

`fact_truck_inventory` and `fact_promo_lines` are batch-only (no streaming event equivalent).

### Gold Layer (au schema)

Gold tables are pre-aggregated for dashboards. They are built by `02-historical-data-load.ipynb` (initial) and rebuilt by `04-streaming-to-gold.ipynb` (every 15 minutes via the `streaming-data-load` pipeline).

| Table | Columns |
|-------|---------|
| `sales_minute_store` | store_id, ts (minute), total_sales, receipts, avg_basket |
| `top_products_15m` | product_id, revenue, units, computed_at |
| `inventory_position_current` | store_id, product_id, on_hand, as_of |
| `dc_inventory_position_current` | dc_id, product_id, on_hand, as_of |
| `truck_dwell_daily` | site, day, avg_dwell_min, trucks |
| `online_sales_daily` | day, orders, subtotal, tax, total, avg_order_value |
| `zone_dwell_minute` | store_id, zone, ts (minute), avg_dwell, customers |
| `marketing_cost_daily` | campaign_id, day, impressions, cost |
| `tender_mix_daily` | day, payment_method, transactions, total_amount |

### Assets

- **Notebooks**: `fabric/lakehouse/02-historical-data-load.ipynb`, `03-streaming-to-silver.ipynb`, `04-streaming-to-gold.ipynb`
- **Pipelines**: `fabric/pipelines/` (`historical-data-load`, `streaming-data-load`, `daily-maintenance`, `machine-learning`); see [Pipelines](../fabric/pipelines.md)

### Notes

- Silver columns mirror datagen historical facts materialized in DuckDB
- Bronze is accessed via shortcuts: `Tables/cusn/<event_table>` (Eventhouse) and `Files/<table_name>` (ADLS parquet)
- Online orders are split into headers and lines to match the generator schema; streaming pick/ship status events land in `ag.fact_online_order_status`
