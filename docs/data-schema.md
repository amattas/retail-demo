# Data Schema

Source-of-truth schemas derive from the data generator.

Event Envelope:
- See `datagen/src/retail_datagen/streaming/schemas.py` for `EventEnvelope` and payloads.
- Event types include: `receipt_created`, `receipt_line_added`, `payment_processed`, `inventory_updated`, `stockout_detected`, `reorder_triggered`, `customer_entered`, `customer_zone_changed`, `ble_ping_detected`, `truck_arrived`, `truck_departed`, `store_opened`, `store_closed`, `ad_impression`, `promotion_applied`.

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

Silver tables are normalized and typed from Bronze, aligned to DuckDB facts.

**Transaction Tables:**
- `silver_receipts`: event_ts, receipt_id_ext, payment_method, discount_amount, tax_cents, subtotal, total_amount, total_cents, receipt_type, subtotal_cents, tax_amount, customer_id, store_id, return_for_receipt_id_ext
- `silver_receipt_lines`: unit_cents, unit_price, event_ts, product_id, quantity, ext_price, line_num, promo_code, ext_cents, receipt_id_ext

**Inventory Tables:**
- `silver_store_inventory_txn`: event_ts, product_id, txn_type, quantity, source, store_id, balance
- `silver_dc_inventory_txn`: event_ts, product_id, txn_type, quantity, dc_id, balance, source

**Customer Tables:**
- `silver_foot_traffic`: count, zone, event_ts, sensor_id, dwell_seconds, store_id
- `silver_ble_pings`: zone, event_ts, rssi, customer_ble_id, customer_id, store_id, beacon_id

**Other Tables:**
- `silver_marketing`: event_ts, campaign_id, device, creative_id, customer_ad_id, impression_id_ext, cost, cost_cents, customer_id, channel
- `silver_online_order_headers`: completed_ts, event_ts, order_id_ext, tax_cents, subtotal_amount, total_amount, total_cents, subtotal_cents, tax_amount, customer_id, payment_method
- `silver_online_order_lines`: unit_cents, shipped_ts, unit_price, fulfillment_status, order_id, delivered_ts, product_id, quantity, ext_price, node_type, fulfillment_mode, picked_ts, node_id, line_num, promo_code, ext_cents
- `silver_truck_moves`: event_ts, truck_id, dc_id, store_id, shipment_id, status, eta, etd

**Silver Views:**
- `silver_receipt_header`: Canonical receipt totals using cents; joins latest payment and promotions
- `silver_receipt_detail`: Canonical line values using cents fields

### Gold Layer (au schema)

Gold tables are pre-aggregated for dashboards.

| Table | Columns |
|-------|---------|
| `gold_sales_minute_store` | store_id, ts (minute), total_sales, receipts, avg_basket |
| `gold_top_products_15m` | product_id, revenue, units, computed_at |
| `gold_inventory_position_current` | store_id, product_id, on_hand, as_of |
| `gold_truck_dwell_daily` | site, day, avg_dwell_min, trucks |
| `gold_campaign_revenue_daily` | campaign_id, day, impressions, conversions, revenue |
| `gold_online_sales_daily` | day, orders, subtotal, tax, total, avg_order_value |
| `gold_fulfillment_daily` | day, fulfillment_mode, fulfillment_status, orders, units |
| `gold_zone_dwell_minute` | store_id, zone, ts (minute), avg_dwell, customers |
| `gold_ble_presence_minute` | store_id, ts (minute), devices |
| `gold_marketing_cost_daily` | campaign_id, day, impressions, cost |

### Assets

- **DDL scripts**: `fabric/lakehouse/silver/ddl.sql`, `fabric/lakehouse/gold/ddl.sql`
- **Notebooks**: `02-onelake-to-silver.ipynb`, `03-silver-to-gold.ipynb`
- **Pipelines**: `pl_bronze_to_silver.template.json`, `pl_silver_to_gold.template.json`

### Notes

- Silver columns mirror datagen historical facts materialized in DuckDB
- Bronze path is `/Tables/bronze/events/`; notebooks read per `event_type` folder
- Online orders are split into headers and lines to match the generator schema
