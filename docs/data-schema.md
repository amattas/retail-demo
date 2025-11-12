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

Lakehouse Silver tables mirror these facts/dimensions; KQL tables mirror event payloads for hot queries. See `fabric/lakehouse/silver/ddl.sql` for canonical Silver columns.
