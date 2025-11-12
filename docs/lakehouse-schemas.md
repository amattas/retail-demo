# Lakehouse Schemas

This page documents the Silver and Gold Lakehouse tables aligned to the current datagen DuckDB schema.

Silver (normalized/typed from Bronze, aligned to DuckDB facts)
- silver_receipts: event_ts, receipt_id_ext, payment_method, discount_amount, tax_cents, Subtotal, total_amount, total_cents, receipt_type, subtotal_cents, tax_amount, customer_id, store_id, return_for_receipt_id_ext
- silver_receipt_lines: unit_cents, unit_price, event_ts, product_id, quantity, ext_price, line_num, promo_code, ext_cents, receipt_id_ext
- silver_store_inventory_txn: event_ts, product_id, txn_type, quantity, source, store_id, balance
- silver_dc_inventory_txn: event_ts, product_id, txn_type, quantity, dc_id, balance, Source
- silver_foot_traffic: count, zone, event_ts, sensor_id, dwell_seconds, store_id
- silver_ble_pings: zone, event_ts, rssi, customer_ble_id, CustomerId, store_id, beacon_id
- silver_marketing: event_ts, campaign_id, device, creative_id, customer_ad_id, impression_id_ext, cost, CostCents, CustomerId, channel
- silver_online_order_headers: completed_ts, event_ts, order_id_ext, tax_cents, subtotal_amount, total_amount, total_cents, subtotal_cents, tax_amount, customer_id, payment_method
- silver_online_order_lines: unit_cents, shipped_ts, unit_price, fulfillment_status, order_id, delivered_ts, product_id, quantity, ext_price, node_type, fulfillment_mode, picked_ts, node_id, line_num, promo_code, ext_cents
- silver_truck_moves: event_ts, truck_id, dc_id, store_id, shipment_id, status, eta, etd

Silver Views (for convenience)
- silver_receipt_header: canonical receipt totals using cents; joins latest payment and promotions
- silver_receipt_detail: canonical line values using cents fields

Gold (aggregated for dashboards)
- gold_sales_minute_store: StoreID, TS(minute), TotalSales, Receipts, AvgBasket
- gold_top_products_15m: ProductID, Revenue, Units, ComputedAt
- gold_inventory_position_current: StoreID, ProductID, OnHand, AsOf
- gold_truck_dwell_daily: Site, Day, AvgDwellMin, Trucks
- gold_campaign_revenue_daily: CampaignId, Day, Impressions, Conversions, Revenue
- gold_online_sales_daily: Day, Orders, Subtotal, Tax, Total, AvgOrderValue
- gold_fulfillment_daily: Day, FulfillmentMode, FulfillmentStatus, Orders, Units
- gold_zone_dwell_minute: StoreID, Zone, TS(minute), AvgDwell, Customers
- gold_ble_presence_minute: StoreID, TS(minute), Devices
- gold_marketing_cost_daily: CampaignId, Day, Impressions, Cost

Assets
- DDL scripts: `fabric/lakehouse/silver/ddl.sql` and `fabric/lakehouse/gold/ddl.sql`
- Notebooks: `fabric/notebooks/bronze_to_silver.py` and `fabric/notebooks/silver_to_gold.py`
- Pipelines: `fabric/pipelines/pl_bronze_to_silver.template.json`, `fabric/pipelines/pl_silver_to_gold.template.json`

Notes
- Silver columns mirror datagen historical facts materialized in DuckDB (`datagen/data/retail.duckdb`).
- Bronze path is `/Tables/bronze/events/`; notebooks read per `event_type` folder.
- Online orders are split into headers and lines to match the generator schema.
