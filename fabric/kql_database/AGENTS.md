# AGENTS.md — KQL Database

Spec for KQL database objects fed by Eventstream.

## Tables (illustrative — align to payloads)
- `receipt_created`: store_id:int, customer_id:int, receipt_id:string, subtotal:real, tax:real, total:real, tender_type:string, item_count:int, ingest_timestamp:datetime, trace_id:string
- `receipt_line_added`: receipt_id:string, line_number:int, product_id:int, quantity:int, unit_price:real, extended_price:real, promo_code:string
- `payment_processed`: receipt_id:string, payment_method:string, amount:real, transaction_id:string, processing_time:datetime, status:string
- `inventory_updated`: store_id:int, dc_id:int, product_id:int, quantity_delta:int, reason:string, source:string
- `stockout_detected`: store_id:int, dc_id:int, product_id:int, last_known_quantity:int, detection_time:datetime
- `reorder_triggered`: store_id:int, dc_id:int, product_id:int, current_quantity:int, reorder_quantity:int, reorder_point:int, priority:string
- `customer_entered`: store_id:int, sensor_id:string, zone:string, customer_count:int, dwell_time:int
- `customer_zone_changed`: store_id:int, customer_ble_id:string, from_zone:string, to_zone:string, timestamp:datetime
- `ble_ping_detected`: store_id:int, beacon_id:string, customer_ble_id:string, rssi:int, zone:string
- `truck_arrived`: truck_id:string, dc_id:int, store_id:int, shipment_id:string, arrival_time:datetime, estimated_unload_duration:int
- `truck_departed`: truck_id:string, dc_id:int, store_id:int, shipment_id:string, departure_time:datetime, actual_unload_duration:int
- `store_opened`: store_id:int, operation_time:datetime
- `store_closed`: store_id:int, operation_time:datetime
- `ad_impression`: channel:string, campaign_id:string, creative_id:string, customer_ad_id:string, impression_id:string, cost:real, device_type:string
- `promotion_applied`: receipt_id:string, promo_code:string, discount_amount:real, discount_type:string, product_ids:dynamic

- `online_order_created`: order_id:string, customer_id:int, fulfillment_mode:string, node_type:string, node_id:int, item_count:int, subtotal:real, tax:real, total:real, tender_type:string
- `online_order_picked`: order_id:string, node_type:string, node_id:int, fulfillment_mode:string, picked_time:datetime
- `online_order_shipped`: order_id:string, node_type:string, node_id:int, fulfillment_mode:string, shipped_time:datetime

All tables include standard envelope columns: `event_type:string, trace_id:string, ingest_timestamp:datetime, source:string, correlation_id:string, partition_key:string, session_id:string, parent_event_id:string`.

## Policies
- Retention: 7–14 days for raw event tables
- Materialized views:
  - `mv_store_sales_minute` (sum totals by store, 1-minute bins)
  - `mv_inventory_levels` (latest inventory per store/product)
  - `mv_truck_sla` (arrival→departure durations)
- Update policies to populate Gold-like rollups

## Functions
- `fn_json_payload()` – helper to parse raw JSON if needed
- `fn_attribution_window(campaign_id, customer_id)` – compute conversions within window

## Done Criteria
- Tables created and ingest mappings validated from Eventstream
- Primary materialized views online and up-to-date
- Querysets runnable with <2s response for key dashboards
