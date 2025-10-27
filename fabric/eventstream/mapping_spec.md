# Eventstream Mapping Spec

These mappings align Fabric Eventstream to the KQL DB tables and Lakehouse Bronze, based on datagen event envelope and payloads in `datagen/src/retail_datagen/streaming/schemas.py`.

Common envelope fields:
- `$.event_type` → `event_type`
- `$.trace_id` → `trace_id`
- `$.ingest_timestamp` → `ingest_timestamp`
- `$.schema_version` → `schema_version`
- `$.source` → `source`
- `$.correlation_id` → `correlation_id`
- `$.partition_key` → `partition_key`
- `$.session_id` → `session_id`
- `$.parent_event_id` → `parent_event_id`

Example payload mappings (JSONPath):

- `receipt_created` → `receipt_created` table
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.customer_id` → `customer_id:long`
  - `$.payload.receipt_id` → `receipt_id:string`
  - `$.payload.subtotal` → `subtotal:real`
  - `$.payload.tax` → `tax:real`
  - `$.payload.total` → `total:real`
  - `$.payload.tender_type` → `tender_type:string`
  - `$.payload.item_count` → `item_count:long`

- `receipt_line_added`
  - `$.payload.receipt_id` → `receipt_id:string`
  - `$.payload.line_number` → `line_number:long`
  - `$.payload.product_id` → `product_id:long`
  - `$.payload.quantity` → `quantity:long`
  - `$.payload.unit_price` → `unit_price:real`
  - `$.payload.extended_price` → `extended_price:real`
  - `$.payload.promo_code` → `promo_code:string`

- `payment_processed`
  - `$.payload.receipt_id` → `receipt_id:string`
  - `$.payload.payment_method` → `payment_method:string`
  - `$.payload.amount` → `amount:real`
  - `$.payload.transaction_id` → `transaction_id:string`
  - `$.payload.processing_time` → `processing_time:datetime`
  - `$.payload.status` → `status:string`

- `inventory_updated`
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.dc_id` → `dc_id:long`
  - `$.payload.product_id` → `product_id:long`
  - `$.payload.quantity_delta` → `quantity_delta:long`
  - `$.payload.reason` → `reason:string`
  - `$.payload.source` → `source_override:string`

- `stockout_detected`
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.dc_id` → `dc_id:long`
  - `$.payload.product_id` → `product_id:long`
  - `$.payload.last_known_quantity` → `last_known_quantity:long`
  - `$.payload.detection_time` → `detection_time:datetime`

- `reorder_triggered`
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.dc_id` → `dc_id:long`
  - `$.payload.product_id` → `product_id:long`
  - `$.payload.current_quantity` → `current_quantity:long`
  - `$.payload.reorder_quantity` → `reorder_quantity:long`
  - `$.payload.reorder_point` → `reorder_point:long`
  - `$.payload.priority` → `priority:string`

- `customer_entered`
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.sensor_id` → `sensor_id:string`
  - `$.payload.zone` → `zone:string`
  - `$.payload.customer_count` → `customer_count:long`
  - `$.payload.dwell_time` → `dwell_time:long`

- `customer_zone_changed`
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.customer_ble_id` → `customer_ble_id:string`
  - `$.payload.from_zone` → `from_zone:string`
  - `$.payload.to_zone` → `to_zone:string`
  - `$.payload.timestamp` → `timestamp:datetime`

- `ble_ping_detected`
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.beacon_id` → `beacon_id:string`
  - `$.payload.customer_ble_id` → `customer_ble_id:string`
  - `$.payload.rssi` → `rssi:long`
  - `$.payload.zone` → `zone:string`

- `truck_arrived`
  - `$.payload.truck_id` → `truck_id:string`
  - `$.payload.dc_id` → `dc_id:long`
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.shipment_id` → `shipment_id:string`
  - `$.payload.arrival_time` → `arrival_time:datetime`
  - `$.payload.estimated_unload_duration` → `estimated_unload_duration:long`

- `truck_departed`
  - `$.payload.truck_id` → `truck_id:string`
  - `$.payload.dc_id` → `dc_id:long`
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.shipment_id` → `shipment_id:string`
  - `$.payload.departure_time` → `departure_time:datetime`
  - `$.payload.actual_unload_duration` → `actual_unload_duration:long`

- `store_opened` / `store_closed`
  - `$.payload.store_id` → `store_id:long`
  - `$.payload.operation_time` → `operation_time:datetime`
  - `$.payload.operation_type` → `operation_type:string`

- `ad_impression`
  - `$.payload.channel` → `channel:string`
  - `$.payload.campaign_id` → `campaign_id:string`
  - `$.payload.creative_id` → `creative_id:string`
  - `$.payload.customer_ad_id` → `customer_ad_id:string`
  - `$.payload.impression_id` → `impression_id:string`
  - `$.payload.cost` → `cost:real`
  - `$.payload.device_type` → `device_type:string`

- `promotion_applied`
  - `$.payload.receipt_id` → `receipt_id:string`
  - `$.payload.promo_code` → `promo_code:string`
  - `$.payload.discount_amount` → `discount_amount:real`
  - `$.payload.discount_type` → `discount_type:string`
  - `$.payload.product_ids` → `product_ids:dynamic`

- `online_order_created`
  - `$.payload.order_id` → `order_id:string`
  - `$.payload.customer_id` → `customer_id:long`
  - `$.payload.fulfillment_mode` → `fulfillment_mode:string`
  - `$.payload.node_type` → `node_type:string`
  - `$.payload.node_id` → `node_id:long`
  - `$.payload.item_count` → `item_count:long`
  - `$.payload.subtotal` → `subtotal:real`
  - `$.payload.tax` → `tax:real`
  - `$.payload.total` → `total:real`
  - `$.payload.tender_type` → `tender_type:string`

- `online_order_picked`
  - `$.payload.order_id` → `order_id:string`
  - `$.payload.node_type` → `node_type:string`
  - `$.payload.node_id` → `node_id:long`
  - `$.payload.fulfillment_mode` → `fulfillment_mode:string`
  - `$.payload.picked_time` → `picked_time:datetime`

- `online_order_shipped`
  - `$.payload.order_id` → `order_id:string`
  - `$.payload.node_type` → `node_type:string`
  - `$.payload.node_id` → `node_id:long`
  - `$.payload.fulfillment_mode` → `fulfillment_mode:string`
  - `$.payload.shipped_time` → `shipped_time:datetime`

Lakehouse Bronze:
- Route original JSON (no projection) partitioned by `event_type` and date (`YYYY/MM/DD`).
- Folder pattern: `/Tables/bronze/events/event_type=<type>/date=<yyyy-mm-dd>/`.

Constraints:
- All columns optional at mapping time to tolerate schema evolution; validation occurs in KQL and Silver transforms.
- Reject events missing required payload fields as per datagen models if Fabric supports validation stage; otherwise route to DLQ sink.

