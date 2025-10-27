-- Silver Delta tables aligned to datagen historical facts
-- Storage layout: /Tables/silver/<table_name>

-- Receipts
CREATE TABLE IF NOT EXISTS silver_receipts
USING DELTA
LOCATION '/Tables/silver/receipts'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS BIGINT) AS customer_id,
          CAST(NULL AS STRING) AS receipt_id, CAST(NULL AS DOUBLE) AS subtotal,
          CAST(NULL AS DOUBLE) AS tax, CAST(NULL AS DOUBLE) AS total,
          CAST(NULL AS STRING) AS tender_type
WHERE 1=0;

-- Receipt lines
CREATE TABLE IF NOT EXISTS silver_receipt_lines
USING DELTA
LOCATION '/Tables/silver/receipt_lines'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS receipt_id, CAST(NULL AS INT) AS line,
          CAST(NULL AS BIGINT) AS product_id, CAST(NULL AS INT) AS qty,
          CAST(NULL AS DOUBLE) AS unit_price, CAST(NULL AS DOUBLE) AS ext_price,
          CAST(NULL AS STRING) AS promo_code
WHERE 1=0;

-- Store inventory transactions
CREATE TABLE IF NOT EXISTS silver_store_inventory_txn
USING DELTA
LOCATION '/Tables/silver/store_inventory_txn'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS BIGINT) AS product_id,
          CAST(NULL AS INT) AS qty_delta, CAST(NULL AS STRING) AS reason,
          CAST(NULL AS STRING) AS source
WHERE 1=0;

-- DC inventory transactions
CREATE TABLE IF NOT EXISTS silver_dc_inventory_txn
USING DELTA
LOCATION '/Tables/silver/dc_inventory_txn'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS dc_id, CAST(NULL AS BIGINT) AS product_id,
          CAST(NULL AS INT) AS qty_delta, CAST(NULL AS STRING) AS reason
WHERE 1=0;

-- Foot traffic
CREATE TABLE IF NOT EXISTS silver_foot_traffic
USING DELTA
LOCATION '/Tables/silver/foot_traffic'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS STRING) AS sensor_id,
          CAST(NULL AS STRING) AS zone, CAST(NULL AS INT) AS dwell,
          CAST(NULL AS INT) AS count
WHERE 1=0;

-- BLE pings
CREATE TABLE IF NOT EXISTS silver_ble_pings
USING DELTA
LOCATION '/Tables/silver/ble_pings'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS STRING) AS beacon_id,
          CAST(NULL AS STRING) AS customer_ble_id, CAST(NULL AS INT) AS rssi,
          CAST(NULL AS STRING) AS zone
WHERE 1=0;

-- Marketing
CREATE TABLE IF NOT EXISTS silver_marketing
USING DELTA
LOCATION '/Tables/silver/marketing'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS channel, CAST(NULL AS STRING) AS campaign_id,
          CAST(NULL AS STRING) AS creative_id, CAST(NULL AS STRING) AS customer_ad_id,
          CAST(NULL AS STRING) AS impression_id, CAST(NULL AS DOUBLE) AS cost,
          CAST(NULL AS STRING) AS device
WHERE 1=0;

-- Promotions applied
CREATE TABLE IF NOT EXISTS silver_promotions
USING DELTA
LOCATION '/Tables/silver/promotions'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS receipt_id, CAST(NULL AS STRING) AS promo_code,
          CAST(NULL AS DOUBLE) AS discount_amount, CAST(NULL AS STRING) AS discount_type,
          CAST(NULL AS STRING) AS product_ids_json
WHERE 1=0;

-- Promotion lines (one row per product in the promo)
CREATE TABLE IF NOT EXISTS silver_promo_lines
USING DELTA
LOCATION '/Tables/silver/promo_lines'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS receipt_id, CAST(NULL AS STRING) AS promo_code,
          CAST(NULL AS BIGINT) AS product_id, CAST(NULL AS DOUBLE) AS discount_amount,
          CAST(NULL AS STRING) AS discount_type
WHERE 1=0;

-- Online orders
CREATE TABLE IF NOT EXISTS silver_online_orders
USING DELTA
LOCATION '/Tables/silver/online_orders'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS order_id, CAST(NULL AS BIGINT) AS customer_id,
          CAST(NULL AS STRING) AS fulfillment_mode,
          CAST(NULL AS STRING) AS fulfillment_node_type,
          CAST(NULL AS BIGINT) AS fulfillment_node_id,
          CAST(NULL AS INT) AS item_count, CAST(NULL AS DOUBLE) AS subtotal,
          CAST(NULL AS DOUBLE) AS tax, CAST(NULL AS DOUBLE) AS total,
          CAST(NULL AS STRING) AS tender_type
WHERE 1=0;

-- Truck moves (derived)
CREATE TABLE IF NOT EXISTS silver_truck_moves
USING DELTA
LOCATION '/Tables/silver/truck_moves'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS truck_id, CAST(NULL AS BIGINT) AS dc_id,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS STRING) AS shipment_id,
          CAST(NULL AS STRING) AS status, CAST(NULL AS TIMESTAMP) AS eta,
          CAST(NULL AS TIMESTAMP) AS etd, CAST(NULL AS TIMESTAMP) AS arrival_time,
          CAST(NULL AS TIMESTAMP) AS departure_time
WHERE 1=0;

-- Stockouts detected
CREATE TABLE IF NOT EXISTS silver_stockouts
USING DELTA
LOCATION '/Tables/silver/stockouts'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS BIGINT) AS dc_id,
          CAST(NULL AS BIGINT) AS product_id, CAST(NULL AS INT) AS last_known_quantity,
          CAST(NULL AS TIMESTAMP) AS detection_time
WHERE 1=0;

-- Reorders triggered
CREATE TABLE IF NOT EXISTS silver_reorders
USING DELTA
LOCATION '/Tables/silver/reorders'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS BIGINT) AS dc_id,
          CAST(NULL AS BIGINT) AS product_id, CAST(NULL AS INT) AS current_quantity,
          CAST(NULL AS INT) AS reorder_quantity, CAST(NULL AS INT) AS reorder_point,
          CAST(NULL AS STRING) AS priority
WHERE 1=0;

-- Payments processed
CREATE TABLE IF NOT EXISTS silver_payments
USING DELTA
LOCATION '/Tables/silver/payments'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS receipt_id, CAST(NULL AS STRING) AS payment_method,
          CAST(NULL AS DOUBLE) AS amount, CAST(NULL AS STRING) AS transaction_id,
          CAST(NULL AS TIMESTAMP) AS processing_time, CAST(NULL AS STRING) AS status
WHERE 1=0;

-- Store operations (opened/closed)
CREATE TABLE IF NOT EXISTS silver_store_ops
USING DELTA
LOCATION '/Tables/silver/store_ops'
AS SELECT CAST(NULL AS STRING) AS trace_id, CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS TIMESTAMP) AS operation_time,
          CAST(NULL AS STRING) AS operation_type
WHERE 1=0;
