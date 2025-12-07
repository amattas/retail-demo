-- Silver Delta tables aligned to datagen historical facts
-- Storage layout: /Tables/silver/<table_name>

-- Receipts
CREATE TABLE IF NOT EXISTS silver_receipts
USING DELTA
LOCATION '/Tables/silver/receipts'
AS SELECT
          -- canonical fields from DuckDB fact_receipts
          CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING)    AS receipt_id_ext,
          CAST(NULL AS STRING)    AS payment_method,
          CAST(NULL AS STRING)    AS discount_amount,
          CAST(NULL AS BIGINT)    AS tax_cents,
          CAST(NULL AS STRING)    AS subtotal,
          CAST(NULL AS STRING)    AS total,
          CAST(NULL AS BIGINT)    AS total_cents,
          CAST(NULL AS STRING)    AS receipt_type,
          CAST(NULL AS BIGINT)    AS subtotal_cents,
          CAST(NULL AS STRING)    AS tax,
          CAST(NULL AS BIGINT)    AS customer_id,
          CAST(NULL AS BIGINT)    AS store_id,
          CAST(NULL AS STRING)    AS return_for_receipt_id_ext
WHERE 1=0;

-- Receipt lines
CREATE TABLE IF NOT EXISTS silver_receipt_lines
USING DELTA
LOCATION '/Tables/silver/receipt_lines'
AS SELECT 
          -- canonical fields from DuckDB fact_receipt_lines
          CAST(NULL AS BIGINT)    AS unit_cents,
          CAST(NULL AS STRING)    AS unit_price,
          CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT)    AS product_id,
          CAST(NULL AS BIGINT)    AS quantity,
          CAST(NULL AS STRING)    AS ext_price,
          CAST(NULL AS BIGINT)    AS line_num,
          CAST(NULL AS STRING)    AS promo_code,
          CAST(NULL AS BIGINT)    AS ext_cents,
          CAST(NULL AS STRING)    AS receipt_id_ext
WHERE 1=0;

-- Store inventory transactions
CREATE TABLE IF NOT EXISTS silver_store_inventory_txn
USING DELTA
LOCATION '/Tables/silver/store_inventory_txn'
AS SELECT 
          -- canonical fields from DuckDB fact_store_inventory_txn
          CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT)    AS product_id,
          CAST(NULL AS STRING)    AS txn_type,
          CAST(NULL AS BIGINT)    AS quantity,
          CAST(NULL AS STRING)    AS source,
          CAST(NULL AS BIGINT)    AS store_id,
          CAST(NULL AS BIGINT)    AS balance
WHERE 1=0;

-- DC inventory transactions
CREATE TABLE IF NOT EXISTS silver_dc_inventory_txn
USING DELTA
LOCATION '/Tables/silver/dc_inventory_txn'
AS SELECT
          -- canonical fields from DuckDB fact_dc_inventory_txn
          CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT)    AS product_id,
          CAST(NULL AS STRING)    AS txn_type,
          CAST(NULL AS BIGINT)    AS quantity,
          CAST(NULL AS BIGINT)    AS dc_id,
          CAST(NULL AS BIGINT)    AS balance,
          CAST(NULL AS STRING)    AS source
WHERE 1=0;

-- Foot traffic
CREATE TABLE IF NOT EXISTS silver_foot_traffic
USING DELTA
LOCATION '/Tables/silver/foot_traffic'
AS SELECT 
          -- canonical fields from DuckDB fact_foot_traffic
          CAST(NULL AS BIGINT)    AS count,
          CAST(NULL AS STRING)    AS zone,
          CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING)    AS sensor_id,
          CAST(NULL AS BIGINT)    AS dwell_seconds,
          CAST(NULL AS BIGINT)    AS store_id
WHERE 1=0;

-- BLE pings
CREATE TABLE IF NOT EXISTS silver_ble_pings
USING DELTA
LOCATION '/Tables/silver/ble_pings'
AS SELECT
          -- canonical fields from DuckDB fact_ble_pings
          CAST(NULL AS STRING)    AS zone,
          CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT)    AS rssi,
          CAST(NULL AS STRING)    AS customer_ble_id,
          CAST(NULL AS BIGINT)    AS customer_id,
          CAST(NULL AS BIGINT)    AS store_id,
          CAST(NULL AS STRING)    AS beacon_id
WHERE 1=0;

-- Marketing
CREATE TABLE IF NOT EXISTS silver_marketing
USING DELTA
LOCATION '/Tables/silver/marketing'
AS SELECT
          -- canonical fields from DuckDB fact_marketing
          CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING)    AS campaign_id,
          CAST(NULL AS STRING)    AS device,
          CAST(NULL AS STRING)    AS creative_id,
          CAST(NULL AS STRING)    AS customer_ad_id,
          CAST(NULL AS STRING)    AS impression_id_ext,
          CAST(NULL AS STRING)    AS cost,
          CAST(NULL AS BIGINT)    AS cost_cents,
          CAST(NULL AS BIGINT)    AS customer_id,
          CAST(NULL AS STRING)    AS channel
WHERE 1=0;

-- Promotions applied
CREATE TABLE IF NOT EXISTS silver_promotions
USING DELTA
LOCATION '/Tables/silver/promotions'
AS SELECT CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS receipt_id, CAST(NULL AS STRING) AS promo_code,
          CAST(NULL AS DOUBLE) AS discount_amount, CAST(NULL AS STRING) AS discount_type,
          CAST(NULL AS STRING) AS product_ids_json
WHERE 1=0;

-- Promotion lines (one row per product in the promo)
CREATE TABLE IF NOT EXISTS silver_promo_lines
USING DELTA
LOCATION '/Tables/silver/promo_lines'
AS SELECT CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS receipt_id, CAST(NULL AS STRING) AS promo_code,
          CAST(NULL AS BIGINT) AS product_id, CAST(NULL AS DOUBLE) AS discount_amount,
          CAST(NULL AS STRING) AS discount_type
WHERE 1=0;

-- Online orders (DuckDB split into headers and lines)
CREATE TABLE IF NOT EXISTS silver_online_order_headers
USING DELTA
LOCATION '/Tables/silver/online_order_headers'
AS SELECT
          CAST(NULL AS TIMESTAMP) AS completed_ts,
          CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING)    AS order_id_ext,
          CAST(NULL AS BIGINT)    AS tax_cents,
          CAST(NULL AS STRING)    AS subtotal,
          CAST(NULL AS STRING)    AS total,
          CAST(NULL AS BIGINT)    AS total_cents,
          CAST(NULL AS BIGINT)    AS subtotal_cents,
          CAST(NULL AS STRING)    AS tax,
          CAST(NULL AS BIGINT)    AS customer_id,
          CAST(NULL AS STRING)    AS payment_method
WHERE 1=0;

CREATE TABLE IF NOT EXISTS silver_online_order_lines
USING DELTA
LOCATION '/Tables/silver/online_order_lines'
AS SELECT 
          CAST(NULL AS BIGINT)    AS unit_cents,
          CAST(NULL AS TIMESTAMP) AS shipped_ts,
          CAST(NULL AS STRING)    AS unit_price,
          CAST(NULL AS STRING)    AS fulfillment_status,
          CAST(NULL AS STRING)    AS order_id,
          CAST(NULL AS TIMESTAMP) AS delivered_ts,
          CAST(NULL AS BIGINT)    AS product_id,
          CAST(NULL AS BIGINT)    AS quantity,
          CAST(NULL AS STRING)    AS ext_price,
          CAST(NULL AS STRING)    AS node_type,
          CAST(NULL AS STRING)    AS fulfillment_mode,
          CAST(NULL AS TIMESTAMP) AS picked_ts,
          CAST(NULL AS BIGINT)    AS node_id,
          CAST(NULL AS BIGINT)    AS line_num,
          CAST(NULL AS STRING)    AS promo_code,
          CAST(NULL AS BIGINT)    AS ext_cents
WHERE 1=0;

-- Truck moves (derived)
CREATE TABLE IF NOT EXISTS silver_truck_moves
USING DELTA
LOCATION '/Tables/silver/truck_moves'
AS SELECT CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS truck_id, CAST(NULL AS BIGINT) AS dc_id,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS STRING) AS shipment_id,
          CAST(NULL AS STRING) AS status, CAST(NULL AS TIMESTAMP) AS eta,
          CAST(NULL AS TIMESTAMP) AS etd
WHERE 1=0;

-- Stockouts detected
CREATE TABLE IF NOT EXISTS silver_stockouts
USING DELTA
LOCATION '/Tables/silver/stockouts'
AS SELECT CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS BIGINT) AS dc_id,
          CAST(NULL AS BIGINT) AS product_id, CAST(NULL AS INT) AS last_known_quantity,
          CAST(NULL AS TIMESTAMP) AS detection_time
WHERE 1=0;

-- Reorders triggered
CREATE TABLE IF NOT EXISTS silver_reorders
USING DELTA
LOCATION '/Tables/silver/reorders'
AS SELECT CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS BIGINT) AS dc_id,
          CAST(NULL AS BIGINT) AS product_id, CAST(NULL AS INT) AS current_quantity,
          CAST(NULL AS INT) AS reorder_quantity, CAST(NULL AS INT) AS reorder_point,
          CAST(NULL AS STRING) AS priority
WHERE 1=0;

-- Payments processed
CREATE TABLE IF NOT EXISTS silver_payments
USING DELTA
LOCATION '/Tables/silver/payments'
AS SELECT CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS STRING) AS receipt_id, CAST(NULL AS STRING) AS payment_method,
          CAST(NULL AS DOUBLE) AS amount, CAST(NULL AS STRING) AS transaction_id,
          CAST(NULL AS TIMESTAMP) AS processing_time, CAST(NULL AS STRING) AS status
WHERE 1=0;

-- Store operations (opened/closed)
CREATE TABLE IF NOT EXISTS silver_store_ops
USING DELTA
LOCATION '/Tables/silver/store_ops'
AS SELECT CAST(NULL AS TIMESTAMP) AS event_ts,
          CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS TIMESTAMP) AS operation_time,
          CAST(NULL AS STRING) AS operation_type
WHERE 1=0;
