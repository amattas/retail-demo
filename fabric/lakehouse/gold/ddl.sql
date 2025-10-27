-- Gold Delta aggregates built from Silver
-- Storage layout: /Tables/gold/<table_name>

-- Sales by minute per store
CREATE TABLE IF NOT EXISTS gold_sales_minute_store
USING DELTA
LOCATION '/Tables/gold/sales_minute_store'
AS SELECT CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS TIMESTAMP) AS ts,
          CAST(NULL AS DOUBLE) AS total_sales, CAST(NULL AS BIGINT) AS receipts,
          CAST(NULL AS DOUBLE) AS avg_basket
WHERE 1=0;

-- Top products (rolling 15m)
CREATE TABLE IF NOT EXISTS gold_top_products_15m
USING DELTA
LOCATION '/Tables/gold/top_products_15m'
AS SELECT CAST(NULL AS BIGINT) AS product_id, CAST(NULL AS DOUBLE) AS revenue,
          CAST(NULL AS BIGINT) AS units, CAST(NULL AS TIMESTAMP) AS computed_at
WHERE 1=0;

-- Inventory position current (store-product)
CREATE TABLE IF NOT EXISTS gold_inventory_position_current
USING DELTA
LOCATION '/Tables/gold/inventory_position_current'
AS SELECT CAST(NULL AS BIGINT) AS store_id, CAST(NULL AS BIGINT) AS product_id,
          CAST(NULL AS INT) AS on_hand, CAST(NULL AS TIMESTAMP) AS as_of
WHERE 1=0;

-- Truck dwell (daily)
CREATE TABLE IF NOT EXISTS gold_truck_dwell_daily
USING DELTA
LOCATION '/Tables/gold/truck_dwell_daily'
AS SELECT CAST(NULL AS STRING) AS site, CAST(NULL AS DATE) AS day,
          CAST(NULL AS DOUBLE) AS avg_dwell_min, CAST(NULL AS BIGINT) AS trucks
WHERE 1=0;

-- Campaign revenue (daily)
CREATE TABLE IF NOT EXISTS gold_campaign_revenue_daily
USING DELTA
LOCATION '/Tables/gold/campaign_revenue_daily'
AS SELECT CAST(NULL AS STRING) AS campaign_id, CAST(NULL AS DATE) AS day,
          CAST(NULL AS DOUBLE) AS impressions, CAST(NULL AS BIGINT) AS conversions,
          CAST(NULL AS DOUBLE) AS revenue
WHERE 1=0;

