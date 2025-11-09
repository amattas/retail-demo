-- Pricing invariants
SELECT 
  SUM(CASE WHEN Cost >= SalePrice THEN 1 ELSE 0 END) AS cost_ge_sale,
  SUM(CASE WHEN SalePrice > MSRP THEN 1 ELSE 0 END) AS sale_gt_msrp,
  SUM(CASE WHEN Cost/SalePrice < 0.50 OR Cost/SalePrice > 0.85 THEN 1 ELSE 0 END) AS ratio_out,
  COUNT(*) AS total
FROM dim_products;

-- Taxability distribution
SELECT taxability, COUNT(*) FROM dim_products GROUP BY 1 ORDER BY 2 DESC;

-- Store tax rate sanity
SELECT MIN(tax_rate) AS min_rate, MAX(tax_rate) AS max_rate, AVG(tax_rate) AS avg_rate FROM dim_stores;

-- Basket size by store_format
WITH per_rc AS (
  SELECT r.store_id, l.receipt_id_ext, COUNT(*) AS lines
  FROM fact_receipt_lines l JOIN fact_receipts r USING (receipt_id_ext)
  GROUP BY 1,2
)
SELECT s.store_format, AVG(lines) AS avg_lines FROM per_rc p JOIN dim_stores s ON s.ID=p.store_id GROUP BY 1 ORDER BY 1;

-- Returns present
SELECT receipt_type, COUNT(*) FROM fact_receipts GROUP BY 1 ORDER BY 2 DESC;

-- Inventory txn types
SELECT txn_type, COUNT(*) FROM fact_store_inventory_txn GROUP BY 1 ORDER BY 2 DESC;
SELECT txn_type, COUNT(*) FROM fact_dc_inventory_txn GROUP BY 1 ORDER BY 2 DESC;

-- Fulfillment mix
SELECT fulfillment_mode, COUNT(*) FROM fact_online_order_lines GROUP BY 1 ORDER BY 2 DESC;

-- Promo usage
SELECT 
  SUM(promo_code IS NOT NULL)::DOUBLE/COUNT(*) AS promo_rate_pos
FROM fact_receipt_lines;
SELECT 
  SUM(promo_code IS NOT NULL)::DOUBLE/COUNT(*) AS promo_rate_online
FROM fact_online_order_lines;

-- Cancellation rate (lines)
SELECT SUM(fulfillment_status='CANCELLED')::DOUBLE/COUNT(*) AS cancelled_rate FROM fact_online_order_lines;

-- Device multipliers sample
WITH base AS (
  SELECT channel, device, AVG(CAST(cost AS DOUBLE)) AS avg_cost FROM fact_marketing GROUP BY 1,2
)
SELECT channel,
  MAX(CASE WHEN device='MOBILE' THEN avg_cost END) AS mobile,
  MAX(CASE WHEN device='TABLET' THEN avg_cost END) AS tablet,
  MAX(CASE WHEN device='DESKTOP' THEN avg_cost END) AS desktop
FROM base GROUP BY 1 ORDER BY 1;

-- Foot traffic zone distribution
SELECT zone, SUM(count) AS total FROM fact_foot_traffic GROUP BY 1 ORDER BY 2 DESC;

-- BLE linkage rate
SELECT SUM(customerid IS NOT NULL)::DOUBLE/COUNT(*) AS known_ble_ratio FROM fact_ble_pings;

