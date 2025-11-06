-- Fabric SQL view: Unified Orders (POS + Online)
-- Use this view to analyze all orders in one place.

CREATE OR ALTER VIEW vw_orders AS
SELECT
  r.event_ts,
  r.receipt_id      AS doc_id,
  'POS'             AS channel,
  r.store_id        AS node_id,
  rl.product_id,
  rl.quantity       AS qty,
  rl.unit_price,
  rl.ext_price,
  r.total_amount    AS doc_total,
  r.payment_method  AS payment_or_status,
  r.receipt_type    AS receipt_type,
  r.return_for_receipt_id AS return_for_id
FROM fact_receipts r
JOIN fact_receipt_lines rl ON rl.receipt_id = r.receipt_id

UNION ALL

SELECT
  o.event_ts,
  o.order_id        AS doc_id,
  'ONLINE'          AS channel,
  CASE WHEN o.node_type = 'STORE' THEN o.node_id ELSE NULL END AS node_id,
  ol.product_id,
  ol.quantity       AS qty,
  ol.unit_price,
  ol.ext_price,
  o.total_amount    AS doc_total,
  o.fulfillment_status AS payment_or_status,
  'SALE'            AS receipt_type,
  NULL              AS return_for_id
FROM fact_online_orders o
JOIN fact_online_order_lines ol ON ol.order_id = o.order_id
WHERE o.fulfillment_status = 'created';

