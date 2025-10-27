-- Silver Views — unified receipt header/detail for analysis
-- These are logical views over Silver facts created by notebooks.

CREATE OR REPLACE VIEW silver_receipt_header AS
SELECT r.trace_id,
       r.event_ts,
       r.store_id,
       r.customer_id,
       r.receipt_id,
       r.subtotal,
       r.tax,
       r.total,
       r.tender_type,
       COALESCE(p.amount, 0.0) AS paid_amount,
       p.payment_method,
       p.status AS payment_status,
       promo.total_discount_amount,
       (r.total - COALESCE(promo.total_discount_amount, 0.0)) AS total_after_discounts
FROM delta.`/Tables/silver/receipts` r
LEFT JOIN (
  SELECT receipt_id,
         MAX_BY(struct(event_ts, receipt_id, payment_method, amount, transaction_id, processing_time, status), processing_time).amount AS amount,
         MAX_BY(struct(event_ts, receipt_id, payment_method, amount, transaction_id, processing_time, status), processing_time).payment_method AS payment_method,
         MAX_BY(struct(event_ts, receipt_id, payment_method, amount, transaction_id, processing_time, status), processing_time).status AS status
  FROM delta.`/Tables/silver/payments`
  GROUP BY receipt_id
) p ON p.receipt_id = r.receipt_id
LEFT JOIN (
  SELECT receipt_id, SUM(discount_amount) AS total_discount_amount
  FROM delta.`/Tables/silver/promotions`
  GROUP BY receipt_id
) promo ON promo.receipt_id = r.receipt_id;


CREATE OR REPLACE VIEW silver_receipt_detail AS
SELECT rl.trace_id,
       rl.event_ts,
       rl.receipt_id,
       rl.line,
       rl.product_id,
       rl.qty,
       rl.unit_price,
       rl.ext_price,
       rl.promo_code,
       pl.discount_amount AS line_discount,
       (rl.ext_price - COALESCE(pl.discount_amount, 0.0)) AS net_ext_price
FROM delta.`/Tables/silver/receipt_lines` rl
LEFT JOIN (
  SELECT receipt_id, product_id, SUM(discount_amount) AS discount_amount
  FROM delta.`/Tables/silver/promo_lines`
  GROUP BY receipt_id, product_id
) pl ON pl.receipt_id = rl.receipt_id AND pl.product_id = rl.product_id;

