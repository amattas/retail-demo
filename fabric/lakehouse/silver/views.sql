-- Silver Views — unified receipt header/detail for analysis
-- These are logical views over Silver facts created by notebooks.

CREATE OR REPLACE VIEW silver_receipt_header AS
SELECT 
       r.event_ts,
       r.store_id,
       r.customer_id,
       r.receipt_id_ext AS receipt_id,
       r.subtotal_cents / 100.0 AS subtotal,
       r.tax_cents / 100.0      AS tax,
       r.total_cents / 100.0    AS total,
       r.payment_method,
       r.receipt_type,
       COALESCE(p.amount, 0.0) AS paid_amount,
       p.status AS payment_status,
       promo.total_discount_amount,
       (r.total_cents / 100.0 - COALESCE(promo.total_discount_amount, 0.0)) AS total_after_discounts
FROM delta.`/Tables/silver/receipts` r
LEFT JOIN (
  SELECT receipt_id,
         MAX_BY(struct(event_ts, receipt_id, payment_method, amount, transaction_id, processing_time, status), processing_time).amount AS amount,
         MAX_BY(struct(event_ts, receipt_id, payment_method, amount, transaction_id, processing_time, status), processing_time).payment_method AS payment_method,
         MAX_BY(struct(event_ts, receipt_id, payment_method, amount, transaction_id, processing_time, status), processing_time).status AS status
  FROM delta.`/Tables/silver/payments`
  GROUP BY receipt_id
) p ON p.receipt_id = r.receipt_id_ext
LEFT JOIN (
  SELECT receipt_id, SUM(discount_amount) AS total_discount_amount
  FROM delta.`/Tables/silver/promotions`
  GROUP BY receipt_id
) promo ON promo.receipt_id = r.receipt_id_ext;


CREATE OR REPLACE VIEW silver_receipt_detail AS
SELECT 
       rl.event_ts,
       rl.receipt_id_ext AS receipt_id,
       rl.line_num,
       rl.product_id,
       rl.quantity,
       rl.unit_cents / 100.0 AS unit_price,
       rl.ext_cents  / 100.0 AS ext_price,
       rl.promo_code,
       pl.discount_amount AS line_discount,
       (rl.ext_cents / 100.0 - COALESCE(pl.discount_amount, 0.0)) AS net_ext_price
FROM delta.`/Tables/silver/receipt_lines` rl
LEFT JOIN (
  SELECT receipt_id, product_id, SUM(discount_amount) AS discount_amount
  FROM delta.`/Tables/silver/promo_lines`
  GROUP BY receipt_id, product_id
) pl ON pl.receipt_id = rl.receipt_id_ext AND pl.product_id = rl.product_id;
