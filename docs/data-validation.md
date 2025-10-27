# Data Validation (Quick Checks)

Use these KQL snippets to validate end-to-end correctness after deployment.

Envelope mapping
- Any null event_type? `T | where isnull(event_type) | count`
- Recent ingest: `receipt_created | where ingest_timestamp > ago(5m) | count`

Schema compliance
- Missing required payloads: `receipt_created | where isnull(store_id) or isnull(total) | take 5`
- Type sanity: `receipt_line_added | summarize min_q=min(quantity), max_q=max(quantity)`

Cross-table consistency
- Receipts with lines: `receipt_line_added | summarize lines=count() by receipt_id | top 5 by lines desc`
- Payments without receipts: `payment_processed | join kind=leftanti (receipt_created) on receipt_id | take 5`

Performance
- Hot tiles under 2s: `set query_datascope=hotcache; mv_store_sales_minute | where ts > ago(10m) | count`
- Ingestion lag estimate (Event Hubs to KQL): `receipt_created | extend lag_ms = datetime_diff('millisecond', ingest_timestamp, now()) | summarize avg(lag_ms)`

Lakehouse
- Bronze partitions: Verify `/Tables/bronze/events/event_type=*/date=*` exist
- Silver row counts: Compare receipts vs receipt_lines totals

