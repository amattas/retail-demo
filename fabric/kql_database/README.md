# Eventhouse KQL database

The ordered scripts in this directory define Eventhouse tables, mappings,
functions, materialized views, anomaly assets, and pricing-approval tables.

Default order:

1. `01-create-tables.kql`
2. `02-create-ingestion-mappings.kql`
3. `03-create-functions.kql`
4. `04-create-materialized-views.kql`
5. `06-ml-anomaly-detection.kql`
6. `07-pricing-approval-tables.kql`

Each file keeps a standalone
`.execute database script with (ThrowOnErrors=true)` wrapper.
`retail-setup deploy` removes exactly that leading wrapper from each selected
file, rejects any embedded wrapper, and combines the commands into one flat
fail-fast payload applied with the operator identity. The generated payload
remains at `deploy\.generated\<env>\database.kql`.

Producers for the derived tables:

- `anomaly_alerts`: `fabric/lakehouse/16-persist-anomaly-alerts.ipynb`, in
  the `streaming-data-load` pipeline, reads `fn_new_anomaly_alerts()` (new
  alerts for closed hourly bins whose deterministic `alert_id` is not yet
  persisted) and appends them. Each run, including one with no anomaly, adds a
  row to `anomaly_detection_runs`; readiness uses that table, not the alert
  count, as detection evidence.
- `pricing_recommendation_created`:
  `fabric/lakehouse/14-ml-dynamic-pricing.ipynb` appends one `PENDING` event
  per recommended price change, with a deterministic `recommendation_id`
  (product, date, price) and no duplicate on rerun. Approval and rejection
  events still come from a future approval UI or a manual process.

Core KPI materialized views use deterministic aggregation queries plus
seven-day materialized-view retention policies. Current pending pricing state
is exposed by `fn_pending_recommendations()` because it must reconcile creation,
approval, and rejection event tables at query time.

The live stream declares twenty-two typed business event tables and emits
twenty of them today. `logistics_incident_started` and
`logistics_incident_resolved` are declared for the logistics incident model;
they follow the same retention and extent-tag retention policies as their
operational peers. `inventory_updated.requested_quantity` carries the demand of
a sale or lost sale; `01-create-tables.kql` restates the table with
`.alter table` so the column sits before the envelope. `unknown_event`
is a catch-all KQL table, not a generated business event.

Every stream event table drops `ingest-by:` extent tags after seven days
(`extent_tags_retention`), so the per-batch idempotency tags don't accumulate
for the full table retention. The event tables set no streaming-ingestion or
ingestion-batching policy: the stream's Spark connector writes are
transactional queued ingestion with `flushImmediately`, so those policies were
never used. Only `unknown_event` keeps them, for queued ingestion.

The Spark Kusto writes are positional, so a new column in an existing event
table must keep the payload-then-envelope order. `01-create-tables.kql`
restates `truck_arrived` and `ad_impression` with `.alter table` after their
`.create-merge` for that reason. `ad_impression.impression_time` is when an
impression was shown; journey touches arrive (`ingest_timestamp`) with their
purchase, so the marketing functions use
`coalesce(impression_time, ingest_timestamp)` as the touch time.

Thresholds in `06-ml-anomaly-detection.kql` (detector threshold, seasonality,
trend, severity bands, lookback) must equal
`utility/src/retail_setup/generation/calibration.py`;
`utility/tests/generation/test_calibration.py` checks them.

See the [event contract](../../docs/design/specifications/modules/streaming/event-contract.md),
[Fabric analytics specification](../../docs/design/specifications/modules/analytics/fabric-analytics.md),
and [data schema](../../docs/design/architecture/data-schema.md).
