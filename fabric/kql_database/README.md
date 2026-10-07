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

Core KPI materialized views use deterministic aggregation queries plus
seven-day materialized-view retention policies. Current pending pricing state
is exposed by `fn_pending_recommendations()` because it must reconcile creation,
approval, and rejection event tables at query time.

The live stream targets eighteen typed business event tables. `unknown_event`
is a catch-all KQL table, not a nineteenth generated business event.

See the [event contract](../../docs/design/specifications/modules/streaming/event-contract.md),
[Fabric analytics specification](../../docs/design/specifications/modules/analytics/fabric-analytics.md),
and [data schema](../../docs/design/architecture/data-schema.md).
