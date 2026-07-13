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

`retail-setup deploy` combines them into an
`.execute database script with (ThrowOnErrors=true)` payload and applies it with
the operator identity. The generated payload remains at
`deploy\.generated\<env>\database.kql`.

The live stream targets eighteen typed business event tables. `unknown_event`
is a catch-all KQL table, not a nineteenth generated business event.

See the [event contract](../../docs/design/specifications/modules/streaming/event-contract.md),
[Fabric analytics specification](../../docs/design/specifications/modules/analytics/fabric-analytics.md),
and [data schema](../../docs/design/architecture/data-schema.md).
