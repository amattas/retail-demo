# Lakehouse notebooks

This directory contains retained medallion-flow notebooks, streaming
Silver/Gold transforms, maintenance, ML, ontology, and administrative utilities.

The supported historical bootstrap is rendered from `utility/` and runs:

1. `setup-01-seed-dictionaries`
2. `setup-02-generate-dimensions`
3. `setup-03-generate-facts`
4. `setup-04-build-gold`

Those notebooks write the base contract directly to Lakehouse Silver (`ag`) and
Gold (`au`).

Notable groups in this directory:

- `03-streaming-to-silver` and `04-streaming-to-gold`: optional Eventhouse to
  Lakehouse projection
- `05-maintain-delta-tables`: maintenance
- `06` through `14`: ML and advanced analytics
- `30-create-ontology`: ontology creation and Eventhouse TimeSeries bindings
- `90` and `99`: manual augmentation/reset utilities

See the [historical data contract](../../docs/design/specifications/modules/generation/data-contract.md),
[Fabric analytics specification](../../docs/design/specifications/modules/analytics/fabric-analytics.md),
and [data flow](../../docs/design/architecture/data-flow.md).
