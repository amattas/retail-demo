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

## Retained notebooks

These two notebooks remain for older or specialized flows. They are not the
current historical bootstrap:

- `01-create-bronze-shortcuts.ipynb` creates Lakehouse shortcuts to Eventhouse
  tables for the optional live projection path.
- `02-historical-data-load.ipynb` is the retained legacy historical loader.
  The supported path now uses setup notebooks 01 through 04 rendered from
  `utility/`.

Do not run the retained historical loader as a substitute for
`retail-setup deploy` or the ordered setup notebooks. The current path includes
run logging, staged validation, rollback behavior, and profile-aware
publication that the legacy notebook does not provide.

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
