# Lakehouse notebooks

This directory contains retained medallion-flow notebooks, streaming
Silver/Gold transforms, maintenance, ML, ontology, and administrative utilities.

The supported historical bootstrap is rendered from `utility/` and runs:

1. `setup-01-seed-dictionaries`
2. `setup-02-generate-dimensions`
3. `setup-03-generate-sales`
4. `setup-04-generate-online-orders`
5. `setup-05-generate-marketing`
6. `setup-06-generate-returns-and-promotions`
7. `setup-07-generate-store-activity`
8. `setup-08-generate-inventory`
9. `setup-09-attribute-marketing`
10. `setup-10-publish-silver`
11. `setup-11-build-gold`

In `setup-pipeline`, 02 (dimension validation), 03, 04, and 05 run in parallel after 01; 06, 07, and 08 run in parallel after 03; 09 waits for 03 to 06. Each notebook runs its own stages concurrently and hands outputs to later notebooks as `gen__*` tables in the Silver staging schema. Setup 10 checks invariants and publishes Silver atomically once 02 to 09 succeed. Setup 11 builds Gold inside `ml-required`, alongside model training, because both read only Silver. Those notebooks write the base contract directly to
Lakehouse Silver (`ag`) and Gold (`au`).

## Retained notebooks

These two notebooks remain for older or specialized flows. They are not the
current historical bootstrap:

- `01-create-bronze-shortcuts.ipynb` guides manual creation of the Lakehouse
  shortcuts to the 18 Eventhouse event tables for the optional live projection
  path, and fails when any of them is missing or unreadable. Its ADLS parquet
  shortcuts are optional and only feed the retained 02 loader.
- `02-historical-data-load.ipynb` is the retained legacy historical loader.
  The supported path now uses setup notebooks 01 through 04 rendered from
  `utility/`. If the retained loader is used, it requires each parquet input to
  match `generation.schemas.TABLES` exactly and fails the run before replacing
  Silver when a source schema differs.

Do not run the retained historical loader as a substitute for
`retail-setup deploy` or the ordered setup notebooks. The current path includes
run logging, staged validation, rollback behavior, and profile-aware
publication that the legacy notebook does not provide.

Notable groups in this directory:

- `03-streaming-to-silver` and `04-streaming-to-gold`: optional Eventhouse to
  Lakehouse projection
- `05-maintain-delta-tables`: maintenance of every Silver, Gold and ML table
  in the contracts, including the owned streaming-only
  `fact_online_order_status`, `ag._watermarks`, `setup_run_log` and the live
  stream's `ag._stream_state_*` tables
- `06` through `14`: ML and advanced analytics. `14` also appends `PENDING`
  pricing recommendations to the Eventhouse approval workflow.
- `16-persist-anomaly-alerts`: runs in `streaming-data-load` and appends new
  Eventhouse anomaly alerts plus a detection-run record
- `17-refresh-availability-signals`: runs in `daily-maintenance` after `06`
  and `12` and reseeds the Operations Agent's `ops_availability_signals`
- `30-create-ontology`: legacy-format ontology creation. It mirrors the
  canonical deploy builder (`deploy/scripts/_ontology_model.py`) and is
  historical-only; its Eventhouse TimeSeries bindings stay disabled
- `90` and `99`: manual augmentation/reset utilities

See the [historical data contract](../../docs/design/specifications/modules/generation/data-contract.md),
[Fabric analytics specification](../../docs/design/specifications/modules/analytics/fabric-analytics.md),
and [data flow](../../docs/design/architecture/data-flow.md).
