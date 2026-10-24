# Fabric analytics contract

## Eventhouse/KQL scripts

Deployment applies numbered scripts in repository order:

1. `01-create-tables.kql`
2. `02-create-ingestion-mappings.kql`
3. `03-create-functions.kql`
4. `04-create-materialized-views.kql`
5. `06-ml-anomaly-detection.kql`
6. `07-pricing-approval-tables.kql`

Each source file is a standalone, fail-fast
`.execute database script with (ThrowOnErrors=true)` batch. `apply_kql.py`
removes that one leading wrapper from each selected source and emits one flat
outer fail-fast script; embedded or repeated wrappers are rejected.

## KQL object inventory

- 18 typed business event tables
- `unknown_event`
- `anomaly_alerts`
- 3 pricing recommendation/decision tables
- 5 core materialized views
- 2 pricing decision metric materialized views
- `fn_pending_recommendations()` for latest cross-event workflow state
- functions for attribution, truck SLA, anomaly detection, and related queries

Core materialized-view definitions are time-independent; seven-day retention is
applied as a materialized-view policy. Anomaly functions build explicitly
bounded hourly series with zero-value defaults, then sort severity through a
numeric rank while preserving the public result schema.

The direct live path writes typed tables and does not use the JSON ingestion
mappings.

## Lakehouse layers

| Layer | Schema/location | Role |
| --- | --- | --- |
| Live Bronze | `cusn` shortcuts | Read-only Eventhouse table references exposed to Spark; `cusn` is the demo schema used for these shortcuts |
| Silver | `ag` | Typed dimensions, facts, and operational state |
| Gold | `au` | Ten reporting aggregates and contract-tiered ML outputs |

The primary historical path writes Silver/Gold directly through setup notebooks.
The Eventhouse shortcut path is optional for incremental live projection.

## ML output tiers and runtime validation

`contracts/retail-demo.json` defines 14 executable output contracts. Each
references its authoritative producer notebook and declares exact schema,
grain, as-of and lineage fields, source tables, intended use, and limitations.
The six required Reporting tables also reference their active TMDL projection.
Repository validation fails when a producer, manifest declaration, validator,
or TMDL projection disagrees.

The [ML and AI contract](../ml-ai/model-contracts.md#ml-contracts) lists all
14 outputs and their Required, Optional, or Experimental tiers.

`15-validate-required-ml-contract` runs after all required producers. It fails
on missing or empty tables, incompatible schemas, null or duplicate grain
keys, invalid probability/bound values, missing as-of or lineage values, and
incomplete demand-forecast horizons. It does not create placeholder tables.
Only its pipeline's terminal success permits Reporting publication.

## Streaming-to-Silver behavior

`03-streaming-to-silver.ipynb` reads Eventhouse shortcuts and replays a
configurable interval from each per-source checkpoint in `ag._watermarks`. The
default overlap is three hours to cover normal Eventhouse OneLake visibility
latency. Once per day by default, a distinct per-source reconciliation
checkpoint permits a bounded-cadence scan of older history. That scan
anti-joins source dedupe keys against Silver and adds only missing rows to the
same idempotent merge. Processing and reconciliation checkpoints advance only
after the merge succeeds; null checkpoint timestamps still fail closed.
Override the defaults with the `REPLAY_OVERLAP_SECONDS` and
`DEEP_RECONCILIATION_INTERVAL_SECONDS` notebook parameters or environment
variables.

The pipeline passes Lakehouse/schema notebook parameters and uses
`concurrency = 1` to prevent overlapping runs of that pipeline. Manual
notebook execution is not protected by that pipeline setting. The daily
deep scan is cadence-gated, not a scan of all source history on every run.

Truck arrival/departure is handled as one lifecycle: Silver joins the two
sources on truck, distribution center, store, and shipment before appending a
completed `fact_truck_moves` row. It scans the retained truck sources and
anti-joins completed Silver lifecycle keys so delayed counterparts are not lost
behind a watermark. `tests/test_truck_dwell_contract.py` verifies the
cross-layer source contract; live Fabric execution remains a separate gate.

Live-specific behavior and divergences:

- `inventory_updated` deterministically continues prior balances, orders deltas
  by event time and trace ID, and repairs legacy null-balance suffixes for
  affected store/product keys;
- picked and shipped events populate `ag.fact_online_order_status`;
- `fact_online_order_status` is an owned streaming-only Silver contract: its
  exact schema and routes are declared in the Silver notebook, its terminal
  exception is validated from `contracts/retail-demo.json`, and
  `05-maintain-delta-tables.ipynb` includes it and `ag._watermarks` in routine
  maintenance;
- it is intentionally absent from `schemas.py` and the active semantic model
  because `TABLES` remains the historical contract;
- live coverage is not equivalent for every historical fact.

Inventory also has runtime test coverage:
`utility/tests/generation/test_streaming_inventory_runtime.py` executes the
actual notebook transformation functions with real Spark DataFrames and
windows. It covers prior-balance continuation, legacy-null repair,
same-timestamp ordering by `trace_id`, and idempotent replay. The recorded local
result is a pass on Spark 4.2 with a checksum-verified session-local JDK 17.
Target-table lookup is stubbed; no live Delta `MERGE`, full Spark suite, or
Fabric execution is proven by this test.

Watermark/replay behavior is fail-closed and contract-tested. The manifest
declares all 18 emitted paths and derives the physical transform inventory from
this notebook. `python scripts/check_data_contracts.py` fails if a route,
terminal, mapping, key, or named boundary drifts.

## Streaming-to-Gold behavior

`04-streaming-to-gold.ipynb` writes each of the ten Gold candidates to a
run-scoped staging schema. All ten must exist and pass row-count/schema
validation before promotion. The notebook captures each prior Delta version;
an attempted promotion failure restores pre-existing targets and drops newly
created targets in reverse order. Rollback continues after individual restore
errors and preserves staging when manual recovery is required.

Nine Gold tables have an emitted-event route.
`dc_inventory_position_current` depends on historical-only
`fact_dc_inventory_txn` and is therefore part of the named historical boundary.
The derived attribution path combines five Silver inputs and terminates at
`fact_marketing_attribution` and `campaign_performance_daily` in the active
semantic model.

## Querysets

Checked-in KQL files are bundled as one `KQLQueryset` item with one tab per
query. Deployment rewrites its cluster/database binding.

## Dashboards and rules

Dashboard JSON/templates and KQL rule definitions are source inputs, not
guaranteed deployable items. They may require manual import and
binding. Activator is Fabric's event-driven alerting and action service.
Claims about five-minute schedules or deployed Activator actions are not
current defaults.

## KPI semantics

Current-state tables are overwrite snapshots. KQL and DAX use producer-aligned
status labels, shared date slicing, source-correct grain, volume-weighted
rollups, and hidden non-aggregatable technical fields. Repository contract
tests guard those semantics.

## Receipt repair utility

`90-augment-and-dedupe-receipts.ipynb` materializes and validates candidates
and backups before replacing receipt-grain targets. It records each attempted
promotion before issuing the replacement statement. A server commit followed
by a client exception therefore includes that target in reverse-order
rollback, rather than silently restoring only earlier targets.
`tests/test_lakehouse_batch_ml_fixes.py` exercises this failure window with
executable mocks. Local batch work is complete; actual Fabric/Delta
rollback execution remains a separate live gate.

## Maintenance

`05-maintain-delta-tables.ipynb` skips genuinely absent optional tables,
including optional ML/streaming state; missing required historical tables
fail its base-inventory check. Checks and maintenance statements use
fully qualified configured Lakehouse/schema/table names. Only genuine table
absence is skipped: permission, catalog, and other real `AnalysisException`
failures propagate rather than being reclassified as optional absence.
Operation failures fail the notebook. Deployment supplies configured
Lakehouse/schema parameters to pipeline notebook activities.

These changes are implemented locally. Fresh remote KQL, Silver/Gold,
pipeline-concurrency, and maintenance smoke tests remain release gates.
