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

- 22 typed business event tables
- `unknown_event`
- `anomaly_alerts` and its run log `anomaly_detection_runs`, written by
  `16-persist-anomaly-alerts` in `streaming-data-load`
- 3 pricing recommendation/decision tables; `14-ml-dynamic-pricing` appends
  `PENDING` rows to `pricing_recommendation_created`
- 6 core materialized views, including `mv_truck_delay_hourly`
- 2 pricing decision metric materialized views
- `fn_pending_recommendations()` for latest cross-event workflow state
- `fn_rule_thresholds()` with the deployed store type's rule thresholds
- `fn_active_incidents()`, `fn_incident_impacted_stores()`, and
  `fn_lane_delays()` over the logistics incident and truck delay tables
- functions for attribution, truck SLA, anomaly detection, and related queries

Core materialized-view definitions are time-independent; seven-day retention is
applied as a materialized-view policy. Anomaly functions build hourly series
that start at the first event in the lookback and end at `bin(now(), 1h)`, so
the open hour is never scored. Counts default to zero; basket and payment-rate
averages default to null and are filled with `series_fill_linear`. Inventory
movement is scored per store, and only after `ANOMALY_MIN_ACTIVE_HOURS` (24)
active hours; rows with a null `store_id` are dropped from every series. The
functions then sort severity through a numeric rank while preserving the public
result schema. Detector parameters come from `calibration.py`.
`fn_new_anomaly_alerts(lookback_hours, persist_hours, closed_end)` keeps only
closed hourly bins and alert ids not yet in `anomaly_alerts`, so the scheduled
persistence is idempotent.

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
`concurrency = 1` to prevent overlapping runs of that pipeline. Across
pipelines and manual runs, 03 holds the `silver` writer lock in
`ag._job_locks` and skips the run when it can't get it within
`LOCK_WAIT_SECONDS` (see `fabric/pipelines/README.md`).
The daily deep scan is cadence-gated, not a scan of all source history on
every run.

Marketing attribution is incremental too. A run reconsiders only purchases
newer than the `marketing_attribution` watermark (the newest purchase it has
processed) minus the seven-day window and the replay overlap, plus purchases
and journeys whose rows were merged this run with older event times (late
arrivals and deep-reconciliation backfills). Touches, payments and promotions
are read only for those candidates, from the earliest candidate minus the
window. The first run, or a run whose late keys exceed
`ATTRIBUTION_LATE_KEY_LIMIT`, reads full history. Journey IDs are trimmed when
read instead of by table-wide `UPDATE`s, and the legacy store-operation casing
repair runs once (recorded in `_watermarks`). If a run fails after merging a
deep-reconciliation backfill but before attribution, those purchases are not
revisited automatically. `utility/tests/generation/test_streaming_silver_bounded_runtime.py`
checks that the candidate set stays the same when history grows tenfold.

Truck arrival/departure is handled as one lifecycle: Silver joins the two
sources on truck, distribution center, store, and shipment before appending a
completed `fact_truck_moves` row. A normal run reads departures from the
replay window and their arrivals (up to a day earlier), and compares them with
Silver lifecycles from the earliest candidate departure on. The daily deep
reconciliation (and the first run) reads the whole retained truck sources, so
delayed counterparts are not lost behind the watermark. `tests/test_truck_dwell_contract.py` verifies the
cross-layer source contract; live Fabric execution remains a separate gate.

Live-specific behavior and divergences:

- `inventory_updated` deterministically continues prior balances, orders deltas
  by event time and trace ID, and repairs legacy null-balance suffixes for
  affected store/product keys;
- `ad_impression` rows are dated by `impression_time` (when a journey touch
  was shown) while the watermark follows `ingest_timestamp` (when it arrived
  with its purchase), so touches fall inside the replay window;
- store operations are written as lower-case `opened`/`closed`, like history,
  and older upper-case rows are normalized once; `promotion_applied.product_ids`
  becomes the historical comma-separated, sorted ID list;
- picked, shipped and delivered events populate `ag.fact_online_order_status`,
  and each run moves recent live `fact_online_order_lines` with their order
  (`PICKED`, `SHIPPED`, `DELIVERED`, or `CANCELLED` for zero-total headers);
- `inventory_updated` rows without a store are DC stock moves and go to
  `fact_dc_inventory_txn`; lost sales keep their demand in
  `requested_quantity` and `unfulfilled_quantity`;
- `logistics_incident_started` inserts one `fact_logistics_incidents` row and
  its `fact_logistics_incident_stores` rows; `logistics_incident_resolved`
  sets `resolved_at` once through a Delta `MERGE`;
- `fact_receipts.local_hour` is derived from `dim_stores.TimeZone`;
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
declares all 22 emitted paths and derives the physical transform inventory from
this notebook. `python scripts/check_data_contracts.py` fails if a route,
terminal, mapping, key, or named boundary drifts.

## Streaming-to-Gold behavior

`04-streaming-to-gold.ipynb` and `setup-11` build the ten Gold tables from one
definition, `utility/src/retail_setup/generation/gold_rules.py`, which
`utility/scripts/build_notebooks.py` copies into notebook 04. Sales tables
count `SALE` receipts with an approved payment, online sales count orders with
an approved payment, `truck_dwell_daily` has one row per completed shipment
lifecycle dated by its arrival, and no source row at or after the Gold cutoff
is used: the end of the generated history (local midnight after `END_DATE`,
allowing 12 hours past UTC midnight) until streaming starts, then the build
time. `utility/tests/generation/test_gold_notebook_parity.py` runs notebook
04's cells and `gold.py` on the same input and compares every table. 04 holds
the `gold` writer lock and skips the run when setup-11 or maintenance holds it.

`04-streaming-to-gold.ipynb` writes each of the ten Gold candidates to a
run-scoped staging schema. All ten must exist and pass row-count/schema
validation before promotion. The notebook captures each prior Delta version;
an attempted promotion failure restores pre-existing targets and drops newly
created targets in reverse order. Rollback continues after individual restore
errors and preserves staging when manual recovery is required.

Nine Gold tables have an emitted-event route.
`dc_inventory_position_current` is rebuilt in batch from the DC ledger and is
part of the named historical boundary, although the stream also appends DC
stock moves to `fact_dc_inventory_txn`.
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

Rule thresholds are calibrated from the generator (`calibration.py`), and
`utility/tests/generation/test_calibration.py` fails when a rule, dashboard
tile, anomaly function or TMDL literal drifts from them. Two thresholds depend
on the store type: the zone-surge arrival count (the entrance arrivals of the
largest store at the store type's busiest hour, weekday and month in the best
weather, plus four Poisson standard deviations; 117 per 5 minutes for
supercenter) and the high-value receipt (10 times the store type's
`avg_ticket_target`, about one ticket in a thousand). They reach the rules
through `fn_rule_thresholds()`. The checked-in function carries the reference
store type's values; `apply_kql.py --environment` writes the deployed store
type's values (custom layouts and the configured transactions per store-day
included) as typed `real`/`long` literals and fails unless the function and
each threshold appear exactly once. Every rule deduplicates on `trace_id`; the
stockout and URGENT reorder rules raise one alert per store (or DC) and
window, listing the products, and the incident-stockout rule one per
incident. The dwell-breach rule keys on the
departure's `ingestion_time()` through `fn_truck_sla().departure_ingested_at`.
See `fabric/rules/README.md` for each rule.

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

`05-maintain-delta-tables.ipynb` maintains every Silver table in
`schemas.TABLES`, every Gold aggregate, every ML output, and the Silver
control tables (`_watermarks`, `_job_locks`, the six `_stream_state_*` tables,
`fact_online_order_status`, `setup_run_log`). It claims each table's writer
lock without waiting and skips tables whose lock is held.

`tests/test_maintenance_table_inventory.py` keeps the lists and their ZORDER
columns equal to those contracts. It skips genuinely absent optional tables,
including optional ML/streaming state; missing required historical tables
fail its base-inventory check. Checks and maintenance statements use
fully qualified configured Lakehouse/schema/table names. Only genuine table
absence is skipped: permission, catalog, and other real `AnalysisException`
failures propagate rather than being reclassified as optional absence.
Operation failures fail the notebook. Deployment supplies configured
Lakehouse/schema parameters to pipeline notebook activities.

These changes are implemented locally. Fresh remote KQL, Silver/Gold,
pipeline-concurrency, and maintenance smoke tests remain release gates.
