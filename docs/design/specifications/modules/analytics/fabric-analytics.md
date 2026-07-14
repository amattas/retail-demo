# Fabric analytics contract

## Eventhouse/KQL scripts

Deployment applies numbered scripts in repository order:

1. `01-create-tables.kql`
2. `02-create-ingestion-mappings.kql`
3. `03-create-functions.kql`
4. `04-create-materialized-views.kql`
5. `06-ml-anomaly-detection.kql`
6. `07-pricing-approval-tables.kql`

`apply_kql.py` wraps the concatenated content in an outer database script with
`ThrowOnErrors=true`. Some source files contain their own database-script
wrappers; live execution remains the final proof of nested behavior.

## KQL object inventory

- 18 typed business event tables
- `unknown_event`
- `anomaly_alerts`
- 3 pricing recommendation/decision tables
- 5 core materialized views
- 3 pricing approval materialized views
- functions for attribution, truck SLA, anomaly detection, and related queries

The direct live path writes typed tables and does not use the JSON ingestion
mappings.

## Lakehouse layers

| Layer | Schema/location | Role |
| --- | --- | --- |
| Live Bronze | `cusn` shortcuts | Eventhouse tables exposed to Spark |
| Silver | `ag` | Typed dimensions, facts, and operational state |
| Gold | `au` | Nine reporting aggregates and optional ML outputs |

The primary historical path writes Silver/Gold directly through setup notebooks.
The Eventhouse shortcut path is optional for incremental live projection.

## Streaming-to-Silver behavior

`03-streaming-to-silver.ipynb` reads Eventhouse shortcuts, filters by per-source
watermarks in `ag._watermarks`, appends transformed output, then advances the
watermark.

Truck arrival/departure is handled as one lifecycle: Silver joins the two
sources on truck, distribution center, store, and shipment before appending a
completed `fact_truck_moves` row. It scans the retained truck sources and
anti-joins completed Silver lifecycle keys so delayed counterparts are not lost
behind a watermark. `tests/test_truck_dwell_contract.py` verifies the
cross-layer source contract; live Fabric execution remains a separate gate.

Known divergences:

- `inventory_updated` populates store inventory transactions with incomplete
  current-balance semantics;
- picked and shipped events populate `ag.fact_online_order_status`;
- `fact_online_order_status` is not in `schemas.py` or the active semantic
  model;
- live coverage is not equivalent for every historical fact.

Watermark/replay correctness is tracked by `IMP-002`; cross-layer schema
ownership is tracked by `IMP-005`.

## Streaming-to-Gold behavior

`04-streaming-to-gold.ipynb` overwrites each of the nine Gold tables when its
prerequisite Silver tables exist. Missing prerequisites cause the affected
aggregate to be skipped rather than manufactured.

## Querysets

Checked-in KQL files are bundled as one `KQLQueryset` item with one tab per
query. Deployment rewrites its cluster/database binding.

## Dashboards and rules

Dashboard JSON/templates and KQL rule definitions are source inputs, not yet
guaranteed first-class deployable items. They may require manual import and
binding. Claims about five-minute schedules or deployed Activator actions are
not current defaults.

## KPI semantics

Prominent state, time, and grain issues are tracked by `IMP-009`, including
pending pricing state, unresolved stockouts, status casing, date relationships,
network grain, and technical-field visibility.
