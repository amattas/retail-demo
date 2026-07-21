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
| Gold | `au` | Ten reporting aggregates and contract-tiered ML outputs |

The primary historical path writes Silver/Gold directly through setup notebooks.
The Eventhouse shortcut path is optional for incremental live projection.

## ML output tiers and runtime validation

`contracts/retail-demo.json` defines 14 executable output contracts. Each
references its authoritative producer notebook and declares exact schema,
grain, as-of and lineage fields, source tables, intended use, and limitations.
The four required Reporting tables also reference their active TMDL projection.
Repository validation fails when a producer, manifest declaration, validator,
or TMDL projection disagrees.

| Tier | Outputs |
| --- | --- |
| Required | `demand_forecast`, `customer_segments`, `churn_predictions`, `stockout_risk` |
| Optional promoted | `product_associations`, `product_recommendations`, `journey_patterns`, `zone_transitions`, `zone_dwell_stats`, `dwell_predictions` |
| Experimental | `price_elasticity`, `promotion_lift`, `pricing_constraints`, `pricing_recommendations` |

`15-validate-required-ml-contract` runs after all required producers. It fails
on missing or empty tables, incompatible schemas, null or duplicate grain
keys, invalid probability/bound values, missing as-of or lineage values, and
incomplete demand-forecast horizons. It does not create placeholder tables.
Only its pipeline's terminal success permits Reporting publication.

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
  model and is declared as the streaming-only terminal exception;
- live coverage is not equivalent for every historical fact.

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

Dashboard JSON/templates and KQL rule definitions are source inputs, not yet
guaranteed first-class deployable items. They may require manual import and
binding. Claims about five-minute schedules or deployed Activator actions are
not current defaults.

## KPI semantics

Current-state tables are overwrite snapshots. KQL and DAX use producer-aligned
status labels, shared date slicing, source-correct grain, volume-weighted
rollups, and hidden non-aggregatable technical fields. Repository contract
tests guard those semantics.
