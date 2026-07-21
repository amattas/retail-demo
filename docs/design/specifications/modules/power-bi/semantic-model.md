# Power BI semantic model

## Source and mode

The source-controlled Power BI Project is
`fabric/powerbi/retail_model.pbip`. The semantic model is Direct Lake over the
target Lakehouse; it is not a current KQL/DirectQuery hybrid.

Deployment rewrites the Direct Lake/OneLake binding to the target workspace and
Lakehouse.

## Active table set

`definition/model.tmdl` currently contains 40 active table references:

- 7 dimensions
- 19 facts
- 10 Gold aggregates
- 4 ML tables

The four active ML tables are `churn_predictions`, `customer_segments`,
`demand_forecast`, and `stockout_risk`.

The semantic-model source is the authority for active tables. The four ML
tables are required Reporting outputs and have executable producer, schema,
grain, temporal, lineage, intended-use, and limitation contracts in
`contracts/retail-demo.json`.

## Model design

- Direct Lake source expression
- explicit measures
- discouraged implicit measures
- date, geography, product, store, DC, and related hierarchies
- relationships from dimensions to fact and aggregate surfaces
- persona-oriented report pages

## ML publication contract

The historical generator remains valid without ML or Reporting. `standard` and
`full-demo` publish infrastructure first, run `setup-pipeline`, and then wait
for the exact `ml-required` run to finish. That pipeline runs the four required
producers and `15-validate-required-ml-contract`. A missing, empty, incompatible,
duplicate, temporally incomplete, or invalid required output fails the run.
Only terminal success permits the semantic model and report to be staged.

`full-demo` runs optional and experimental ML pipelines after Reporting is
published. Failures in those isolated tiers leave required Reporting available
and mark the deployment journal as degraded.

## Deployment and validation

The gated Reporting phase stages the semantic model and report under
`Reporting`.
Use `scripts/configure_semantic_model.py` only for supported manual workflows
that need explicit workspace/Lakehouse binding.

Validation should confirm:

1. all active tables exist;
2. Direct Lake points to the intended Lakehouse;
3. required relationships and measures load;
4. all four required ML tables pass their runtime contract;
5. access and field visibility match the intended persona.

Relevant tests:

- `tests/scripts/test_configure_semantic_model.py`
- `tests/scripts/test_reference_integrity.py`
- `tests/scripts/test_ml_semantic_contract_imp008.py`
- `utility/tests/contracts/test_ml_contracts.py`
- `utility/tests/generation/test_schema_contract.py`
