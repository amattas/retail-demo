# Power BI semantic model

## Source and mode

The source-controlled Power BI Project is
`fabric/powerbi/retail_model.pbip`. The semantic model is Direct Lake over the
target Lakehouse; it is not a current KQL/DirectQuery hybrid.

Deployment rewrites the Direct Lake/OneLake binding to the target workspace and
Lakehouse.

## Active table set

`definition/model.tmdl` currently contains 38 active table references:

- 7 dimensions
- 18 facts
- 9 Gold aggregates
- 4 ML tables

The four active ML tables are `churn_predictions`, `customer_segments`,
`demand_forecast`, and `stockout_risk`.

The semantic-model source is the authority for active tables. Component prose
must not claim that ML definitions are excluded while these references remain.

## Model design

- Direct Lake source expression
- explicit measures
- discouraged implicit measures
- date, geography, product, store, DC, and related hierarchies
- relationships from dimensions to fact and aggregate surfaces
- persona-oriented report pages

## Current contract gaps

- The base generator contract does not own the four active ML tables.
- Some operational facts have incomplete date/current-state semantics.
- Technical identifiers, cents, and helper columns require visibility and
  summarization review.
- Customer-like fields are exposed and no checked-in RLS roles were found.

These gaps are owned by `IMP-008`, `IMP-009`, and `IMP-011`.

## Deployment and validation

The normal deploy stages the semantic model and report under `Reporting`.
Use `scripts/configure_semantic_model.py` only for supported manual workflows
that need explicit workspace/Lakehouse binding.

Validation should confirm:

1. all active tables exist;
2. Direct Lake points to the intended Lakehouse;
3. required relationships and measures load;
4. optional ML pages have data or honest empty states;
5. access and field visibility match the intended persona.

Relevant tests:

- `tests/scripts/test_configure_semantic_model.py`
- `tests/scripts/test_reference_integrity.py`
- `utility/tests/generation/test_schema_contract.py`
