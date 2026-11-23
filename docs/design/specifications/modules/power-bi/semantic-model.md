# Power BI semantic model

## Source and mode

The Power BI Project (PBIP) at `fabric/powerbi/retail_model.pbip` versions the
report and semantic model as source-controlled folders.
The semantic model is Direct Lake over the target Lakehouse. Direct Lake reads
OneLake tables directly without importing a second copy; the model is not a
current KQL/DirectQuery hybrid.

Deployment rewrites the Direct Lake/OneLake binding to the target workspace and
Lakehouse.

## Active table set

`definition/model.tmdl` currently contains 43 active table references. TMDL
means Tabular Model Definition Language, the text format used to define the
model:

- 7 dimensions
- 20 facts
- 10 Gold aggregates
- 6 ML tables

The six active ML tables are `churn_predictions`, `customer_segments`,
`demand_forecast`, `stockout_risk`, `product_recommendations`, and
`price_elasticity`. Two relationships bind `product_recommendations.product_id`
and `price_elasticity.product_id` to `dim_products`.

The semantic-model source is the authority for active tables. The six ML
tables are required Reporting outputs and have executable producer, schema,
grain, temporal, lineage, intended-use, and limitation contracts in
`contracts/retail-demo.json`.

## Model design

- Direct Lake source expression
- explicit measures
- discouraged implicit measures
- date, geography, product, store, DC, and related hierarchies
- relationships from dimensions to facts and aggregates
- persona-oriented report pages

## Measure conventions

Measures that look alike say their basis in their description, and
`tests/test_semantic_model_measures.py` keeps them honest:

- **Sales basis.** `Total Sales`, `Total Store Sales` and `Online Revenue` are
  receipt or order totals including tax. `Net Sales`, `Line Revenue`,
  `Online Line Revenue` and `Omnichannel Revenue` are line revenue excluding
  tax. "Net" means net of returns (RETURN rows are negative); `Gross Sales` is
  SALE line revenue before returns.
- **Returns.** `Receipt Count` counts SALE receipts only, so `Average Basket
  Size` (`Gross Sales` per receipt) and `Units per Receipt` describe
  purchases. `Return Rate %` divides refunded totals by gross SALE totals.
  Gold `sales_minute_store` still counts return receipts as notebook 04
  builds it.
- **Payments.** Payment amounts (`Total Amounts`, `Store Payment Amount`,
  `Online Payment Amount`) are APPROVED payments; payment counts are attempts,
  approved and declined, which the decline-rate measures need.
- **Year over year.** Prior-year measures (`Store Sales PY`, `Omnichannel
  Revenue PY`, `Net Sales (Prior Year)`, `Omnichannel Revenue (Prior Year)`)
  compare like for like: the same calendar days one year earlier, for the days
  in context that have data. A partial latest year, or one with a gap between
  history and a later live stream, is never compared with a whole prior year.
- **Forecast intervals.** `Forecast Lower Bound` and `Forecast Upper Bound`
  are the row's bounds on a single forecast row. Over several rows the
  per-row half-widths are combined in quadrature around the summed prediction
  (independent errors), not summed; the seven-day measures do the same.
- **Attribution dates.** `Attributed Revenue` (`fact_marketing_attribution`)
  is dated by the purchase. `Campaign Attributed Revenue`
  (`campaign_performance_daily`) is dated by the marketing touch, so spend
  and the revenue it drove share a day. The two differ for the same date
  filter by design.
- **Traffic.** `Traffic Conversion Proxy %` is SALE receipts per entrance
  passage, not a measured conversion rate.
- **Twins.** Twin "as of" measures observe events up to the source as-of: the
  latest recorded store sale (history's end, or the latest live receipt),
  capped at the current UTC time. Pre-generated future truck stages do not
  become "observed" as the clock moves.

## ML publication contract

The historical generator remains valid without ML or Reporting. `standard` and
`full-demo` publish infrastructure first, run `setup-pipeline`, and then wait
for the exact `ml-required` run to finish. That pipeline runs the six required
producers and `15-validate-required-ml-contract`. A missing, empty, incompatible,
duplicate, temporally incomplete, or invalid required output fails the run.
Only terminal success permits the semantic model and report to be staged.

`full-demo` runs optional and experimental ML pipelines after Reporting is
published. Failures in those isolated tiers leave required Reporting available
and mark the deployment journal as degraded.

## Deployment and validation

The gated Reporting phase stages the semantic model and report under
`Reporting`.
During artifact staging, deployment sets every saved report date filter to the
month containing `end_date` from `utility/out/<env>/render-manifest.json`. The
checked-in PBIP remains a reusable template, while each deployed report opens
on the latest period generated for that environment.

After setup and ML, deployment refreshes Lakehouse SQL endpoint metadata so
new tables become visible to SQL and Power BI. A refresh failure is optional
at the step level, but readiness still fails when a required model table cannot
be queried.

Use `scripts/configure_semantic_model.py` only for supported manual workflows
that need explicit workspace/Lakehouse binding.

Validation should confirm:

1. All active tables exist.
2. Direct Lake points to the intended Lakehouse.
3. Required relationships and measures load.
4. All six required ML tables pass their runtime contract.
5. Access and field visibility match the intended persona.

Relevant tests:

- `tests/scripts/test_configure_semantic_model.py`
- `tests/scripts/test_reference_integrity.py`
- `tests/test_semantic_model_measures.py`
- `tests/scripts/test_ml_semantic_contract_imp008.py`
- `utility/tests/contracts/test_ml_contracts.py`
- `utility/tests/generation/test_schema_contract.py`
