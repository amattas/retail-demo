# ML, ontology, and agent contracts

## Pipeline sequence

The deployment separates data setup, required Reporting dependencies, and
extended ML:

1. `setup-pipeline` runs setup notebooks 01 through 11.
2. `ml-required` runs demand forecast (06), customer segmentation (08), churn
   (09), stockout (12), market-basket mining (07), and promotion/elasticity
   analysis (10) producers serially, in that order, to avoid oversubscribing
   the shared Spark pool.
3. `15-validate-required-ml-contract` runs only after all six producers
   succeed, and validates all six required output tables.
4. Reporting can publish only after that exact pipeline run reaches terminal
   `Completed`.
5. `full-demo` runs `ml-optional` (notebooks 11 and 13) and `ml-experimental`
   (notebook 14, which consumes `price_elasticity` produced by the required
   run) after Reporting.

Optional or experimental failure cannot block required Reporting. Ontology
creation is a separate preview phase after Reporting and extended ML, but
full-demo executes it automatically. The task-flow metadata mirrors the
runtime order as `Required ML Reporting Gate` -> `Reporting` ->
`Post-Reporting Extended ML`.

## ML contracts

The repository includes demand forecast, market-basket, customer segmentation,
churn, promotion effectiveness, journey, stockout, delivery, and dynamic
pricing notebooks.

`contracts/retail-demo.json` records typed support tiers and one contract for
each of their 14 outputs:

| Tier | Outputs |
| --- | --- |
| Required | `demand_forecast`, `customer_segments`, `churn_predictions`, `stockout_risk`, `product_recommendations`, `price_elasticity` |
| Optional promoted | `product_associations`, `journey_patterns`, `zone_transitions`, `zone_dwell_stats`, `dwell_predictions` |
| Experimental | `promotion_lift`, `pricing_constraints`, `pricing_recommendations` |

Every contract identifies its producer and source tables, exact output schema
and grain, as-of and lineage fields, intended use, and limitations. Producer
notebooks declare the same schema and validate types and non-null constraints
immediately before writing that exact physical target. Required contracts
additionally reference the active Tabular Model Definition Language (TMDL)
projection and the runtime validator.
This checked agreement prevents the manifest from becoming an independent,
unvalidated physical schema.

For all six required outputs, `generated_at` is the true Gold publication
timestamp and `model_run_id` identifies that generation. Source/business
cutoffs remain separate lineage: `source_as_of` for demand, `segmented_at` for
segments, `prediction_date` for churn, and `predicted_at` plus
`inventory_as_of` for stockout. `product_recommendations` and
`price_elasticity` carry the same `generated_at`, `model_run_id`, and
`schema_version` lineage columns as the original four; their business as-of
remains `computed_at`. Readiness orders and ages `generated_at` from
the same row as a nonblank run ID; it never treats a business as-of date as a
generation timestamp.

Promoting `product_recommendations` and `price_elasticity` to the required
tier means the Reporting gate now also depends on market-basket mining and
promotion/elasticity analysis succeeding with non-empty results, not only on
the original demand/segmentation/churn/stockout producers.

The runtime validator creates no tables. It rejects missing/empty required
outputs, incompatible columns or types, null/duplicate grain keys, invalid
probabilities or bounds, NaN or infinity in any floating output, missing
as-of/lineage, and incomplete forecast horizons. Repository validation parses
the validator's required grain, as-of, lineage, probability, and horizon rules
and compares them exactly with the manifest.

Optional outputs may be empty when the business preconditions for a result do
not exist. For example, sparse baskets can produce no association rules, and a
delivery run can have no currently open arrivals to score. In those cases the
producer overwrites the target with an empty table that still has the exact
contract schema. Readiness accepts that empty snapshot only when the owning
optional pipeline has exact, recent terminal-success evidence.

Demand evaluation freezes store/product eligibility at the training cutoff;
current production inference selects its cohort independently. Churn and
stockout partition on label-availability dates and purge 90-day and three-day
forward-label horizons, respectively.

### Stockout inventory semantics

The inventory ledger retains signed balances; a deficit is not a negative
number of available units. Notebook 12 selects the latest inventory event
per store/product/day, then derives model `current_inventory` as
`max(balance, 0)` for known balances. Both training and current inference use
that same snapshot function, so derived `days_of_inventory` and inventory
ratios remain nonnegative. This does not rewrite the underlying ledger.

Unknown balances remain null, and the producer rejects any null inventory
balance before training. A null must never be converted to zero by Spark's
`greatest` null-skipping behavior. Notebook 15 continues to reject negative
inventory/demand/coverage, invalid classes/horizons/ranks, and inconsistent
as-of timestamps. Its stockout error reports per-rule violation counts.
`utility/tests/generation/test_stockout_contract_runtime.py` executes the
producer feature functions and the downstream domain validator together.

Churn retains hidden nullable `is_churned_actual` solely as a deprecated
compatibility projection and always writes it as null. All formerly exposed
required columns remain present, so a failed required-ML gate can leave the
previous Reporting artifact query-compatible while Reporting publication stays
blocked.

Optional and experimental corrections are also contract-bound:

- recommendation support, confidence, and lift come from one singleton-pair
  market-basket rule;
- promotion prices use net extended cents per unit and comparisons include only
  episodes with complete baseline and post windows inside each store's observed
  receipt range;
- delivery training uses matched `cusn` Bronze lifecycle events (`cusn` is the
  Lakehouse schema that exposes Eventhouse tables as read-only shortcuts),
  partitions on
  departure-time label availability with a purge, and scores only unmatched
  arrivals with arrival-known features; missing training sources fail, while
  no inference-ready arrivals publish an empty contract-valid snapshot;
- pricing uses a non-null no-estimate evidence sentinel, advances cooldown state
  only for accepted price changes, and applies the log-log quantity response
  `(new_price / old_price) ** elasticity - 1`.

## Semantic-model dependency

The active semantic model currently references six ML output tables:

- `churn_predictions`
- `customer_segments`
- `demand_forecast`
- `stockout_risk`
- `product_recommendations`
- `price_elasticity`

`core` does not publish ML or Reporting. `standard` and `full-demo` use the
required runtime gate; a skipped, failed, cancelled, deduplicated, or unknown
run status performs no Reporting publication and records a journal failure.
On an upgrade, an already deployed report is left in place rather than replaced
with an artifact built against an unvalidated schema.

## Ontology

`30-create-ontology.ipynb`:

- discovers the current workspace and source items;
- represents business entities and relationships;
- binds Lakehouse tables and Eventhouse time-series context;
- prefers update-in-place for an existing ontology;
- falls back to delete/recreate with polling and retry behavior when needed.

Ontology creation is not part of the required ML pipeline. Full-demo runs it
automatically after Reporting and extended ML, but only after live
tenant/capacity preflight passes. `post-ontology` reruns this phase for
recovery.

The Fabric ontology public definition `EntityTypeProperty` schema doesn't
currently expose field descriptions. After creating or updating the public
definition, the deployment notebook uses the same Digital Operations authoring
API as the Fabric UI to apply descriptions to every regular and time-series
property, then reads the entity types back and fails if any description wasn't
persisted.

Descriptions come from source metadata when available, otherwise from
deterministic field context. The notebook identity obtains a Power BI token,
exchanges it for a Digital Operations workload token, and sends credentials only
to HTTPS service URLs whose hosts end in `.analysis.windows.net` or
`.pbidedicated.windows.net`; any other Fabric-returned URL fails the run. The
apply-and-verify sequence retries up to five times only for HTTP 404, 409,
429, 500, 502, 503, or 504; other failures fail immediately.

### New-experience (TMDL) publisher

The notebook above publishes the old entity-type JSON format, which Fabric
retires on Jan 31, 2027 (`ENH-010`). `deploy/scripts/deploy_ontology.py`
publishes the replacement definition. Full-demo deployment runs it as the
non-required `publish-ontology-tmdl` step, after `ensure-ontology` and before
the Data Agents are published. A failure keeps the existing ontology and
degrades the run instead of failing it. Once the ontology is in TMDL format,
`ensure_ontology` no longer runs the legacy notebook, so the migration is not
reverted. Live acceptance is still pending.

`deploy/scripts/_ontology_model.py` renders the definition deterministically
from the physical contracts, not from runtime inspection:

- Silver tables come from `utility/src/retail_setup/generation/schemas.py`,
  Gold ML outputs from `ml_contracts` in `contracts/retail-demo.json`, and
  Bronze event tables from `fabric/kql_database/01-create-tables.kql`.
- All tables are DirectLake partitions behind one `Sql.Database` expression
  for the Lakehouse SQL analytics endpoint, in schemas `ag`, `au`, and `cusn`.
  Each partition also carries the `ONT_*` annotations (workspace, Lakehouse
  item ID/name/kind, SQL endpoint, pin time) that Fabric uses to identify the
  backing item. Without them Data Agents can't resolve the ontology's
  underlying source. The publisher fails if readback drops them.
  Eventhouse time series bind through the Bronze `cusn` shortcuts. The
  publisher lists `Tables/cusn` in OneLake, and missing shortcuts drop only
  their bindings.
- The model has the same 12 entities and 17 relationship names as the
  notebook; three of the relationships use junction tables. Keys and
  properties use `snake_case` column names, and time-series properties are
  named `<event_table>_<column>` and ordered by `ingest_timestamp`.
- TOM relationships that `retail_model` marks inactive are also inactive
  here. Each event table has exactly one active outgoing relationship, so
  the TOM graph has no ambiguous active paths.
- Descriptions are native `///` comments, so the authoring-API workaround
  and its token exchange are unnecessary in this path.
- Lineage tags are UUIDv5 values of stable element names, so updates replace
  elements in place.
- Semantic-model DAX measures are **not** published as metrics. TMDL cannot
  express `projected` metrics, and the definition does not duplicate DAX as
  explicit metrics.

Publication behavior:

| Existing item | Action |
| --- | --- |
| None | Create `RetailOntology_AutoGen` |
| TMDL format (`.platform` `config.version` `2.0`) | `updateDefinition` in place |
| Old format (`definition.json` or `EntityTypes/` parts) | Create and verify `RetailOntology_AutoGen_tmdl`, then delete the old item and rename the staged item; a failed staging readback leaves the old item intact |

Readback must be in TMDL format and contain every generated table, entity,
and rule part. The service elides defaults, so readback compares element
sets rather than text. Evidence is written to
`deploy/.generated/<env>/ontology-report.json`. Run
`python -m deploy.scripts.deploy_ontology --render <dir>` to inspect the
definition without service calls.

### Business rules

`deploy/scripts/_ontology_rules.py` publishes nine natural-language rules
(`ENH-011`): minimum margin, MSRP ceiling, price-change cadence, aged-inventory
markdown, pricing approval, return window, stockout definition and risk
bands, replenishment policy, and churn definition. Each numeric value is
checked against the code that enforces it by
`tests/deploy/test_ontology_rules.py`. Rules are guidance only; ontology does
not execute them. Agents read them with the ontology MCP `list_ontology_rules`
tool.

## Data Agents

Source-controlled Data Agent definitions require target-specific rebinding.
They are staged after ontology creation in the same `full-demo` deployment.
That completion phase rewrites:

- `workspaceId`
- semantic-model `artifactId`
- ontology `artifactId`

Current semantic-model draft and published datasource snapshots select every
`retail_model` table, column, and measure (the data is synthetic) and include
`dataSourceInstructions` and `userDescription`; `.platform` descriptions
distinguish semantic-model and ontology scope. The ontology agent binds the
TMDL ontology as an ontology-as-context source
(`ontologies/RetailOntology_AutoGen/ontology.json`, schema
`dataAgent/definition/ontology/1.0.0`). That schema has no datasource
instructions, so every ontology entity type is selected and the stage
`aiInstructions` carry the answer rules. Both agents' instructions keep
individual customers' names, addresses, phone numbers, and loyalty-card values
out of answers. The legacy
`ontology-<name>/datasource.json` binding fails against TMDL ontologies.
Source schema validation and
matching local draft/published snapshots do not prove remote publication,
permissions, or behavior. Rebind and verify the deployed agents in Fabric.
Mandatory per-agent governance metadata remains outside the default release;
richer persona-specific instructions and approved-question packs remain
optional under `ENH-003`.

## Preview and support boundary

Ontology and related capabilities require explicit tenant capability checks.
Semantic-model agents and ontology agents are separate surfaces; failure or
unavailability of one must not be presented as failure of the other.
