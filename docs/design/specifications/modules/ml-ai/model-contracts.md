# ML, ontology, and agent contracts

## Pipeline sequence

The deployment separates data setup, required Reporting dependencies, and
extended ML:

1. `setup-pipeline` runs setup notebooks 01 through 04.
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

## Data Agents

Source-controlled Data Agent definitions reference authoring-workspace GUIDs.
They are staged after ontology creation in the same `full-demo` deployment.
That completion phase rewrites:

- `workspaceId`
- semantic-model `artifactId`
- ontology `artifactId`

Current datasource files leave `dataSourceInstructions` and `userDescription`
unset. Mandatory per-agent governance metadata is outside the default release.
Persona-specific instructions and approved-question packs remain optional under
`ENH-003`.

## Preview and support boundary

Ontology and related capabilities require explicit tenant capability checks.
Semantic-model agents and ontology agents are separate surfaces; failure or
unavailability of one must not be presented as failure of the other.
