# ML, ontology, and agent contracts

## Pipeline sequence

The deployment separates data setup, required Reporting dependencies, and
extended ML:

1. `setup-pipeline` runs setup notebooks 01 through 04.
2. `ml-required` runs demand forecast, customer segmentation, churn, and
   stockout producers in parallel.
3. `15-validate-required-ml-contract` runs only after all four producers
   succeed.
4. Reporting can publish only after that exact pipeline run reaches terminal
   `Completed`.
5. `full-demo` runs `ml-optional` and `ml-experimental` after Reporting.

Optional or experimental failure cannot block required Reporting. Ontology
creation is a separate manual/preview boundary. The task-flow metadata mirrors
the runtime order as `Required ML Reporting Gate` -> `Semantic Model` ->
`Post-Reporting Extended ML`.

## ML contracts

The repository includes demand forecast, market-basket, customer segmentation,
churn, promotion effectiveness, journey, stockout, delivery, and dynamic
pricing notebooks.

`contracts/retail-demo.json` records typed support tiers and one contract for
each of their 14 outputs:

| Tier | Outputs |
| --- | --- |
| Required | `demand_forecast`, `customer_segments`, `churn_predictions`, `stockout_risk` |
| Optional promoted | `product_associations`, `product_recommendations`, `journey_patterns`, `zone_transitions`, `zone_dwell_stats`, `dwell_predictions` |
| Experimental | `price_elasticity`, `promotion_lift`, `pricing_constraints`, `pricing_recommendations` |

Every contract identifies its producer and source tables, exact output schema
and grain, as-of and lineage fields, intended use, and limitations. Producer
notebooks declare the same schema and validate types and non-null constraints
immediately before writing that exact physical target. Required contracts
additionally reference the active TMDL projection and the runtime validator.
This checked agreement prevents the manifest from becoming an independent,
unvalidated physical schema.

For all four required outputs, `generated_at` is the true Gold publication
timestamp and `model_run_id` identifies that generation. Source/business
cutoffs remain separate lineage: `source_as_of` for demand, `segmented_at` for
segments, `prediction_date` for churn, and `predicted_at` plus
`inventory_as_of` for stockout. Readiness orders and ages `generated_at` from
the same row as a nonblank run ID; it never treats a business as-of date as a
generation timestamp.

The runtime validator creates no tables. It rejects missing/empty required
outputs, incompatible columns or types, null/duplicate grain keys, invalid
probabilities or bounds, NaN or infinity in any floating output, missing
as-of/lineage, and incomplete forecast horizons. Repository validation parses
the validator's required grain, as-of, lineage, probability, and horizon rules
and compares them exactly with the manifest.

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
- delivery training uses matched `cusn` Bronze lifecycle events, partitions on
  departure-time label availability with a purge, and scores only unmatched
  arrivals with arrival-known features; missing sources or no inference-ready
  arrivals fail before replacing prior output;
- pricing uses a non-null no-estimate evidence sentinel, advances cooldown state
  only for accepted price changes, and applies the log-log quantity response
  `(new_price / old_price) ** elasticity - 1`.

## Semantic-model dependency

The active semantic model currently references four ML output tables:

- `churn_predictions`
- `customer_segments`
- `demand_forecast`
- `stockout_risk`

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

Ontology creation is not part of the required ML pipeline. Run it deliberately
after its preview/capacity boundaries are accepted, then use the acknowledged
`post-ontology` command to publish Data Agents and task flow.

## Data Agents

Source-controlled Data Agent definitions reference authoring-workspace GUIDs.
They are not staged during initial `full-demo` publication. After ontology
creation is validated, the post-ontology phase rewrites:

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
