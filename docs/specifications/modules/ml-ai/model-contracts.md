# ML, ontology, and agent contracts

## Setup-pipeline sequence

The current `setup-pipeline` executes:

1. setup notebooks 01 through 04;
2. ML notebooks 06 through 14;
3. `30-create-ontology`.

The pipeline activities are chained on success. This is broader than the older
README description of setup notebooks only.

## ML notebooks

The repository includes demand forecast, market-basket, customer segmentation,
churn, promotion effectiveness, journey, stockout, delivery, and dynamic
pricing notebooks.

These notebooks have different maturity and output contracts. Presence in the
setup pipeline does not make every output a required or verified base table.
Methodology and contract work is tracked by `IMP-008` and `ENH-007`.

## Semantic-model dependency

The active semantic model currently references four ML output tables:

- `churn_predictions`
- `customer_segments`
- `demand_forecast`
- `stockout_risk`

A default setup that publishes the active report therefore needs either those
tables or an explicit gating/empty-state design.

## Ontology

`30-create-ontology.ipynb`:

- discovers the current workspace and source items;
- represents business entities and relationships;
- binds Lakehouse tables and Eventhouse time-series context;
- prefers update-in-place for an existing ontology;
- falls back to delete/recreate with polling and retry behavior when needed.

The ontology is created after ML in `setup-pipeline`, so task-flow and
ontology-agent bindings may require a post-pipeline rebind.

## Data Agents

Source-controlled Data Agent definitions reference authoring-workspace GUIDs.
Deployment rewrites:

- `workspaceId`
- semantic-model `artifactId`
- ontology `artifactId`

Current datasource files leave `dataSourceInstructions` and `userDescription`
unset. Agent ownership, persona, approved questions, and prohibited detail are
required by `REQ-MLAI-004` and tracked by `IMP-011`/`ENH-003`.

## Preview and support boundary

Ontology and related capabilities require explicit tenant capability checks.
Semantic-model agents and ontology agents are separate surfaces; failure or
unavailability of one must not be presented as failure of the other.
