# Operations runbook

## Deployment readiness

Record the environment, tenant, workspace, operator identity, commit, generated
output path, and deployment run before mutation.

After publication, verify:

- required Fabric items and workspace folders;
- KQL tables, functions, and materialized views;
- notebook default-Lakehouse bindings;
- pipeline notebook references;
- semantic-model binding and required tables;
- task-flow references;
- optional ontology and agent rebinding;
- a minimal historical and live data query.

Local `validate_deployment.py` output is necessary but not sufficient evidence
of live readiness.

## Pipeline state

| Pipeline | Current scope | Current schedule state |
| --- | --- | --- |
| `setup-pipeline` | Setup 01-04, ML 06-14, ontology | On demand |
| `historical-data-load` | Retained historical-load notebook | On demand |
| `streaming-data-load` | Streaming Silver then Gold | Schedule file present, disabled |
| `daily-maintenance` | Delta maintenance | Daily schedule enabled |
| `machine-learning` | ML 06-14 | On demand |

Do not describe the disabled streaming schedule as an active five-minute or
fifteen-minute service.

## Freshness evidence

Use together:

- `setup_run_log`
- `ag._watermarks`
- pipeline/notebook run history
- Eventhouse max ingestion/event timestamps
- Gold max source/event timestamps
- model/run IDs for ML output
- alert/action state where deployed

The unified operator surface is open work in `IMP-013`.

## Failure handling

- Retain failed command, pipeline, and notebook run IDs.
- Do not advance a checkpoint or watermark after failed required publication.
- Do not overwrite healthy historical output before validating replacement
  output.
- Distinguish required, optional, degraded, and manual-fallback results.
- Preserve failed payloads or durable replay evidence.

Current code does not satisfy every rule; see `IMP-002`.

## Recovery

### Setup pipeline failed

Resume from the first failed activity only after confirming upstream tables are
valid. Retain the original run ID and compare row counts/freshness after rerun.

### KQL application failed

Inspect the generated `database.kql`, confirm the target database and operator
permissions, then rerun the ordered script. Do not apply fragments out of order
without recording the resulting state.

### Streaming stopped or stale

Check notebook errors, KQL permissions, resolved Query URI, ingestion failures,
 checkpoint path, Eventhouse shortcuts, and Silver watermarks. A partial
Eventhouse batch may have advanced its checkpoint under current behavior.

### Ontology/task-flow binding missing

Wait for ontology creation to complete, then redeploy the task flow and
dependent ontology agent.

### Power BI table missing

Confirm whether the table is base or optional. Run the owning setup/ML notebook
or gate the dependent report surface; do not create placeholder business data.

## Destructive actions

`--recreate`, Terraform destroy, and `99-reset-lakehouse` require explicit
target validation and confirmation. `99-reset-lakehouse` is manual and is not
orchestrated by normal pipelines.

Current recreate behavior uses a fixed wait; resource deletion/status polling is
required by the operations backlog.

## Capacity

Start with a bounded store count and history window. Measure Spark duration,
Eventhouse ingestion, storage, and report behavior before increasing capacity
or enabling ML/ontology groups. Static runtime promises are not a contract.
