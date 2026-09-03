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
- Lakehouse SQL endpoint metadata visibility;
- a minimal historical and live data query.

Local `validate_deployment.py` output is necessary but not sufficient evidence
of live readiness.

The supported live command is:

```powershell
retail-setup verify --env <env>
```

It is read-only by default and writes the redacted, atomic
`deploy/.generated/<env>/readiness-report.json`. The 26-row taxonomy covers
target identity (1), item inventory (2), live definitions and bindings (6),
task flow (1), KQL objects (4), schedules (1), pipeline results (3), and
freshness (8). Only unselected capabilities may be `SKIPPED`.

Required `FAIL` or `UNKNOWN` produces `FAILED` and exit `1`. Optional `FAIL` or
`UNKNOWN` produces `DEGRADED` and exit `3`; full success exits `0`, and command
usage errors exit `2`. The report stores bounded operational evidence, hashes,
target IDs/names, and lineage only. It excludes tokens, definition payloads,
business rows, and raw tenant-sensitive diagnostics.

`DEGRADED` means required capabilities are usable but one or more optional
capabilities need attention. It is not equivalent to `FAILED`.

`retail-setup verify --env <env> --run-pipeline` is an explicit mutation. It
starts only the profile-required post-publish pipeline and polls the exact job
instance to terminal success. Missing, unknown, timed-out, stale, or mismatched
evidence fails closed. The option is invalid when the profile has no such
pipeline.

Standard and full-demo deployment run the read-only verification after their
existing setup and required-ML gates. Deployment never adds
`--run-pipeline`. A failed required check fails the deployment; optional
readiness gaps mark the deploy journal `DEGRADED`. Core remains
operator-executed: run setup notebooks 01-04, then invoke verification.
Full-demo verification runs only after the ontology, both Data Agents, and the
complete task flow have been created or validated in the same deploy.

The deploy plan also runs optional SQL endpoint metadata synchronization after
post-Reporting ML. This tells Fabric's SQL endpoint about newly created
Lakehouse tables. A synchronization failure degrades the journal; readiness
still fails the deployment when a required table is not queryable.

## Pipeline state

| Pipeline | Current scope | Current schedule state |
| --- | --- | --- |
| `setup-pipeline` | Setup 01-04 | On demand; mandatory for Reporting profiles |
| `historical-data-load` | Retained historical-load notebook | On demand |
| `streaming-data-load` | Streaming Silver then Gold | Schedule file present, disabled |
| `daily-maintenance` | Delta maintenance | Schedule file present, disabled |
| `ml-required` | Demand, segmentation, churn, stockout, market-basket, and promotion/elasticity producers, then runtime contract validation | Terminal Reporting gate |
| `ml-optional` | Promoted optional outputs | Full-demo post-Reporting |
| `ml-experimental` | Experimental outputs (consumes `price_elasticity` from `ml-required`) | Full-demo post-Reporting |

Do not describe the disabled streaming schedule as an active five-minute or
fifteen-minute service.

## Freshness evidence

The verifier evaluates these signals together:

- `setup_run_log`
- `ag._watermarks`
- pipeline/notebook run history
- Eventhouse max ingestion/event timestamps
- Gold max source/event timestamps
- generation timestamps and model/run IDs for ML output
- alert/action state where deployed

| Check | Required? | Operational meaning |
| --- | --- | --- |
| Setup run log | Yes | Historical setup completed and published a known run. |
| Silver watermarks | No | The manually started stream has recently reached Silver. |
| Eventhouse ingestion | No | Eventhouse has received recent optional events. |
| Checkpoint identity | No | Recent Eventhouse extents can be tied to a stream and batch. |
| Required model outputs | Yes | The six Reporting models were published by the required pipeline. |
| Optional model outputs | No | Full-demo optional outputs are recent or have a valid empty snapshot. |
| Experimental model outputs | No | Full-demo experimental outputs are recent or have a valid empty snapshot. |
| Alerts | No | Optional alert queries are available and recent when rows exist. |

Required ML evidence selects the latest `generated_at` and a nonblank
`model_run_id` from the same Gold row. Source/business dates remain separate
lineage and cannot make an old model generation appear fresh. Setup and model
evidence use a seven-day maximum age. Streaming watermark,
Eventhouse ingestion, and checkpoint evidence use a 30-minute maximum age.
Pipeline and data evidence must match the exact triggered run or the retained
successful deployment-journal step. Required full-demo execution has live
evidence. Readiness correlates each model table's freshness with the pipeline
that runs its producer notebook: because notebooks 07 (market-basket mining)
and 10 (promotion/elasticity analysis) now also run inside `ml-required` to
produce the required `product_recommendations` and `price_elasticity`
outputs, the optional/experimental `product_associations` and `promotion_lift`
outputs they produce in the same run are correlated with the `ml-required`
window rather than the `ml-optional`/`ml-experimental` windows. The remaining
[IMP-013](../../../requirements/modules/operations/backlog.md#imp-013) boundary
is a recent bounded stream that satisfies the three optional streaming checks.

## Failure handling

- Retain failed command, pipeline, and notebook run IDs.
- Do not advance a checkpoint or watermark after failed required publication.
- Do not overwrite healthy historical output before validating replacement
  output.
- Distinguish required, optional, degraded, and manual-fallback results.
- Preserve failed payloads or durable replay evidence.
- Do not retry with `--run-pipeline` unless starting the setup pipeline is an
  intentional operator action.

Eventhouse micro-batches fail without checkpoint advancement, Silver replays
merge by stable keys before watermark advancement, and setup attempts append
durable status history. Historical and streaming publication stage and validate
all candidate tables before promotion; partial promotion invokes compensating
Delta restore/drop rollback. Deployment required-step outcomes persist in
`deploy/.generated/<env>/deploy-run.json`; its readiness step links the
separate bounded readiness report.

The exact-run recovery command accepts only explicitly named, recent,
terminal-successful Fabric job IDs, verifies setup-to-ML dependency order,
writes those IDs to the journal, and runs readiness before finalizing:

```powershell
python -m deploy.scripts.adopt_pipeline_runs `
  --environment <env> `
  --run setup-pipeline=<job-id> `
  --run ml-required=<job-id>
```

Full-demo recovery also supplies `ml-optional` and `ml-experimental`.

## Recovery

### Setup pipeline failed

Inspect `setup_run_log` for the terminal state:

- `FAILED`: staging/validation failed before final tables changed;
- `ROLLED_BACK`: promotion failed and prior targets were restored;
- `ROLLBACK_FAILED`: one or more targets could not be restored; preserve staging
  and repair from the logged target/version evidence;
- `COMPLETED_CLEANUP_FAILED` or `ROLLED_BACK_CLEANUP_FAILED`: final data is
  promoted or restored correctly, but logged staging artifacts require cleanup.

Resume only after confirming the terminal state. Use a new run ID; duplicate run
IDs are rejected.

### KQL application failed

Inspect the generated `database.kql`, confirm the target database and operator
permissions, then rerun the ordered script. Do not apply fragments out of order
without recording the resulting state.

### Streaming stopped or stale

If streaming has never been started, `DEGRADED` results for watermarks,
Eventhouse ingestion, and checkpoint identity are expected. Start a bounded
stream only when the live-event story is required.

Check notebook errors, KQL permissions, resolved Query URI, ingestion failures,
checkpoint path, persisted stream ID, Eventhouse shortcuts, and Silver
watermarks. Restart with the same checkpoint root so the failed micro-batch
retains its event identities and ingestion tags.

### Ontology/task-flow binding missing

Run `retail-setup post-ontology --env <env>`. It creates the ontology when
absent, rejects duplicates, republishes both agents, writes all selected task
references, reads the graph back, and runs complete readiness.

### Power BI table missing

Confirm whether the table exists in the Lakehouse. If it exists but SQL or
Power BI cannot see it, run
`python -m deploy.scripts.refresh_sql_endpoint --environment <env>`. If the
table does not exist, run the owning setup/ML notebook or gate the dependent
report surface; do not create placeholder business data.

## Destructive actions

`--recreate`, Terraform destroy, and `99-reset-lakehouse` require explicit
target validation and confirmation. `99-reset-lakehouse` is manual and is not
orchestrated by normal pipelines.

Recreate polls for workspace-name absence after destroy. A timeout or repeated
pagination marker fails the deploy before apply; do not bypass it with a fixed
sleep.

## Capacity

Start with a bounded store count and history window. Measure Spark duration,
Eventhouse ingestion, storage, and report behavior before increasing capacity
or enabling ML/ontology groups. Static runtime promises are not a contract.
