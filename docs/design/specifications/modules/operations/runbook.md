# Operations runbook

## Deployment readiness

Record the environment, tenant, workspace, operator identity, commit, generated
output path, and deployment run before changing resources.

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

Local `validate_deployment.py` checks are required, but they do not establish
live readiness.

The supported live command is:

```powershell
retail-setup verify --env <env>
```

It is read-only by default and writes the redacted, atomic
`deploy/.generated/<env>/readiness-report.json`. The 27-row taxonomy covers
target identity (1), item inventory (2), live definitions and bindings (6),
task flow (1), KQL objects (4), schedules (1), pipeline results (3), and
freshness (9). Only unselected capabilities may be `SKIPPED`.

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
operator-executed: run setup notebooks 01-11, then invoke verification.
Full-demo verification runs only after the ontology, both Data Agents, and the
complete task flow have been created or validated in the same deploy.

The deploy plan also runs optional SQL endpoint metadata synchronization after
post-Reporting ML. This tells Fabric's SQL endpoint about newly created
Lakehouse tables. A synchronization failure degrades the journal; readiness
still fails the deployment when a required table is not queryable.

## Pipeline state

| Pipeline | Current scope | Current schedule state |
| --- | --- | --- |
| `setup-pipeline` | Setup 01-10 (02-05 and 06-08 in parallel) | On demand; mandatory for Reporting profiles |
| `historical-data-load` | Retained historical-load notebook | On demand |
| `streaming-data-load` | Streaming Silver then Gold, with `16-persist-anomaly-alerts` in parallel | Committed disabled, every 30 minutes (UTC); `schedules.streaming_data_load` enables it, and by default follows `stream.start_after_deploy` |
| `daily-maintenance` | Delta maintenance, then `06-ml-demand-forecast` and `12-ml-stockout-prediction`, then `17-refresh-availability-signals` | Committed disabled, daily 00:00 Eastern; enabled by `schedules.daily_maintenance` (default) |
| `ml-required` | Demand, segmentation, churn, stockout, market-basket, and promotion/elasticity producers, then runtime contract validation | Terminal Reporting gate |
| `ml-optional` | Promoted optional outputs | Full-demo post-Reporting |
| `ml-experimental` | Experimental outputs (consumes `price_elasticity` from `ml-required`) | Full-demo post-Reporting |

Do not describe the disabled streaming schedule as an active five-minute or
fifteen-minute service.

The streaming pipeline passes configured Lakehouse/schema parameters to its
notebooks and sets `concurrency = 1`. This serializes that pipeline's runs,
not arbitrary manual notebook executions.

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
| Silver watermarks | No | Recent progress plus bounded receipt/inventory/sales consistency evidence. |
| Eventhouse ingestion | No | Eventhouse has received recent optional events. |
| Checkpoint identity | No | Recent Eventhouse extents can be tied to a stream and batch. |
| Required model outputs | Yes | The six Reporting models were published by the required pipeline. |
| Optional model outputs | No | Full-demo optional outputs are recent or have a valid empty snapshot. |
| Experimental model outputs | No | Full-demo experimental outputs are recent or have a valid empty snapshot. |
| Alerts | No | Scheduled anomaly detection has run recently. No `anomaly_detection_runs` row is `UNKNOWN`; a last run older than three streaming intervals is `FAIL` when the streaming schedule is enabled and `UNKNOWN` when it is not. An empty `anomaly_alerts` table passes only with recent run evidence. |
| Operations signals | No | The Operations Agent's newest `source_as_of`/`forecast_as_of` is inside its seven-day window. Older or missing signals are `UNKNOWN`, because the agent's query then returns no case; a disabled agent passes with `enabled=false`. |

### Console severity versus report status

Required failed/unknown checks are printed as errors and block deployment.
Optional failures or unavailable evidence are warnings, not deployment
errors. When all three optional streaming checks have no observed run
evidence, the console groups them as an informational notice explaining that
this is expected before `stream-events` is started. A partial stream or a
network/permission failure is not classified as expected inactivity.

A stream that has never run does not degrade readiness: those checks, and
`freshness.alerts` when no detection run exists, stay `UNKNOWN` in the report
but are left out of the overall status, so a fresh deployment reports
`SUCCEEDED`. No evidence is invented, marked passed, or silently skipped. Once
any core stream evidence exists, missing or stale stream evidence degrades
readiness again. Standalone verification retains
its exit code 3 for optional degradation. The deployment command treats that
as non-blocking and prints that required deployment is ready, while its
journal retains `DEGRADED`. Actual optional workload failures remain named
in the completion summary.

The persisted `checks` and `freshness_lineage` collections retain every
structured record. Redaction and evidence-size limits apply inside each
record, not to the number of checks; a truncation marker must never replace
a check object. This keeps saved reports renderable and their check inventory
consistent with the reported totals.

Required ML evidence selects the latest `generated_at` and a nonblank
`model_run_id` from the same Gold row. Source/business dates remain separate
lineage and cannot make an old model generation appear fresh. Setup and model
evidence use a seven-day maximum age. Eventhouse ingestion and checkpoint
evidence use a 30-minute maximum age, checked per live event table: each
table's own `max(ingestion_time())` must be recent, so one busy table cannot
make the rest look fresh. Live volume follows store hours, so tables that are
quiet by design get extra time (`stream_control.EVENT_TABLE_QUIET`): 26 hours
for store opened/closed; 12 hours for store-visit, purchase, inventory,
reorder, truck and online-order tables (stores close overnight, loads wait to
consolidate, small fleets have few orders per hour); 24 hours for stockouts;
36 hours for online deliveries, which come a day or more after the order; and
6 hours for ad impressions. The Silver watermark check adds the same quiet time. The logistics incident
events, which happen only a few times a year per DC
(`stream_control.SPORADIC_EVENT_TYPES`), are not expected to be fresh;
`stream_control.PENDING_EVENT_TYPES` (declared but not emitted) is empty now
that the stream emits all 22 types. Silver watermark
evidence uses the stalest live table's `last_processed_ts` (the event time
Silver has processed), not `updated_at`, which the daily deep reconciliation
refreshes without new rows. Its maximum age is two `streaming-data-load`
intervals (60 minutes for the committed 30-minute schedule), so a run that
is merely due does not flap the check. Checkpoint evidence reads
`.show tables (...) extents where tags contains 'ingest-by:retail-demo:'`
for the last day, one row per table.
Pipeline and data evidence must match the exact triggered run or the retained
successful deployment-journal step. Historical full-demo execution is baseline
evidence only; current repairs require new live verification.

Readiness correlates each model table with the pipeline that runs its
producer. Notebooks 07 (market-basket mining) and 10 (promotion/elasticity)
run inside `ml-required` to produce `product_recommendations` and
`price_elasticity`. Their additional `product_associations` and
`promotion_lift` outputs therefore use the `ml-required` freshness window,
even though those outputs are optional or experimental. The remaining
[IMP-013](../../../requirements/modules/operations/backlog.md#imp-013) boundary
includes a recent bounded stream that satisfies the three optional streaming
checks, plus fresh changed-path readiness and cleanup/degradation validation.

### Bounded streaming consistency

The existing optional `freshness.watermarks` check calls
`adapter.data_consistency_signal()` after checking watermark freshness.
`deploy/scripts/data_consistency.py` owns the SQL; this does not add another
taxonomy row or make streaming a required deployment capability.

- Receipt and inventory shortcut-to-Silver key anti-joins sample ingestion
  timestamps from each source checkpoint minus 24 hours up to checkpoint
  minus three hours. The newest overlap is excluded for OneLake visibility.
- Receipt identity joins Silver `receipt_id_ext` to the Eventhouse
  `receipt_created.receipt_id` column; inventory identity uses `trace_id`.
  The test fixture builds `cusn` tables from `01-create-tables.kql`, `ag`/`au`
  tables from `schemas.TABLES`, and `ag._watermarks` from notebook 03, so a
  renamed column fails the test.
- Recent Silver inventory rows are checked for null and negative balances
  (`negative_balances`; the live stream turns short stock into lost sales, so
  a balance never goes below zero), recent online order lines for a lifecycle
  out of order (`lifecycle_out_of_order`: picked before the order, shipped
  before picked, or delivered before shipped or picked), and receipts
  for missing store or non-null customer dimension keys. These integrity
  scans start at the respective window's lower bound; unlike the ingestion
  anti-joins, they do not exclude the newest three hours.
- Gold `sales_minute_store` receipt counts and summed sales cents are compared
  by store with Silver receipts. The comparison uses interior complete hours,
  capped by both the settled receipt cutoff and Gold's latest timestamp;
  receipt counts must match and a cents difference greater than one is a
  mismatch.

The same check compares each Eventhouse event table's newest
`ingest_timestamp` with its Silver watermark. When Silver is behind, the
backlog age is measured from the watermark (or from the oldest row of a table
Silver never processed) against that table's soft-delete retention in
`01-create-tables.kql` (7 days for customer and BLE tables). A backlog older
than three days is `UNKNOWN` with a warning to run `streaming-data-load`; a
backlog at or beyond the table's retention is `FAIL`, because unprocessed
events may already be purged.

Missing/invalid evidence or an empty receipt, inventory, or sales sample is
`UNKNOWN`, not a zero-defect success. An observed positive mismatch is `FAIL`.
Both degrade overall readiness because this streaming check is optional;
other required failures still determine `FAILED`.

This is bounded receipt/inventory/sales sampling, not universal all-table or
full-history integrity proof. `tests/deploy/test_data_consistency.py` executes
equivalent SQL in SQLite and exercises the real adapter/runner path; live
Fabric SQL execution and service consistency remain external gates.

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

When a different run becomes latest, the previous deploy journal is first
archived atomically at
`deploy/.generated/<env>/run-history/<hashed-run-id>.json`. Inspect that history
when comparing retries; do not overwrite evidence or hand-edit fingerprints
to make legacy journals resumable.

Cleanup-only failure after successful promotion returns success with a
warning. The setup freshness check retains `cleanup_required` evidence and
the overall readiness result is `DEGRADED`, not `FAILED`, when no required
check fails. Maintenance likewise distinguishes absent optional tables from
real operation failures; the latter fail the notebook.

The setup-log row and remaining staging-table count are fetched in separate
SQL statements. Fabric rejects combining distributed Lakehouse data with
`INFORMATION_SCHEMA` catalog metadata in one query (SQL error 15816).
Unavailable staging metadata remains `UNKNOWN`; it is not assumed to mean
zero leftover tables.

Read-only readiness KQL requests retry `KustoNetworkError` up to three times,
with five- and ten-second waits and a fresh client per attempt. Permission,
query, and other service errors are not blindly retried. Exhausted reads
remain errors, never empty success-shaped inventories. The four KQL inventory
checks reuse one complete snapshot; if acquisition fails after retries,
subsequent inventory checks remain `UNKNOWN` for that same run rather than
mixing earlier failures with a later successful snapshot. Rerun read-only
readiness after a persistent connection issue is resolved; do not rerun
schema deployment or data generation solely to repeat these reads.

Cleanup readiness considers both the latest terminal setup status and
remaining staging-table inventory. The SQL adapter counts tables in
`ag_stage`, `au_stage`, and `au_staging`; a later successful Gold run cannot
hide retained Silver staging artifacts merely by becoming the latest
`COMPLETED` log row. Cleanup remains actionable until the leftovers are
resolved.

Maintenance resolves fully qualified configured Lakehouse names. Only actual
table absence is skippable; catalog, permission, and other real
`AnalysisException` failures must not be treated as absent optional tables.

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

Once the Silver watermark table has been initialized, the historical writer
refuses replacement. Stop every writer and use a coordinated Silver/checkpoint
reset for a deliberate rebuild; never delete only checkpoint or watermark
state to bypass this guard.

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
watermarks. The notebook already retries transient Kusto errors and restarts
a failed live query up to `max_restarts` times; a notebook that ended has a
permanent error or exhausted that budget.

Restart with the same checkpoint root so the failed micro-batch replays with
its event identities, catalog date, and ingestion tags. A restart after a long
stop does not replay the downtime: only the last `max_lag_seconds` (10 minutes)
of source rows are kept, so expect a gap for the stop.

Do not delete only `eventhouse-live/`. The notebook then refuses to start,
because the checkpoint's incarnation record says batches were already
ingested. To start a new incarnation (new tags and IDs, same stream ID),
delete `eventhouse-live/` and `_metadata/eventhouse-live.json` together. To
start a new stream ID, delete the whole checkpoint root. A new incarnation
also re-runs the history handoff check; set `allow_history_gap = True` if
history ends more than a day before the restart. A new incarnation ignores
the earlier incarnation's rows in `ag._stream_state_*` and seeds store state
and stock from Silver; the old rows can be deleted.

### Writer lock held

Notebook 03 or 04 logs that it skipped its run, notebook 90 or setup-11
fails, or an ML producer fails after 30 minutes, because another writer holds
its lock in `ag._job_locks`. A healthy holder renews its lease every two
minutes; a crashed one's lease expires within 15 minutes. Check
`SELECT * FROM ag._job_locks` for the holder and `expires_at`. Delete a row by
hand (`DELETE FROM ag._job_locks WHERE lock_name = 'silver'`) only when you
are sure its holder is no longer running.

### Ontology/task-flow binding missing

Run `retail-setup post-ontology --env <env>`. It creates the ontology when
absent, rejects duplicates, republishes both agents, writes all selected task
references, reads the graph back, and runs complete readiness.

### Power BI table missing

Confirm whether the table exists in the Lakehouse. If it exists but SQL or
Power BI cannot see it, run
`python -m deploy.scripts.refresh_sql_endpoint --environment <env>`. If the
table does not exist, run the owning setup/ML notebook or gate the dependent
report page; do not create placeholder business data.

## Destructive actions

`--recreate`, Terraform destroy, and `99-reset-lakehouse` require explicit
target validation and confirmation. `99-reset-lakehouse` is manual and is not
orchestrated by normal pipelines.

Recreate polls for workspace-name absence after destroy. A timeout or repeated
pagination marker fails the deploy before apply; do not bypass it with a fixed
sleep.

## CI platform coverage

`.github/workflows/tests.yml` keeps the full sharded Spark and Parquet/E2E
paths on Linux. Windows runs its non-Spark suite and discovers real Spark
expression smoke tests with `-m "spark and spark_no_filesystem"`. Current
marked examples cover inventory replay/repair, delivery keyed as-of history,
and truck lifecycle key pairing:

- `utility/tests/generation/test_streaming_inventory_runtime.py`
- `utility/tests/generation/test_delivery_history_runtime.py`
- `utility/tests/generation/test_streaming_truck_runtime.py`

The Windows job provisions JDK 17 through the existing SHA-pinned
`actions/setup-java` reference, sets `TMPDIR` to `${{ runner.temp }}`, and
uses `SPARK_LOCAL_HOSTNAME=localhost`. This is non-filesystem execution;
missing Windows `HADOOP_HOME`/`winutils` still prevents the local Parquet/E2E
path. Do not describe Windows as excluding every Spark test.

The recorded local results include passes for these three examples on Windows
with the session's JDK 17. A separate generator selection passed 85
non-filesystem Spark tests; it is not the same selection as the representative
Windows CI marker.
The filename-discovery regression was fixed by marker selection, followed by
18 passing workflow/pipeline tests. The changed hosted workflow, full Spark
suite, and live Delta/Fabric execution still require their own evidence.

## Capacity

Start with a bounded store count and history window. Measure Spark duration,
Eventhouse ingestion, storage, and report behavior before increasing capacity
or enabling ML/ontology groups. Static runtime promises are not a contract.
