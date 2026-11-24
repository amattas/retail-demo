# Operations guide

Use this guide to check readiness, monitor data, and recover failures in an
existing workspace. It is written for operators, analysts validating a demo,
and support engineers. The [plain-language glossary](glossary.md) explains
unfamiliar Fabric and data-layer terms.

See [Deployment](deployment.md) for provisioning and publication, the
[operations runbook](../design/specifications/modules/operations/runbook.md)
for exact failure behavior, and the
[workspace inventory](workspace-inventory.md) for selected assets and counts.

## Readiness checklist

Run the profile-aware live verifier after the selected workloads have run:

```powershell
retail-setup verify --env <env>
```

This command is read-only. It resolves the selected profile and manifest,
Terraform target outputs, and deployment journal, then writes an atomic,
redacted report to:

```text
deploy/.generated/<env>/readiness-report.json
```

The report provides one structured result for each target, item inventory,
binding, KQL object group, schedule, pipeline, and freshness expectation.
An unselected capability is `SKIPPED`; selected checks must produce evidence.
Contributors can find the exact 27-row taxonomy in the
[operations runbook](../design/specifications/modules/operations/runbook.md).

`full-demo` runs this taxonomy only after the ontology, both Data Agents, and
the task flow have been published. The task-flow check compares the persisted
workspace graph with the complete source graph, including every selected item
reference and edge.

| Result | Meaning | Exit |
| --- | --- | --- |
| `SUCCEEDED` | Every selected check passed. | `0` |
| `FAILED` | A required check failed or its evidence is unknown. | `1` |
| usage error | The command or profile request is invalid. | `2` |
| `DEGRADED` | Required checks passed, but optional evidence failed or is unknown. | `3` |

`DEGRADED` means required scenarios are usable, but an optional feature is not
ready to present. A new deployment where `stream-events` has not been started
normally reports three optional unknowns:

- no recent Silver streaming watermark;
- no recent Eventhouse ingestion; and
- no checkpoint identity attached to Eventhouse ingestion.

A recent watermark alone is not enough for a passing streaming check.
The verifier also samples settled receipt/inventory keys and Gold sales
against Silver, checking orphan keys, null or negative balances, and online
order lines whose picked, shipped and delivered times are out of order. Missing or empty
samples remain `UNKNOWN`; observed mismatches are `FAIL`. Both are optional
streaming degradation. This bounded sample is not proof of every table or
all historical data. See the
[sampling contract](../design/specifications/modules/operations/runbook.md#bounded-streaming-consistency).

The report contains bounded counts, timestamps, the canonical manifest
version/hash, resolved profile support and asset boundaries, target identities,
and freshness lineage. It never stores access tokens, item-definition payloads,
business rows, or raw service diagnostics.

Before running the verifier:

1. Record the repository commit, environment, tenant, workspace, operator, and
   deployment run.
2. Install `utility/requirements-deploy.txt`. On Windows and Linux it includes
   the bundled `mssql-python` driver; macOS requires Microsoft ODBC Driver 17
   or 18. The verifier uses the existing Lakehouse SQL endpoint connection for
   setup, watermark, and model evidence.
3. Confirm the configured identity can read Fabric items, definitions, job
   history, Eventhouse metadata, and the Lakehouse SQL endpoint.
4. For `core`, which deploys no pipelines, run setup notebooks 01–11
   manually in order before verification. Reporting profiles run setup
   01–10 through `setup-pipeline` and `setup-11-build-gold` inside
   `ml-required`.
5. For Reporting profiles, retain the deployment journal that correlates the
   exact setup and required-ML pipeline runs.
6. Confirm optional ML, ontology, agent, dashboard, and rule features are ready
   before including them.
7. Run a bounded stream before expecting streaming freshness checks to pass.

Use `--run-pipeline` only when you intentionally want the verifier to mutate
the target by starting the profile's required post-publish pipeline:

```powershell
retail-setup verify --env <env> --run-pipeline
```

The command waits for the exact returned job instance. It fails closed on an
unknown state, timeout, mismatched run, or missing `Location` evidence. It does
not start streaming, required ML, optional ML, or any schedule. A profile
without a post-publish pipeline rejects this option as a usage error.

Different checks establish different facts:

| Evidence | What it proves |
| --- | --- |
| Deploy command completed | Required local subprocesses returned success. |
| Artifact inventory passed | Staged item count, folders, descriptions, and manifest/profile metadata match the selected publication phase. |
| `validate_deployment.py` passed | Generated config and staged files passed offline checks. |
| Readiness report succeeded | Required and optional selected live checks passed at the report timestamp. |
| Readiness report degraded | Required checks passed, but the deployment is not ready for the affected optional story. |
| Pipeline trigger returned success | Fabric accepted a run request; without exact-run terminal polling this is not completion. |
| Pipeline/notebook run succeeded | The selected activities completed in Fabric. |
| Table row counts and timestamps advanced | Data reached the expected serving layer. |
| Report, ontology, or agent opened | The surface exists; its binding and answer still need validation. |

## Monitor

Use Fabric Monitoring Hub and item run histories for pipelines and notebooks.
Use these signals together:

| Signal | What it tells you | Where to check |
| --- | --- | --- |
| Historical setup run | Whether the setup pipeline completed and `setup-10` published the historical Silver snapshot (Gold is built by `setup-11-build-gold`) | `ag.setup_run_log` in the Lakehouse |
| Streaming progress | How far the Eventhouse-to-Silver transform has processed | `ag._watermarks` in the Lakehouse |
| Live event freshness | Whether Eventhouse is receiving recent events | Eventhouse row counts and `ingest_timestamp` |
| Gold freshness | Whether business summaries reflect the latest selected inputs | Representative maximum business time in `au` tables |
| Machine-learning publication | Whether a model output was generated by a known run | Output generation time and model run identifier |
| Alert state | Whether optional detections or actions are pending | The deployed alert or action table, when selected |
| Deployment identity | Which source revision and Fabric resources produced the evidence | Git commit, `deploy-run.json`, and `terraform-output.json` |

`retail-setup verify` checks these signals without saving source rows.
For required machine learning, it checks the publication time and the
identifier of the run that produced the same row. Business "as of" fields
describe the source period; they do not prove when the model output was
published.
Setup and model evidence may be at most seven days old. Eventhouse ingestion
and checkpoint evidence may be at most 30 minutes old, for every live event
table separately. Silver watermarks may be two `streaming-data-load` intervals
old (60 minutes on the committed schedule), measured from the event time Silver
last processed.
Pipeline and data timestamps must correlate with the retained deployment or
explicitly triggered run.

## Quick KQL checks

KQL (Kusto Query Language) is Fabric's query language for Eventhouse. These
queries are read-only examples.

Representative stream freshness (`standard` or `full-demo`):

```kql
receipt_created
| summarize rows = count(), latest = max(ingest_timestamp)
```

Representative event coverage:

```kql
union withsource=table_name receipt_created, payment_processed,
  inventory_updated, online_order_created
| summarize rows = count(), latest = max(ingest_timestamp) by table_name
| order by table_name asc
```

Truck lifecycle readiness:

```kql
fn_truck_sla()
| where arrival_ingest_timestamp > ago(30m)
| summarize rows = count(), latest = max(departure_ingest_timestamp),
    breaches = countif(dwell_minutes > 90)
```

Use table-specific event timestamps when a query depends on business time
rather than ingestion time.

## Common recovery paths

| Symptom | First action |
| --- | --- |
| CLI tenant mismatch | Reauthenticate to the configured tenant and rerun the dry-run target review. |
| Capacity unavailable | Confirm the capacity display name, active state, tenant, and operator access. |
| Custom Spark pool provisioning fails | Reconfigure with the starter pool unless preview support is intentional. |
| Rendered notebooks missing | Run `retail-setup render --env <env>`. |
| Fabric publication fails | Inspect the failing item type and generated `deploy/.generated/<env>/fabric-cicd/parameter.yml`. |
| KQL objects missing | Inspect and rerun the generated ordered `database.kql` against the intended database. |
| Item inventory or notebook binding check fails | Read the check's `evidence` in `readiness-report.json` for exact missing, duplicated, mismatched, or incorrectly bound items. Repair only the owning publication step; do not regenerate successful historical data. |
| Notebook/report definition reads stall or lose their connection | Each definition/export HTTP request has a 30-second timeout and bounded read-only retries. Once Fabric accepts the export, retries poll that exact operation rather than starting another export. Exhausted reads remain `UNKNOWN`, not successful evidence. |
| Pipeline evidence reports `ConnectionError` | Fabric collection reads retry connection resets and timeouts up to three times, within any collection deadline. If retries are exhausted, restore connectivity and rerun read-only readiness; do not rerun a successful pipeline just to recover its run evidence. |
| Deployment exits because local pipeline monitoring timed out | Keep the pipeline evidence file and run `retail-setup deploy --env <env> --resume`. Resume polls the exact recorded `Location`; it does not submit or cancel a Fabric run. |
| Pipeline POST response was lost | Do not retry manually until reconciliation finishes. The runner adopts only one recent exact-pipeline candidate in the request window; zero or multiple candidates fail closed and require an operator-supplied exact run ID. |
| Late Fabric checks fail after a long verification run | The Fabric REST session renews tokens within five minutes of expiry using the same configured credential and tenant. Renewal happens before sending a request, without replaying mutations or changing identities. If renewal fails, requests fail closed rather than sending an expired token. |
| KQL readiness reports `KustoNetworkError` | The read-only checker retries transient network errors. If retries are exhausted, restore Eventhouse connectivity and rerun readiness; this does not by itself indicate missing schema objects or require redeployment. |
| Operations tables are missing after the native publisher failed | Retry `python -m deploy.scripts.deploy_operations_planning --environment <env> --operations-only` when the environment excludes Planning. Missing tables are reported as `FAIL` with their names; the verifier does not query their mappings or discard otherwise readable KQL inventory. |
| Operations source freshness validation fails | The publisher retains its supporting tables and draft notebook, but does not seed invalid signals or create the agent. Inspect `operations-planning-report.json`; refresh the owning source/ML outputs before retrying monitoring publication. Do not bypass the seven-day freshness guard. |
| `retail-twins` is missing after its publisher failed | Retry `python -m deploy.scripts.deploy_twins --environment <env>`, then rerun read-only readiness. The selected twins report must exist and bind to the existing model; a non-blocking publisher failure is not evidence that this selected surface is ready. |
| Setup pipeline not started | Start `setup-pipeline` manually and retain its run ID. |
| Setup pipeline failed | Resume from the first failed activity only after validating upstream tables. |
| Required ML gate failed | Inspect producer/validator errors; Reporting is intentionally unpublished until a new exact run succeeds. |
| Optional delivery training finds no valid completed dwell records | Use the updated `13-ml-delivery-prediction` notebook, which combines completed Silver `fact_truck_moves` with Bronze lifecycle events. Empty live shortcuts do not replace historical training data, and unmatched live arrivals remain prediction candidates. Republish the affected notebook and rerun only the failed optional pipeline; do not regenerate successful setup data. |
| A failed optional pipeline hides successful sibling ML evidence | Update the verifier and rerun read-only readiness. It correlates failed terminal runs as failures, verifies sibling pipelines independently, and checks each model's producer before querying its output. Missing producer evidence is not proof that the model SQL query or a sibling pipeline failed. |
| `freshness.setup_run_log` reports `ProgrammingError` after deployment | Update the local verifier to query setup data and staging catalog metadata separately, then rerun read-only readiness. Fabric error 15816 rejects a mixed catalog/data query; do not regenerate setup data for this checker error. |
| Ontology, Data Agent, or task-flow links missing | Run `retail-setup post-ontology --env <env>` to recreate any missing ontology, republish agents, and verify the exact persisted graph. |
| Live rows absent | Verify notebook parameters, resolved Query URI, KQL permissions, connector errors, and ingestion timestamps. |
| `stream-events` refuses to start with a checkpoint error | Its Spark checkpoint and incarnation record disagree. Follow the file names in the error; see [Restart or reset the live stream](#restart-or-reset-the-live-stream). |
| `stream-events` refuses to start with a history handoff error | History ends more than a day before now, or overlaps it. Rerender and rerun setup, or set `allow_history_gap = True` to accept the gap. |
| Silver data stale | Inspect Eventhouse shortcuts, transform run history, source timestamps, and `ag._watermarks`. |
| Gold data stale | Confirm the Silver run completed, then run the Gold transform. |
| Power BI cannot see newly created tables | Run `python -m deploy.scripts.refresh_sql_endpoint --environment <env>`, then reload the report and rerun readiness. |
| Power BI shows the wrong workspace or data | Confirm required tables exist and the Direct Lake binding targets the intended Lakehouse. |
| Local validation passed but live assets fail | Treat the deploy as not ready and perform item, binding, KQL, run, and data checks. |
| Verifier reports `UNKNOWN` | Restore the missing permission, locked dependency, output, journal, or live evidence; required unknowns are failures. |
| Verifier reports stale evidence | Run only the owning workload, retain its exact run evidence, and rerun the read-only verifier. |

### Negative stockout inventory at the required ML gate

If notebook 15 rejects negative `stockout_risk.current_inventory` or
`days_of_inventory`, update notebook 12's available-inventory calculation:
known negative ledger balances represent zero available units, while unknown
balances must still fail. Update notebook 15 for per-rule violation counts.

Republish the corrected notebooks and regenerate `stockout_risk` with
`12-ml-stockout-prediction`, then run `15-validate-required-ml-contract`.
Historical setup tables do not need regeneration for this derived-output
repair. Reporting publication still requires a successful exact
`ml-required` pipeline run; standalone notebook results do not replace that
pipeline evidence. Do not weaken the gate or mark its failed run successful.

## Restart or reset the live stream

`stream-events` keeps its state under `Files/setup/stream/checkpoint` in the
Lakehouse. Live runs (`run_seconds = 0`) use `eventhouse-live/`; bounded runs
use `eventhouse-smoke/`, so a smoke test never interferes with the live
stream. Each folder has an incarnation record in `_metadata/` whose nonce is
part of every generated ID and Kusto ingest tag.

- **Restart after a stop or failure.** Run the notebook again with the same
  checkpoint. A batch that failed part-way replays with the same rows, IDs,
  and tags, so the tables that already succeeded are skipped. Only the last
  10 minutes (`max_lag_seconds`) of the downtime are generated; the rest of
  the stop is a gap in the live data.
- **Transient errors.** Kusto throttling, 5xx responses, timeouts, and expired
  tokens are retried within the batch. A live query that still fails is
  restarted up to five times (`max_restarts`) with backoff. The notebook ends
  only on a permanent error or once that budget is spent.
- **Start a new incarnation** (new IDs and tags, same stream ID): stop the
  notebook, then delete both `eventhouse-live/` and
  `_metadata/eventhouse-live.json`. Deleting only the folder is refused,
  because reusing batch numbers would make Eventhouse silently skip the
  writes.
- **Start a new stream ID:** stop the notebook and delete the whole
  `Files/setup/stream/checkpoint` folder.
- **Upgrade from an earlier version:** the old `eventhouse/` folder is no
  longer read and can be deleted.

The stream's own state is in the Lakehouse tables `ag._stream_state_stores`
(store open/closed), `ag._stream_state_visits` and `ag._stream_state_orders`
(visits and online orders in progress), `ag._stream_state_inventory` (store
stock the stream has touched), `ag._stream_state_logistics` (loads waiting at
the DCs, shipments on the road, trucks) and `ag._stream_state_meta` (the last
batch clock and the inventory seed time), keyed by incarnation. A new
incarnation starts from a fresh state: store open/closed from Silver
`fact_store_ops`, and store stock from the latest Silver
`fact_store_inventory_txn` balances at its start, read only when the stream
first touches a store and product. Trucks start idle at their DC; shipments
history had on the road are not carried over. Rows of earlier incarnations are
ignored and can be deleted with
`DELETE FROM ag._stream_state_<name> WHERE incarnation <> '<current>'`.
Notebook 05 maintains the six tables.

What to expect from a fresh stream: inventory, reorder and stockout events
follow the purchases, so they stop overnight with the stores. Reorders wait
four hours at the DC to consolidate, so the first truck arrivals come several
hours after the start, and online orders are delivered a day or more after they
are placed (BOPIS pickups within hours). The Fulfillment Pipeline tile follows
the orders created in the last 24 hours through picking, shipping and delivery,
so its later stages trail the created count.

Live volume follows history by default: `source_rows_per_second = 0` derives
the rate from the store count and `transactions_per_store_day`, and visits
follow each store's local hours, so expect few events at night and none from
a store while it is closed. Set `volume_scale` (for example `5.0`) for a busier
demo; it multiplies every live volume.

A new incarnation checks the history handoff again. If history ends more than
a day before the start, rerun setup or set `allow_history_gap = True`.

## Upgrading after the data-review changes

The data-review changes on `main` (after release 1.3.0) change the data and
event contracts. An environment deployed before them can't be updated in
place by a normal redeploy:

- **Historical data.** Silver timestamps are now true UTC (they were store
  local), and setup adds `dim_stores.TimeZone`, `dim_customers.HomeStoreID`,
  `fact_receipts.local_hour` and `original_receipt_id_ext`, the truck delay
  columns, `fact_logistics_incidents`, `fact_logistics_incident_stores`, and
  more trucks per DC. Prices, tax, customers, returns, payments and
  promotions are generated differently. Setup must run again; its first
  notebook re-seeds the dictionaries, which picks up the new
  `_shared/tax_rules.json`.
- **Eventhouse.** KQL adds four event tables, `impression_time`,
  `requested_quantity` and the truck delay columns (restating
  `ad_impression`, `inventory_updated` and `truck_arrived` with
  `.alter table`), new functions and a materialized view, an ingest-tag
  retention policy, and `fn_rule_thresholds()` with your store type's values.
- **Live stream.** The stream keeps state in new `ag._stream_state_*` tables
  and its checkpoints in new folders (`eventhouse-live/`, `eventhouse-smoke/`
  with incarnation records). The old `eventhouse/` checkpoint is not read.
- **Historical Silver is protected.** Once `ag._watermarks` exists, setup
  refuses to replace Silver, so a workspace that has run the streaming
  pipeline needs a reset first.

The simplest upgrade is a clean rebuild of a disposable demo workspace:

```powershell
retail-setup configure --workspace-name <workspace>  # keeps your settings; pins anchor_date
retail-setup render --env <env>                      # renders the new tokens into the notebooks
retail-setup deploy --env <env> --recreate
```

`--recreate` deletes the workspace and everything in it; see
[Reset and recreate](#reset-and-recreate). To keep the workspace instead:

1. Stop `stream-events` and disable the `streaming-data-load` schedule.
2. Run `99-reset-lakehouse` by hand. It drops the Silver and Gold schemas,
   including `_watermarks`, the ML outputs, `_job_locks` and any stream state.
3. Delete the Lakehouse folder `Files/setup/stream/checkpoint`.
4. Optionally clear the old live rows from Eventhouse
   (`.clear table <event_table> data` for each event table), so notebook 03
   does not project pre-upgrade events into the new Silver.
5. Run `retail-setup configure`, `retail-setup render`, and
   `retail-setup deploy --env <env>`. The
   deployment applies the KQL changes, re-runs setup and the ML pipelines,
   and republishes the reports.
6. Start the stream again; it begins a new incarnation from the new history.

The `.alter table` restatements and the in-place path have not been verified
against a live Eventhouse yet.

### New tunables and defaults

Stream notebook parameters (the **Parameters** cell of `stream-events`):

| Parameter | Default | Meaning |
| --- | --- | --- |
| `source_rows_per_second` | `0` | `0` derives the rate from the store count, `transactions_per_store_day`, online orders and marketing volume. A larger value only adds resolution; a value below the peak under-produces peak hours and prints a warning. |
| `volume_scale` | `1.0` | Multiplies every live volume; use it for a busier demo. |
| `trigger_seconds` | `60` | Live micro-batch interval: one write per event table per batch. Bounded runs use at most 10 seconds. |
| `max_lag_seconds` | `600` | Backlog cap: after a stop, only the last 10 minutes of the downtime are generated. |
| `max_restarts` | `5` | Supervised restarts of a failed live query. |
| `allow_history_gap` | `False` | Start a new stream even when history ends more than a day before now. |

Generation settings (`retail-setup configure`, saved per environment):

| Setting | Default |
| --- | --- |
| `transactions_per_store_day` | The store type's profile: supercenter 400, grocery 280, hardware 180, luxury 40 |
| `online_orders_per_day` | Derived from the profile's online share of all orders |
| `customer_count` | Derived from identified traffic and visit frequency (about 14,000 per supercenter) |
| `anonymous_share` | `0.45` of receipts have no customer |
| `annual_churn_rate` | `0.20` |
| `annual_volume_growth` | `0.03` |
| `anchor_date` | Pinned at the first `configure` (the history start) and kept afterwards, so dimensions don't change when you re-render later |

Calibrated rule and detector thresholds live in
`utility/src/retail_setup/generation/calibration.py`, and contract tests keep
the KQL rules, dashboard tiles, anomaly functions and semantic model equal to
them. The main values:

| Threshold | Value |
| --- | --- |
| Truck dwell SLA | 90 minutes |
| Late truck | 30 minutes after the planned arrival |
| Lane-delay rule | 60 minutes |
| High-value receipt | 10 × the store type's `avg_ticket_target` (written into `fn_rule_thresholds()` at deployment) |
| Zone surge | Entrance arrivals per 5 minutes at the store type's busiest hour plus 4 standard deviations (117 for supercenter; written into `fn_rule_thresholds()`) |
| Incident stockouts | At least 3 stockouts in 60 minutes for one incident |
| Anomaly detection | 168-hour lookback, threshold 1.5; inventory movement scored per store after 24 active hours |

## Rerun safely

For a normal update:

1. Preserve the failed or prior run identifiers.
2. Confirm the target environment and workspace.
3. Rerender notebooks when configuration or source changed.
4. Preview the deployment plan.
5. Deploy in place without recreate.
6. Rerun only affected data workloads.
7. Compare row counts, timestamps, and bindings with the prior known-good run.

Each workspace environment has isolated generation configuration, rendered
output, Terraform state/data, and `deploy/.generated/<env>/workspace/`
staging, including post-ontology. Serialize runs for the same environment or
Fabric target; local path isolation does not prevent concurrent remote writes.

## Reset and recreate

Use destructive operations only after:

- validating the selected environment and live target;
- retaining run, failure, and recovery evidence;
- confirming which data and items will be removed;
- receiving explicit operator confirmation.

`retail-setup deploy --recreate` destroys the workspace, exhausts paginated
workspace listings until the target name is absent, and only then rebuilds it.
A bounded timeout fails before apply.

`99-reset-lakehouse` is a manual destructive asset. It is not part of the
normal pipeline.

## Capacity

Runtime depends on store count, history months, Fabric capacity, Spark pool,
notebook groups, and optional ML/ontology work. Begin with a bounded
configuration and scale after observing Spark, Eventhouse, and Power BI
utilization. Do not promise fixed runtimes without a measured profile.

## Current live-stream boundary

The required historical, Reporting, ontology, Data Agent, and task-flow paths
have live deployment evidence. The stream remains intentionally manual. Until
a bounded `stream-events` run completes, the three optional streaming
freshness checks (and `freshness.alerts`) remain `UNKNOWN`, but overall
readiness is `SUCCEEDED`: a stream that has never run is expected after a
deployment. The console presents the missing stream evidence as an
informational notice rather than a warning or `ERROR`. There is no need to redeploy to clear that notice. Run the
optional stream only when live-event functionality is needed; genuine stream
failures and partial evidence still produce warnings.
