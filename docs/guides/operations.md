# Operations guide

This guide covers routine readiness, monitoring, and recovery. Provisioning and
publication are covered by [Deployment](deployment.md). Exact failure and
recovery behavior is owned by the
[operations runbook](../design/specifications/modules/operations/runbook.md).
Use the [workspace and profile inventory](workspace-inventory.md) for canonical
prerequisites, selected assets, expected counts, folders, and support
boundaries.

**Audience:** workspace operators, analysts validating a demo, and support
engineers. The [plain-language glossary](glossary.md) explains the Fabric and
data-layer terms used below.

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
Contributors can find the exact 26-row taxonomy in the
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

`DEGRADED` does not mean the required deployment failed. It means the
workspace is usable for required scenarios, but an optional story should not
be presented yet. A new deployment where `stream-events` has not been started
normally reports three optional unknowns:

- no recent Silver streaming watermark;
- no recent Eventhouse ingestion; and
- no checkpoint identity attached to Eventhouse ingestion.

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
4. For `core`, run setup notebooks 01-04 in order before verification.
5. For Reporting profiles, retain the deployment journal that correlates the
   exact setup and required-ML pipeline runs.
6. Confirm optional ML, ontology, agent, dashboard, and rule surfaces are ready
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

Do not treat these as equivalent:

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
| Historical setup run | Whether the four setup stages published a complete historical snapshot | `ag.setup_run_log` in the Lakehouse |
| Streaming progress | How far the Eventhouse-to-Silver transform has processed | `ag._watermarks` in the Lakehouse |
| Live event freshness | Whether Eventhouse is receiving recent events | Eventhouse row counts and `ingest_timestamp` |
| Gold freshness | Whether business summaries reflect the latest selected inputs | Representative maximum business time in `au` tables |
| Machine-learning publication | Whether a model output was generated by a known run | Output generation time and model run identifier |
| Alert state | Whether optional detections or actions are pending | The deployed alert or action table, when selected |
| Deployment identity | Which source revision and Fabric resources produced the evidence | Git commit, `deploy-run.json`, and `terraform-output.json` |

`retail-setup verify` unifies these signals without persisting source rows.
For required machine learning, it checks the publication time and the
identifier of the run that produced the same row. Business “as of” fields
describe the source period; they do not prove when the model output was
published.
Setup and model evidence may be at most seven days old. Streaming watermarks,
Eventhouse ingestion, and checkpoint evidence may be at most 30 minutes old.
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
| Setup pipeline not started | Start `setup-pipeline` manually and retain its run ID. |
| Setup pipeline failed | Resume from the first failed activity only after validating upstream tables. |
| Required ML gate failed | Inspect producer/validator errors; Reporting is intentionally unpublished until a new exact run succeeds. |
| Ontology, Data Agent, or task-flow links missing | Run `retail-setup post-ontology --env <env>` to recreate any missing ontology, republish agents, and verify the exact persisted graph. |
| Live rows absent | Verify notebook parameters, resolved Query URI, KQL permissions, connector errors, and ingestion timestamps. |
| Silver data stale | Inspect Eventhouse shortcuts, transform run history, source timestamps, and `ag._watermarks`. |
| Gold data stale | Confirm the Silver run completed, then run the Gold transform. |
| Power BI cannot see newly created tables | Run `python -m deploy.scripts.refresh_sql_endpoint --environment <env>`, then reload the report and rerun readiness. |
| Power BI shows the wrong workspace or data | Confirm required tables exist and the Direct Lake binding targets the intended Lakehouse. |
| Local validation passed but live assets fail | Treat the deploy as not ready and perform item, binding, KQL, run, and data checks. |
| Verifier reports `UNKNOWN` | Restore the missing permission, locked dependency, output, journal, or live evidence; required unknowns are failures. |
| Verifier reports stale evidence | Run only the owning workload, retain its exact run evidence, and rerun the read-only verifier. |

## Rerun safely

For a normal update:

1. preserve the failed or prior run identifiers;
2. confirm the target environment and workspace;
3. rerender notebooks when configuration or source changed;
4. preview the deployment plan;
5. deploy in place without recreate;
6. rerun only affected data workloads;
7. compare row counts, timestamps, and bindings with the prior known-good run.

Each workspace environment has isolated Terraform state and backend data.
Concurrent full publication still requires separate checkouts because artifact
staging uses one `deploy/workspace/` tree.

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
freshness checks remain `UNKNOWN` and overall readiness remains `DEGRADED`.
