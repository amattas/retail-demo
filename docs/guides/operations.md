# Operations guide

This guide covers routine readiness, monitoring, and recovery. Provisioning and
publication are covered by [Deployment](deployment.md). Exact failure and
recovery behavior is owned by the
[operations runbook](../design/specifications/modules/operations/runbook.md).

## Readiness checklist

Before a demo:

1. Record the repository commit, environment, tenant, workspace, operator, and
   deployment run.
2. Confirm required Fabric items and KQL objects exist in that workspace.
3. Confirm setup notebooks or `setup-pipeline` completed successfully.
4. Confirm expected `ag` and `au` tables are populated.
5. Confirm the semantic model is bound to the intended Lakehouse.
6. Confirm optional ML, ontology, agent, dashboard, and rule surfaces are ready
   before including them.
7. Run a bounded stream and confirm recent Eventhouse ingestion when the live
   story is required.

Do not treat these as equivalent:

| Evidence | What it proves |
| --- | --- |
| Deploy command completed | Required local subprocesses returned success. |
| `validate_deployment.py` passed | Generated config and staged files passed offline checks. |
| Pipeline trigger returned success | Fabric accepted an asynchronous run request. |
| Pipeline/notebook run succeeded | The selected activities completed in Fabric. |
| Table row counts and timestamps advanced | Data reached the expected serving layer. |
| Report, ontology, or agent opened | The surface exists; its binding and answer still need validation. |

## Monitor

Use Fabric Monitoring Hub and item run histories for pipelines and notebooks.
Correlate them with:

- `setup_run_log` for historical setup;
- `ag._watermarks` for streaming-to-Silver progress;
- Eventhouse ingestion timestamps and row counts;
- Gold refresh timestamps or representative max event time;
- model/run identifiers for optional predictive output;
- pending alert/action state where deployed;
- the deployment commit and generated Terraform output used for the run.

These signals are not yet unified; see
[IMP-013](../design/requirements/modules/operations/backlog.md#imp-013).

## Quick KQL checks

Core stream freshness:

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
| Ontology/task-flow links missing | Complete ontology creation, then redeploy the task flow and dependent agent. |
| Live rows absent | Verify notebook parameters, resolved Query URI, KQL permissions, connector errors, and ingestion timestamps. |
| Silver data stale | Inspect Eventhouse shortcuts, transform run history, source timestamps, and `ag._watermarks`. |
| Gold data stale | Confirm the Silver run completed, then run the Gold transform. |
| Power BI errors | Confirm required tables exist and the Direct Lake binding targets the correct Lakehouse. |
| Local validation passed but live assets fail | Treat the deploy as not ready and perform item, binding, KQL, run, and data checks. |

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

`retail-setup deploy --recreate` destroys the workspace, waits 90 seconds, and
then rebuilds it. If Fabric deletion is still in progress, stop and verify the
resource state rather than repeatedly applying.

`99-reset-lakehouse` is a manual destructive asset. It is not part of the
normal pipeline.

## Capacity

Runtime depends on store count, history months, Fabric capacity, Spark pool,
notebook groups, and optional ML/ontology work. Begin with a bounded
configuration and scale after observing Spark, Eventhouse, and Power BI
utilization. Do not promise fixed runtimes without a measured profile.

## Known reliability work

- Live readiness and freshness:
  [IMP-013](../design/requirements/modules/operations/backlog.md#imp-013)
- Active-path CI:
  [IMP-014](../design/requirements/modules/operations/backlog.md#imp-014)
