# Operations guide

This guide summarizes routine operation. Exact failure and recovery behavior is
owned by the [operations runbook](../design/specifications/modules/operations/runbook.md).

## Readiness checklist

Before a demo:

1. Confirm the signed-in tenant and workspace.
2. Confirm required Fabric items and KQL objects exist.
3. Confirm setup notebooks or the setup pipeline completed.
4. Confirm expected `ag` and `au` tables are populated.
5. Confirm the semantic model is bound to the intended Lakehouse.
6. Confirm optional ML, ontology, agent, dashboard, and rule surfaces are ready
   before including them.
7. Run a bounded stream and confirm Eventhouse ingestion.

## Monitor

Use Fabric Monitoring Hub and item run histories for pipelines and notebooks.
Correlate them with:

- `setup_run_log` for historical setup.
- `ag._watermarks` for streaming-to-Silver progress.
- Eventhouse ingestion timestamps and row counts.
- Gold refresh timestamps or representative max event time.
- Model/run identifiers for optional predictive output.
- Pending alert/action state where deployed.

These signals are not yet unified; see
[IMP-013](../design/requirements/modules/operations/backlog.md#imp-013).

## Quick KQL checks

```kql
receipt_created
| summarize rows = count(), latest = max(ingest_timestamp)
```

```kql
union withsource=table_name receipt_created, payment_processed,
  inventory_updated, online_order_created
| summarize rows = count(), latest = max(ingest_timestamp) by table_name
| order by table_name asc
```

Use table-specific timestamps when a query depends on event time rather than
ingestion time.

## Common recovery paths

| Symptom | First action |
| --- | --- |
| CLI target or auth failure | Reconfirm tenant, auth mode, environment, workspace, and KQL database. |
| Rendered notebooks missing | Run `retail-setup render --env <env>`. |
| KQL objects missing | Review and run the generated ordered database script against the intended KQL database. |
| Setup pipeline not started | Start `setup-pipeline` manually and retain its run ID. |
| Ontology/task-flow links missing | Complete ontology creation, then redeploy/rebind the task flow and dependent agent. |
| Live rows absent | Verify notebook parameters, resolved Query URI, KQL permissions, and recent notebook errors. |
| Silver data stale | Inspect Eventhouse shortcuts, transform run, and `ag._watermarks`. |
| Gold data stale | Confirm the Silver run completed, then run the Gold transform. |
| Power BI errors | Confirm required tables exist and the Direct Lake binding targets the correct Lakehouse. |

## Reset and recreate

Use destructive operations only after:

- validating the selected environment and live target;
- retaining run, failure, and recovery evidence;
- confirming which data and items will be removed;
- receiving explicit operator confirmation.

`99-reset-lakehouse` is a manual destructive asset. It is not part of the normal
pipeline.

## Capacity

Runtime depends on store count, history months, Fabric capacity, Spark pool,
notebook groups, and optional ML/ontology work. Begin with a bounded
configuration and scale after observing Spark, Eventhouse, and Power BI
utilization. Do not promise fixed runtimes without a measured profile.

## Known reliability work

- Fail-fast and replay safety:
  [IMP-002](../design/requirements/modules/operations/backlog.md#imp-002)
- Environment isolation:
  [IMP-004](../design/requirements/modules/deployment/backlog.md#imp-004)
- Active-path CI:
  [IMP-014](../design/requirements/modules/operations/backlog.md#imp-014)
