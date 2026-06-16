# Pipelines

Data Pipelines orchestrating the medallion notebooks, setup, and scheduled
processing. `fabric/pipelines/` contains **Git-integration `.DataPipeline`
items** (`.platform`, `pipeline-content.json`, and an optional `.schedules`) that
`retail-setup deploy` publishes directly into the workspace.

## Pipeline Summary

| Pipeline | Recommended Schedule | Activities |
|----------|---------------------|------------|
| `setup-pipeline` | On demand | `setup-00-apply-kql` → `setup-01` → `setup-02` → `setup-03` → `setup-04` |
| `historical-data-load` | Once (manual) | `02-historical-data-load` |
| `streaming-data-load` | Cron (every 5 minutes) | `03-streaming-to-silver` → `04-streaming-to-gold` (sequential) |
| `daily-maintenance` | Daily 00:00 | `05-maintain-delta-tables` |
| `machine-learning` | Daily/weekly (after maintenance) | ML notebooks `06`–`14` |

Notebook activities use a 12-hour timeout and no retries. Schedules present in
`.schedules` (cron/daily) deploy with the pipeline; others are configured as
triggers in the Fabric portal.

## Deployment

`retail-setup deploy` stages each pipeline whose notebooks are part of the
deploy into a **Pipelines** workspace folder, and adds `DataPipeline` to the
fabric-cicd scope so they publish automatically. `setup-pipeline` is the
exception — it publishes into the **Setup** folder alongside the setup notebooks
it orchestrates. With the default `core setup ml` groups, all five pipelines
deploy.

Each activity references its notebook by the **source** workspace's
`notebookId`/`workspaceId`. `deploy/fabric-cicd/parameter.yml` remaps them to the
target at publish time (generated from the pipeline sources):

- `workspaceId` → `$workspace.$id`
- each `notebookId` GUID → `$items.Notebook.<activity-name>.$id`

fabric-cicd publishes Notebooks before Data Pipelines, so the references resolve.

After a successful deploy, `retail-setup deploy` offers to **run `setup-pipeline`
now** (via the Fabric Job Scheduler API) to apply the KQL setup and generate the
dimensions, facts, and Gold tables.

`setup-pipeline` is authored in this repo; its first step `setup-00-apply-kql` is
a notebook **generated** from `fabric/kql_database/*.kql` that applies the
Eventhouse KQL setup with the Kusto Python SDK (`azure-kusto-data`). The other
four pipelines were exported from a live workspace with
`deploy.scripts.export_pipelines`.

## Re-exporting

Refresh the exported pipelines from a live workspace (reuses your Azure CLI
login):

```powershell
python -m deploy.scripts.export_pipelines --workspace-name "Retail Demo" --output-dir fabric/pipelines
```

## Pipeline Details

### setup-pipeline

One-shot environment bootstrap: applies the Eventhouse KQL schema
(`setup-00-apply-kql`), then runs the rendered setup notebooks in order to seed
dictionaries, generate dimensions and facts, and build the Gold tables.

The `setup-00-apply-kql` activity sets the boolean parameter
`_inlineInstallationEnabled = True` so its `%pip install azure-kusto-data` runs
during the pipeline — Fabric disables inline installs in pipeline runs by
default.

### historical-data-load

One-time load of historical batch data from `Files/` parquet shortcuts through
Silver (`ag`) and Gold (`au`). Runs `02-historical-data-load.ipynb`.

### streaming-data-load

The streaming refresh loop: `03-streaming-to-silver.ipynb` (watermark-based
incremental load from Eventhouse `cusn` shortcuts), then
`04-streaming-to-gold.ipynb` (Gold aggregation rebuild). Cron schedule.

### daily-maintenance

Runs `05-maintain-delta-tables.ipynb`: Delta `OPTIMIZE` (with ZORDER) and
`VACUUM` across Silver and Gold. Daily.

### machine-learning

Runs the ML notebooks `06`–`14`. `14-ml-dynamic-pricing` depends on
`10-ml-promotion-effectiveness` (it consumes `au.price_elasticity`); if
elasticity data is unavailable, notebook 14 falls back to rule-based constrained
pricing. See [Phase 9: ML Notebooks](../setup/09-ml-notebooks.md).

## Monitoring

- **Fabric Portal** → Data Factory → Pipelines → View runs
- Check execution status, duration, and error logs
