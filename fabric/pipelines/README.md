# Pipelines

Fabric Data Pipeline definitions for the retail demo, stored as source-control
items (`<name>.DataPipeline/` with `.platform`, `pipeline-content.json`, and an
optional `.schedules`). These were exported from the **Retail Demo** Fabric
workspace and use the Git-integration format that fabric-cicd publishes.

## Pipelines

| Pipeline | Orchestrates | Schedule |
| --- | --- | --- |
| `setup-pipeline` | `setup-01`…`setup-04` (dimensions, facts, gold) | On demand |
| `historical-data-load` | `02-historical-data-load` | On demand |
| `streaming-data-load` | `03-streaming-to-silver`, `04-streaming-to-gold` | Cron |
| `daily-maintenance` | `05-maintain-delta-tables` | Daily 00:00 |
| `machine-learning` | `06`–`14` ML notebooks | On demand |

`setup-pipeline` is authored in this repo (not exported). It runs the rendered
setup notebooks in order to seed dictionaries and generate the dimension, fact,
and Gold tables. It publishes into the **Setup** workspace folder alongside those
notebooks (not the general **Pipelines** folder). After `retail-setup deploy`
completes, it offers to run `setup-pipeline` on demand (via
`deploy.scripts.run_pipeline`).

The Eventhouse KQL schema is **not** applied by this pipeline. `retail-setup
deploy` applies it directly with `deploy.scripts.apply_kql --execute`, using the
operator's Azure CLI credentials (which have Eventhouse admin rights) and the
Kusto Python SDK — see [deploy/README.md](../../deploy/README.md).

## Re-exporting from Fabric

To refresh these definitions from a live workspace, use the export script. It
reuses your Azure CLI login (`auth.mode: azure_cli`), so no extra sign-in is
required:

```powershell
python -m deploy.scripts.export_pipelines --workspace-name "Retail Demo" --output-dir fabric/pipelines
```

`export_pipelines` is a thin wrapper over the generic `deploy.scripts.export_items`
(`--item-type DataPipeline`), which exports any Fabric item type.

The official Microsoft Fabric CLI (`fab`) does the same export, but maintains
its **own** login separate from `az` (run `fab auth login` first):

```powershell
fab auth login
fab export "Retail Demo.Workspace/daily-maintenance.DataPipeline" -o fabric/pipelines -f
```

## Deployment

`retail-setup deploy` publishes these pipelines into a **Pipelines** workspace
folder — except `setup-pipeline`, which publishes into the **Setup** folder with
the setup notebooks it orchestrates. A pipeline is staged **only when every
notebook it orchestrates is part of the deploy** — so with the default
`core setup` groups the `historical-data-load`, `streaming-data-load`, and
`daily-maintenance` pipelines publish, while `machine-learning` publishes only
when the `ml` notebook group is included. This keeps every
`$items.Notebook.<name>.$id` reference resolvable.

Each activity references its notebook by the **source** workspace's
`notebookId`/`workspaceId`. `deploy/fabric-cicd/parameter.yml` remaps them to the
target at publish time (generated from these files by
`deploy.scripts.generate_configs`):

- `workspaceId` → `$workspace.$id` (one `key_value_replace`)
- each `notebookId` GUID → `$items.Notebook.<activity-name>.$id` (one
  `find_replace` per notebook; the activity `name` is the notebook display name)

fabric-cicd publishes Notebooks before Data Pipelines, so the notebook
references resolve. Add or refresh a pipeline by re-exporting (above) and
redeploying — the parameter rules regenerate automatically.

## Recommended setup sequence

For a clean workspace, run the rendered setup notebooks manually first:

| Order | Notebook | Purpose |
| --- | --- | --- |
| 1 | `setup-01-seed-dictionaries` | Seed dictionary JSON under `Files/setup/dictionaries`. |
| 2 | `setup-02-generate-dimensions` | Generate dimension tables and `dim_date`. |
| 3 | `setup-03-generate-facts` | Generate the Silver fact tables and `setup_run_log`. |
| 4 | `setup-04-build-gold` | Build the Gold aggregate tables. |
