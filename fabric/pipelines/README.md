# Pipelines

Fabric Data Pipeline definitions for the retail demo, stored as source-control
items (`<name>.DataPipeline/` with `.platform`, `pipeline-content.json`, and an
optional `.schedules`). These were exported from the **Retail Demo** Fabric
workspace and use the Git-integration format that fabric-cicd publishes.

## Pipelines

| Pipeline | Orchestrates | Schedule |
| --- | --- | --- |
| `historical-data-load` | `02-historical-data-load` | On demand |
| `streaming-data-load` | `03-streaming-to-silver`, `04-streaming-to-gold` | Cron |
| `daily-maintenance` | `05-maintain-delta-tables` | Daily 00:00 |
| `machine-learning` | `06`–`14` ML notebooks | On demand |

## Re-exporting from Fabric

To refresh these definitions from a live workspace, use the export script. It
reuses your Azure CLI login (`auth.mode: azure_cli`), so no extra sign-in is
required:

```powershell
python -m deploy.scripts.export_pipelines --workspace-name "Retail Demo" --output-dir fabric/pipelines
```

The official Microsoft Fabric CLI (`fab`) does the same export, but maintains
its **own** login separate from `az` (run `fab auth login` first):

```powershell
fab auth login
fab export "Retail Demo.Workspace/daily-maintenance.DataPipeline" -o fabric/pipelines -f
```

## Deployment status

These pipelines are **not yet** published by `retail-setup deploy`. Each
activity references a notebook by `notebookId` and `workspaceId` that point at
the **source** workspace (`Retail Demo`). Before deploying to another workspace
(for example `retail-demo-dev`), those references must be remapped to the target:

- `workspaceId` → `$workspace.$id`
- `notebookId` → `$items.Notebook.<activity-name>.$id`

The activity `name` matches the notebook display name, which makes the remap
mechanical. `deploy/fabric-cicd/parameter.yml` already carries example
`key_value_replace` rules for `DataPipeline`; wiring the full set (and adding
`DataPipeline` to `build_artifacts` staging and `item_types_in_scope`) is the
remaining step to publish them automatically.

## Recommended setup sequence

For a clean workspace, run the rendered setup notebooks manually first:

| Order | Notebook | Purpose |
| --- | --- | --- |
| 1 | `setup-01-seed-dictionaries` | Seed dictionary JSON under `Files/setup/dictionaries`. |
| 2 | `setup-02-generate-dimensions` | Generate dimension tables and `dim_date`. |
| 3 | `setup-03-generate-facts` | Generate the Silver fact tables and `setup_run_log`. |
| 4 | `setup-04-build-gold` | Build the Gold aggregate tables. |
