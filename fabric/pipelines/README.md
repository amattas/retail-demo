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

## Deployment

`retail-setup deploy` publishes these pipelines into a **Pipelines** workspace
folder. A pipeline is staged **only when every notebook it orchestrates is part
of the deploy** — so with the default `core setup` groups the
`historical-data-load`, `streaming-data-load`, and `daily-maintenance` pipelines
publish, while `machine-learning` publishes only when the `ml` notebook group is
included. This keeps every `$items.Notebook.<name>.$id` reference resolvable.

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
