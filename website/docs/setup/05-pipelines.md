# Phase 5: Pipelines

`retail-setup deploy` publishes Data Pipelines automatically into a **Pipelines**
workspace folder — no manual creation required. The pipelines orchestrate setup,
streaming, maintenance, and ML.

## What deploys

| Pipeline | Trigger | Notebook(s) | Purpose |
| --- | --- | --- | --- |
| `setup-pipeline` | On demand | `00-apply-kql`, setup `01`–`04` | Apply KQL schema, then build historical demo data |
| `historical-data-load` | Manual | `02-historical-data-load` | Load historical batch data |
| `streaming-data-load` | Cron (5 min) | `03-streaming-to-silver`, `04-streaming-to-gold` | Near-real-time Silver/Gold refresh |
| `daily-maintenance` | Daily | `05-maintain-delta-tables` | Delta maintenance |
| `machine-learning` | Daily/weekly | `06`–`14` | Train ML models |

A pipeline is staged only when every notebook it orchestrates is part of the
deploy, so its notebook references always resolve. The default `core setup ml`
groups deploy all five.

## Run the setup pipeline

After a successful deploy, `retail-setup deploy` asks:

> Run the setup pipeline now (apply KQL setup, then generate dimensions, facts,
> and gold)?

Answer **yes** to kick off `setup-pipeline` immediately (via the Fabric Job
Scheduler API). You can also run it later from the Fabric portal, or with:

```powershell
python -m deploy.scripts.run_pipeline --environment dev --pipeline setup-pipeline
```

## How references are remapped

Each notebook activity references a `notebookId`/`workspaceId` from the source
workspace. The deploy framework generates `parameter.yml` rules that rewrite them
to the target workspace (`$workspace.$id` and `$items.Notebook.<name>.$id`). See
[Fabric → Pipelines](../fabric/pipelines.md) for the full mechanism and the
re-export command.

## Next step

Continue to [Phase 6: Optional live streaming](06-streaming.md).
