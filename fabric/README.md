# Fabric Assets

This folder contains source assets for the Microsoft Fabric retail demo.

For a new workspace, start with the root `README.md` and `utility\README.md`.
The current setup path is:

1. Configure and render notebooks with `retail-setup`.
2. Deploy or manually import Fabric items.
3. Run setup notebooks 01-04 to generate Lakehouse data.
4. Run the generated KQL database script manually in the target KQL database.
5. Optionally import and run `utility\notebooks\stream-events.ipynb`
   for live synthetic events.

## Subfolders

- `lakehouse\` — Fabric notebooks for historical setup, streaming transforms,
  Gold aggregates, ML, and maintenance.
- `kql_database\` — KQL table, mapping, function, and materialized-view scripts.
- `powerbi\` — Power BI semantic model and report source files.
- `pipelines\` — Pipeline notes for orchestrating notebook execution.
- `dashboards\`, `querysets\`, `rules\` — Real-Time Intelligence source assets
  and runbook content as they become deployable.

## Current schema source of truth

For Fabric-native setup notebooks, the Lakehouse table contract is defined in:

```text
utility\src\retail_setup\generation\schemas.py
```

The legacy generator under `datagen-deprecated\` remains useful for reference,
but new workspaces should use the `utility\` setup notebooks.
