# Lakehouse Notebooks

This folder contains Fabric Lakehouse notebooks for the legacy medallion flow,
streaming transforms, maintenance, ML, and manual administration.

For a new workspace, the primary historical setup notebooks come from
`utility\out\` after running:

```powershell
retail-setup configure
retail-setup render --env dev
```

Run the rendered setup notebooks in Fabric:

1. `setup-01-seed-dictionaries`
2. `setup-02-generate-dimensions`
3. `setup-03-generate-facts`
4. `setup-04-build-gold`

These write directly to the Lakehouse:

- Silver schema `ag`: 6 dimensions, `dim_date`, 18 fact tables, and
  `setup_run_log`.
- Gold schema `au`: 9 aggregate tables for reporting.

## Notebook groups in this folder

- `01-create-bronze-shortcuts.ipynb` through `05-maintain-delta-tables.ipynb`
  are the legacy/core medallion notebooks for shortcut-based Bronze/Silver/Gold
  operation and maintenance.
- `03-streaming-to-silver.ipynb` and `04-streaming-to-gold.ipynb` process
  Eventhouse event data into Silver/Gold.
- `06-ml-*` through `14-ml-*` are optional ML/advanced analytics notebooks.
- `30-create-ontology.ipynb` creates a Fabric ontology from core Silver retail
  tables.
- `90-augment-and-dedupe-receipts.ipynb` and `99-reset-lakehouse.ipynb` are
  manual utilities.

## Current setup-vs-legacy note

The `utility\` setup notebooks are Fabric-native and use the explicit schema
contract in `utility\src\retail_setup\generation\schemas.py`. The legacy
shortcut flow loads parquet columns from the deprecated generator and normalizes
only selected columns. Use `utility\` for new workspace data generation.
