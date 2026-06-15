# Microsoft Fabric Real-Time Intelligence Retail Demo

This repository contains a Microsoft Fabric retail demo that generates synthetic
retail data, deploys Fabric assets, and builds Lakehouse Silver/Gold tables plus
a Power BI semantic model.

The current supported setup path is the Fabric-native `retail-setup` utility in
`utility\`. The older FastAPI/DuckDB generator is kept under `datagen-deprecated\`
for reference and compatibility investigations, but it is not the recommended
path for a new workspace.

## What you get

- A Fabric workspace with a Lakehouse, Eventhouse, KQL database, notebooks,
  semantic model, and report artifacts.
- Setup notebooks that generate deterministic synthetic retail data directly in
  Fabric Spark.
- Lakehouse Silver tables in schema `ag`: dimensions, `dim_date`, 18 fact
  tables, and `setup_run_log`.
- Lakehouse Gold tables in schema `au`: 9 aggregate tables for reporting.
- Optional live event generation through `setup-05-stream-events.ipynb`.

## Prerequisites

Required for notebook render/manual import:

- Python 3.11 or later.
- Git.
- A Microsoft Fabric tenant, capacity, and workspace permissions.

Required for automated deployment:

- Terraform on `PATH`.
- Azure CLI or Azure PowerShell authenticated to the target tenant.
- Python packages `azure-identity` and `fabric-cicd`.
- Permission to create or update the target Fabric workspace, Lakehouse,
  Eventhouse, KQL database, semantic model, and report.

Fabric provides the Spark runtime used by the setup notebooks. Local PySpark is
only needed for utility development/tests.

## New workspace walkthrough

Run these commands from PowerShell unless noted otherwise.

### 1. Clone and run the guided setup

```powershell
git clone https://github.com/amattas/retail-demo.git
Set-Location retail-demo
python .\scripts\setup.py
```

The guided setup detects Windows, macOS, or Linux; offers to install missing
CLI prerequisites with the OS package manager; installs Python dependencies into
the environment that launched the script; runs `retail-setup configure`; renders
notebooks; and finally asks whether to deploy.

Use `--env` to select the deployment environment file under
`deploy\config\environments\`. For example, `--env dev` uses
`deploy\config\environments\dev.yml` and writes generated deployment files under
`deploy\.generated\dev\`.

```powershell
python .\scripts\setup.py --env dev
python .\scripts\setup.py --env dev --deploy
python .\scripts\setup.py --env dev --dry-run
```

### 2. Manual install path

Use this path if you prefer to create or activate an environment yourself before
running setup. If you use conda, activate the conda environment first; if you use
venv, create and activate it first.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -e .\utility
```

For automated deployment, also install the deployment helpers:

```powershell
python -m pip install azure-identity fabric-cicd
```

### 3. Configure the target workspace and data generation

Interactive:

```powershell
retail-setup configure
```

The interactive prompts show current config/default values in brackets and list
the available store types.

Non-interactive example:

```powershell
retail-setup configure `
  --env dev `
  --tenant-id 00000000-0000-0000-0000-000000000000 `
  --workspace-name retail-demo-dev `
  --capacity-name F64 `
  --lakehouse-name retail_lakehouse `
  --eventhouse-name retail_eventhouse `
  --kql-database-name retail_kql `
  --store-type supercenter `
  --start-date 2025-01-01 `
  --end-date 2025-03-31 `
  --store-count 50 `
  --seed 42
```

This updates:

- `deploy\config\deploy.yml`
- `deploy\config\environments\dev.yml`
- `utility\config.yaml`

`utility\config.yaml` is intentionally ignored by Git because it contains local
environment choices.

### 4. Render the setup notebooks

```powershell
retail-setup render --env dev
```

This writes rendered setup notebooks to `utility\out\`:

- `setup-01-seed-dictionaries.ipynb`
- `setup-02-generate-dimensions.ipynb`
- `setup-03-generate-facts.ipynb`
- `setup-04-build-gold.ipynb`

### 5. Deploy or import the artifacts

Manual path:

1. Create or open the Fabric workspace.
2. Create the target Lakehouse using the same name passed to
   `--lakehouse-name`.
3. Import the rendered notebooks from `utility\out\`.
4. Attach each notebook to the target Lakehouse.

Automated path:

```powershell
retail-setup deploy --env dev --dry-run
retail-setup deploy --env dev --yes
```

`retail-setup deploy` renders the deployment plan, runs Terraform unless
`--skip-terraform` is used, stages Fabric item folders, deploys supported items
with `fabric-cicd`, and writes a combined KQL database script to
`deploy\.generated\dev\database.kql`.

The KQL script is not executed automatically. Open the generated script and run
it in the target Fabric KQL database after the Eventhouse/KQL database exists.

### 6. Run the setup notebooks in Fabric

Run these notebooks in order:

1. `setup-01-seed-dictionaries` seeds dictionary JSON under
   `Files/setup/dictionaries`.
2. `setup-02-generate-dimensions` writes dimension tables and `dim_date`.
3. `setup-03-generate-facts` writes the full Silver data contract and
   `setup_run_log`.
4. `setup-04-build-gold` builds the 9 Gold aggregate tables from persisted
   Silver facts.

Expected Lakehouse output:

- Silver schema `ag`: `dim_geographies`, `dim_stores`,
  `dim_distribution_centers`, `dim_trucks`, `dim_customers`, `dim_products`,
  `dim_date`, 18 `fact_*` tables, and `setup_run_log`.
- Gold schema `au`: `sales_minute_store`, `top_products_15m`,
  `inventory_position_current`, `dc_inventory_position_current`,
  `truck_dwell_daily`, `online_sales_daily`, `zone_dwell_minute`,
  `marketing_cost_daily`, and `tender_mix_daily`.

### 7. Optional live event generation

`setup-05-stream-events.ipynb` is committed under `utility\notebooks\`, but it is
not currently rendered to `utility\out\` or staged by `retail-setup deploy`.
Import it manually if you want a live stream driver.

The notebook can write to:

- a Fabric Eventstream Custom Endpoint (`sink = "eventstream"`), or
- a Delta landing table (`sink = "delta"`) for smoke testing.

Set its parameters in Fabric before running: `source_rows_per_second`, `sink`,
`run_seconds`, and Eventstream connection settings.

## Project structure

```text
retail-demo\
├── utility\              # Current Fabric-native setup utility and notebooks
├── deploy\               # Terraform/fabric-cicd deployment framework
├── fabric\               # Fabric source assets, KQL, Lakehouse, Power BI
├── datagen-deprecated\   # Legacy FastAPI/DuckDB/Event Hub generator
└── scripts\              # Supporting scripts for semantic model/local state
```

## More documentation

- `utility\README.md` — detailed setup utility usage.
- `deploy\README.md` — deployment framework details.
- `fabric\lakehouse\README.md` — Lakehouse notebook groups and outputs.
- `fabric\kql_database\README.md` — KQL database scripts and expected event
  tables.

All generated data is synthetic and for demo purposes only.
