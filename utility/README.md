# retail-setup

`retail-setup` is the current Fabric-native setup utility for this repository.
It configures a target Fabric environment, renders setup notebooks, and can call
the deployment framework for Terraform/fabric-cicd deployment.

Use this utility for new workspaces. `datagen-deprecated\` is retained for
legacy reference only.

## Supported workflow

1. Run the guided setup script, or install the utility manually.
2. Run `retail-setup configure` to write deployment and generation settings.
3. Run `retail-setup render` to produce workspace-specific setup notebooks.
4. Import notebooks manually, or run `retail-setup deploy`.
5. Run setup notebooks 01-04 in Fabric.
6. Optionally run `stream-events.ipynb` (the live streaming driver).

## Prerequisites

Required:

- Python 3.11 or later.
- A Microsoft Fabric tenant and capacity.
- Permission to create or update the target Fabric workspace and items.

Required only for `retail-setup deploy`:

- Terraform on `PATH`.
- Azure CLI or Azure PowerShell authenticated to the target tenant.
- `azure-identity` and `fabric-cicd` installed in the active Python environment.

Fabric provides Spark for the notebooks. Local PySpark is only needed for
development tests.

## Guided setup

On Windows, from the repository root:

```powershell
.\scripts\setup.ps1
```

`setup.ps1` works even with nothing installed: it uses Python 3.11+ if present,
otherwise installs Miniforge with winget and creates a conda environment, then
delegates to `scripts\setup.py`. If you already have Python 3.11+ (Windows,
macOS, or Linux), run `python ./scripts/setup.py` directly to skip the Miniforge
download.

The guided setup:

1. Detects Windows, macOS, or Linux.
2. Offers to install missing CLI prerequisites with the OS package manager
   (`winget`/Chocolatey, Homebrew, `apt-get`, `dnf`, or `yum`).
3. Uses the Python environment that launched the script.
4. Installs Python dependencies into that environment.
5. Runs `retail-setup configure`.
6. Runs `retail-setup render`.
7. Asks whether to run `retail-setup deploy`. Before deploying, it always signs
   in to the configured Azure tenant — `az login --tenant <tenant_id>` for
   `auth.mode: azure_cli`, or `Connect-AzAccount -Tenant <tenant_id>` for
   `auth.mode: azure_powershell` — so deployment never runs under the wrong
   account.

Examples:

```powershell
.\scripts\setup.ps1 --env dev
.\scripts\setup.ps1 --env dev --deploy
.\scripts\setup.ps1 --env dev --dry-run
```

`--env` selects `deploy\config\environments\<env>.yml` and generated outputs
under `deploy\.generated\<env>\`. Use separate environment names for separate
workspace targets, such as `dev`, `test`, or `prod`.

## Manual install

From the repository root:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .\utility
```

If you prefer conda, activate your conda environment before running
`python .\scripts\setup.py` or before installing the package manually.

For automated deployment:

```powershell
python -m pip install azure-identity fabric-cicd
```

Confirm the CLI is available:

```powershell
retail-setup --help
```

## Configure

`configure` writes both deployment and data-generation settings:

- `deploy\config\deploy.yml`
- `deploy\config\environments\<env>.yml`
- `utility\config.yaml`

`utility\config.yaml` and `utility\out\` are ignored by Git because they contain
local environment-specific values.

Interactive:

```powershell
retail-setup configure
```

Interactive prompts show the current config/default value in brackets. The
store type prompt also lists the available dictionary profiles:
`grocery`, `hardware`, `luxury`, and `supercenter`. After you pick the
generation settings, `configure` prints an approximate **record-count estimate**
(in-store receipts and lines, online orders, payments, customers) so you can
gauge the output volume and runtime; interactively it then asks whether to use
the settings or re-enter them.

You choose **how many months** of history to generate rather than an explicit
date range. The window **ends yesterday**, so real-time streaming continues
seamlessly from today (`start_date`/`end_date` are derived from `months`).

Non-interactive example:

```powershell
retail-setup configure `
  --env dev `
  --tenant-id 00000000-0000-0000-0000-000000000000 `
  --workspace-name retail-demo-dev `
  --capacity-name F64 `
  --lakehouse-name retail_lakehouse `
  --eventhouse-name retail_eventhouse `
  --kql-database-name retail_eventhouse `
  --store-type supercenter `
  --months 3 `
  --store-count 50 `
  --seed 42
```

### Generation defaults and validation

The persisted generation settings are:

| Setting | Meaning |
| --- | --- |
| `store_type` | Dictionary/profile to use: `grocery`, `hardware`, `luxury`, or `supercenter`. |
| `months` | Months of historical data to generate. The window ends yesterday; `start_date`/`end_date` are derived from it (1–120). |
| `store_count` | Number of stores to generate. Must be between 1 and 2000. |
| `seed` | Deterministic random seed. |

Derived defaults are applied by the engine:

| Derived setting | Default |
| --- | --- |
| `silver_db` | `ag` |
| `gold_db` | `au` |
| `dc_count` | `max(1, store_count // 10)` |
| `customer_count` | `store_count * 1000` |
| `online_orders_per_day` | `store_count * 8` |
| `transactions_per_store_day` | `400` |
| `return_rate` | `0.01` |
| `brands_per_product` | `3` |
| `truck_capacity` | `15000` |

## Render setup notebooks

```powershell
retail-setup render --env dev
```

By default, render pins dictionaries to the current Git `HEAD` SHA. Use `--ref`
to pin a specific ref:

```powershell
retail-setup render --env dev --ref main
```

Rendered notebooks are written to `utility\out\`:

- `setup-01-seed-dictionaries.ipynb`
- `setup-02-generate-dimensions.ipynb`
- `setup-03-generate-facts.ipynb`
- `setup-04-build-gold.ipynb`

These notebooks receive the nine render tokens:

| Token | Source |
| --- | --- |
| `{{LAKEHOUSE_NAME}}` | `deploy\config` lakehouse name for the target environment |
| `{{SILVER_DB}}` | generation config (`ag` by default) |
| `{{GOLD_DB}}` | generation config (`au` by default) |
| `{{STORE_TYPE}}` | generation config |
| `{{START_DATE}}` | generation config |
| `{{END_DATE}}` | generation config |
| `{{STORE_COUNT}}` | generation config |
| `{{SEED}}` | generation config |
| `{{DICTIONARY_REF}}` | current Git SHA, or `--ref` |

## Manual Fabric import

Use this path when you already have a workspace and Lakehouse, or when you do
not want Terraform/fabric-cicd deployment.

1. Create or open the target Fabric workspace.
2. Create the Lakehouse using the same name passed to `--lakehouse-name`.
3. Import the rendered notebooks from `utility\out\`.
4. Attach each notebook to the target Lakehouse.
5. Run setup notebooks 01-04 in order.

## Automated deploy

Use this path for a new workspace or for repeatable artifact deployment.

Preview first:

```powershell
retail-setup deploy --env dev --dry-run
```

Run the full deployment:

```powershell
retail-setup deploy --env dev --yes
```

Skip Terraform when the workspace/resources already exist and you only want to
stage/deploy supported Fabric items:

```powershell
retail-setup deploy --env dev --skip-terraform
```

For a clean slate, `--recreate` destroys the existing workspace (and every item
in it), waits 30 seconds, then recreates everything. This is destructive and is
gated by a confirmation prompt:

```powershell
retail-setup deploy --env dev --recreate
```

The deploy command runs these steps in order:

1. Generate Terraform and fabric-cicd config files.
2. Run `terraform init`, `terraform plan`, and `terraform apply` unless
   `--skip-terraform` is set.
3. Capture Terraform outputs.
4. Stage Fabric source-control item folders, including rendered setup notebooks
   01-04.
5. Deploy staged items through `fabric-cicd`.
6. Write a combined KQL database script to
   `deploy\.generated\<env>\database.kql`.
7. Run offline deployment-file validation.

The KQL script is not executed automatically. Open the generated
`.execute database script` payload and run it in the target Fabric KQL database
after the Eventhouse and KQL database exist.

## Run setup notebooks in Fabric

Run these in order:

1. `setup-01-seed-dictionaries` seeds dictionary JSON under
   `Files/setup/dictionaries`.
2. `setup-02-generate-dimensions` writes dimensions and `dim_date`.
3. `setup-03-generate-facts` writes the full Silver contract and
   `setup_run_log`.
4. `setup-04-build-gold` builds the Gold aggregate tables from persisted facts.

Expected output:

| Layer | Schema | Tables |
| --- | --- | --- |
| Silver | `ag` | 6 dimensions, `dim_date`, 18 `fact_*` tables, `setup_run_log` |
| Gold | `au` | 9 aggregate tables |

Gold tables:

- `sales_minute_store`
- `top_products_15m`
- `inventory_position_current`
- `dc_inventory_position_current`
- `truck_dwell_daily`
- `online_sales_daily`
- `zone_dwell_minute`
- `marketing_cost_daily`
- `tender_mix_daily`

## Optional live stream notebook

`stream-events.ipynb` is committed under `utility\notebooks\`. It is rendered to
`utility\out\` alongside the setup notebooks and staged by `retail-setup deploy`
(the `stream` notebook group) into the **Streaming** workspace folder. It is the
optional long-running live driver, started/stopped manually — not part of the
ordered setup pipeline.

The notebook emits the same 18 event type names as the batch pipeline and writes
each event **directly to its Eventhouse KQL table** with the Fabric Spark
connector for Kusto (`com.microsoft.kusto.spark.synapse.datasource`), splitting
each micro-batch by `event_type` inside `foreachBatch`. This follows the RTI
tutorial *Use a notebook with Apache Spark to query a KQL database*
(https://learn.microsoft.com/fabric/real-time-intelligence/spark-connector). It
can write to:

- `sink = "eventhouse"` (default): the Eventhouse KQL event tables.
- `sink = "delta"`: a Lakehouse landing table for smoke testing.

Set these notebook parameters before running:

| Parameter | Meaning |
| --- | --- |
| `source_rows_per_second` | Spark rate-source rows per second. Each row emits one scenario bundle. |
| `sink` | `eventhouse` or `delta`. |
| `run_seconds` | `0` runs forever; a positive value stops after N seconds. |
| `kusto_uri` | KQL database **Query URI** (copy from the KQL database details card). |
| `kql_database` | KQL database name (default `retail_eventhouse`). |

The KQL event tables must already exist (created by the KQL setup); the notebook
writes with `tableCreateOptions=FailIfNotExist`, and its identity needs ingestor
rights on the database.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `utility\config.yaml not found` | Run `retail-setup configure` before `render`. |
| `store_type ... not found` | Use one of `grocery`, `hardware`, `luxury`, or `supercenter`. |
| `setup notebooks not rendered` during deploy | Run `retail-setup render --env <env>` first. |
| Terraform is not found | Install Terraform and ensure it is on `PATH`, or use `--skip-terraform`. |
| `fabric_cicd` or `azure.identity` import error | Run `python -m pip install azure-identity fabric-cicd`. |
| KQL tables are missing after deploy | Run `deploy\.generated\<env>\database.kql` manually in the target KQL database. |
| Rendered notebooks still show `{{TOKEN}}` | Re-run `retail-setup render`; rendering fails if required tokens remain. |

## Development

Install dev dependencies:

```powershell
Set-Location utility
python -m pip install -e ".[dev]"
```

Run utility tests from `utility\`:

```powershell
python -m pytest -q
```

Rebuild committed notebooks from templates:

```powershell
python scripts\build_notebooks.py
python scripts\build_notebooks.py --check
```

On Windows, use Python 3.11 for local PySpark tests. Fabric notebook execution
does not require a local Spark installation.
