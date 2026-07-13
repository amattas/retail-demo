# Getting started

This guide creates a new Fabric demo workspace through the supported
Fabric-native path.

## Prerequisites

- Git.
- Python 3.11 or later.
- A Microsoft Fabric tenant and capacity.
- Permission to create or update the target workspace and items.
- Terraform plus Azure CLI or Azure PowerShell for automated provisioning.

Fabric supplies the Spark runtime. Local PySpark is needed only for development
and selected tests.

## 1. Clone and run guided setup

=== "Windows PowerShell"

    ```powershell
    git clone https://github.com/amattas/retail-demo.git
    Set-Location retail-demo
    .\scripts\setup.ps1 --env dev
    ```

=== "macOS or Linux"

    ```bash
    git clone https://github.com/amattas/retail-demo.git
    cd retail-demo
    ./scripts/setup.sh --env dev
    ```

The bootstrap selects or creates a Python environment, installs the supported
utility, runs configuration, renders notebooks, and can start deployment.
Package-manager support varies by operating system; install missing tools
manually if the guided installer cannot do so.

To manage Python yourself:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .\utility
python .\scripts\setup.py --env dev
```

## 2. Configure the environment

Interactive:

```powershell
retail-setup configure --env dev
```

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

`--months` is the supported historical-range input. The generated range ends
yesterday so live streaming can begin today.

Configuration writes shared/environment deployment YAML and ignored local
generation settings. See the [CLI specification](../specifications/modules/setup/cli.md).

## 3. Render setup notebooks

```powershell
retail-setup render --env dev
```

The command writes five notebooks under `utility/out/`:

1. `setup-01-seed-dictionaries.ipynb`
2. `setup-02-generate-dimensions.ipynb`
3. `setup-03-generate-facts.ipynb`
4. `setup-04-build-gold.ipynb`
5. `stream-events.ipynb`

## 4. Preview and deploy

```powershell
retail-setup deploy --env dev --dry-run
retail-setup deploy --env dev --yes
```

The deploy path provisions or resolves Fabric resources, stages supported item
folders, publishes with `fabric-cicd`, applies ordered KQL scripts, validates
local deployment output, and offers to start the setup pipeline.

Review the current limitations before relying on a live deploy:

- Authentication/target propagation repair: [IMP-001](../requirements/modules/deployment/backlog.md#imp-001)
- Environment isolation: [IMP-004](../requirements/modules/deployment/backlog.md#imp-004)
- Live readiness validation: [IMP-013](../requirements/modules/operations/backlog.md#imp-013)

See the [deployment specification](../specifications/modules/deployment/framework.md).

## 5. Run historical setup

Run setup notebooks 01 through 04 in order, either through the setup pipeline or
manually. Expected base output:

- Silver schema `ag`: seven dimensions, eighteen fact tables, and run metadata.
- Gold schema `au`: nine aggregate tables.

Confirm the table contract against
[`schemas.py`](https://github.com/amattas/retail-demo/blob/main/utility/src/retail_setup/generation/schemas.py)
and the [data-contract specification](../specifications/modules/generation/data-contract.md).

## 6. Start optional live streaming

Open `stream-events.ipynb` in Fabric and set:

- `sink = "eventhouse"`
- `kql_database` to the deployed KQL database display name
- `kusto_uri` blank to resolve the Query URI in the current workspace
- `source_rows_per_second` and `run_seconds` for the demo volume

The notebook writes typed micro-batches directly to Eventhouse through the Spark
Kusto connector. It does not require Kafka, Event Hubs, or a Fabric Eventstream.

Run the streaming-to-Silver and streaming-to-Gold path only after the Eventhouse
shortcuts and required tables exist. See the
[event contract](../specifications/modules/streaming/event-contract.md).

## 7. Open analytics surfaces

- Run KQL directly or use the deployed queryset.
- Import or deploy validated dashboard assets only when their bindings are
  configured.
- Open `fabric/powerbi/retail_model.pbip` for the Direct Lake report.
- Treat ML, ontology, and data-agent surfaces as optional and capability-gated.

## 8. Validate readiness

Use the [operations guide](operations.md) and
[runbook](../specifications/modules/operations/runbook.md). Do not treat a local
staging validation as proof that the workspace is usable.
