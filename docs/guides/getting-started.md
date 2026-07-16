# Getting started

This guide creates a Microsoft Fabric Retail Demo workspace through the
supported Fabric-native path. It takes you from a clean clone to historical
Lakehouse data and an optional bounded Eventhouse stream.

For deployment modes, generated files, reruns, existing workspaces, and
recovery, use the [deployment guide](deployment.md).

!!! warning "Cost and destructive operations"

    Deployment creates or updates Microsoft Fabric items and can consume
    capacity. `--recreate` destroys the selected workspace and every item in it.
    Use a dedicated demo workspace, confirm the tenant and workspace name, and
    review the dry-run plan before applying changes.

## What the supported path creates

The default deployment currently includes more than the minimum historical
demo:

- a Fabric workspace, schema-enabled Lakehouse, Eventhouse, and default KQL
  database;
- rendered setup and streaming notebooks;
- data pipelines, KQL scripts, querysets, a Direct Lake semantic model, and a
  report;
- ML notebooks, ontology creation, Data Agent sources, and a manual reset
  notebook.

Setup notebooks 01 through 04 create the core historical data. Live streaming,
ML, ontology, agents, dashboards, and rules have separate readiness or manual
steps.

## 1. Check prerequisites

| Requirement | Why it is needed | Quick check |
| --- | --- | --- |
| Git | Clone the repository and resolve a dictionary revision | `git --version` |
| Python 3.11 or later | Run the bootstrap and `retail-setup` | `python --version` |
| Terraform 1.8 or later, below 2.0 | Provision or resolve Fabric resources | `terraform version` |
| Azure CLI | Required by the guided bootstrap and the default auth mode | `az version` |
| Fabric tenant and active capacity | Host the workspace, Spark, Eventhouse, and Power BI items | Confirm in the Fabric portal |
| Operator permissions | Create or update the workspace and its items, use the capacity, apply KQL, and start pipelines | Confirm with the tenant or capacity administrator |

The Windows and macOS/Linux wrappers can prepare Python and offer to install
Git, Terraform, and Azure CLI with a detected package manager. Install them
manually when the package manager cannot provide a supported package.

The lower-level deploy framework also supports Azure PowerShell authentication.
The guided bootstrap still checks for Azure CLI, so use the
[manual deployment path](deployment.md#authentication) for an Azure
PowerShell-only workstation.

### Capacity and Spark choice

The committed configuration enables a custom Spark pool sized for an F64
capacity. The custom-pool resource uses Fabric provider preview support.

- Choose the starter pool for the broadest compatibility.
- Choose the custom pool only when the target capacity and tenant support it.
- Start with a small history window and store count, then scale after measuring
  the setup run.

## 2. Clone the repository

=== "Windows PowerShell"

    ```powershell
    git clone https://github.com/amattas/retail-demo.git
    Set-Location retail-demo
    ```

=== "macOS or Linux"

    ```bash
    git clone https://github.com/amattas/retail-demo.git
    cd retail-demo
    ```

Use a clean clone or review existing local deployment configuration before
continuing. Workspace-specific target values are stored in ignored local
environment files.

## 3. Choose a setup path

### Guided bootstrap

Use this path for a first deployment.

=== "Windows PowerShell"

    ```powershell
    .\scripts\setup.ps1 --workspace-name retail-demo-alice
    ```

=== "macOS or Linux"

    ```bash
    ./scripts/setup.sh --workspace-name retail-demo-alice
    ```

The wrapper:

1. uses or creates a Python environment;
2. checks Git, Terraform, and Azure CLI;
3. installs `retail-setup`, `azure-identity`, `azure-kusto-data`, and
   `fabric-cicd`;
4. runs interactive configuration;
5. renders five workspace-specific notebooks;
6. offers to deploy.

To proceed directly to the deploy phase after configuration:

=== "Windows PowerShell"

    ```powershell
    .\scripts\setup.ps1 --workspace-name retail-demo-alice --deploy
    ```

=== "macOS or Linux"

    ```bash
    ./scripts/setup.sh --workspace-name retail-demo-alice --deploy
    ```

Useful bootstrap flags:

| Flag | Behavior |
| --- | --- |
| `--workspace-name <name>` | Names the Fabric workspace and derives its local environment key. |
| `--deploy` | Runs deploy after configure and render. |
| `--dry-run` | Previews setup-engine commands; the wrapper may still prepare or activate Python first. |
| `--skip-prereqs` | Skips package-manager installation of Git, Terraform, and Azure CLI. |
| `--verbose` | Shows full command and package-install output. |
| `--recreate` | Deploys in destructive clean-slate mode. |

### Manually managed Python environment

Use this path when you want to run each command explicitly.

=== "Windows PowerShell"

    ```powershell
    py -3.11 -m venv .venv
    .\.venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    python -m pip install -e .\utility
    python -m pip install azure-identity azure-kusto-data fabric-cicd
    ```

=== "macOS or Linux"

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install --upgrade pip
    python -m pip install -e ./utility
    python -m pip install azure-identity azure-kusto-data fabric-cicd
    ```

## 4. Configure the target and data volume

Interactive configuration:

```powershell
retail-setup configure --workspace-name retail-demo-alice
```

Review these choices:

| Choice | Guidance |
| --- | --- |
| Workspace/environment | Use a dedicated workspace. The normalized workspace name becomes its local environment key; `retail-demo-alice` becomes `alice`. |
| Tenant | Use the Entra tenant that contains the Fabric capacity. |
| Capacity | The capacity must be active and usable by the deploy operator. |
| Lakehouse | Keep the default unless another checked-in binding requires a deliberate rename. |
| Eventhouse and KQL database | Use the same name. The supported topology uses the default KQL database created with the Eventhouse. |
| Spark pool | Prefer the starter pool unless the F64-oriented preview custom pool is intentional. |
| Store type | `supercenter`, `grocery`, `hardware`, or `luxury`. |
| History | `--months` defines a range ending yesterday. |
| Store count and seed | Control scale and deterministic reproduction. |

The CLI shows an estimated record count before writing configuration.

Non-interactive starter-pool example:

=== "Windows PowerShell"

    ```powershell
    retail-setup configure `
      --tenant-id 00000000-0000-0000-0000-000000000000 `
      --workspace-name retail-demo-alice `
      --capacity-name my-fabric-capacity `
      --lakehouse-name retail_lakehouse `
      --eventhouse-name retail_eventhouse `
      --kql-database-name retail_eventhouse `
      --no-custom-spark-pool `
      --store-type supercenter `
      --months 1 `
      --store-count 10 `
      --seed 42
    ```

=== "macOS or Linux"

    ```bash
    retail-setup configure \
      --tenant-id 00000000-0000-0000-0000-000000000000 \
      --workspace-name retail-demo-alice \
      --capacity-name my-fabric-capacity \
      --lakehouse-name retail_lakehouse \
      --eventhouse-name retail_eventhouse \
      --kql-database-name retail_eventhouse \
      --no-custom-spark-pool \
      --store-type supercenter \
      --months 1 \
      --store-count 10 \
      --seed 42
    ```

Configuration writes:

| Path | Purpose | Git status |
| --- | --- | --- |
| `deploy/config/deploy.yml` | Shared deployment defaults | Tracked |
| `deploy/config/environments/<env>.yml` | Workspace target overlay | Ignored |
| `utility/config.yaml` | Local generation settings | Ignored |

`configure` prints the derived environment key. Keep the local overlay out of
source control, and never add credentials or bearer tokens to configuration.

## 5. Render the notebooks

```powershell
retail-setup render --env alice
```

The command validates all substitutions before writing:

1. `setup-01-seed-dictionaries.ipynb`
2. `setup-02-generate-dimensions.ipynb`
3. `setup-03-generate-facts.ipynb`
4. `setup-04-build-gold.ipynb`
5. `stream-events.ipynb`

Output is written to `utility/out/`. The first four notebooks are the ordered
historical path. The stream notebook is optional and deployed separately.

## 6. Preview and deploy

Always preview the command plan:

```powershell
retail-setup deploy --env alice --dry-run
```

The dry run does not authenticate, contact Fabric, run Terraform, or prove that
the target exists. Confirm the environment, workspace, Terraform variable file,
notebook groups, auth mode, and KQL target in the printed plan.

Run an interactive deployment:

```powershell
retail-setup deploy --env alice
```

Or pre-confirm the Terraform apply gate:

```powershell
retail-setup deploy --env alice --yes
```

`--yes` does not start the setup pipeline automatically because it suppresses
the post-deploy prompt. Run the pipeline or the core notebooks in the next
step.

See [Deployment](deployment.md) before using `--skip-terraform`, `--recreate`,
an existing workspace, Azure PowerShell authentication, or repeated
environment deployments.

## 7. Generate historical data

Choose one path.

### Core historical path

In the Fabric workspace, run setup notebooks 01 through 04 in order. This is
the smallest supported path and creates:

- Silver schema `ag`: seven dimensions, eighteen fact tables, and run metadata;
- Gold schema `au`: nine aggregate tables.

### Full setup pipeline

Run `setup-pipeline` from the Fabric workspace when you also want the checked-in
ML notebooks and ontology creation. The pipeline is asynchronous and can take
from several minutes to more than an hour depending on history, store count,
capacity, and Spark configuration.

You can also trigger it from the repository after a successful Terraform
deployment:

```powershell
python -m deploy.scripts.run_pipeline `
  --environment alice `
  --pipeline setup-pipeline `
  --auth-mode azure_cli
```

Monitor the Fabric run history. A successful trigger is not proof that every
activity completed.

## 8. Validate the core workspace

Before using the demo:

1. Confirm `retail_lakehouse` and `retail_eventhouse` exist in the intended
   workspace.
2. Confirm setup notebooks or `setup-pipeline` completed successfully.
3. Confirm the `ag` and `au` schemas and expected tables are populated.
4. Inspect `setup_run_log` and retain the successful run identifier.
5. Confirm the KQL database contains the numbered tables, functions, and
   materialized views.
6. Confirm the semantic model is bound to the intended Lakehouse before opening
   the report.
7. Skip ML, ontology, agent, dashboard, or rule surfaces that have not passed
   their separate readiness checks.

Local `validate_deployment.py` output validates generated files, not live
workspace usability. Use the [operations guide](operations.md) for live
readiness and recovery.

## 9. Start an optional bounded stream

Open the deployed `stream-events` notebook and use a bounded first run:

```python
source_rows_per_second = 5
sink = "eventhouse"
run_seconds = 180
kusto_uri = ""
kql_database = "retail_eventhouse"
```

Leaving `kusto_uri` blank makes the notebook resolve the KQL Query URI by
database display name in the current workspace. The stream writes typed
micro-batches directly through the Spark Kusto connector; it does not require
Kafka, Event Hubs, or a Fabric Eventstream.

After the notebook stops, allow for asynchronous ingestion and verify recent
rows:

```kql
receipt_created
| where ingest_timestamp > ago(10m)
| summarize rows = count(), latest = max(ingest_timestamp)
```

Proceed to incremental Silver and Gold transforms only after Eventhouse
shortcuts, source tables, and watermarks are ready.

## Next steps

- [Deployment](deployment.md): update, recreate, or troubleshoot the workspace.
- [Deployed walkthrough](deployed-walkthrough.md): tour the deployed assets.
- [Presenter demo](demo-script.md): prepare a defensible presentation.
- [Operations](operations.md): monitor freshness and recover failures.
- [Security controls](../design/security/controls.md): review the shared-demo
  baseline.
