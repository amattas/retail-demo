# Getting started

This guide creates a Microsoft Fabric Retail Demo workspace through the
supported Fabric-native path. It takes you from a clean clone to historical
Lakehouse data and an optional bounded Eventhouse stream.

**Audience:** first-time operators, business analysts working with a technical
partner, and entry-level developers.

You do not need to know every Fabric term before starting. The
[plain-language glossary](glossary.md) explains the products, data layers, and
deployment terms used in this guide.

For deployment modes, generated files, reruns, existing workspaces, and
recovery, use the [deployment guide](deployment.md).

!!! tip "How to read the command examples"

    Replace values inside angle brackets, such as `<env>` or `<tenant-id>`,
    with values from your environment. In PowerShell, a backtick at the end of
    a line continues the same command. In macOS/Linux examples, a backslash
    performs the same job.

!!! warning "Cost and destructive operations"

    Deployment creates or updates Microsoft Fabric items and can consume
    capacity. `--recreate` destroys the selected workspace and every item in it.
    Use a dedicated demo workspace, confirm the tenant and workspace name, and
    review the dry-run plan before applying changes.

## What the supported path creates

The manifest-default `core` profile creates the smallest supported path: a
Fabric workspace, an empty Lakehouse ready to receive tables, and four
historical setup notebooks. The operator runs setup 01 through 04 in order.

`standard` adds Eventhouse for optional live events, automated pipelines,
required machine-learning outputs, a Power BI semantic model, and a report.
`full-demo` adds preview and manually completed experiences such as Ontology,
Data Agents, and task flow. No profile deploys the destructive reset notebook
or starts the long-running stream automatically. Choose with `--profile`; see the
[canonical workspace and profile inventory](workspace-inventory.md) before
selecting an opt-in profile.

The guided `setup.ps1` and `setup.sh` wrappers intentionally select
`full-demo` when `--profile` is omitted. This makes the guided path a complete
demo install while keeping direct `retail-setup` commands on the conservative
manifest default.

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

The lower-level deploy framework supports Azure PowerShell authentication for
Python Fabric clients, not for the Terraform provider. Use validated
`--skip-terraform` outputs or configure a provider-supported credential. The
guided bootstrap still checks for Azure CLI, so use the
[manual deployment path](deployment.md#authentication) for an Azure
PowerShell-only workstation.

### Capacity and Spark choice

The committed default disables the custom Spark pool. `core` and `standard`
use the workspace starter pool. Only `full-demo` selects the source-defined custom pool. Before deployment,
live preflight confirms that the chosen capacity is active and large enough
for the requested Spark workers. The current full-demo minimum is F64.
Reporting profiles require at least 18 months of history so that the report and
machine-learning examples have enough seasonal context. Start with a small
store count, measure one setup run, and increase the size only when the
capacity has room.

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
3. installs `retail-setup`, version-locked Fabric libraries such as
   `azure-identity`, `azure-kusto-data`, and `fabric-cicd`, plus the SQL
   connectivity package supported by the operating system. The complete pinned
   package list is in `utility/requirements-deploy.txt`;
4. runs interactive configuration;
5. renders five workspace-specific notebooks;
6. offers to deploy.

On Windows and Linux, the locked dependency set includes `mssql-python` for
live Lakehouse freshness verification. If Microsoft ODBC Driver 17 or 18 is
already installed, readiness uses it. On macOS, install Microsoft ODBC Driver
17 or 18 separately.

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
| `--profile <name>` | Selects `core`, `standard`, or `full-demo`; guided wrappers default to `full-demo`. |
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
    python -m pip install --require-hashes -r .\utility\requirements-deploy.txt
    python -m pip install --no-deps -e .\utility
    ```

=== "macOS or Linux"

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install --require-hashes -r ./utility/requirements-deploy.txt
    python -m pip install --no-deps -e ./utility
    ```

## 4. Configure the target and data volume

Interactive configuration:

```powershell
retail-setup configure --workspace-name retail-demo-alice --profile core
```

Review these choices:

| Choice | Guidance |
| --- | --- |
| Workspace/environment | Use a dedicated workspace. The normalized workspace name becomes its local environment key; `retail-demo-alice` becomes `alice`. |
| Tenant | Use the Microsoft Entra tenant, or organization directory, that contains the Fabric capacity. |
| Capacity | The capacity supplies the computing resources for notebooks, pipelines, queries, and reports. It must be active and usable by the deploy operator. |
| Lakehouse | The Lakehouse stores historical and analytical tables. Keep the default name unless you are deliberately updating every checked-in reference. |
| Eventhouse and KQL database | Eventhouse stores optional live events, and KQL (Kusto Query Language) queries them. Use the same display name for both in this demo. |
| Profile and Spark pool | `core` and `standard` use the starter pool. `full-demo` validates and selects the custom pool. |
| Store type | `supercenter`, `grocery`, `hardware`, or `luxury`. |
| History | `--months` defines a range ending yesterday. Reporting profiles use at least 18 months by default. |
| Store count and seed | Control scale and deterministic reproduction. |

The CLI shows an estimated record count before writing configuration.

Non-interactive starter-pool example:

=== "Windows PowerShell"

    ```powershell
    retail-setup configure `
      --tenant-id 00000000-0000-0000-0000-000000000000 `
      --workspace-name retail-demo-alice `
      --profile core `
      --capacity-name my-fabric-capacity `
      --lakehouse-name retail_lakehouse `
      --eventhouse-name retail_eventhouse `
      --kql-database-name retail_eventhouse `
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
      --profile core \
      --capacity-name my-fabric-capacity \
      --lakehouse-name retail_lakehouse \
      --eventhouse-name retail_eventhouse \
      --kql-database-name retail_eventhouse \
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

Rendering also records the configured history end date. During deployment, the
artifact builder uses that date to set the Power BI report's saved date filters
to the latest generated month. This keeps a three-month setup, an eighteen-month
setup, and a two-year setup aligned with their own data instead of a hard-coded
calendar month.

## 6. Preview and deploy

Always preview the command plan:

```powershell
retail-setup deploy --env alice --dry-run
```

The dry run validates existing configuration and the Terraform authentication
boundary, but does not authenticate, contact Fabric, run Terraform, or prove
that the target exists. With `--skip-terraform`, it also validates the captured
outputs. Confirm the environment, workspace, Terraform variable file, notebook
groups, auth mode, and KQL target in the printed plan.

Run an interactive deployment:

```powershell
retail-setup deploy --env alice
```

Or pre-confirm the Terraform apply gate:

```powershell
retail-setup deploy --env alice --yes
```

For `core`, `--yes` only pre-confirms the Terraform apply gate; setup remains
an operator-run notebook sequence. For Reporting profiles, deployment still
runs and waits for the required setup and ML gates. `--yes` never bypasses
those gates.

See [Deployment](deployment.md) before using `--skip-terraform`, `--recreate`,
an existing workspace, Azure PowerShell authentication, or repeated
environment deployments.

## 7. Generate historical data

Choose one path.

### Core historical path

In the Fabric workspace, run setup notebooks 01 through 04 in order. This is
the smallest supported path and creates:

- **Silver (`ag`)**: cleaned, typed historical data, including seven descriptive
  dimensions, nineteen business facts, and setup-run metadata.
- **Gold (`au`)**: ten business-ready summary tables used for analysis.

The names `ag` and `au` are the short schema identifiers used throughout this
demo. See [Data-layer terms](glossary.md#data-layer-terms) for more detail.

### Reporting profiles

`retail-setup deploy` automatically waits for `setup-pipeline` and
`ml-required` when `standard` or `full-demo` is selected. The required ML
validator must succeed before the semantic model and report publish. Use at
least 18 months of configured history. Ontology remains a separate
preview/manual step. For `full-demo`, continue with
[Complete post-ontology publication](deployment.md#complete-post-ontology-publication)
after the initial deployment succeeds.

You can retry setup deliberately from the repository:

```powershell
python -m deploy.scripts.run_pipeline `
  --environment alice `
  --pipeline setup-pipeline `
  --auth-mode azure_cli `
  --wait
```

Monitor the exact Fabric run and retain its run ID.

## 8. Validate the selected workspace

Before using the demo:

1. Confirm the profile-selected inventory exists in the intended workspace.
   `core` includes a Lakehouse but excludes Eventhouse, KQL, ML, and Reporting.
2. Confirm setup notebooks or `setup-pipeline` completed successfully.
3. Confirm the `ag` and `au` schemas and expected tables are populated.
4. Inspect `setup_run_log`, the metadata table written by setup, and retain the
   successful run identifier.
5. For `standard` or `full-demo`, confirm the KQL database contains the selected
   tables, functions, mappings, and materialized views.
6. For a Reporting profile, confirm the semantic model is bound to the intended
   Lakehouse before opening the report.
7. Skip ML, ontology, agent, dashboard, or rule surfaces that have not passed
   their separate readiness checks.

Local `validate_deployment.py` output validates generated files, not live
workspace usability. Run `retail-setup verify --env alice` after the
selected workloads, and use the [operations guide](operations.md) for evidence
and recovery.

## 9. Start an optional bounded stream

This step requires the `standard` or `full-demo` profile. The `core` profile
does not deploy Eventhouse or the stream notebook.

Open the deployed `stream-events` notebook in Fabric. Find the cell labeled
**Parameters**, change the values there, and then select **Run all**. Use a
bounded first run:

```python
source_rows_per_second = 5
sink = "eventhouse"
run_seconds = 180
kusto_uri = ""
kql_database = "retail_eventhouse"
```

!!! warning "Do not leave the first run unbounded"

    The source default is `run_seconds = 0`, which means “continue until
    manually stopped.” Set a positive value such as `180` for the first run so
    that the notebook stops after three minutes.

Leaving `kusto_uri` blank makes the notebook resolve the KQL Query URI by
database display name in the current workspace. Every few seconds, the
notebook groups generated events and writes them directly to Eventhouse through
the Fabric Spark Kusto connector. It does not require Kafka, Event Hubs, or a
Fabric Eventstream.

Eventhouse receives data asynchronously, so the last event may appear shortly
after the notebook stops. Wait briefly, then verify recent rows with this KQL
(Kusto Query Language) query:

```kql
receipt_created
| where ingest_timestamp > ago(10m)
| summarize rows = count(), latest = max(ingest_timestamp)
```

`ingest_timestamp` is the time Eventhouse received the event. It is the best
field for checking whether the stream is currently arriving; the business
event time can be slightly earlier.

Proceed to incremental Silver and Gold transforms only after Eventhouse
shortcuts, source tables, and watermarks are ready.

## Next steps

- [Deployment](deployment.md): update, recreate, or troubleshoot the workspace.
- [Workspace and profile inventory](workspace-inventory.md): check exact
  counts, folders, support, and manual boundaries.
- [Deployed walkthrough](deployed-walkthrough.md): tour the deployed assets.
- [Presenter demo](demo-script.md): prepare a defensible presentation.
- [Operations](operations.md): monitor freshness and recover failures.
- [Security controls](../design/security/controls.md): review the shared-demo
  baseline.
