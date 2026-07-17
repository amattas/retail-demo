# Deployment

This guide covers the supported `retail-setup deploy` workflow for provisioning,
publishing, updating, and recovering a Microsoft Fabric Retail Demo workspace.
Complete [Getting started](getting-started.md) first if the repository has not
been configured and rendered.

Exact command order and failure behavior are owned by the
[deployment framework specification](../design/specifications/modules/deployment/framework.md).

!!! danger "Confirm the target"

    `--recreate` destroys the configured workspace and every item in it. The
    deploy polls Fabric until the workspace name is absent and fails before
    apply if deletion times out or pagination is incomplete. Use recreate only
    for a disposable demo workspace after confirming the tenant, environment,
    workspace name, and Terraform state.

## Deployment outcome

The current deploy plan:

1. merges shared and environment configuration;
2. generates Terraform and `fabric-cicd` inputs;
3. initializes and applies Terraform unless skipped;
4. captures workspace, Lakehouse, Eventhouse, and KQL database identifiers;
5. stages Fabric item folders;
6. publishes supported items through `fabric-cicd`;
7. builds and executes the ordered KQL database script;
8. validates generated deployment files;
9. deploys the workspace task flow as a required step when its source exists;
10. optionally offers to start `setup-pipeline`; accepting makes the trigger
    required for that run.

Each live run atomically updates
`deploy/.generated/<env>/deploy-run.json`. Inspect it for target names,
required/optional classification, exact failed step, exit code, and final
`SUCCEEDED`, `DEGRADED`, or `FAILED` status. Credential-like error text is
redacted and raw subprocess output is not stored.

Terraform creates or resolves the workspace and provisions the Lakehouse,
Eventhouse, default KQL database, role assignments, and optional custom Spark
pool. `fabric-cicd` publishes these item types:

- Lakehouse
- Notebook
- SemanticModel
- Report
- KQLQueryset
- DataPipeline
- MLExperiment
- DataAgent

The staged notebook groups are currently `core`, `setup`, `ml`, `ontology`,
`reset`, and `stream`. The reset notebook is published but never included in a
normal pipeline. Dashboard templates and rule definitions remain manual source
inputs until first-class deployment and binding are validated.

## Prerequisites

### Local tools

- Git
- Python 3.11 or later
- Terraform `>= 1.8, < 2.0`
- `retail-setup`
- `azure-identity`
- `azure-kusto-data`
- `fabric-cicd`
- Azure CLI for the default auth mode, or Azure PowerShell for the lower-level
  alternative

Install the Python dependencies manually when you are not using the guided
bootstrap:

```powershell
python -m pip install -e .\utility
python -m pip install azure-identity azure-kusto-data fabric-cicd
```

### Fabric target

The operator must be able to:

- use the selected tenant and active Fabric capacity;
- create or update the target workspace and Fabric items;
- apply KQL schema to the Eventhouse database;
- publish semantic-model, report, pipeline, notebook, and agent items;
- start the setup pipeline when requested.

Use a dedicated workspace. Review workspace and item roles before sharing the
demo, because synthetic customer-like fields still require governance.

## Authentication

### Azure CLI

Azure CLI is the default and the only mode the guided bootstrap installs and
checks automatically.

```powershell
az login --tenant 00000000-0000-0000-0000-000000000000
az account show --query tenantId -o tsv
```

Set:

```yaml
auth:
  mode: azure_cli
```

`retail-setup deploy` rejects a configured tenant that differs from the active
Azure CLI tenant.

### Azure PowerShell

For a manually prepared workstation:

```powershell
Connect-AzAccount -Tenant 00000000-0000-0000-0000-000000000000
```

Set:

```yaml
auth:
  mode: azure_powershell
```

The deploy REST and KQL helpers then use `AzurePowerShellCredential`. The guided
`scripts/setup.*` prerequisite check still expects Azure CLI, so invoke
`retail-setup` directly for an Azure PowerShell-only flow.

## Configure an environment

The workspace name defines the environment. `configure` normalizes the name to
lowercase hyphenated text and omits a leading `retail-demo-` prefix. For
example, workspace `retail-demo-alice` uses environment `alice`. Each workspace
gets its own ignored target overlay, generated inputs, Terraform data
directory, state, outputs, and run journal.

Run:

```powershell
retail-setup configure --workspace-name retail-demo-alice
retail-setup render --env alice
```

`configure` prints the derived environment key. Configure another workspace to
create another local environment without replacing the first.

Important configuration boundaries:

| Path | Purpose | Tracked |
| --- | --- | --- |
| `deploy/config/deploy.yml` | Shared deployment defaults | Yes |
| `deploy/config/environments/<env>.yml` | Workspace-specific target overlay | No |
| `utility/config.yaml` | Local generation scale and seed | No |
| `utility/out/` | Rendered workspace-specific notebooks | No |

Keep the Eventhouse and KQL database display names aligned. The Eventhouse
creates one default KQL database with the Eventhouse display name; provisioning
a separately named database is not a supported topology.

### Starter pool or custom pool

Use `--no-custom-spark-pool` for the workspace starter pool. Use
`--use-custom-spark-pool` only when the target supports the preview Spark pool
resource and the F64-oriented defaults are intentional:

- MemoryOptimized Medium nodes
- autoscale from 1 to 10 nodes
- dynamic executor allocation enabled

Change these values in the workspace environment overlay only after checking
capacity limits.

## Preview the plan

```powershell
retail-setup deploy --env alice --dry-run
```

The preview prints the ordered commands and confirmation gates. It does not:

- validate credentials or permissions;
- contact Fabric;
- validate capacity state;
- prove that generated IDs target the intended workspace;
- run Terraform plan as a separate command.

The CLI asks for confirmation before invoking Terraform apply. Terraform then
prints its change preview and proceeds with `-auto-approve`; there is no
separate plan review before the confirmation.

## Run a normal deployment

Interactive:

```powershell
retail-setup deploy --env alice
```

Pre-confirm the Terraform apply gate:

```powershell
retail-setup deploy --env alice --yes
```

Without `--yes`, an existing workspace detected by display name produces a
choice:

- keep it and update in place; or
- reset it and follow the recreate path.

With `--yes`, the CLI does not perform that interactive existing-workspace
check and does not offer to start `setup-pipeline` after publication.

## Understand generated files

Deployment writes or refreshes:

| Path | Content | Git status |
| --- | --- | --- |
| `deploy/.generated/<env>/terraform.tfvars` | Terraform input generated from merged YAML | Ignored |
| `deploy/.generated/<env>/terraform.tfstate` | Environment-local Terraform state | Ignored |
| `deploy/.generated/<env>/.terraform/` | Environment-local Terraform data directory | Ignored |
| `deploy/.generated/<env>/fabric-cicd/config.yml` | `fabric-cicd` environment and item scope | Ignored |
| `deploy/.generated/<env>/fabric-cicd/parameter.yml` | Workspace, item, OneLake, KQL, and agent rewrites | Ignored |
| `deploy/.generated/<env>/terraform-output.json` | Captured live Fabric item identifiers | Ignored |
| `deploy/.generated/<env>/database.kql` | Combined ordered KQL script | Ignored |
| `deploy/workspace/` | Staged Fabric item folders | Ignored except `.gitkeep` |

Terraform operations for different environments use different backend and data
paths. Full publication still shares `deploy/workspace/` staging; use separate
checkouts for concurrent full deploys. Never commit local target overlays,
credentials, tokens, or generated bindings.

If an older checkout left state at `deploy/terraform/terraform.tfstate`, deploy
stops before Terraform. Verify which workspace owns that state, then move it to
`deploy/.generated/<env>/terraform.tfstate`. Never copy one legacy state into
multiple environments.

## Existing workspaces

For a workspace that Terraform should resolve rather than create, set
`workspace.existing_id` in deployment configuration and run the normal
Terraform path. This resolves the workspace only; it does not automatically
discover or import pre-existing Lakehouse, Eventhouse, role-assignment, or
Spark resources into Terraform state. Avoid name collisions and import or
reconcile existing child resources deliberately before apply.

Use `--skip-terraform` only when all required resources already exist and
`deploy/.generated/<env>/terraform-output.json` contains correct identifiers
from an earlier deployment:

```powershell
retail-setup deploy --env alice --skip-terraform
```

Before publication, the command verifies the captured environment, workspace
and resource names, required Fabric IDs, and absence of placeholder IDs. A
first-time or wrong-workspace `--skip-terraform` run fails before mutation.

`--skip-terraform` cannot be combined with `--recreate`.

## Recreate a disposable workspace

Preview:

```powershell
retail-setup deploy --env alice --recreate --dry-run
```

Execute:

```powershell
retail-setup deploy --env alice --recreate
```

The current sequence is Terraform destroy, bounded polling until the workspace
name is absent, Terraform apply, and normal publication. Preserve run evidence
before retrying if Fabric does not release the workspace within 180 seconds.

Do not use `99-reset-lakehouse` as part of normal deployment. It is a manual,
destructive data reset asset.

## Post-deploy work

### 1. Treat local validation correctly

`deploy.scripts.validate_deployment` checks generated files, YAML, staging, and
placeholder rewrites. It does not query the live workspace. A successful deploy
still requires live item, binding, KQL, run, and data checks.

### 2. Generate data

Choose either:

- run setup notebooks 01 through 04 for the core historical path; or
- run `setup-pipeline` for setup 01 through 04, ML notebooks 06 through 14, and
  ontology creation.

If you used `--yes`, trigger the pipeline manually in Fabric or run:

```powershell
python -m deploy.scripts.run_pipeline `
  --environment alice `
  --pipeline setup-pipeline `
  --auth-mode azure_cli
```

The trigger is asynchronous. Track the pipeline and notebook run histories.

### 3. Relink the ontology task-flow node

The first task-flow deployment occurs before the setup pipeline creates the
ontology. After the pipeline succeeds, run:

```powershell
python -m deploy.scripts.taskflow deploy `
  --workspace retail-demo-alice `
  --auth-mode azure_cli
```

A later `retail-setup deploy` also attempts the task-flow deployment again.

### 4. Validate live readiness

Use the [operations readiness checklist](operations.md#readiness-checklist).
At minimum, verify:

- target tenant and workspace;
- required item inventory and bindings;
- ordered KQL objects;
- successful historical setup;
- populated `ag` and `au` tables;
- semantic-model binding;
- recent Eventhouse rows when live streaming is part of the demo.

## Update an existing deployment

For normal source or configuration changes:

1. pull or check out the intended revision;
2. rerun `retail-setup configure` when target or generation values changed;
3. rerun `retail-setup render`;
4. inspect `retail-setup deploy --env <env> --dry-run`;
5. run the normal deploy without recreate;
6. rerun only the data or optional workloads affected by the change;
7. validate live bindings and freshness.

Environment-local Terraform paths make switching between workspaces safe.
Preserve each environment's ignored `.generated/<env>/` directory when its
state and output evidence must survive cleanup.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| Azure CLI tenant mismatch | Run `az login --tenant <configured-tenant>` and confirm `az account show`. |
| Azure PowerShell credential failure | Run `Connect-AzAccount -Tenant <tenant>` and confirm `auth.mode: azure_powershell`. |
| Capacity not found or inactive | Confirm the capacity display name, state, tenant, and operator access. |
| Custom Spark pool fails | Reconfigure with `--no-custom-spark-pool` unless preview support and capacity sizing are intentional. |
| Terraform executable missing | Install Terraform or use `--skip-terraform` only with valid prior outputs. |
| Workspace already exists | Update in place, configure `workspace.existing_id`, or explicitly recreate a disposable target. |
| Fabric item publish fails | Inspect the failing item type and generated `.generated/<env>/fabric-cicd/parameter.yml`; do not treat later steps as completed. |
| KQL application fails | Inspect `deploy/.generated/<env>/database.kql`, target IDs, database name, and operator permissions; rerun the ordered script as one database script. |
| Local validation passes but workspace is unusable | Perform the live checks in the operations guide; local validation is offline only. |
| Setup pipeline did not start | Start `setup-pipeline` in Fabric or use `deploy.scripts.run_pipeline`. |
| Ontology task-flow link is absent | Wait for ontology creation, then redeploy the task flow. |
| Live rows are absent | Check notebook sink parameters, Query URI resolution, KQL permissions, connector errors, and ingestion timestamps. |
| `--skip-terraform` rejects outputs | Use outputs captured for the same environment and workspace; run the normal Terraform path to refresh missing or stale identities. |

## Current limitations

- Alternate authentication and renamed-target verification:
  [IMP-001](../design/requirements/modules/deployment/backlog.md#imp-001)
- Tiered GA-safe deployment profiles:
  [IMP-012](../design/requirements/modules/deployment/backlog.md#imp-012)
- Live post-deploy readiness:
  [IMP-013](../design/requirements/modules/operations/backlog.md#imp-013)

These limitations are part of the current contract. Do not describe local
staging success, a task-flow node, a pipeline trigger, or a checked-in optional
asset as proof of a usable live deployment.
