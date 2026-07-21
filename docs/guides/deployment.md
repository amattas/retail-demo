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

Every deployment resolves `deployment.profile` from
`contracts/retail-demo.json`. The manifest selects stable logical asset IDs,
existing notebook groups, existing pipeline folders, and existing KQL scripts.
It does not redefine physical items or schemas: `deploy/config/deploy.yml`,
`deploy/scripts/build_artifacts.py`, and the source folders remain authoritative
for those definitions.

The current deploy plan:

1. resolves the exact profile inventory;
2. runs local preflight before Terraform destroy/apply, publication, or KQL;
3. generates profile-aware Terraform and `fabric-cicd` inputs;
4. initializes and applies Terraform unless skipped;
5. captures the selected Terraform-owned identifiers;
6. stages and publishes infrastructure without Reporting;
7. executes only the selected ordered KQL scripts and validates infrastructure;
8. for gated profiles, waits for setup and required ML validation to complete;
9. stages and publishes Reporting only after required ML terminal success;
10. runs selected optional/experimental ML after Reporting;
11. runs read-only readiness verification; the initial `full-demo` pass
    explicitly defers ontology-dependent evidence;
12. after the operator creates the ontology, publishes Data Agents and task
    flow only through the acknowledged `post-ontology` command and verifies
    complete readiness.

Each live run atomically updates
`deploy/.generated/<env>/deploy-run.json`. Inspect it for target names,
the manifest version/hash, profile support and expected counts/folders,
core/optional/preview/manual boundaries, exact asset/group/pipeline/KQL
inventory, provided acknowledgement IDs, required/optional classification,
failed step, exit code, and final `SUCCEEDED`, `DEGRADED`, or `FAILED` status.
Artifact build steps link phase-specific `artifact-inventory-<phase>.json`
evidence. Credential-like error text is redacted and raw subprocess output is
not stored.

### Profiles

`core` is the default, `standard` is the supported opt-in Reporting path, and
`full-demo` contains acknowledged preview/manual surfaces. The
[workspace and profile inventory](workspace-inventory.md) is the single
canonical guide for exact asset IDs, groups, pipelines, KQL scripts, staged
counts, folders, and support status.

All profiles exclude the destructive `reset` group. Pipeline selection is
explicit; a notebook reference does not implicitly select a pipeline. No
profile enables a schedule or starts the long-running stream.

### Runtime, capacity, cost, preview, and manual boundaries

The dry-run prints the manifest-owned runtime, capacity, cost, preview, and
manual boundaries for the resolved profile. `full-demo` requires one explicit
acknowledgement for each undetectable preview, custom-pool, task-flow API, and
manual-asset boundary. Reporting remains absent unless the current required ML
pipeline run, including the runtime contract validator, finishes successfully.

## Prerequisites

### Local tools

- Git
- Python 3.11 or later
- Terraform `>= 1.8, < 2.0`
- `retail-setup`
- `azure-identity`
- `azure-kusto-data`
- `fabric-cicd`
- `pyodbc`
- Microsoft ODBC Driver 17 or 18 for SQL Server (an operating-system
  prerequisite for live Lakehouse freshness queries)
- Azure CLI for the default auth mode, or Azure PowerShell for the lower-level
  alternative

Install the Python dependencies manually when you are not using the guided
bootstrap:

```powershell
python -m pip install --require-hashes -r .\utility\requirements-deploy.txt
python -m pip install --no-deps -e .\utility
```

### Fabric target

The operator must be able to:

- use the selected tenant and active Fabric capacity;
- create or update the target workspace and Fabric items;
- create or publish every item type selected by the chosen profile;
- apply KQL schema when the profile includes Eventhouse;
- perform the profile's documented manual steps.

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

The deploy REST and KQL helpers use `AzurePowerShellCredential` with the
configured tenant. The Fabric Terraform provider cannot consume an Azure
PowerShell session. Therefore this mode supports either:

- Python-only publication with `--skip-terraform` and previously captured,
  validated outputs; or
- normal Terraform with exactly one provider-supported service-principal
  secret/certificate, OIDC, or managed-identity credential configured through
  the provider's `FABRIC_*` or `ARM_*` environment variables.

Without one of those boundaries, apply and destroy fail before any command
runs. Terraform always receives the configured `tenant_id`; Azure CLI use is
explicitly disabled in Azure PowerShell mode, so an unrelated CLI login is
never used silently. A conflicting `FABRIC_TENANT_ID` or `ARM_TENANT_ID` also
fails before mutation. The guided `scripts/setup.*` prerequisite check still
expects Azure CLI, so invoke `retail-setup` directly for this advanced flow.

## Configure an environment

The workspace name defines the environment. `configure` normalizes the name to
lowercase hyphenated text and omits a leading `retail-demo-` prefix. For
example, workspace `retail-demo-alice` uses environment `alice`. Each workspace
gets its own ignored target overlay, generated inputs, Terraform data
directory, state, outputs, and run journal.

Run:

```powershell
retail-setup configure --workspace-name retail-demo-alice --profile core
retail-setup render --env alice
```

`configure` prints the derived environment key. Configure another workspace to
create another local environment without replacing the first.

Valid profiles are `core`, `standard`, and `full-demo`. Omitting `--profile`
prompts with `core` as the default. The selected value is stored in the
workspace environment overlay as `deployment.profile`.

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

`core` and `standard` use the workspace starter pool. Only `full-demo` selects
the custom pool, and its capacity acknowledgement is mandatory. Legacy
`--use-custom-spark-pool` and
`spark.use_custom_pool` overrides fail clearly; select the profile instead.
Review the source-defined pool configuration against the target capacity.

## Preview the plan

```powershell
retail-setup deploy --env alice --dry-run
```

The preview prints the resolved inventory, profile blockers, required
acknowledgement IDs, ordered commands, and confirmation gates. It validates
the existing environment configuration and authentication boundary but does
not execute subprocesses or:

- contact identity endpoints or validate live permissions;
- contact Fabric;
- validate capacity state;
- prove live generated IDs unless `--skip-terraform` is selected;
- run Terraform plan as a separate command.

With `--skip-terraform`, dry-run validates the existing captured outputs just
like a live run. Malformed or invalid existing YAML always fails; the default
core fallback applies only when a legacy environment overlay is genuinely
absent. The CLI asks for confirmation before invoking Terraform apply. Terraform then
prints its change preview and proceeds with `-auto-approve`; there is no
separate plan review before the confirmation.

## Preflight and acknowledgements

On a live run, profile preflight is the first plan step. It checks only local or
otherwise queryable conditions: manifest/source pointers, rendered notebooks,
selected pipeline references, KQL sources, selected Power BI/agent/task-flow
sources, custom-pool configuration, prior local Terraform state and captured
profile outputs, blockers, and
acknowledgement syntax. It does not claim to detect tenant preview enrollment,
capacity suitability, or completion of manual boundaries.

A `full-demo` run must provide all four explicit acknowledgements:

```powershell
retail-setup deploy --env alice `
  --acknowledge ack.full-demo.preview-surfaces `
  --acknowledge ack.full-demo.custom-pool-capacity `
  --acknowledge ack.full-demo.task-flow-api `
  --acknowledge ack.full-demo.manual-assets
```

Unknown, missing, or repeated acknowledgement IDs fail preflight. Profile
blockers are not acknowledgements and cannot be bypassed.

Whenever environment-local Terraform state exists, missing or stale captured
outputs and missing state profile evidence fail closed, including for state
with no managed instances. A profile downgrade that would delete an Eventhouse
or custom Spark pool is rejected unless the operator explicitly selects
`--recreate`.

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
check. It still runs mandatory setup and required ML gates for profiles that
publish Reporting.

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
`full-demo` capture must also include the custom Spark pool ID. A first-time or
wrong-workspace `--skip-terraform` run fails before mutation.

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

The current sequence is profile preflight, configuration generation, Terraform
initialization, Terraform destroy, bounded polling until the workspace name is
absent, Terraform apply, and normal publication. Preflight therefore cannot
destroy the workspace on failure. Preserve run evidence before retrying if
Fabric does not release the workspace within the documented timeout.

Do not use `99-reset-lakehouse` as part of normal deployment. It is a manual,
destructive data reset asset.

## Post-deploy work

### 1. Treat local validation correctly

`deploy.scripts.validate_deployment` checks generated files, YAML, staging, and
placeholder rewrites. It does not query the live workspace. A successful deploy
still requires live item, binding, KQL, run, and data checks.

### 2. Generate data

For `core`, run setup notebooks 01 through 04 in order. `core` intentionally
has no automatic pipeline run.

For `standard` and `full-demo`, the deploy command already waits for
`setup-pipeline` and `ml-required`, publishes Reporting only after both
succeed, and waits for selected post-Reporting ML pipelines. `--yes` does not
skip these gates. No profile starts the long-running stream or enables an automatic schedule;
the committed daily-maintenance schedule is disabled.

To retry one pipeline deliberately, run:

```powershell
python -m deploy.scripts.run_pipeline `
  --environment alice `
  --pipeline setup-pipeline `
  --auth-mode azure_cli `
  --tenant-id <tenant-id> `
  --wait
```

`--wait` polls the exact submitted run to a bounded terminal state. Omit it
only for an intentional asynchronous manual trigger.

### 3. Complete post-ontology publication

Ontology creation is a separate preview/manual boundary. The initial
`full-demo` deploy intentionally does not stage Data Agents or publish task
flow, so a fresh workspace cannot fail on an ontology reference before this
boundary. Run `30-create-ontology`, wait for exactly one
`RetailOntology_AutoGen` item, then run:

```powershell
retail-setup post-ontology --env alice `
  --acknowledge ack.full-demo.ontology-created
```

The command validates the ontology and captured target IDs before mutation,
then stages/publishes Data Agents, publishes the fully resolved task flow, and
runs final readiness verification. Task-flow deployment fails closed instead
of publishing a partial graph when any selected item is absent.

### 4. Validate live readiness

For `standard`, deployment runs the live verifier after the normal exact-run
setup and ML gates. The initial `full-demo` pass does the same while marking
ontology, Data Agents, and task flow as deferred; only the acknowledged
post-ontology command runs the complete pass. Verification is read-only and
never adds another pipeline run. A required failure or unknown
result fails deployment; an optional failure or unknown result records
`DEGRADED` in `deploy-run.json`. The journal links:

```text
deploy/.generated/<env>/readiness-report.json
```

For `core`, run setup notebooks 01-04 manually, then run:

```powershell
retail-setup verify --env <env>
```

You can rerun the read-only command for any profile. Add `--run-pipeline` only
to explicitly start and wait for the profile-required post-publish pipeline:

```powershell
retail-setup verify --env <env> --run-pipeline
```

The flag is invalid for `core`. It does not start streaming or other pipelines.
See the [operations readiness checklist](operations.md#readiness-checklist)
for prerequisites, exit codes, taxonomy, freshness windows, and report
contents. Offline `validate_deployment.py` behavior is unchanged and does not
substitute for this live check.

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
| Azure PowerShell Terraform boundary | Use validated `--skip-terraform` outputs, switch to Azure CLI, or configure exactly one provider-supported service-principal, OIDC, or managed-identity credential. |
| Capacity not found or inactive | Confirm the capacity display name, state, tenant, and operator access. |
| Custom Spark pool fails | Select `core` or `standard`, or review the `full-demo` source configuration and capacity acknowledgement. Legacy pool overrides are unsupported. |
| Required ML gate fails or is not terminal-successful | Inspect the exact `ml-required` pipeline/notebook run and `15-validate-required-ml-contract`; Reporting is intentionally unpublished. |
| Full-demo reports missing acknowledgements | Review each named preview, capacity, task-flow, and manual boundary, then repeat `--acknowledge` once per required ID. |
| Terraform executable missing | Install Terraform or use `--skip-terraform` only with valid prior outputs. |
| Workspace already exists | Update in place, configure `workspace.existing_id`, or explicitly recreate a disposable target. |
| Fabric item publish fails | Inspect the failing item type and generated `.generated/<env>/fabric-cicd/parameter.yml`; do not treat later steps as completed. |
| KQL application fails | Inspect `deploy/.generated/<env>/database.kql`, target IDs, database name, and operator permissions; rerun the ordered script as one database script. |
| Local validation passes but workspace is unusable | Perform the live checks in the operations guide; local validation is offline only. |
| Readiness verification is `UNKNOWN` | Install Microsoft ODBC Driver 17 or 18, confirm the deploy dependency set and identity permissions, and restore the matching Terraform output and deployment journal evidence. |
| Readiness verification is `DEGRADED` | Read the optional failed/unknown check rows; do not present that optional capability until its evidence passes. |
| Setup pipeline fails | Inspect the exact run and retry with `deploy.scripts.run_pipeline --wait`; required ML and Reporting remain unpublished. |
| Ontology task-flow link is absent | Wait for ontology creation, then run the acknowledged `retail-setup post-ontology` command. |
| Live rows are absent | Check notebook sink parameters, Query URI resolution, KQL permissions, connector errors, and ingestion timestamps. |
| `--skip-terraform` rejects outputs | Use outputs captured for the same environment and workspace; run the normal Terraform path to refresh missing or stale identities. |

## Current limitations

- Live alternate-authentication and renamed-target smoke verification:
  [IMP-001](../design/requirements/modules/deployment/backlog.md#imp-001)
- A fresh workspace must still prove the required ML validator and two-phase
  Reporting path live; this is the remaining
  [IMP-008](../design/requirements/modules/ml-ai/backlog.md#imp-008) gate.
- Live profile/capability proof keeps
  [IMP-012](../design/requirements/modules/deployment/backlog.md#imp-012) open.
- Live execution and freshness evidence for the implemented readiness surface:
  [IMP-013](../design/requirements/modules/operations/backlog.md#imp-013)

These limitations are part of the current contract. Do not describe local
staging success, a task-flow node, a pipeline trigger, or a checked-in optional
asset as proof of a usable live deployment.
