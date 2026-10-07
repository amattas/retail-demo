# Deployment framework

## Current order

The supported deploy orchestrator is `retail-setup deploy`.

```mermaid
flowchart LR
    Resolve[Resolve manifest profile]
    Preflight[Validate profile preflight]
    Config[Generate environment config]
    TF[Terraform init/apply or resolve existing outputs]
    StageInfra[Stage infrastructure without Reporting]
    PublishInfra[Publish infrastructure]
    KQL[Build and execute ordered KQL script]
    Setup[Wait for setup-pipeline]
    RequiredML[Wait for ml-required + validator]
    StageReporting[Stage Reporting]
    PublishReporting[Publish Reporting]
    ExtendedML[Run selected post-Reporting ML]
    RefreshSQL[Synchronize Lakehouse SQL endpoint metadata]
    Validate[Validate publication]
    Ontology[Create or validate ontology]
    Agents[Publish Data Agents]
    TaskFlow[Deploy and read back complete task flow]
    FinalVerify[Verify complete readiness]

    Resolve --> Preflight --> Config --> TF --> StageInfra --> PublishInfra --> KQL
    KQL --> Setup --> RequiredML --> StageReporting --> PublishReporting
    PublishReporting --> ExtendedML --> RefreshSQL --> Validate --> Ontology
    Ontology --> Agents --> TaskFlow --> FinalVerify
```

Local and live preflight are the first executable plan steps and precede every
destroy, apply, publish, and KQL mutation. Local preflight validates source and
state contracts. The orchestrator invokes it with `--source-only` to check
committed notebook inputs before rendering; it does not require a prior
rendered output set. Explicit rendered-output validation resolves
`utility/out/<env>/`, not the shared legacy output directory.
Live preflight uses documented Fabric APIs to validate tenant
switches and capacity state, size/tier (SKU), region, and Spark sizing.

Deployment passes `--resume-capacity` to live preflight. Its active-capacity
fast path is read-only; paused capacity may be resumed through Azure Resource
Manager after resolving the configured capacity name. An explicit
`workspace.capacity_resource_id` bypasses discovery. Otherwise, the configured
subscription is searched, or all accessible subscriptions owned by the
configured tenant are enumerated. All pages and subscription ownership must
be validated, and exactly one capacity name match must remain. An inaccessible
page or subscription is not silently skipped to manufacture a unique match.
Only `Microsoft.Fabric/capacities` F-SKU resources in `Paused` or `Suspended`
are resumed, with API version `2023-11-01`. Identity/name/subscription
mismatches, inactive trial/Premium capacities, and ambiguous Fabric lookup
results fail before the resume request. Fabric `Inactive` alone is not proof
of an ARM paused state.

Resume uses a separate ARM token for the configured tenant/authentication
mode, submits at most one POST, honors asynchronous operation headers and
`Retry-After`, and polls resource and Fabric states with a bounded wait.
HTTP 429 GET responses honor `Retry-After` within that deadline, including
discovery and operation polling. POST retries are not introduced by throttling.
Lost POST responses and conflicts trigger state reads, not blind repeat
mutations. Polling URLs must remain on the trusted HTTPS ARM host.
Resuming restarts billing; no scale, assignment, creation, or later automatic
pause is performed. Default wait is 600 seconds. Standalone live preflight
without the flag, deploy dry runs, and readiness remain nonmutating.
`deploy --resume` reexecutes live preflight even when the prior run recorded it
as successful, while retaining completed setup/ML steps.
Before publication, target-access validation also proves that captured
workspace IDs match managed state and resolve to the configured live workspace
for the deployment operator.

The CLI saves `terraform.tfplan` and validates its JSON resource changes before
apply confirmation. Review that file with `terraform show`; the plan guard
captures its subprocess output rather than displaying a human-readable diff.
Apply consumes that exact saved plan
with `-auto-approve`; it does not recalculate an unreviewed plan. Deletion or
replacement of `fabric_workspace`, `fabric_lakehouse`, or `fabric_eventhouse`
fails the normal plan guard. Intentional destructive changes require the
explicit `--recreate` workflow, including its destroy confirmation.

## Command modes

| Mode | Exact behavior |
| --- | --- |
| `--dry-run` | Validates existing configuration and the Terraform/Python authentication boundary, then prints the plan without subprocess execution or live target access. With `--skip-terraform`, it also validates captured outputs. |
| `--yes` | Pre-confirms gated Terraform steps and existing-workspace handling; it never skips required pipeline gates. |
| `--skip-terraform` | Omits Terraform only after captured outputs match the selected environment, workspace, resource names, and non-placeholder IDs. |
| `--resume` | Reuses a compatible incomplete journal's contiguous `SUCCEEDED` prefix, then retries from the first other state. |
| `--recreate` | Runs destroy, polls Fabric until the workspace name is absent (bounded at 180 seconds), then applies and publishes. |

`--recreate` and `--skip-terraform` are mutually exclusive. A normal
interactive run detects an existing workspace by display name and offers
update-in-place or recreate. `--yes` skips that prompt.

`--resume` cannot be combined with `--dry-run` or `--recreate`. The environment,
targets, profile, manifest hash, ordered step IDs, and content/command
fingerprint must match. The fingerprint covers deployment sources, selected
configuration, dictionary files, and ordered commands. Legacy journals without
the required plan/fingerprint fields are rejected rather than guessed
compatible. Failed, degraded, skipped, or pending steps do not
extend the reusable prefix. Completed runs cannot be resumed. An interrupted
recreate preserves its prior recreate target identity only when every prior
step through `deletion-wait` succeeded. An unfinished destructive phase refuses
automatic resume; explicit operator review and restart are required.

Before retrying a pending apply, resume regenerates and checks the saved plan.
Before retrying a pending item-publication phase, it rebuilds that phase's
staging, including post-ontology artifacts. A succeeded build entry is not
proof that ephemeral local files remain intact. Resume is not a substitute
for live target validation or readiness evidence.

## Workspace-scoped environments

`configure` derives one environment key from the Fabric workspace name. It
lowercases the name, converts punctuation and spaces to hyphens, and omits a
leading `retail-demo-` prefix. For example, `retail-demo-alice` becomes
`alice`.

Each key owns one ignored target overlay and one ignored generated directory.
The overlay contains operator-specific tenant, capacity, workspace, and
optional existing-item identifiers. The tracked `deploy.yml` contains shared
defaults only. After Terraform, `fabric-cicd` targets the validated workspace
ID rather than resolving a potentially duplicate display name.

## Generated files

| Path | Role | Tracked |
| --- | --- | --- |
| `deploy/config/environments/<env>.yml` | Workspace-specific target overlay | No |
| `utility/config/environments/<env>.yaml` | Environment-specific generation settings; legacy `utility/config.yaml` is a read fallback | No |
| `utility/out/<env>/` | Rendered notebooks and render manifest | No |
| `deploy/.generated/<env>/terraform.tfvars` | Terraform input rendered from merged YAML | No |
| `deploy/.generated/<env>/terraform.tfplan` | Saved, guarded Terraform plan applied after confirmation | No |
| `deploy/.generated/<env>/terraform.tfstate` | Local Terraform backend state | No |
| `deploy/.generated/<env>/.terraform/` | Terraform backend and provider data | No |
| `deploy/.generated/<env>/fabric-cicd/config.yml` | Publication environment and item scope | No |
| `deploy/.generated/<env>/fabric-cicd/parameter.yml` | Workspace, item, OneLake, KQL, and agent rewrites | No |
| `deploy/.generated/<env>/terraform-output.json` | Captured live identifiers | No |
| `deploy/.generated/<env>/database.kql` | Combined ordered KQL script | No |
| `deploy/.generated/<env>/deploy-run.json` | Atomic step/status journal for the latest deploy run | No |
| `deploy/.generated/<env>/run-history/<hashed-run-id>.json` | Prior journal archived before a different new/resumed run replaces the latest journal | No |
| `deploy/.generated/<env>/artifact-inventory-<phase>.json` | Validated manifest/profile/count/folder inventory for one publication phase | No |
| `deploy/.generated/<env>/readiness-report.json` | Redacted profile-aware live evidence | No |
| `deploy/.generated/<env>/workspace/` | Staged Fabric items for every publication phase | No |

Terraform receives an environment-specific `TF_DATA_DIR` and local backend
path. Parallel Terraform operations therefore cannot select or mutate another
workspace's state. The orchestrator also isolates generation configuration,
rendered output, and staging, including post-ontology staging, by environment.
Do not run concurrent deploys against the same environment; different local
paths are not a distributed lock on a shared Fabric target. Lower-level build
helpers may retain legacy defaults, so use the orchestrator for isolation.
The supported state path is a local backend; remote-backend coordination and
distributed serialization across separate pipelines/manual writers remain
external design and validation gates.

A legacy `deploy/terraform/terraform.tfstate` with no environment-local state
fails preflight. The operator must verify its workspace ownership and move it
to exactly one `deploy/.generated/<env>/terraform.tfstate` path.

If environment-local state exists, preflight reconciles its profile, resource,
and target-output signals with `terraform-output.json`. Missing profile
evidence, missing captures, or stale identities fail closed even when the state
currently has no managed instances. A downgrade that would remove a
profile-controlled Eventhouse or custom Spark pool requires explicit
`--recreate`; absent evidence never authorizes deletion.

## Authentication boundary

Azure CLI mode is shared by the Python clients and Fabric Terraform provider.
The CLI tenant is validated before mutation, and Terraform receives an
explicit `tenant_id`.

Azure PowerShell mode applies only to Python Fabric clients. Terraform
apply/destroy is rejected before command execution unless exactly one
provider-supported service-principal secret/certificate, OIDC, or
managed-identity credential is configured. Otherwise the operator must use
validated prior outputs with `--skip-terraform`. In this mode the provider's
Azure CLI fallback is disabled, preventing an unrelated CLI context from being
used silently; conflicting provider tenant variables are rejected.

## Manifest authority and executable profiles

`contracts/retail-demo.json` owns stable asset IDs/descriptions, support
metadata, dependencies, profile-to-existing-asset/group selection, expected
publication counts/folders, canonical commands/prerequisites, and readiness
expectations. Physical item and schema definitions remain in `deploy/config/deploy.yml`,
`deploy/scripts/build_artifacts.py`, and the source folders. Manifest source
pointers are validated rather than used to duplicate physical schemas.

Resolution computes dependency closure and rejects unknown, cyclic, or
unclassified selected assets. Pipeline selection is exact and independent of
notebook references. All profiles exclude the destructive `reset` group.

| Profile | Assets | Groups | Pipelines | KQL scripts | Publication behavior |
| --- | ---: | --- | ---: | ---: | --- |
| `core` (manifest/direct-CLI default) | 1 | `setup` | 0 | 0 | Lakehouse shell and four rendered historical setup notebooks; no Eventhouse, Reporting, ML, stream, preview, task flow, or custom pool |
| `standard` | 8 | `setup`, `core`, `stream`, `ml-required` | 5 | 6 | Supported real-time/streaming plus fail-closed required ML/Reporting path; starter pool and no preview surfaces |
| `full-demo` | 14 | standard groups plus `ml-optional`, `ml-experimental`, `ontology`, `utility` | 7 | 6 | Required Reporting gate plus post-Reporting extended ML and advanced/preview surfaces except reset |

The exact initial infrastructure/Reporting counts are `5+0`, `30+2`, and `40+2`
respectively. They and the workspace-folder sets are validated while staging;
every staged `.platform` description is projected from the selected manifest
asset. See the canonical
[workspace inventory](../../../../guides/workspace-inventory.md).
Checked-in Data Agent `.platform` files may retain nonempty agent-specific
summaries distinguishing semantic-model and ontology scope. This source
metadata exception does not change staging's manifest-description projection
or exact description contracts for other artifact types.
The prior `IMP-008` profile blockers are removed because the required path is
now executable and fail-closed. Full-demo checks Ontology/Data Agent tenant
switches with the admin tenant-settings API and validates the selected
capacity with the capacities API. Settings changes remain administrator-owned
because Fabric publishes no tenant-settings update API.

The guided `setup.ps1` and `setup.sh` entry points default to `full-demo`.
Direct `retail-setup` commands and the manifest retain the conservative `core`
default.

No profile enables schedules or starts the live stream. The source
daily-maintenance schedule remains present but disabled. Dashboard templates
and rule definitions remain manual sources even though full-demo classifies
them in its logical inventory.

## Two-phase Reporting publication

For `standard` and `full-demo`, the orchestrator:

1. stages and publishes infrastructure, notebooks, experiments, and pipelines
   without the `Reporting` folder;
2. waits for `setup-pipeline` to complete;
3. starts `ml-required` and polls that exact run to a bounded terminal state;
4. stages and publishes the semantic model and report only after status
   `Completed`;
5. runs any selected optional/experimental pipelines after Reporting;
6. synchronizes Lakehouse SQL endpoint metadata so newly written tables become
   visible to SQL and Power BI clients.

The required pipeline's final activity is
`15-validate-required-ml-contract`. A missing, empty, schema-incompatible,
duplicate, invalid, or temporally incomplete required output fails the
pipeline. Skipped, failed, cancelled, deduplicated, timed-out, or unknown run
states all fail closed and leave Reporting unpublished. Optional and
experimental failures mark the journal `DEGRADED` but do not retract required
Reporting.

Phased Reporting profiles reject `deployment.unpublish_skip: false`.
Unpublishing from a partial infrastructure or Reporting stage could otherwise
delete healthy items absent from that stage. This setting is a configuration
safety check, not permission to skip the required publication gates.

SQL endpoint metadata synchronization is also optional at the step level. A
failure marks the journal `DEGRADED`; the later readiness check still fails
closed if any required Reporting table is unavailable.

## Workspace folder mapping

| Asset | Workspace location |
| --- | --- |
| Lakehouse shell and bundled KQL queryset | Workspace root |
| Setup notebooks and `setup-pipeline` | `Setup` |
| Core, ML, and ontology notebooks | `Notebooks` |
| `stream-events` | `Streaming` |
| Semantic model and report | `Reporting` |
| Other Data Pipelines | `Pipelines` |
| ML experiment shells | `ML` |
| Data Agents (post-ontology only) | `Data Agents` |

Terraform owns Eventhouse/KQL database resources. The staging process does not
publish `.platform`-only Eventhouse shells. The reset notebook has a physical
source but no automatic profile stages it.

## Item types

The resolver derives `item_types_in_scope` from the selected assets:

- `core`: `Lakehouse`, `Notebook`
- `standard`: core plus `SemanticModel`, `Report`, `KQLQueryset`,
  `DataPipeline`, and `MLExperiment`
- `full-demo`: standard plus `DataAgent`

The configured broad list is an available-type allowlist, not an instruction to
publish every type. A selected type absent from that allowlist fails
resolution.

## Parameter rewrites

Generated `parameter.yml` rules rewrite:

- OneLake and Direct Lake source identifiers;
- KQL database item IDs and query URIs;
- semantic-model connection IDs where configured;
- Data Agent workspace, semantic-model, and ontology item IDs.

Pipeline staging replaces same-workspace notebook references with each
notebook's deterministic logical ID and the default-workspace sentinel.
`fabric-cicd` resolves those values natively during publication, avoiding
generated per-notebook parameter rules.

Bulk publish is not enabled by default. The upstream API remains experimental,
and full-demo post-ontology publication still uses dynamic target-item
resolution for Data Agents.

## KQL application

`deploy/scripts/apply_kql.py` concatenates only the ordered KQL files selected
by the profile into one outer database-script payload and can execute it
against the resolved KQL database with the Kusto Python SDK. Executing KQL
without an environment/profile inventory is unsupported. Core selects no KQL.

The required target is the configured KQL database, not a hard-coded default.
The current topology uses the default database created with the Eventhouse and
therefore requires the same display name. Artifact staging rewrites only the
known shortcut, ontology, stream, and queryset target names in generated
copies. Both credential modes receive the configured tenant. Live Azure
PowerShell and renamed-target verification are tracked by
[IMP-001](../../../requirements/modules/deployment/backlog.md#imp-001).

## Task flow and ontology timing

The orchestrator supplies Terraform output to task-flow deployment, so the
workspace, Lakehouse, Eventhouse, and KQL database bind by resolved ID.
Published items not owned by Terraform still bind by type and display name.
`--workspace <name-or-id>` remains an alternative target, but deployment still
requires matching `--environment` and `--profile full-demo`; unscoped legacy
task-flow deployment fails.

Ontology creation is separate from `setup-pipeline` and the required Reporting
gate, but it completes automatically later in the same `full-demo` deploy.
The orchestrator starts the deployed `30-create-ontology` notebook on every
full-demo deployment. The notebook updates an existing ontology in place or
creates it when absent, requires the derived-graph definition rebuild to
succeed, and then the orchestrator waits for both terminal notebook success
and exactly one stable ontology item.

The next phase publishes both Data Agents and deploys the source-controlled
task flow. Before mutation, source coverage must include every selected
full-demo artifact. After mutation, the metadata service is read back and all
11 tasks, 48 item bindings, and 11 edges must match exactly.

The recovery command reruns the same idempotent phase:

```powershell
retail-setup post-ontology --env <env>
```

This command creates the ontology when absent, rejects duplicate ontology
items, stages and publishes Data Agents, deploys and reads back the task flow,
and runs complete readiness verification.

Task-flow deployment fails before publication when any selected reference is
unresolved; it never publishes a silently partial graph. The normal full-demo deploy and the recovery command use the same completion
path.

Task-flow publication currently relies on Fabric/Power BI metadata behavior
that is not a stable public source-control item contract.

## Failure semantics

- Required initial plan and ontology/task-flow completion commands fail their
  respective run.
- Blockers, missing selected sources, invalid pipeline references, disabled
  tenant switches, unsuitable capacities, and unsafe profile downgrades fail
  before mutation.
- For gated profiles, setup and required ML are mandatory exact-run terminal
  gates. `--yes` suppresses prompts but never skips either gate.
- Post-Reporting optional/experimental ML failures are recorded and execution
  continues.
- SQL endpoint metadata synchronization failures are recorded as optional
  degradation. Required-table visibility is checked again by readiness.
- Recreate polls every visible Fabric workspace page and fails closed on timeout
  or malformed pagination before Terraform apply.
- `deploy-run.json` records `PENDING`, `RUNNING`, `SUCCEEDED`, `DEGRADED`,
  `SKIPPED`, and `FAILED` step states plus overall `RUNNING`, `SUCCEEDED`,
  `DEGRADED`, or `FAILED`. It stores no raw command output, environment
  variables, tokens, or tenant identifiers and redacts credential-like
  exception text.
- Before a new or resumed run replaces the latest journal, the prior journal
  is atomically archived under `run-history/`, using the SHA-256 hash of its
  run ID as the filename. Latest-journal replacement happens only after that
  archive write succeeds. In-run status updates do not create history copies.
  Archive and latest files are separately atomic, not a distributed transaction.
- Local deployment validation checks generated files only; it does not query
  live item, binding, run, or data readiness.

An overall `DEGRADED` result means the required workspace is usable, but one or
more optional capabilities have failed or unknown evidence. Operators inspect
the linked readiness report before presenting those optional capabilities.
`FAILED` means a required capability is not ready.

Standard and full-demo run the profile-aware live verifier after their
pipeline gates. Full-demo verification runs after automatic ontology,
Data Agent, and exact task-flow completion. Verification is read-only: it does
not trigger a second pipeline. Required failed/unknown evidence fails deployment; optional
failed/unknown evidence marks the journal and linked readiness report
`DEGRADED`. Operators may explicitly trigger the profile's post-publish
pipeline only with `retail-setup verify --env <env> --run-pipeline`.

The verifier, report, and local contracts are implemented. Historical
full-demo evidence predates the current remediation; no fresh live deployment
was performed in this repair session. Readiness consistency and remote smoke
tests remain open, as does recent manually started streaming evidence under
[IMP-013](../../../requirements/modules/operations/backlog.md#imp-013).

## Evidence

- `utility/src/retail_setup/cli/main.py`
- `deploy/scripts/build_artifacts.py`
- `deploy/scripts/deploy_config.py`
- `deploy/scripts/apply_kql.py`
- `deploy/scripts/taskflow.py`
- `deploy/scripts/run_pipeline.py`
- `deploy/scripts/fabric_runtime.py`
- `deploy/scripts/verify_readiness.py`
- `tests/deploy/`
