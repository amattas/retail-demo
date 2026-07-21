# Fabric deployment framework

`deploy/` contains Terraform, generated `fabric-cicd` configuration, Fabric
item staging, KQL application, pipeline triggering, task-flow deployment, and
offline validation.

Use the high-level CLI for normal operation:

```powershell
retail-setup configure --workspace-name retail-demo-alice --profile core
retail-setup deploy --env alice --dry-run
retail-setup deploy --env alice
retail-setup verify --env alice
```

The user-facing [deployment guide](../docs/guides/deployment.md) covers
prerequisites, authentication, existing workspaces, generated files, reruns,
recreate, post-deploy work, and troubleshooting.

## Ordered deploy plan

1. Run profile/source preflight.
2. Generate inputs and apply Terraform unless `--skip-terraform` is selected.
3. Stage and publish selected infrastructure without Reporting.
4. Build and execute the ordered KQL database script.
5. For Reporting profiles, wait for setup and required ML validation.
6. Stage and publish Reporting only after terminal success.
7. Run selected post-Reporting ML and validate publication.
8. For standard/full-demo, run read-only live readiness verification; the
   initial full-demo pass defers ontology-dependent checks.
9. After ontology creation, use the acknowledged post-ontology command to
   publish Data Agents, task flow, and complete readiness.

The CLI confirmation occurs before Terraform apply. Apply then prints the
change preview and proceeds with `-auto-approve`; there is no separate
Terraform plan gate.

## Authentication

`deploy/config/deploy.yml` selects:

```yaml
auth:
  mode: azure_cli
```

Supported values are `azure_cli` and `azure_powershell`. The selected operator
credential is propagated to Fabric REST, KQL, task-flow, item publication, and
pipeline helpers. Azure CLI mode validates that the active tenant matches the
configured `tenant_id`.

Azure PowerShell authenticates only the Python clients. It is not a Fabric
Terraform provider credential. Normal apply/destroy therefore requires
exactly one provider-supported service-principal, OIDC, or managed-identity
credential; otherwise use `--skip-terraform` with validated prior outputs.
Terraform is tenant-bound and its Azure CLI fallback is disabled in Azure
PowerShell mode.

The guided `scripts/setup.*` path currently checks for Azure CLI even when a
manual deployment later selects Azure PowerShell.

## Resource ownership

Terraform provisions or resolves:

- the Fabric workspace;
- workspace role assignments;
- a schema-enabled Lakehouse;
- an Eventhouse and its default KQL database;
- an optional custom Spark pool and workspace Spark settings.

`fabric-cicd` publishes:

- Lakehouse
- Notebook
- SemanticModel
- Report
- KQLQueryset
- DataPipeline
- MLExperiment
- DataAgent

Dashboard templates and rule definitions are explicitly manual source assets,
not guaranteed publishable workspace items. See the canonical
[workspace and profile inventory](../docs/guides/workspace-inventory.md).

## Generated paths

| Path | Purpose | Tracked |
| --- | --- | --- |
| `config/environments/<env>.yml` | Local workspace target overlay | No |
| `.generated/<env>/terraform.tfvars` | Generated Terraform input | No |
| `.generated/<env>/terraform.tfstate` | Isolated local Terraform state | No |
| `.generated/<env>/fabric-cicd/config.yml` | Publication configuration | No |
| `.generated/<env>/fabric-cicd/parameter.yml` | Binding rewrites | No |
| `.generated/<env>/terraform-output.json` | Captured live item identifiers | No |
| `.generated/<env>/database.kql` | Combined ordered KQL script | No |
| `.generated/<env>/deploy-run.json` | Atomic deployment step journal | No |
| `.generated/<env>/artifact-inventory-<phase>.json` | Manifest/version/profile-aware staged item evidence | No |
| `.generated/<env>/readiness-report.json` | Atomic redacted live readiness and freshness evidence | No |
| `workspace/` | Staged Fabric item folders | No, except `.gitkeep` |

The environment key is derived from the normalized workspace name. The
`retail-demo-` prefix is omitted, so workspace `retail-demo-alice` uses
environment `alice`. Target overlays, generated bindings, state, and live
identifiers are local-only and isolated by that key.

## Command modes

| Mode | Behavior |
| --- | --- |
| `--dry-run` | Validates existing configuration and the authentication boundary, then prints the plan without live access; with `--skip-terraform`, validates captured outputs too. |
| `--yes` | Pre-confirms interactive gates; required setup/ML pipeline gates still run. |
| `--skip-terraform` | Skips provisioning only after prior outputs match the configured environment, workspace, resource names, and non-placeholder IDs. |
| `--recreate` | Destroys the workspace, polls for name release for up to 180 seconds, and rebuilds it. |

`--recreate` and `--skip-terraform` cannot be combined.

## Post-deploy behavior

- `validate_deployment.py` is offline validation, not a live workspace test.
- Standard/full-demo deployment runs `retail-setup verify --env <env>` in
  read-only mode and links the report from the deploy journal. Required
  failed/unknown checks fail deployment; optional gaps degrade it.
- `retail-setup verify --env <env> --run-pipeline` is a separate explicit
  operator action that starts only the profile-required post-publish pipeline.
- `setup-pipeline` runs setup 01-04. Reporting profiles then wait for the exact
  `ml-required` run and its validator before publishing the semantic model and
  report. A failed upgrade performs no Reporting publication or replacement;
  the prior report remains compatible because required ML schemas retain their
  legacy physical bindings.
- Full-demo runs optional and experimental ML only after Reporting.
- Ontology creation is a separate preview/manual step. Initial publication
  omits Data Agents and task flow. After creating the ontology, run:

  ```powershell
  retail-setup post-ontology --env <env> `
    --acknowledge ack.full-demo.ontology-created
  ```

  to validate the ontology, publish deferred items, and verify full readiness.
- KQL application uses the selected local operator identity.
- Secrets must come from identity, environment variables, a secret store, or
  ignored local files.

See the
[deployment specification](../docs/design/specifications/modules/deployment/framework.md),
[operations guide](../docs/guides/operations.md), and
[deployment backlog](../docs/design/requirements/modules/deployment/backlog.md).
