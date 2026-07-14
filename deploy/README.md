# Fabric deployment framework

`deploy/` contains Terraform, generated `fabric-cicd` configuration, Fabric
item staging, KQL application, pipeline triggering, task-flow deployment, and
offline validation.

Use the high-level CLI for normal operation:

```powershell
retail-setup deploy --env dev --dry-run
retail-setup deploy --env dev
```

The user-facing [deployment guide](../docs/guides/deployment.md) covers
prerequisites, authentication, existing workspaces, generated files, reruns,
recreate, post-deploy work, and troubleshooting.

## Ordered deploy plan

1. Generate environment-specific Terraform and `fabric-cicd` inputs.
2. Run Terraform init/apply unless `--skip-terraform` is selected.
3. Capture Terraform outputs and regenerate binding parameters.
4. Stage `core`, `setup`, `ml`, `ontology`, `reset`, and `stream` notebook
   groups plus other supported Fabric items.
5. Publish through `fabric-cicd`.
6. Build and execute the ordered KQL database script.
7. Validate generated files and placeholder rewrites.
8. Attempt task-flow deployment.
9. In interactive mode, offer to start the asynchronous `setup-pipeline`.

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

Dashboard templates and rule definitions are not yet guaranteed publishable
workspace items.

## Generated paths

| Path | Purpose | Tracked |
| --- | --- | --- |
| `terraform/environments/<env>.tfvars` | Generated Terraform input | Yes |
| `fabric-cicd/config.yml` | Generated publication configuration | Yes |
| `fabric-cicd/parameter.yml` | Generated environment and binding rewrites | Yes |
| `.generated/<env>/terraform-output.json` | Captured live item identifiers | No |
| `.generated/<env>/database.kql` | Combined ordered KQL script | No |
| `workspace/` | Staged Fabric item folders | No, except `.gitkeep` |

The tracked generated files are reviewable templates and can become modified
during configure/deploy. Generated state and local Terraform state are ignored.

## Command modes

| Mode | Behavior |
| --- | --- |
| `--dry-run` | Prints the plan only; it does not authenticate or prove live readiness. |
| `--yes` | Pre-confirms Terraform apply and suppresses the post-deploy setup-pipeline prompt. |
| `--skip-terraform` | Skips provisioning; requires accurate prior Terraform outputs for downstream IDs. |
| `--recreate` | Destroys the workspace, waits 90 seconds, and rebuilds it. |

`--recreate` and `--skip-terraform` cannot be combined.

## Post-deploy behavior

- `validate_deployment.py` is offline validation, not a live workspace test.
- `setup-pipeline` runs setup 01-04, ML notebooks 06-14, and ontology creation.
- Ontology creation occurs after the initial task-flow deployment. Re-run:

  ```powershell
  python -m deploy.scripts.taskflow deploy `
    --workspace <workspace> `
    --auth-mode azure_cli
  ```

  after the pipeline succeeds.
- KQL application uses the selected local operator identity.
- Secrets must come from identity, environment variables, a secret store, or
  ignored local files.

See the
[deployment specification](../docs/design/specifications/modules/deployment/framework.md),
[operations guide](../docs/guides/operations.md), and
[deployment backlog](../docs/design/requirements/modules/deployment/backlog.md).
