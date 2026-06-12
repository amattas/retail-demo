# Fabric Deployment Framework Design

## Status

Drafted for review. This design assumes a hybrid deployment model: a single configurable workspace quickstart, with the same configuration structure supporting dev/test/prod environments when needed.

## Context

The repository has production-ready Fabric assets under `fabric\`, but deployment is split across manual setup steps and custom REST scripts:

- `fabric\powerbi\retail_model.SemanticModel` and `fabric\powerbi\retail_model.Report` are already in Fabric source-control item folder format.
- `fabric\lakehouse` contains raw `.ipynb` notebooks that need default Lakehouse binding before deployment.
- `fabric\kql_database` contains ordered `.kql` database scripts for tables, ingestion mappings, functions, materialized views, anomaly detection, and pricing approval tables.
- `fabric\pipelines` contains exported pipeline JSON/manifest assets, not normalized `.DataPipeline` item folders.
- `fabric\dashboards` contains dashboard templates with a literal placeholder for the Fabric KQL database resource ID.
- `STATUS.md` identifies Eventstream and semantic model deployment as remaining workspace-dependent steps.

The deployment framework should live under `deploy\`, use `microsoft/terraform-provider-fabric` to provision the target Fabric environment, and use `microsoft/fabric-cicd` to deploy source-controlled Fabric item definitions.

## Goals

1. Provide a repeatable local and GitHub Actions deployment path for Fabric workspace assets.
2. Keep environment configuration centralized and CLI-friendly.
3. Separate environment provisioning from artifact deployment.
4. Preserve `fabric\` as the source asset folder and generate or stage deployable artifacts under `deploy\`.
5. Avoid committing secrets, Terraform state, generated outputs, or environment-specific credentials.

## Non-goals

1. Provision Azure Event Hubs or other non-Fabric Azure resources.
2. Replace the synthetic data generator deployment flow.
3. Merge or publish pull requests automatically.
4. Store credentials in repository files.
5. Depend on Power BI Desktop for automated deployment.

## Considered approaches

### Recommended: split Terraform provisioning plus fabric-cicd artifact deployment

Terraform creates or references the workspace, assigns capacity, grants roles, and creates durable shells such as Lakehouse, Eventhouse, KQL Database, and optionally Eventstream. fabric-cicd then deploys item definitions from a normalized workspace folder and applies environment-specific parameterization.

This approach matches the requested frameworks, gives Terraform ownership of environment state, and lets fabric-cicd handle item ordering, parameter replacement, and orphan cleanup. It also leaves room for a light CLI to generate both Terraform variables and fabric-cicd configuration from one canonical environment config.

### Alternative: Terraform owns both shells and definitions

Terraform can update some item definitions directly through `definition` blocks. This would reduce Python deployment code but would push many item-definition concerns into HCL, make source-control item folders less natural, and duplicate fabric-cicd features such as item dependency ordering and `parameter.yml` support.

### Alternative: fabric-cicd only

fabric-cicd can deploy many Fabric item definitions, but it does not fully replace environment provisioning. Workspace creation, capacity assignment, role assignments, and stable shell resources are better expressed as infrastructure state.

## Recommended architecture

The framework should use `deploy\` as the deployment root:

```text
deploy\
  README.md
  config\
    deploy.yml
    environments\
      dev.yml
      test.yml
      prod.yml
  terraform\
    providers.tf
    variables.tf
    main.tf
    outputs.tf
    environments\
      dev.tfvars
      test.tfvars
      prod.tfvars
  fabric-cicd\
    config.yml
    parameter.yml
  workspace\
    retail_lakehouse.Lakehouse\
    retail_eventhouse.Eventhouse\
    retail_kql.KQLDatabase\
    01-create-bronze-shortcuts.Notebook\
    02-historical-data-load.Notebook\
    03-streaming-to-silver.Notebook\
    retail_model.SemanticModel\
    retail_model.Report\
  scripts\
    build_artifacts.py
    deploy_items.py
    apply_kql.py
    validate_deployment.py
```

`deploy\config\deploy.yml` is the canonical schema for shared defaults and deployment behavior. `deploy\config\environments\environment-name.yml` contains environment-specific values. The light CLI under development should update these YAML files, then derive Terraform `.tfvars`, fabric-cicd `config.yml`, fabric-cicd `parameter.yml`, and generated deployment inputs from them.

`deploy\terraform` owns Fabric infrastructure state. `deploy\fabric-cicd` owns fabric-cicd deployment settings. `deploy\workspace` contains deployable Fabric source-control item folders staged from the repo's `fabric\` assets. Generated files and Terraform outputs should be placed under `deploy\.generated\` and ignored by Git.

## Provisioning design

Terraform should manage:

- Fabric workspace, by creating a workspace or referencing an existing workspace by ID.
- Capacity assignment, by ID or display name, with `skip_capacity_state_validation` configurable for limited-permission callers.
- Workspace role assignments for users, groups, service principals, and managed identities.
- Lakehouse shell with schemas enabled.
- Eventhouse shell and configurable minimum consumption units.
- KQL Database shell attached to the Eventhouse.
- Optional Eventstream shell when eventstream definition artifacts are present.
- Optional workspace folders if the project wants to group notebooks, KQL, dashboards, and Power BI items.

Terraform should output at least:

- `workspace_id`
- `workspace_name`
- `lakehouse_id`
- `lakehouse_name`
- `lakehouse_sql_endpoint`
- `lakehouse_sql_endpoint_id`
- `eventhouse_id`
- `eventhouse_query_service_uri`
- `kql_database_id`
- `kql_database_name`
- `eventstream_id`, when enabled

The deploy scripts should consume Terraform output JSON rather than scrape portal URLs or source files.

## Artifact deployment design

fabric-cicd should deploy from `deploy\workspace` using the `deploy_with_config` function and explicit Azure credentials.

The initial item types in scope should be:

- `Lakehouse`
- `Eventhouse`
- `KQLDatabase`
- `Notebook`
- `SemanticModel`
- `Report`
- `DataPipeline`
- `KQLDashboard`
- `KQLQueryset`
- `Eventstream`, when an eventstream item definition exists

The framework should treat `fabric\` as source input:

- Copy existing Power BI item folders directly from `fabric\powerbi`.
- Stage notebooks as `.Notebook` item folders with Fabric-compatible `notebook-content.ipynb` definitions.
- Stage pipeline artifacts only after they are available in Fabric source-control item folder format. The current ARM-like pipeline export can remain a source reference, but it should not be treated as a guaranteed fabric-cicd deployment format without a conversion/export validation step.
- Stage dashboard and queryset definitions from source-control exports when available. Existing dashboard templates should be parameterized and converted only if their target Fabric item format is confirmed.
- Keep ordered KQL `.kql` database scripts as post-provision database content operations, not as fabric-cicd item definitions, unless a future source-control KQL Database export includes the equivalent objects.

This avoids inventing unsupported item formats while still moving all deployable assets toward fabric-cicd.

## Configuration design

The canonical config should support these fields:

- Environment identity: `environment`, `tenant_id`, optional `subscription_id`, and auth mode.
- Workspace: name, optional existing ID, description, capacity ID or capacity display name, capacity validation behavior, and role assignments.
- Lakehouse: name, schema support, optional default schema names, and shortcut publish behavior.
- Eventhouse: name, minimum consumption units, KQL database name, and KQL script execution order.
- Eventstream: name, enabled flag, Event Hub source connection ID, Event Hub namespace/name, consumer group, and target Lakehouse/KQL destinations.
- Notebooks: included notebook groups such as `core`, `ml`, `ontology`, and `reset`, plus default Lakehouse binding.
- Pipelines: enabled flag, schedule defaults, notebook dependencies, and whether schedules should be deployed disabled by default.
- Power BI: semantic model name, report name, DirectLake Lakehouse binding, optional semantic model connection ID, and optional post-deploy refresh.
- Dashboards and querysets: item names, KQL database binding, and whether real-time dashboards are deployed.
- Deployment behavior: item types in scope, publish skip, unpublish skip, orphan exclusion regex, feature flags, and dry-run/validation options.
- Terraform backend: local or remote backend configuration, without secrets.

The CLI should never write secrets. Secret values should come from environment variables, GitHub Actions secrets, Azure login/OIDC, or a local ignored override file.

## Parameterization design

fabric-cicd `parameter.yml` should be generated from canonical config and Terraform outputs.

Required replacements:

- DirectLake OneLake URL in `expressions.tmdl`, replacing source workspace and Lakehouse IDs.
- Semantic model expression name when the Lakehouse name changes.
- Notebook default Lakehouse metadata: `default_lakehouse`, `default_lakehouse_name`, and `default_lakehouse_workspace_id`.
- Pipeline notebook IDs and workspace IDs using dynamic variables such as `$items.Notebook.02-historical-data-load.$id` and `$workspace.$id`.
- Dashboard and queryset KQL bindings using target KQL Database/Eventhouse values.
- Eventstream destination Lakehouse/KQL IDs when eventstream deployment is enabled.
- Optional `semantic_model_binding` when a target semantic model connection ID is configured.

Use scoped file filters in `parameter.yml` so GUID replacements do not accidentally update unrelated files. Regex replacements should include surrounding context and one capture group, following fabric-cicd guidance.

## Deployment workflow

Local deployment:

1. Authenticate with Azure CLI or Azure PowerShell.
2. Run the light CLI to configure or select an environment.
3. Run Terraform init, plan, and apply from `deploy\terraform`.
4. Export Terraform outputs to `deploy\.generated\environment-name\terraform-output.json`.
5. Build or refresh `deploy\workspace` from `fabric\` source assets.
6. Deploy Fabric item definitions through fabric-cicd.
7. Execute ordered KQL database scripts against the provisioned KQL Database.
8. Optionally refresh the semantic model.
9. Run deployment validation.

GitHub Actions deployment:

1. Trigger manually with an `environment` input, and optionally on protected branches.
2. Authenticate through GitHub OIDC and `azure/login`.
3. Install Terraform, Python, `azure-identity`, and `fabric-cicd`.
4. Run Terraform plan on pull requests and Terraform apply only on approved environment deployments.
5. Run artifact build, fabric-cicd deployment, KQL apply, and validation.
6. Upload deployment logs as workflow artifacts while avoiding sensitive debug traces.

## Error handling and safety

- Fail fast when required config values are missing.
- Validate GUIDs and item names before mutating files.
- Treat Terraform output as the source of deployed IDs.
- Avoid broad exception swallowing in deploy scripts.
- Do not commit Terraform state, fabric-cicd debug logs, token caches, generated workspace output, or secrets.
- Default unpublish behavior should be conservative: skip unpublish or exclude critical shared items until the user explicitly enables cleanup.
- Keep destructive feature flags such as hard delete and Lakehouse/Eventhouse unpublish disabled by default.
- Generate clear errors for unsupported source formats instead of silently skipping artifacts.

## Validation and testing

Static validation should cover:

- Canonical config schema and environment overlays.
- Generated Terraform variable files.
- fabric-cicd `config.yml` and `parameter.yml`.
- Absence of unresolved placeholders in staged deployable artifacts.
- `.platform` metadata type/display name consistency.
- KQL script order and `.execute database script` compatibility.

Automated checks should include:

- `terraform fmt -check`
- `terraform validate`
- Python unit tests for config loading, artifact staging, and parameter generation.
- A dry-run or no-op validation mode that builds staged artifacts without contacting Fabric.
- Optional integration smoke tests against a real Fabric workspace that verify item existence, KQL tables/functions/materialized views, notebook Lakehouse binding, semantic model binding, and report deployment.

## Migration plan

1. Create the `deploy\` skeleton and canonical config schema.
2. Add Terraform for workspace, capacity assignment, roles, Lakehouse, Eventhouse, and KQL Database.
3. Add fabric-cicd config and parameter generation from canonical config plus Terraform outputs.
4. Stage and deploy existing Power BI item folders.
5. Stage and deploy notebooks with Lakehouse binding.
6. Add KQL script execution after KQL Database provisioning.
7. Normalize or source-control-export pipelines, dashboards, querysets, and eventstream artifacts before enabling them in fabric-cicd.
8. Add GitHub Actions workflows for plan, deploy, and validation.
9. Retire or wrap the existing one-off deployment scripts once the framework covers their behavior.

## Open decisions for review

These are resolved to safe defaults in the design but should be reviewed before implementation:

1. Use the hybrid environment model, with single-workspace quickstart and optional dev/test/prod mappings.
2. Keep Terraform unpublish/destructive cleanup disabled by default.
3. Treat existing pipeline and dashboard templates as source references until source-control item formats are captured or validated.
4. Leave Azure Event Hub provisioning outside this framework and configure only Fabric-side Eventstream bindings.
5. Place generated deployment outputs under `deploy\.generated\` and keep them out of source control.
