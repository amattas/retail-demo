# Fabric Deployment Framework

This folder contains the deployment framework for Fabric items under `fabric\`
and rendered setup notebooks under `utility\out\`.

Use `retail-setup` for the normal workflow. The scripts here are the lower-level
commands that `retail-setup deploy` orchestrates.

## What lives here

- `config\deploy.yml` contains shared deployment defaults.
- `config\environments\*.yml` contains environment-specific overrides.
- `terraform\` provisions Fabric resources with
  `microsoft/terraform-provider-fabric`.
- `fabric-cicd\` contains generated configuration consumed by
  `microsoft/fabric-cicd`.
- `workspace\` is generated staging output for Fabric source-control item
  folders.
- `scripts\` contains config generation, artifact staging, deployment, KQL
  script preparation, and offline validation helpers.

Generated outputs under `deploy\.generated\` and `deploy\workspace\` are ignored
by Git.

## Prerequisites

- Python 3.11 or later.
- Terraform on `PATH` for provisioning.
- Azure CLI or Azure PowerShell authenticated to the target tenant.
- `azure-identity` and `fabric-cicd` for item deployment.
- Rendered setup notebooks in `utility\out\` when using notebook group `setup`.

Install deployment Python dependencies in your active environment:

```powershell
python -m pip install azure-identity fabric-cicd
```

## Preferred command

From the repository root:

```powershell
retail-setup deploy --env dev --dry-run
retail-setup deploy --env dev --yes
```

Use `--skip-terraform` only when the workspace/resources already exist:

```powershell
retail-setup deploy --env dev --skip-terraform
```

After the items publish, the deploy offers to run the **setup pipeline**, which
generates the historical data in Fabric (dimensions, then facts, then gold). That
generation runs asynchronously in Fabric and **can take a while** — often several
minutes to an hour or more depending on the configured months of history and store
count. You can close the CLI and track progress in the Fabric workspace.

Transient failures self-heal: cold `az` token calls and flaky Fabric/Power BI REST
calls are retried with backoff (token acquisition for the KQL apply, task flow, and
pipeline trigger), and the pipeline trigger itself is retried a few times.

## Manual script workflow

Run from the repository root:

```powershell
python -m deploy.scripts.generate_configs --environment dev
terraform -chdir=deploy\terraform init
terraform -chdir=deploy\terraform plan -var-file=environments\dev.tfvars
terraform -chdir=deploy\terraform apply -var-file=environments\dev.tfvars
terraform -chdir=deploy\terraform output -json > deploy\.generated\dev\terraform-output.json
python -m deploy.scripts.generate_configs --environment dev --terraform-output deploy\.generated\dev\terraform-output.json
retail-setup render --env dev
python -m deploy.scripts.build_artifacts --notebook-groups core setup ml --lakehouse-name retail_lakehouse
python -m deploy.scripts.deploy_items --environment dev
python -m deploy.scripts.apply_kql --output deploy\.generated\dev\database.kql
python -m deploy.scripts.validate_deployment --environment dev
```

`build_artifacts --notebook-groups setup` stages the rendered setup notebooks
01-04 from `utility\out\`. The streaming generator `stream-events.ipynb` is
staged separately by the `stream` notebook group (into the **Streaming** folder).

## KQL script execution

`apply_kql` builds one ordered `.execute database script with (ThrowOnErrors=true)`
payload from `fabric\kql_database\*.kql` and writes it to the path passed with
`--output`.

With `--execute --environment <env>` it also **applies** the script to the
Fabric Eventhouse KQL database. It resolves the database `queryServiceUri` from
the Fabric REST API (workspace/database ids come from the generated
`terraform-output.json`) and runs the batch with the Kusto Python SDK
(`azure-kusto-data`), authenticating with your Azure CLI login. `retail-setup
deploy` runs this step automatically. `ThrowOnErrors=true` makes a failed command
fail the deploy instead of reporting silent success.

```powershell
python -m deploy.scripts.apply_kql --execute --environment dev `
    --output deploy\.generated\dev\database.kql
```

This runs in the deploy process — using **your** credentials, which have
Eventhouse admin rights — rather than inside a Fabric notebook whose identity
does not.

## Configuration notes

- Published items are organized into Fabric workspace folders: demo/pipeline
  notebooks under **Notebooks**, one-time setup notebooks under **Setup**, the
  semantic model and report under **Reporting**, Data Pipelines under
  **Pipelines**, and bootstrapped MLflow experiments under **ML** (with the `ml`
  notebook group). The Lakehouse and queryset stay at the workspace root.
- `unpublish_skip` defaults to `true` to avoid deleting items unexpectedly.
- Curated KQL queries in `fabric\querysets\*.kql` deploy as a single
  `retail_querysets.KQLQueryset` item (one tab per `.kql` file) bound to the
  Eventhouse KQL database. `clusterUri` is resolved by fabric-cicd at publish
  time and `databaseItemId` is rewritten from the Terraform KQL database id.
- Data Pipelines in `fabric\pipelines\*.DataPipeline` deploy into a **Pipelines**
  folder; each pipeline is staged only when its notebooks are part of the deploy,
  and notebook references are remapped via generated `parameter.yml` rules.
- The **setup pipeline** orchestrates the full one-time setup end to end:
  `setup-01..04` (seed -> dimensions -> facts -> gold), then the **ML notebooks**
  (06-14), then `30-create-ontology`. The ontology reads Silver/Gold business
  tables, ML outputs, and Eventhouse table schemas, so it runs only after every
  ML notebook completes and after the KQL tables exist. (The ML notebooks are
  inlined as activities rather than invoking the standalone `machine-learning`
  pipeline, because Fabric's Invoke pipeline activity requires a connection
  object the deploy can't yet provision; the notebooks themselves are shared
  items, and the `machine-learning` pipeline still exists for
  manual/standalone runs.) The ontology is a one-time **setup** step; there is no
  scheduled ontology refresh after incremental loads.
- Data Agents in `fabric\data-agents\*.DataAgent` deploy into a **Data Agents**
  folder (item type `DataAgent`, which must be in `item_types_in_scope`). Their
  datasource configs reference the source workspace and the semantic model by
  GUID; generated `parameter.yml` rules remap those to the target workspace and
  the deployed `SemanticModel`. The ontology agent also references the ontology,
  which is created when the setup pipeline runs `30-create-ontology`; the agent
  binds to its ontology after that pipeline run completes.
- The `retail-setup deploy` plan stages the `core`, `setup`, `ml`, `ontology`, and
  `reset` notebook groups, so `30-create-ontology` and `99-reset-lakehouse` are
  deployed alongside the core pipeline and ML notebooks. `99-reset-lakehouse` is
  **not** orchestrated (it destroys lakehouse contents) — run it manually only.
- Dashboard assets remain source inputs until their Fabric source-control item
  formats are validated. Task flows are deployed separately by
  `deploy.scripts.taskflow` (offered as a prompt at the end of deploy). The task
  flow links items by display name, so a node binds once its item exists in the
  workspace. The ontology node (`RetailOntology_AutoGen`) and ontology agent link
  after the setup pipeline has run (it creates the ontology) and the task flow is
  re-deployed.
- Secrets must come from Azure login, GitHub Actions secrets, environment
  variables, Key Vault, or ignored local files. Do not commit secrets to YAML,
  Terraform files, notebooks, or generated artifacts.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `setup notebooks not rendered` | Run `retail-setup render --env <env>` before building/deploying the `setup` notebook group. |
| `terraform` command not found | Install Terraform or use `--skip-terraform` when resources already exist. |
| `fabric_cicd` import error | Install `fabric-cicd` in the active Python environment. |
| Authentication failure | Run `az account show --query "{tenantId:tenantId,user:user.name,name:name}" -o table` and confirm it matches deploy config. Run `az login --tenant <tenant-id>` for `azure_cli`, or `Connect-AzAccount -Tenant <tenant-id>` for `azure_powershell`. |
| KQL tables missing | Run the generated `database.kql` script manually in the target KQL database. |
| `az` token timeout / cold-start hang | A cold `az account get-access-token` can take ~90s; the deploy uses a 120s credential timeout and retries transient auth failures. Warming `az` first (`az account get-access-token --resource https://api.fabric.microsoft.com -o none`) avoids the delay. |
| Setup pipeline not triggered | The CLI retries the trigger a few times, then prints a fallback. Open the workspace in Fabric and run `setup-pipeline` manually. |
