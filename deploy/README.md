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
python -m deploy.scripts.build_artifacts --notebook-groups core setup --lakehouse-name retail_lakehouse
python -m deploy.scripts.deploy_items --environment dev
python -m deploy.scripts.apply_kql --output deploy\.generated\dev\database.kql
python -m deploy.scripts.validate_deployment --environment dev
```

`build_artifacts --notebook-groups setup` stages the rendered setup notebooks
01-04 from `utility\out\`. It does not stage `setup-05-stream-events.ipynb`.
Import `setup-05-stream-events.ipynb` manually if you want the optional live
stream driver.

## KQL script execution

`apply_kql` currently prepares one ordered `.execute database script` payload and
writes it to the path passed with `--output`. It does **not** execute the script
against Fabric.

After the Eventhouse and KQL database exist:

1. Open `deploy\.generated\<env>\database.kql`.
2. Copy the full script.
3. Run it in the target Fabric KQL database.

## Configuration notes

- `unpublish_skip` defaults to `true` to avoid deleting items unexpectedly.
- Eventstream deployment is disabled by default because Fabric source-control
  item definitions for Eventstream are not yet part of this framework.
- Pipeline, dashboard, and queryset assets remain source inputs until their
  Fabric source-control item formats are validated.
- Secrets must come from Azure login, GitHub Actions secrets, environment
  variables, Key Vault, or ignored local files. Do not commit secrets to YAML,
  Terraform files, notebooks, or generated artifacts.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `setup notebooks not rendered` | Run `retail-setup render --env <env>` before building/deploying the `setup` notebook group. |
| `terraform` command not found | Install Terraform or use `--skip-terraform` when resources already exist. |
| `fabric_cicd` import error | Install `fabric-cicd` in the active Python environment. |
| Authentication failure | Run `az login --tenant <tenant-id>` for `azure_cli`, or `Connect-AzAccount -Tenant <tenant-id>` for `azure_powershell`. |
| KQL tables missing | Run the generated `database.kql` script manually in the target KQL database. |
