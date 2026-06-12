# Fabric Deployment Framework

This folder contains the deployment framework for the Fabric items under `fabric\`.

## What lives here

- `config\` is the canonical deployment configuration. Edit `deploy.yml` for defaults and `config\environments\*.yml` for environment-specific values.
- `terraform\` provisions Fabric environment resources with `microsoft/terraform-provider-fabric`.
- `fabric-cicd\` contains generated configuration consumed by `microsoft/fabric-cicd`.
- `workspace\` is the staging folder for fabric-cicd item folders. It is generated from `fabric\` source assets.
- `scripts\` contains offline-safe helpers for config generation, artifact staging, deployment, KQL script preparation, and validation.

## Local workflow

From the repository root:

```powershell
python -m deploy.scripts.generate_configs --environment dev
terraform -chdir=deploy\terraform init
terraform -chdir=deploy\terraform plan -var-file=environments\dev.tfvars
terraform -chdir=deploy\terraform apply -var-file=environments\dev.tfvars
terraform -chdir=deploy\terraform output -json > deploy\.generated\dev\terraform-output.json
python -m deploy.scripts.generate_configs --environment dev --terraform-output deploy\.generated\dev\terraform-output.json
python -m deploy.scripts.build_artifacts --notebook-groups core
python -m deploy.scripts.deploy_items --environment dev
python -m deploy.scripts.apply_kql --output deploy\.generated\dev\database.kql
python -m deploy.scripts.validate_deployment --environment dev
```

The KQL helper currently writes one ordered `.execute database script` file. Run the generated script in the target KQL Database after Terraform creates the Eventhouse and KQL Database.

## Configuration notes

The initial framework is intentionally conservative:

- `unpublish_skip` defaults to `true`.
- Eventstream deployment is disabled until source-control item definitions are available.
- Pipeline, dashboard, and queryset templates remain source inputs until their Fabric source-control item formats are validated.
- Secrets must come from Azure login, GitHub Actions secrets, environment variables, or ignored local files. Do not add secrets to committed YAML or Terraform files.
