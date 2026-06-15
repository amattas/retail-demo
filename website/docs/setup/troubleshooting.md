# Troubleshooting

Common issues and fixes for the current `retail-setup` workflow.

## `utility/config.yaml not found`

Run configuration before rendering:

```powershell
retail-setup configure
retail-setup render --env dev
```

## Invalid store type

Use one of the supported store types:

- `grocery`
- `hardware`
- `luxury`
- `supercenter`

## Setup notebooks not rendered

`retail-setup deploy` expects rendered notebooks 01-04 in `utility/out/` when
the `setup` notebook group is staged.

Fix:

```powershell
retail-setup render --env dev
retail-setup deploy --env dev --dry-run
```

## Terraform command not found

Install Terraform and make sure it is on `PATH`, or use `--skip-terraform` when
the workspace and resources already exist:

```powershell
retail-setup deploy --env dev --skip-terraform
```

## Fabric `FeatureNotAvailable` during Terraform apply

**Symptom**: Terraform creates the workspace, then fails creating the Lakehouse
or Eventhouse with:

```text
Error Code: FeatureNotAvailable
Could not create resource: The feature is not available
```

This means the target Fabric tenant/capacity/workspace does not currently expose
that Fabric item type through the API/provider in the selected region or
capacity.

Fix:

1. Confirm the selected capacity supports Lakehouse and Real-Time Intelligence.
2. Confirm the workspace is assigned to the expected Fabric capacity.
3. Try creating a Lakehouse and Eventhouse manually in the Fabric portal in the
   same workspace. If the portal blocks creation, fix the tenant/capacity/region
   first.
4. If the workspace was created but item creation failed, either delete the
   partial workspace and rerun deploy, or set `workspace.existing_id` in
   `deploy/config/environments/<env>.yml` after the workspace is usable.

## `fabric_cicd` or `azure.identity` import error

Install deployment dependencies:

```powershell
python -m pip install azure-identity fabric-cicd
```

## Authentication failure

For `auth.mode: azure_cli`:

```powershell
az login --tenant <tenant-id>
```

Before deploying, confirm the active Azure CLI tenant matches
`deploy/config/deploy.yml` or `deploy/config/environments/<env>.yml`:

```powershell
az account show --query "{tenantId:tenantId,user:user.name,name:name}" -o table
```

If it does not match, rerun `az login --tenant <tenant-id>`. Terraform and
fabric-cicd use the active Azure CLI identity; the `tenant_id` value in config is
used for validation and generated files, but it does not switch the logged-in
Azure account for you.

For `auth.mode: azure_powershell`:

```powershell
Connect-AzAccount -Tenant <tenant-id>
```

## KQL tables are missing after deploy

The deployment framework generates the KQL script but does not execute it.

Fix:

1. Open `deploy/.generated/<env>/database.kql`.
2. Copy the full `.execute database script <|` payload.
3. Run it in the target Fabric KQL database.

## Silver or Gold tables are empty

Check that setup notebooks 01-04 ran in order and completed successfully.

Useful checks:

```sql
SELECT COUNT(*) FROM ag.dim_stores;
SELECT COUNT(*) FROM ag.fact_receipts;
SELECT COUNT(*) FROM au.sales_minute_store;
```

If tables exist but are empty, re-run `setup-03-generate-facts` and then
`setup-04-build-gold`.

## `setup-05-stream-events` is missing from `utility/out`

This is expected. `setup-05-stream-events.ipynb` is committed under
`utility/notebooks/`, but it is not currently rendered or staged by
`retail-setup`. Import it manually if you want live synthetic events.

## Live stream has no KQL rows

Check:

1. `setup-05-stream-events.ipynb` is using `sink = "eventstream"`.
2. Eventstream Custom Endpoint parameters are set.
3. The connection string is available from Key Vault.
4. The generated KQL script has been run in the target KQL database.

For a smoke test without Eventstream, use `sink = "delta"` in the stream
notebook.
