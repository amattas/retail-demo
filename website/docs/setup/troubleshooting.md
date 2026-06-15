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
