# Phase 2: Create Fabric Resources and KQL Tables

This phase creates or verifies the Fabric resources used by the setup notebooks
and optional live streaming path.

If you use `retail-setup deploy` without `--skip-terraform`, Terraform creates
the configured workspace resources. If you use manual import, create the
resources in the Fabric portal.

## Step 2.1: Create or verify resources

Required for historical setup:

- Fabric workspace.
- Lakehouse with schemas enabled.

Required for optional live streaming:

- Eventhouse.
- The Eventhouse's default KQL database, named `retail_eventhouse` by default.

Use the same names passed to `retail-setup configure`.

## Step 2.2: Deploy supported Fabric items

Preview deployment:

```powershell
retail-setup deploy --env dev --dry-run
```

Run deployment:

```powershell
retail-setup deploy --env dev --yes
```

Skip Terraform when resources already exist:

```powershell
retail-setup deploy --env dev --skip-terraform
```

## Step 2.3: Run the generated KQL script

`retail-setup deploy` generates:

```text
deploy/.generated/dev/database.kql
```

Open that file and run the full `.execute database script <|` payload in the
target Fabric KQL database. This creates the event tables, ingestion mappings,
functions, and materialized views used by the live path.

The KQL script is not executed automatically by the deployment framework.

## Event tables

The KQL script creates event tables for these live event types:

- `receipt_created`
- `receipt_line_added`
- `payment_processed`
- `inventory_updated`
- `stockout_detected`
- `reorder_triggered`
- `customer_entered`
- `customer_zone_changed`
- `ble_ping_detected`
- `truck_arrived`
- `truck_departed`
- `store_opened`
- `store_closed`
- `ad_impression`
- `promotion_applied`
- `online_order_created`
- `online_order_picked`
- `online_order_shipped`

## Step 2.4: Eventhouse OneLake shortcuts for live data

For live streaming, enable OneLake availability on the KQL database and create
shortcuts from Eventhouse tables into the Lakehouse `cusn` schema. The legacy
`01-create-bronze-shortcuts.ipynb` notebook can still be used as a guided
checklist/validator for shortcut setup.

Historical setup notebooks 01-04 do not require ADLS parquet shortcuts.

## Next step

Continue to [Phase 3: Generate Silver tables](03-silver-layer.md).
