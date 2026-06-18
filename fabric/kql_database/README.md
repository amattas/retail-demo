# KQL Database

This folder contains ordered KQL scripts for the Fabric Eventhouse KQL database.
They define hot-path event tables, ingestion mappings, functions, materialized
views, anomaly-detection assets, and pricing-approval tables.

## Deployment

`retail-setup deploy` runs `deploy\scripts\apply_kql.py --execute`, which builds
a combined KQL database script and **applies it** to the Fabric Eventhouse KQL
database. The combined script is also written to:

```text
deploy\.generated\<env>\database.kql
```

The script is wrapped in:

```kql
.execute database script with (ThrowOnErrors=true) <|
```

`ThrowOnErrors=true` makes the batch fail on the first command error (by default
`.execute database script` reports success even when commands fail). `apply_kql`
connects with the Kusto Python SDK (`azure-kusto-data`) using your Azure CLI
login — which has Eventhouse admin rights — so no manual step is required. To
prepare the script without applying it, run `apply_kql` without `--execute`.

## Script order

The default deployment config includes these scripts in order:

1. `01-create-tables.kql`
2. `02-create-ingestion-mappings.kql`
3. `03-create-functions.kql`
4. `04-create-materialized-views.kql`
5. `06-ml-anomaly-detection.kql`
6. `07-pricing-approval-tables.kql`

## Event tables

The live stream path uses one KQL table per event type:

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

`utility\notebooks\stream-events.ipynb` can emit these event type names and
write them **directly to the Eventhouse KQL tables** with the Fabric Spark
connector for Kusto (routed by `event_type`). It is rendered and staged by
`retail-setup deploy` (the optional `stream` notebook group); run it manually to
generate live events.

## Use cases

- Live receipts per store, top products, and tender distribution.
- Inventory deltas, stockouts, and reorder backlog.
- Customer presence, dwell, and zone heatmaps.
- Truck arrival/departure and dwell KPIs.
- Campaign impression and promotion tracking.
