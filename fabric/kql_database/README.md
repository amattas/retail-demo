# KQL Database

This folder contains ordered KQL scripts for the Fabric Eventhouse KQL database.
They define hot-path event tables, ingestion mappings, functions, materialized
views, anomaly-detection assets, and pricing-approval tables.

## Deployment

`retail-setup deploy` and `deploy\scripts\apply_kql.py` generate a combined KQL
database script at:

```text
deploy\.generated\<env>\database.kql
```

The script is wrapped in:

```kql
.execute database script <|
```

It is not executed automatically. After the Eventhouse and KQL database exist,
open the generated script and run it in the target Fabric KQL database.

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

`utility\notebooks\setup-05-stream-events.ipynb` can emit these event type names
to a Fabric Eventstream Custom Endpoint. Import that notebook manually if you
want live events; it is not currently rendered or staged by `retail-setup`.

## Use cases

- Live receipts per store, top products, and tender distribution.
- Inventory deltas, stockouts, and reorder backlog.
- Customer presence, dwell, and zone heatmaps.
- Truck arrival/departure and dwell KPIs.
- Campaign impression and promotion tracking.
