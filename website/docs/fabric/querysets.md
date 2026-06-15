# Querysets

Curated KQL queries in `fabric/querysets/` for operations, investigations, and Real-Time Dashboard tiles. Each file is a standalone query against the Eventhouse tables or materialized views.

## Queries (11)

| Query File | Description |
|------------|-------------|
| `q_receipts_minute_by_store.kql` | Store sales by minute from `mv_store_sales_minute` (60-minute lookback) |
| `q_top_products_by_sales.kql` | Top 25 products by revenue (last 15 minutes) |
| `q_tender_mix.kql` | Payment method breakdown by amount (last 15 minutes) |
| `q_online_orders_15m.kql` | Online order counts, totals, and average order value (last 15 minutes) |
| `q_fulfillment_pipeline_24h.kql` | Online order pipeline counts — created, picked, shipped (last 24 hours) |
| `q_stockouts_open_by_store.kql` | Latest stockout events by store/product (last 24 hours) |
| `q_ble_presence_30m.kql` | Unique BLE devices detected per store (last 30 minutes) |
| `q_zone_dwell_heatmap.kql` | Customer dwell and foot traffic by store/zone (last 30 minutes) |
| `q_truck_dwell_by_site.kql` | Average truck dwell minutes by store/DC (last 24 hours) |
| `q_marketing_cost_24h.kql` | Ad impressions and total cost per campaign (last 24 hours) |
| `q_campaign_conversion_funnel.kql` | Campaign impressions → conversions/revenue with attribution (last 24 hours) |

## Collections

- **Sales Ops**: receipts/minute, top SKUs, tender mix
- **Omnichannel**: online orders, fulfillment pipeline
- **Inventory Health**: open stockouts by store
- **Customer Journey**: BLE presence, zone dwell heatmap
- **Logistics**: truck dwell by site
- **Marketing**: campaign cost and conversion funnel attribution

Several of these queries back the tiles in the [retail-ops dashboard template](./dashboards.md).

## Deployment

`retail-setup deploy` bundles every `.kql` file in `fabric/querysets/` into a
single `retail_querysets.KQLQueryset` item (one tab per file) bound to the
Eventhouse KQL database, and publishes it at the workspace root. Add a query by
dropping a new `.kql` file in `fabric/querysets/` and redeploying — no other
configuration is required. The data source `clusterUri` is resolved by
fabric-cicd at publish time and `databaseItemId` is rewritten from the Terraform
KQL database id.
