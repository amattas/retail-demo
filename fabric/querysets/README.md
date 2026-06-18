# Querysets

Curated KQL Querysets for operations, investigations, and dashboard tiles.

Collections:
- Sales Ops: receipts/minute, top SKUs, tender mix, promo lift
- Inventory Health: low stock, stockouts, reorder backlog, shrink signals
- Customer Journey: entries, dwell, zone flows, conversions
- Logistics: truck dwell, on-time arrivals, lane performance
- Marketing: impressions → visits → purchases attribution

## Deployment

`retail-setup deploy` (via `deploy.scripts.build_artifacts`) bundles every
`*.kql` file in this folder into one Fabric `retail_querysets.KQLQueryset`
item, with one tab per file (the tab title is the file name without its
extension). The queryset's data source is bound to the Eventhouse KQL database:

- `clusterUri` is left empty so fabric-cicd fills it from the deployed KQL
  database, matched by `databaseItemName` (default `retail_eventhouse`).
- `databaseItemId` carries the `FABRIC_KQL_DATABASE_RESOURCE_ID` placeholder,
  which `parameter.yml` rewrites with the Terraform-provisioned KQL database id.

Add a new query by dropping a `.kql` file here and redeploying — no other
configuration is required. Use a different KQL database name with
`build_artifacts --kql-database-name <name>`.

