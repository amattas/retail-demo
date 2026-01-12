# Lakehouse

Fabric Lakehouse for batch processing and historical analytics using the medallion architecture.

## Schema Naming Convention

| Schema | Layer | Purpose |
|--------|-------|---------|
| `cusn` | Bronze | Eventhouse event table shortcuts (Tables/) |
| `ag` | Silver | Cleaned, deduplicated, typed Delta tables |
| `au` | Gold | Pre-aggregated KPIs for dashboards |

**Note:** ADLS parquet shortcuts are stored in `Files/` (not in a schema).

## Notebooks

| Notebook | Schedule | Description |
|----------|----------|-------------|
| `01-create-bronze-shortcuts.ipynb` | Once | Creates 42 shortcuts to batch and streaming data |
| `02-onelake-to-silver.ipynb` | Every 5 min | Bronze → Silver transforms (JSON normalization) |
| `03-silver-to-gold.ipynb` | Every 15 min | Silver → Gold aggregations |
| `04-maintain-delta-tables.ipynb` | Daily | OPTIMIZE and VACUUM routines |

## Execution Order

For initial setup:

1. Run `01-create-bronze-shortcuts.ipynb` to create Bronze layer
2. Verify shortcuts with `SELECT * FROM cusn.{table} LIMIT 10`
3. Run `02-onelake-to-silver.ipynb` to populate Silver
4. Run `03-silver-to-gold.ipynb` to populate Gold
5. Schedule pipelines for ongoing refresh

## Bronze Layer (42 shortcuts)

### Batch Parquet (24)
- **Dimensions (6)**: geographies, stores, distribution_centers, trucks, customers, products
- **Facts (18)**: receipts, receipt_lines, inventory transactions, truck moves, foot traffic, marketing, online orders, etc.

### Streaming (18)
All KQL event tables from Eventhouse (receipt_created, inventory_updated, etc.)

## Silver Layer

Cleaned and typed Delta tables:
- `ag.silver_receipts`, `ag.silver_receipt_lines`
- `ag.silver_store_inventory_txn`, `ag.silver_dc_inventory_txn`
- `ag.silver_foot_traffic`, `ag.silver_ble_pings`
- `ag.silver_marketing`, `ag.silver_online_order_*`
- `ag.silver_truck_moves`

## Gold Layer

Pre-aggregated tables for dashboards:
- `au.gold_sales_minute_store` - Sales by store/minute
- `au.gold_top_products_15m` - Top products by revenue
- `au.gold_inventory_position_current` - Current inventory
- `au.gold_truck_dwell_daily` - Logistics SLAs
- `au.gold_campaign_revenue_daily` - Marketing attribution

## Pipelines

| Pipeline | Schedule | Description |
|----------|----------|-------------|
| `pl_bronze_to_silver` | Every 5 min | Runs Silver transform notebook |
| `pl_silver_to_gold` | Every 15 min | Runs Gold aggregation notebook |
| `pl_maintenance` | Daily | Runs OPTIMIZE/VACUUM |

All pipelines: 3 retries, 30s intervals, 1-hour timeout.

## Related Documentation

- [Architecture](../architecture.md) - Bronze layer details
- [Data Schema](../data-schema.md) - Table schemas
