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
| `01-create-bronze-shortcuts.ipynb` | Once | Creates 42 shortcuts (24 Files/ + 18 Tables/cusn/) |
| `02-historical-data-load.ipynb` | Once | Loads Files/ parquet → Silver → Gold |
| `03-streaming-to-silver.ipynb` | Every 5 min | Eventhouse events → Silver (incremental) |
| `04-streaming-to-gold.ipynb` | Every 15 min | Silver → Gold aggregations |
| `05-maintain-delta-tables.ipynb` | Daily | OPTIMIZE and VACUUM routines |
| `99-reset-lakehouse.ipynb` | Manual | Drop all Silver/Gold tables and databases |

## Execution Order

### Initial Setup (Historical Data)

1. Run `01-create-bronze-shortcuts.ipynb` to create Bronze shortcuts
2. Verify shortcuts:
   - `mssparkutils.fs.ls("Files/")` for parquet
   - `SHOW TABLES IN cusn` for Eventhouse
3. Run `02-historical-data-load.ipynb` to load historical data through Silver → Gold

### Ongoing Operations (Streaming)

Schedule pipelines:
- `03-streaming-to-silver.ipynb` every 5 minutes
- `04-streaming-to-gold.ipynb` every 15 minutes
- `05-maintain-delta-tables.ipynb` daily

## Bronze Layer (42 shortcuts)

### Files/ - Batch Parquet (24)
- **Dimensions (6)**: dim_geographies, dim_stores, dim_distribution_centers, dim_trucks, dim_customers, dim_products
- **Facts (18)**: fact_receipts, fact_receipt_lines, fact_payments, fact_store_inventory_txn, fact_dc_inventory_txn, fact_truck_moves, fact_truck_inventory, fact_foot_traffic, fact_ble_pings, fact_customer_zone_changes, fact_marketing, fact_online_order_headers, fact_online_order_lines, fact_store_ops, fact_stockouts, fact_promotions, fact_promo_lines, fact_reorders

### Tables/cusn/ - Eventhouse Streaming (18)
- **Transaction (3)**: receipt_created, receipt_line_added, payment_processed
- **Inventory (3)**: inventory_updated, stockout_detected, reorder_triggered
- **Customer (3)**: customer_entered, customer_zone_changed, ble_ping_detected
- **Operational (4)**: truck_arrived, truck_departed, store_opened, store_closed
- **Marketing (2)**: ad_impression, promotion_applied
- **Omnichannel (3)**: online_order_created, online_order_picked, online_order_shipped

## Silver Layer

Delta tables in `ag` schema (same names as Bronze):
- `ag.dim_*` - 6 dimension tables
- `ag.fact_*` - 18 fact tables

## Gold Layer

Pre-aggregated tables in `au` schema:
- `au.sales_minute_store` - Sales by store/minute
- `au.top_products_15m` - Top products by revenue (15m windows)
- `au.inventory_position_current` - Current store inventory
- `au.dc_inventory_position_current` - Current DC inventory
- `au.truck_dwell_daily` - Truck dwell time by site
- `au.online_sales_daily` - Online order aggregates
- `au.zone_dwell_minute` - Customer zone dwell times
- `au.marketing_cost_daily` - Marketing spend by campaign
- `au.tender_mix_daily` - Payment method breakdown

## Pipelines

| Pipeline | Schedule | Description |
|----------|----------|-------------|
| `pl_historical_load` | Once | Runs `02-historical-data-load.ipynb` |
| `pl_streaming_silver` | Every 5 min | Runs `03-streaming-to-silver.ipynb` |
| `pl_streaming_gold` | Every 15 min | Runs `04-streaming-to-gold.ipynb` |
| `pl_maintenance` | Daily | Runs `05-maintain-delta-tables.ipynb` |

All pipelines: 3 retries, 30s intervals, 1-hour timeout.

## Related Documentation

- [Architecture](../architecture.md) - Bronze layer details
- [Data Schema](../data-schema.md) - Table schemas
