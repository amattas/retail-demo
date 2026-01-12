# Phase 4: Gold Layer Aggregation

The Gold layer creates pre-aggregated KPI tables for fast dashboard queries.

## Step 4.1: Verify Gold Tables

The Gold layer tables are created automatically by `02-historical-data-load.ipynb` (run in Phase 3).

For ongoing streaming data, the Gold layer is updated by `04-streaming-to-gold.ipynb` which runs via pipeline (Phase 5).

## Step 4.2: Gold Aggregation Tables

**Gold tables created** (9 aggregated tables):

| Gold Table | Granularity | Source | Description |
|------------|-------------|--------|-------------|
| `sales_minute_store` | Per minute, per store | fact_receipts | Sales velocity |
| `top_products_15m` | Rolling 15 min | fact_receipt_lines | Product rankings |
| `inventory_position_current` | Current snapshot | fact_store_inventory_txn | Current stock levels |
| `dc_inventory_position_current` | Current snapshot | fact_dc_inventory_txn | DC stock levels |
| `truck_dwell_daily` | Daily, per truck | fact_truck_moves | Logistics performance |
| `tender_mix_daily` | Daily | fact_receipts | Payment method distribution |
| `online_sales_daily` | Daily | fact_online_order_headers | Online revenue |
| `zone_dwell_minute` | Per minute, per zone | fact_foot_traffic | Customer dwell times |
| `marketing_cost_daily` | Daily, per campaign | fact_marketing | Marketing spend |

**Time Estimate**: 5-15 minutes

## Verification

```sql
SHOW TABLES IN au;
-- Should show 9+ aggregated tables

-- Check latest sales data
SELECT * FROM au.sales_minute_store
ORDER BY ts DESC
LIMIT 10;

-- Verify aggregation logic
SELECT
    DATE(ts) as date,
    SUM(total_sales) as daily_total,
    SUM(receipts) as transaction_count
FROM au.sales_minute_store
GROUP BY DATE(ts)
ORDER BY date DESC
LIMIT 7;
```

## Expected Gold Layer

| Component | Value |
|-----------|-------|
| **Schema** | `au` |
| **Tables** | 9+ aggregated KPI tables |
| **Format** | Delta Lake |
| **Purpose** | Fast dashboard queries |

## Next Step

Continue to [Phase 5: Pipeline Setup](05-pipelines.md)
