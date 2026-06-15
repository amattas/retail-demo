# Phase 4: Build Gold Tables

The Gold layer contains pre-aggregated KPI tables for reporting. It is built
from persisted Silver facts.

## Step 4.1: Run setup-04

Run this notebook after `setup-03-generate-facts` completes:

```text
setup-04-build-gold
```

It reads Silver tables from schema `ag` and writes Gold tables to schema `au`.

## Gold tables

| Gold table | Source | Purpose |
| --- | --- | --- |
| `sales_minute_store` | `fact_receipts` | Sales velocity by minute/store |
| `top_products_15m` | `fact_receipt_lines` | Product revenue and units in 15-minute windows |
| `inventory_position_current` | `fact_store_inventory_txn` | Latest store inventory position |
| `dc_inventory_position_current` | `fact_dc_inventory_txn` | Latest DC inventory position |
| `truck_dwell_daily` | `fact_truck_moves` | Daily logistics dwell metrics |
| `online_sales_daily` | `fact_online_order_headers` | Daily online sales |
| `zone_dwell_minute` | `fact_foot_traffic` | Zone dwell and customer counts |
| `marketing_cost_daily` | `fact_marketing` | Daily marketing impressions and cost |
| `tender_mix_daily` | `fact_receipts` | Payment method mix |

## Verification

```sql
SHOW TABLES IN au;

SELECT * FROM au.sales_minute_store
ORDER BY ts DESC
LIMIT 10;

SELECT
    day,
    orders,
    total,
    avg_order_value
FROM au.online_sales_daily
ORDER BY day DESC
LIMIT 10;
```

## Next step

Continue to [Phase 5: Optional pipelines](05-pipelines.md).
