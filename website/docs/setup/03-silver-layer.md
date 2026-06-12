# Phase 3: Silver Layer Transformation

The Silver layer combines batch historical data with streaming real-time data into validated Delta tables.

## Step 3.1: Upload Lakehouse Notebooks

Upload the following notebooks to your Lakehouse:

**In Lakehouse → Notebooks → Import**:

| Notebook | Purpose |
|----------|---------|
| `01-create-bronze-shortcuts.ipynb` | Create Bronze shortcuts |
| `02-historical-data-load.ipynb` | Load historical data to Silver/Gold |
| `03-streaming-to-silver.ipynb` | Process streaming events to Silver |
| `04-streaming-to-gold.ipynb` | Aggregate Silver to Gold |
| `05-maintain-delta-tables.ipynb` | OPTIMIZE and VACUUM routines |
| `06-ml-demand-forecast.ipynb` | GBT demand forecasts |
| `07-ml-market-basket.ipynb` | FP-Growth product associations |
| `08-ml-customer-segmentation.ipynb` | RFM + K-means customer segments |
| `09-ml-churn-prediction.ipynb` | Spark ML GBT churn risk scores |
| `10-ml-promotion-effectiveness.ipynb` | Price elasticity & promotion lift |
| `11-ml-journey-analysis.ipynb` | BLE beacon journey patterns |
| `12-ml-stockout-prediction.ipynb` | Spark ML GBT stockout risk |
| `13-ml-delivery-prediction.ipynb` | Spark ML GBT dwell predictions with empirical intervals |
| `14-ml-dynamic-pricing.ipynb` | Elasticity-aware pricing + constraints |
| `30-create-ontology.ipynb` | Optional: create or refresh a Fabric ontology from core Silver tables |
| `90-augment-and-dedupe-receipts.ipynb` | Optional one-time migration: backfill `event_ts`/`event_date` on legacy Silver receipt-related tables and dedupe |
| `99-reset-lakehouse.ipynb` | Reset all tables (optional, for testing) |

## Step 3.2: Run Historical Data Load

**Run `02-historical-data-load.ipynb`** - it will:

1. ✅ Load 6 dimension tables from Files/ parquet to Silver (ag)
2. ✅ Load 18 fact tables from Files/ parquet to Silver (ag)
3. ✅ Create Gold aggregation tables in (au)
4. ✅ Handle schema conflicts in parquet files automatically

After the Silver tables exist, you can optionally run `30-create-ontology.ipynb` to create or refresh a Fabric ontology item over the core retail entities and relationships.

### Processing Logic

- **Dimensions**: Direct copy from Files/ parquet
- **Facts**:
  - Read parquet files from Files/
  - Handle schema conflicts (e.g., Source column type variations)
  - Write to Delta tables in Silver (ag)
- **Gold**: Aggregate Silver tables into pre-computed KPIs in Gold (au)

**Time Estimate**: 10-30 minutes (depends on data volume)

## Verification

```sql
-- In Lakehouse SQL Analytics
SHOW TABLES IN ag;
-- Should show 24 tables: 6 dims + 18 facts

-- Check row counts
SELECT 'fact_receipts' as table_name, COUNT(*) as rows FROM ag.fact_receipts
UNION ALL
SELECT 'fact_receipt_lines', COUNT(*) FROM ag.fact_receipt_lines
UNION ALL
SELECT 'dim_stores', COUNT(*) FROM ag.dim_stores;

-- Verify schema (should match batch schema exactly)
DESCRIBE TABLE ag.fact_receipt_lines;
```

## Expected Silver Layer

| Component | Value |
|-----------|-------|
| **Schema** | `ag` |
| **Dimensions** | 6 tables |
| **Facts** | 18 tables |
| **Total Tables** | 24 Delta tables |
| **Format** | Delta Lake |

## Next Step

Continue to [Phase 4: Gold Layer](04-gold-layer.md)
