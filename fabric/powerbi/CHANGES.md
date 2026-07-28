# Semantic Model Changes - Date Dimension & Type Fixes

> **Historical change record**
>
> This file explains a past semantic-model change. It is not the current
> deployment procedure, and the historical table counts below do not describe
> the current 40-table model. Use the
> [deployment guide](../../docs/guides/deployment.md) for current commands and
> the [semantic-model specification](../../docs/design/specifications/modules/power-bi/semantic-model.md)
> for the current model contract.

## Summary

This update added a date dimension to the Silver layer and fixed ID type
mismatches across the data pipeline.

## Changes Made

### 1. New Date Dimension (`dim_date`)

**Location**: `ag.dim_date` (Silver layer)

**Key**: `date_key` (int64, YYYYMMDD format)

**Attributes**:
- `date` (date) - Actual date value
- `year`, `quarter`, `month` - Calendar hierarchy
- `month_name`, `day_name` - Display names
- `day`, `day_of_week`, `week_of_year` - Granular attributes
- `is_weekend` (0/1) - Weekend flag
- `fiscal_year`, `fiscal_quarter` - Fiscal calendar (July start)

**Date Range**: Automatically determined from fact data + 1 year forward

**Creation at the time:** Generated in notebook
`02-historical-data-load.ipynb` after dimension loads

### 2. ID Type Casting

Fixed type mismatches where IDs were stored as `double` instead of `int64/long`:

**Affected IDs**:
- `store_id` → `long`
- `dc_id` → `long`
- `truck_id` → `long`
- `customer_id` → `long`
- `product_id` → `long`
- `geography_id` → `long`
- `quantity`, `line_number`, `count`, `dwell_seconds`, `rssi` → `int`

**Implementation**:
- Added `cast_id_columns()` helper function
- Applied in both historical load (`02-historical-data-load.ipynb`) and streaming (`03-streaming-to-silver.ipynb`)
- Ensures consistent types across Bronze → Silver → Gold pipeline

### 3. Semantic Model Updates

**New Table**: `dim_date` added to model

**New Relationships** (4):
- `online_sales_daily.day` → `dim_date.date`
- `tender_mix_daily.day` → `dim_date.date`
- `marketing_cost_daily.day` → `dim_date.date`
- `truck_dwell_daily.day` → `dim_date.date`

**Perspectives Updated**: `dim_date` added to all 4 perspectives (Operations, Merchandising, Logistics, Marketing)

## Files Modified

### Notebooks
1. **`fabric/lakehouse/02-historical-data-load.ipynb`**
   - Added dim_date generation cell (after cell-5)
   - Updated helper functions with `cast_id_columns()`
   - Applied type casting in `load_to_silver()`

2. **`fabric/lakehouse/03-streaming-to-silver.ipynb`**
   - Added `cast_id_columns()` function
   - Updated `process_events()` to apply type casting
   - Updated transform functions with explicit casts

### Semantic Model
3. **`fabric/powerbi/retail_model.SemanticModel/definition/tables/dim_date.tmdl`**
   - New file: Date dimension definition

4. **`fabric/powerbi/retail_model.SemanticModel/definition/relationships.tmdl`**
   - Added 4 new date relationships

5. **`fabric/powerbi/README.md`**
   - Updated table count (11 → 12)
   - Documented dim_date attributes

## Current deployment and verification

Do not run `02-historical-data-load.ipynb` as the primary setup path. The
current deployment renders and publishes setup notebooks 01 through 04, then
runs them in order for Reporting profiles:

```powershell
retail-setup render --env <env>
retail-setup deploy --env <env>
```

For `core`, run the four deployed setup notebooks in order after deployment.

After setup, verify that `dim_date` exists:

```sql
-- Check dim_date was created
SELECT COUNT(*) FROM ag.dim_date;

-- Verify date range
SELECT
    MIN(date) as min_date,
    MAX(date) as max_date,
    COUNT(*) as total_dates
FROM ag.dim_date;

-- Check structure
SELECT * FROM ag.dim_date LIMIT 10;
```

Then verify the deployed relationships in Power BI Model view:

- `dim_date` appears;
- the four relationships to Gold daily tables are active; and
- date filtering works across the intended report pages.

Test ID type consistency with read-only SQL:

Run these queries to verify type consistency:

```sql
-- Check store_id is now long in fact_receipts
DESCRIBE ag.fact_receipts;

-- Verify join works without type casting
SELECT
    r.store_id,
    s.ID as store_id_dim,
    COUNT(*) as receipt_count
FROM ag.fact_receipts r
INNER JOIN ag.dim_stores s ON r.store_id = s.ID
GROUP BY r.store_id, s.ID
LIMIT 10;
```

## Benefits

### 1. Time Intelligence
- Filter all daily aggregations by calendar attributes
- Year-over-year comparisons
- Fiscal calendar reporting
- Weekend vs weekday analysis

### 2. Type Safety
- Eliminates implicit type conversions
- Improves query performance
- Prevents precision loss in joins
- Cleaner data model

### 3. Model Consistency
- All IDs use proper integer types
- Date filtering works across all daily tables
- Relationships leverage optimal join types

## Troubleshooting

### Issue: dim_date not created

**Likely cause:** The supported setup sequence did not complete.

**Solution:** Inspect `ag.setup_run_log`, correct the failed setup stage, and
rerun the supported setup pipeline or notebooks. Do not edit generated date
ranges directly unless you are intentionally developing the generator.

### Issue: Type mismatch errors in Gold tables

**Likely cause:** Gold tables were produced by an older deployment.

**Solution:** Rerender and redeploy the current setup notebooks. The supported
publication path stages and validates replacements before promotion; do not
manually drop all Gold tables as a first recovery action.

### Issue: Relationships not showing in model

**Cause**: Column names don't match

**Solution**: Verify column names in Gold tables
```sql
-- Check column names in Gold daily tables
DESCRIBE au.online_sales_daily;
DESCRIBE au.tender_mix_daily;

-- Should have a 'day' column of type date
```

## Rollback

Use normal source-control review and a deliberate revert when this historical
change needs to be undone in a development branch. In a deployed workspace,
follow the [operations guide](../../docs/guides/operations.md); do not delete
`dim_date` or restore arbitrary notebook versions without checking dependent
tables and report relationships.

## Next Steps

1. **Test Date Filtering**: Create reports using dim_date attributes
2. **Performance Tuning**: Add indexes on date columns if needed
3. **Extend Calendar**: Add holiday flags, business day calculations
4. **Fiscal Calendar**: Customize fiscal year start month if needed

## Contact

For issues or questions, refer to:
- [Deployment guide](../../docs/guides/deployment.md)
- [Operations and troubleshooting](../../docs/guides/operations.md)
