# Semantic Model Changes - Date Dimension & Type Fixes

## Summary

This update adds a date dimension to the Silver layer and fixes ID type mismatches across the data pipeline.

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

**Creation**: Generated in notebook `02-historical-data-load.ipynb` after dimension loads

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
3. **`fabric/semantic_model/retail_model.SemanticModel/definition/tables/dim_date.tmdl`**
   - New file: Date dimension definition

4. **`fabric/semantic_model/retail_model.SemanticModel/definition/relationships.tmdl`**
   - Added 4 new date relationships

5. **`fabric/semantic_model/README.md`**
   - Updated table count (11 → 12)
   - Documented dim_date attributes

## Deployment Steps

### Step 1: Re-run Historical Load

The historical load notebook will now create `dim_date` automatically:

```bash
# In Fabric Lakehouse, run:
fabric/lakehouse/02-historical-data-load.ipynb
```

**Expected outcome**:
- `ag.dim_date` created with ~700-1000 rows (depends on data range)
- All dimension and fact tables have corrected ID types

### Step 2: Verify dim_date Created

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

### Step 3: Update Semantic Model

If using **Power BI Desktop**:
1. Open the `.pbip` project
2. Refresh the model (dim_date should appear automatically)
3. Verify relationships in Model view
4. Publish to Fabric

If using **Fabric Portal**:
1. The model will auto-sync via Git integration
2. Or re-upload the semantic model folder
3. Refresh the semantic model

### Step 4: Verify Relationships

In Power BI Model view, verify:
- ✅ `dim_date` table appears
- ✅ 4 relationships to Gold daily tables are active
- ✅ Date filtering works across perspectives

### Step 5: Test ID Type Fixes

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

**Cause**: No fact data exists yet

**Solution**:
```python
# Manually set date range in notebook cell
min_date = datetime(2024, 1, 1).date()
max_date = datetime(2025, 12, 31).date()
```

### Issue: Type mismatch errors in Gold tables

**Cause**: Gold tables created before type fixes

**Solution**: Drop and recreate Gold tables
```sql
DROP TABLE IF EXISTS au.sales_minute_store;
DROP TABLE IF EXISTS au.inventory_position_current;
-- ... repeat for all Gold tables

-- Then re-run:
fabric/lakehouse/02-historical-data-load.ipynb
```

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

If issues occur, revert to previous state:

```bash
# Restore previous notebook versions
git checkout HEAD~1 fabric/lakehouse/02-historical-data-load.ipynb
git checkout HEAD~1 fabric/lakehouse/03-streaming-to-silver.ipynb

# Drop dim_date
DROP TABLE IF EXISTS ag.dim_date;

# Restore semantic model
git checkout HEAD~1 fabric/semantic_model/
```

## Next Steps

1. **Test Date Filtering**: Create reports using dim_date attributes
2. **Performance Tuning**: Add indexes on date columns if needed
3. **Extend Calendar**: Add holiday flags, business day calculations
4. **Fiscal Calendar**: Customize fiscal year start month if needed

## Contact

For issues or questions, refer to:
- Deployment guide: `docs/setup/08-semantic-model-deployment.md`
- Troubleshooting: `docs/setup/troubleshooting.md`
