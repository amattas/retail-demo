# Troubleshooting

Common issues and solutions for the Retail Demo deployment.

## Issue: Bronze Layer Incomplete

**Symptom**: `cusn` schema has < 42 tables

**Diagnosis**:
```sql
-- Count Bronze tables
SELECT
    CASE
        WHEN TABLE_NAME LIKE 'dim_%' THEN 'dimension'
        WHEN TABLE_NAME LIKE 'fact_%' THEN 'fact_batch'
        ELSE 'event_stream'
    END as table_type,
    COUNT(*) as count
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'cusn'
GROUP BY table_type;
```

**Solutions**:

1. **Missing batch shortcuts** (< 24):
   - Re-run `01-create-bronze-shortcuts.ipynb`
   - Check ADLSv2 connection string
   - Verify parquet files exist in storage account

2. **Missing streaming shortcuts** (< 18):
   - Eventhouse shortcuts must be created manually (see [Phase 2](02-bronze-layer.md))
   - Verify Eventhouse URI is correct
   - Check KQL database has event tables

---

## Issue: Schema Mismatch Errors in Silver

**Symptom**: `02-historical-data-load.ipynb` fails with "Schema mismatch detected"

**Diagnosis**: Check notebook output for exact columns mismatched

**Solutions**:

1. **Development Mode** (allow fallback):
   ```python
   FAIL_ON_SCHEMA_MISMATCH = "false"
   ```
   - Notebook will log warning and fall back to batch-only
   - Check logs for which columns are missing/extra

2. **Fix schema alignment**:
   - Compare batch parquet schema (from datagen)
   - Compare streaming event schema (from KQL database)
   - Update field mappings in notebook transform functions

---

## Issue: No Data in Silver/Gold

**Symptom**: Silver or Gold tables exist but have 0 rows

**Diagnosis**:
```sql
-- Check Bronze has data
SELECT COUNT(*) FROM cusn.fact_receipts;
SELECT COUNT(*) FROM cusn.receipt_created;

-- Check pipeline execution history
-- In Fabric Portal → Pipelines → View runs
```

**Solutions**:

1. **Bronze empty**: Generate more data (see [Phase 1](01-data-generation.md))
2. **Pipeline not running**: Check pipeline schedule is active
3. **Pipeline failing**: Check execution logs for errors
4. **Transformation error**: Run notebook manually to see detailed errors

---

## Issue: Dashboard Shows No Data

**Symptom**: Real-Time Dashboard tiles are empty

**Diagnosis**:
```kql
// In KQL Database Query Editor
receipt_created | count
mv_store_sales_minute | count
```

**Solutions**:

1. **No streaming data**: Start data generator streaming (see [Phase 1](01-data-generation.md))
2. **Eventstream not running**: Check Eventstream status in Fabric Portal
3. **Materialized views not refreshing**: Check MV policies in KQL Database
4. **Dashboard query errors**: Test each KQL query individually

---

## Issue: Slow Dashboard Performance

**Symptom**: Dashboard takes > 5 seconds to load tiles

**Solutions**:

1. **Use materialized views**: Pre-aggregate in KQL Database
   ```kql
   // Create materialized view for dashboard query
   .create materialized-view mv_sales_15m on table receipt_created {
       receipt_created
       | where ingest_timestamp > ago(15m)
       | summarize total_sales=sum(total) by bin(ingest_timestamp, 1m), store_id
   }
   ```

2. **Add table policies**:
   ```kql
   // Set hot cache for faster queries
   .alter table receipt_created policy caching hot = 7d
   ```

3. **Use Gold layer for historical**: Switch to `au` schema for queries > 7 days

---

## Issue: Pipeline Failures

**Symptom**: Pipelines failing with connection or timeout errors

**Solutions**:

1. Check pipeline parameters (SILVER_DB, BRONZE_SCHEMA, GOLD_DB)
2. Verify environment variables are set correctly
3. Increase timeout if processing large data volumes
4. Check Fabric capacity is not throttled

---

## Issue: Parquet Schema Conflicts

**Symptom**: Error loading parquet files with mismatched schemas

**Example error**: `CANNOT_MERGE_SCHEMAS: Failed to merge schemas`

**Solution**: The `02-historical-data-load.ipynb` notebook handles this automatically by:
- Reading each parquet file individually
- Casting conflicting columns (e.g., Source) to string
- Using `unionByName(allowMissingColumns=True)`

If issues persist, run `99-reset-lakehouse.ipynb` and reload data.
