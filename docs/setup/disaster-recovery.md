# Disaster Recovery

Rollback procedures and recovery scenarios.

## Rollback Bronze Layer

To remove Bronze shortcuts and start fresh:

```python
# Run in Fabric notebook
BRONZE_SCHEMA = "cusn"

# List all shortcuts in Bronze schema
shortcuts = spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").collect()

# Drop schema (removes all shortcuts)
spark.sql(f"DROP SCHEMA IF EXISTS {BRONZE_SCHEMA} CASCADE")

print(f"Dropped Bronze schema: {BRONZE_SCHEMA}")
print(f"Removed {len(shortcuts)} shortcuts")
```

**Manual Cleanup** (if programmatic drop fails):
1. Navigate to Lakehouse in Fabric workspace
2. Expand Tables → `cusn` folder
3. Right-click each shortcut → Delete
4. Confirm deletion for all 42 shortcuts

**Recreate Bronze Layer**:
- Re-run `01-create-bronze-shortcuts.ipynb` to recreate shortcuts

---

## Rollback Silver Layer

To remove Silver Delta tables:

```python
# Run in Fabric notebook or via portal
SILVER_DB = "ag"

# List all tables in Silver schema
tables = spark.sql(f"SHOW TABLES IN {SILVER_DB}").collect()

# Drop database (removes all Delta tables)
spark.sql(f"DROP DATABASE IF EXISTS {SILVER_DB} CASCADE")

print(f"Dropped Silver database: {SILVER_DB}")
print(f"Removed {len(tables)} tables")
```

**Manual Cleanup** (if programmatic drop fails):
1. Navigate to Lakehouse SQL Endpoint
2. Expand Schemas → `ag`
3. Right-click schema → Delete
4. Confirm deletion

**Recreate Silver Layer**:
- Re-run `02-historical-data-load.ipynb`

---

## Rollback Gold Layer

To remove Gold aggregation tables:

```python
# Run in Fabric notebook or via portal
GOLD_DB = "au"

# List all tables in Gold schema
tables = spark.sql(f"SHOW TABLES IN {GOLD_DB}").collect()

# Drop database (removes all Delta tables)
spark.sql(f"DROP DATABASE IF EXISTS {GOLD_DB} CASCADE")

print(f"Dropped Gold database: {GOLD_DB}")
print(f"Removed {len(tables)} tables")
```

**Manual Cleanup** (if programmatic drop fails):
1. Navigate to Lakehouse SQL Endpoint
2. Expand Schemas → `au`
3. Right-click schema → Delete
4. Confirm deletion

**Recreate Gold Layer**:
- Re-run `02-historical-data-load.ipynb` or `04-streaming-to-gold.ipynb`

---

## Quick Reset (All Layers)

Use the reset notebook for complete cleanup:

```python
# Run 99-reset-lakehouse.ipynb
# This will:
# - Drop all tables in Gold (au)
# - Drop all tables in Silver (ag)
# - Remove both databases
```

---

## Rollback Pipelines

**Via Fabric Portal**:
1. Navigate to Data Factory in your Fabric workspace
2. Select Pipelines
3. Right-click each pipeline → Delete
   - `pl_historical_load`
   - `pl_streaming_silver`
   - `pl_streaming_gold`
   - `pl_maintenance`
4. Confirm deletion

**Recreate Pipelines**:
- Follow [Phase 5: Pipeline Setup](05-pipelines.md)

---

## Rollback Semantic Model

**Via Fabric Portal**:
1. Navigate to your workspace
2. Find semantic model (e.g., `RetailDemoModel`)
3. Right-click → Delete
4. Confirm deletion

**Recreate Semantic Model**:
- Re-deploy using `fabric/semantic_model/model.tmdl`

---

## Recovery Scenarios

### Scenario 1: Corrupted Silver Tables

**Symptoms**: Silver tables have incorrect data, schema mismatches, or duplicate records

**Recovery**:
```python
# Drop and recreate specific table
table_name = "fact_receipts"
spark.sql(f"DROP TABLE IF EXISTS ag.{table_name}")

# Re-run historical load notebook
# This will recreate the table from Bronze shortcuts
```

### Scenario 2: Missing Bronze Shortcuts

**Symptoms**: Bronze shortcuts lost connection or deleted accidentally

**Recovery**:
1. Run `01-create-bronze-shortcuts.ipynb`
2. Manually recreate Eventhouse shortcuts
3. Verify with `SHOW TABLES IN cusn`

### Scenario 3: Pipeline Failures

**Symptoms**: Pipelines failing with schema mismatch or connection errors

**Recovery**:
1. Check pipeline parameters (SILVER_DB, BRONZE_SCHEMA, GOLD_DB)
2. Verify environment variables
3. Re-run pipeline with corrected parameters
4. Check logs for specific error messages

### Scenario 4: Complete Environment Reset

**Symptoms**: Need to start over completely

**Recovery** (in order):
1. Stop all running pipelines
2. Run `99-reset-lakehouse.ipynb` (drops Gold and Silver)
3. Delete Eventhouse shortcuts manually
4. Delete pipelines via Fabric portal
5. Delete semantic model
6. Re-run complete deployment from Phase 1

---

## Backup Recommendations

- Export Fabric workspace as JSON (for pipelines, notebooks)
- Document custom configuration values
- Maintain copy of connection strings in Azure Key Vault
- Version control all notebook and pipeline templates

## Data Preservation

Bronze shortcuts reference source data (ADLSv2, Eventhouse) - no data loss on rollback.

Silver/Gold Delta tables can be backed up via:
```python
# Export to Parquet before rollback
df = spark.table("ag.fact_receipts")
df.write.parquet("abfss://backup@storage.dfs.core.windows.net/silver_backup/fact_receipts")
```
