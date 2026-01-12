# Phase 2: Bronze Layer Setup

The Bronze layer creates shortcuts to both batch historical data (ADLSv2) and streaming real-time data (Eventhouse).

## Step 2.1: Create Fabric Resources

**In Microsoft Fabric Portal**:

### Create Lakehouse

1. Navigate to your workspace
2. New → Lakehouse
3. Name: `retail_lakehouse`
4. **Important**: Ensure the **Lakehouse Schemas** checkbox is enabled during creation

### Create Eventhouse

1. New → Eventhouse
2. Name: `retail_eventhouse`

**Note**: Creating an Eventhouse automatically creates a KQL Database with the same name (`retail_eventhouse`). Do not create a separate KQL Database.

## Step 2.2: Set Up KQL Event Tables

**In KQL Database Query Editor**, run the KQL scripts in order:

```bash
# From retail-demo/fabric/kql_database/

# 1. Create event tables (18 streaming tables)
01-create-tables.kql

# 2. Create ingestion mappings (JSON mappings for each table)
02-create-ingestion-mappings.kql

# 3. Create reusable functions
03-create-functions.kql

# 4. Create materialized views for pre-aggregated KPIs
04-create-materialized-views.kql
```

**Tables Created**: 18 streaming event tables
- `receipt_created`, `receipt_line_added`, `payment_processed`
- `inventory_updated`, `stockout_detected`, `reorder_triggered`
- _(... all 18 event types)_

## Step 2.3: Enable OneLake Availability for Eventhouse

Before creating shortcuts, enable OneLake availability for the Eventhouse database:

1. **In Eventhouse** → Select `retail_eventhouse` database
2. Open **Database details** panel
3. Enable **OneLake availability** toggle
4. Wait for the setting to propagate (may take a few minutes)

This allows the Lakehouse to create shortcuts to Eventhouse tables.

## Step 2.4: Create Bronze Layer Shortcuts

**Upload and run Bronze shortcuts notebook**:

### Upload Notebook

1. In Lakehouse → Notebooks
2. Import: `fabric/lakehouse/01-create-bronze-shortcuts.ipynb`

### Configure Environment Variables

In Fabric workspace settings or notebook parameters:

```python
ADLS_ACCOUNT = "stdretail"
ADLS_CONTAINER = "supermarket"
EVENTHOUSE_URI = "https://YOUR-EVENTHOUSE.kusto.windows.net"
EVENTHOUSE_DATABASE = "retail_eventhouse"
BRONZE_SCHEMA = "cusn"
REQUIRE_EVENTHOUSE = "true"  # Fail if Eventhouse not configured
```

### Run Notebook

The notebook will:
- Create 24 ADLSv2 parquet shortcuts in `Files/`
- Provide instructions for 18 Eventhouse shortcuts in `Tables/cusn/`

### Create Eventhouse Shortcuts Manually

Eventhouse shortcuts cannot be created programmatically via Spark. Follow instructions printed by notebook:

1. In Lakehouse Explorer → Tables → Right-click → New shortcut
2. Source: Eventhouse
3. Connection: Your Eventhouse URI
4. For each of 18 event tables, create shortcut to `cusn` schema

## Important Notes

- **Leave ADLS shortcuts as Parquet** - Do not convert to Delta format. The Silver and Gold layers handle Delta conversion automatically.
- ADLS parquet shortcuts go in `Files/` (not Tables/)
- Eventhouse shortcuts go in `Tables/cusn/`

## Verification

```sql
-- In Lakehouse SQL Analytics
SHOW TABLES IN cusn;
-- Should show 42 tables: 24 batch + 18 streaming

-- Test batch shortcut
SELECT * FROM cusn.dim_stores LIMIT 10;

-- Test streaming shortcut
SELECT * FROM cusn.receipt_created LIMIT 10;
```

## Expected Bronze Layer

| Component | Count |
|-----------|-------|
| **Schema** | `cusn` |
| **Dimensions** | 6 (batch only) |
| **Facts** | 18 (batch parquet) |
| **Events** | 18 (streaming from Eventhouse) |
| **Total Tables** | 42 |

## Next Step

Continue to [Phase 3: Silver Layer](03-silver-layer.md)
