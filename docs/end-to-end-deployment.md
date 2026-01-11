# End-to-End Deployment Walkthrough

**Complete guide from data generation through user-facing dashboards**

This document provides step-by-step instructions for deploying the complete Retail Demo solution using the Bronze/Silver/Gold medallion architecture.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Phase 1: Data Generation](#phase-1-data-generation)
3. [Phase 2: Bronze Layer Setup](#phase-2-bronze-layer-setup)
4. [Phase 3: Silver Layer Transformation](#phase-3-silver-layer-transformation)
5. [Phase 4: Gold Layer Aggregation](#phase-4-gold-layer-aggregation)
6. [Phase 5: User-Facing Artifacts](#phase-5-user-facing-artifacts)
7. [Validation & Testing](#validation--testing)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Azure Resources
- **Azure Event Hubs Namespace** with hub named `retail-events`
- **Azure Data Lake Storage Gen2** account (e.g., `stdretail`)
  - Container: `supermarket`
- **Microsoft Fabric Workspace** with Real-Time Intelligence capacity

### Local Development
- **Python 3.9+** for data generator
- **Git** for cloning repository
- **Azure CLI** (optional, for automated deployment)

### Access & Permissions
- Fabric workspace Contributor or Admin
- ADLSv2 Storage Blob Data Contributor
- Event Hubs Data Sender

---

## Phase 1: Data Generation

### Step 1.1: Install Data Generator

```bash
# Clone repository
git clone https://github.com/amattas/retail-demo.git
cd retail-demo/datagen

# Install dependencies (using uv - recommended)
uv pip install -e .

# OR using pip
pip install -e .
```

### Step 1.2: Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
```

**Required Environment Variables**:
```bash
# Azure Event Hubs
EVENTHUB_CONNECTION_STRING="Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;..."
EVENTHUB_NAME="retail-events"

# Azure Storage (for parquet export)
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=stdretail;..."
AZURE_STORAGE_CONTAINER="supermarket"
```

### Step 1.3: Generate Master Data

Master data (dimensions) should be generated first as they're referenced by fact tables.

```bash
# Start FastAPI server
uv run python -m retail_datagen.api.main
# Server starts at http://localhost:8000

# In another terminal, generate master data
curl -X POST http://localhost:8000/api/export/master
```

This creates dimension CSV files:
- `data/export/dim_geographies/`
- `data/export/dim_stores/`
- `data/export/dim_distribution_centers/`
- `data/export/dim_trucks/`
- `data/export/dim_customers/`
- `data/export/dim_products/`

### Step 1.4: Generate Historical Facts

Generate historical transactional data (batch parquet files):

```bash
# Generate 1 year of historical data
curl -X POST http://localhost:8000/api/export/fact \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "batch_size": 1000
  }'
```

This creates 18 fact table parquet files (monthly partitions):
- `data/export/fact_receipts/`
- `data/export/fact_receipt_lines/`
- `data/export/fact_payments/`
- _(... all 18 fact tables)_

**⏱️ Time Estimate**: 30-60 minutes for 1 year of data

### Step 1.5: Upload to Azure Data Lake

```bash
# Using Azure CLI
az storage blob upload-batch \
  --account-name stdretail \
  --destination supermarket \
  --source data/export/ \
  --pattern "dim_*/*" \
  --pattern "fact_*/*"

# OR using AzCopy (faster for large datasets)
azcopy copy "data/export/*" \
  "https://stdretail.blob.core.windows.net/supermarket/" \
  --recursive
```

**✅ Verification**: Check Azure Portal → Storage Account → Containers → supermarket
- Should see folders: `dim_stores/`, `fact_receipts/`, etc.

---

## Phase 2: Bronze Layer Setup

The Bronze layer creates shortcuts to both batch historical data (ADLSv2) and streaming real-time data (Eventhouse).

### Step 2.1: Create Fabric Resources

**In Microsoft Fabric Portal**:

1. **Create Lakehouse**:
   - Navigate to your workspace
   - New → Lakehouse
   - Name: `retail_lakehouse`

2. **Create Eventhouse KQL Database**:
   - New → Eventhouse
   - Name: `retail_eventhouse`
   - Create KQL Database: `kql_retail_db`

### Step 2.2: Set Up KQL Event Tables

**In KQL Database Query Editor**, run the KQL scripts in order:

```bash
# From retail-demo/fabric/kql_database/
# 1. Create event tables
02-create-tables.kql

# 2. Add ingestion mappings
# For each JSON file in ingestion_mappings/*.json:
.create-or-alter table receipt_created ingestion json mapping 'mapping-json' '<paste JSON contents>'
# Repeat for all 18 event tables
```

**Tables Created**: 18 streaming event tables
- `receipt_created`, `receipt_line_added`, `payment_processed`
- `inventory_updated`, `stockout_detected`, `reorder_triggered`
- _(... all 18 event types)_

### Step 2.3: Configure Eventstream

**Create Eventstream for real-time ingestion**:

1. New → Eventstream
   - Name: `retail_events_stream`

2. **Add Source**: Azure Event Hubs
   - Connection: Your Event Hubs namespace
   - Hub: `retail-events`
   - Consumer Group: `$Default`

3. **Add Destinations** (2 destinations):

   **Destination 1: KQL Database**
   - Target: `kql_retail_db` (from Step 2.1)
   - Input data format: JSON
   - Routing: Route by `event_type` field
   - Auto-create tables: ✅ Enabled
   - Ingestion mapping: Use mappings from Step 2.2

   **Destination 2: Lakehouse Files** (optional, for raw event backup)
   - Target: `retail_lakehouse`
   - Folder: `/Files/bronze/raw_events/`
   - Partitioning: By `event_type` and `date` (from `ingest_timestamp`)

4. **Start Eventstream**

**✅ Verification**: Send test event via data generator
```bash
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{"duration_seconds": 60, "burst": 10}'
```

Check KQL Database:
```kql
receipt_created | take 10
```

### Step 2.4: Create Bronze Layer Shortcuts

**Upload and run Bronze shortcuts notebook**:

1. **Upload Notebook**:
   - In Lakehouse → Notebooks
   - Import: `fabric/lakehouse/00-create-bronze-shortcuts.ipynb`

2. **Configure Environment Variables**:
   - In Fabric workspace settings or notebook parameters:
   ```python
   ADLS_ACCOUNT = "stdretail"
   ADLS_CONTAINER = "supermarket"
   EVENTHOUSE_URI = "https://YOUR-EVENTHOUSE.kusto.windows.net"
   EVENTHOUSE_DATABASE = "kql_retail_db"
   BRONZE_SCHEMA = "cusn"
   REQUIRE_EVENTHOUSE = "true"  # Fail if Eventhouse not configured
   ```

3. **Run Notebook**:
   - Creates `cusn` schema
   - Creates 24 ADLSv2 parquet shortcuts
   - Provides instructions for 18 Eventhouse shortcuts

4. **Create Eventhouse Shortcuts Manually**:
   - Eventhouse shortcuts cannot be created programmatically via Spark
   - Follow instructions printed by notebook:
     1. In Lakehouse Explorer → Tables → Right-click → New shortcut
     2. Source: Eventhouse
     3. Connection: Your Eventhouse URI
     4. For each of 18 event tables, create shortcut to `cusn` schema

**✅ Verification**:
```sql
-- In Lakehouse SQL Analytics
SHOW TABLES IN cusn;
-- Should show 42 tables: 24 batch + 18 streaming

-- Test batch shortcut
SELECT * FROM cusn.dim_stores LIMIT 10;

-- Test streaming shortcut
SELECT * FROM cusn.receipt_created LIMIT 10;
```

**Expected Bronze Layer**:
- **Schema**: `cusn`
- **Tables**: 42 total
  - 6 dimensions (batch only)
  - 18 facts (batch parquet)
  - 18 events (streaming from Eventhouse)

---

## Phase 3: Silver Layer Transformation

The Silver layer combines batch historical data with streaming real-time data into validated Delta tables.

### Step 3.1: Upload Silver Transformation Notebook

1. **Upload Notebook**:
   - In Lakehouse → Notebooks
   - Import: `fabric/lakehouse/02-onelake-to-silver.ipynb`

2. **Configure Environment Variables**:
   ```python
   SILVER_DB = "ag"
   BRONZE_SCHEMA = "cusn"
   FAIL_ON_SCHEMA_MISMATCH = "true"  # Production mode
   ```

### Step 3.2: Run Silver Transformation

**Run the notebook** - it will:
1. ✅ Load 6 dimension tables from Bronze (batch only)
2. ✅ Load 18 fact tables from Bronze (batch + streaming combined)
3. ✅ Validate schemas before UNION operations
4. ✅ Write to `ag` schema as Delta tables

**Processing Logic**:
- **Dimensions**: Direct copy from batch parquet
- **Facts**:
  - Read batch parquet (`cusn.fact_receipts`)
  - Read streaming events (`cusn.receipt_created`)
  - Map streaming fields to batch schema
  - Validate column compatibility
  - UNION ALL (no deduplication - data generator ensures no overlap)
  - Write to Delta (`ag.fact_receipts`)

**⏱️ Time Estimate**: 10-30 minutes (depends on data volume)

### Step 3.3: Create Bronze → Silver Pipeline

**Automate Silver transformation with scheduled pipeline**:

1. **Create Pipeline**:
   - New → Data pipeline
   - Name: `pl_bronze_to_silver`

2. **Add Notebook Activity**:
   - Activity: Notebook
   - Notebook: `02-onelake-to-silver` (from Step 3.1)
   - Parameters:
     ```json
     {
       "SILVER_DB": "ag",
       "BRONZE_SCHEMA": "cusn",
       "FAIL_ON_SCHEMA_MISMATCH": "true"
     }
     ```

3. **Set Schedule**:
   - Trigger: Scheduled
   - Recurrence: Every 5 minutes
   - Start time: Current UTC time

4. **Save and Run**

**✅ Verification**:
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

**Expected Silver Layer**:
- **Schema**: `ag`
- **Tables**: 24 Delta tables
  - 6 dimensions
  - 18 facts (combined batch + streaming)
- **Format**: Delta Lake
- **Refresh**: Every 5 minutes

---

## Phase 4: Gold Layer Aggregation

The Gold layer creates pre-aggregated KPI tables for fast dashboard queries.

### Step 4.1: Upload Gold Aggregation Notebook

1. **Upload Notebook**:
   - In Lakehouse → Notebooks
   - Import: `fabric/lakehouse/03-silver-to-gold.ipynb`

2. **Configure Environment Variables**:
   ```python
   SILVER_DB = "ag"
   GOLD_DB = "au"
   ```

### Step 4.2: Run Gold Aggregation

**Run the notebook** - it creates 17+ aggregated tables:

| Gold Table | Granularity | Source | Description |
|------------|-------------|--------|-------------|
| `sales_minute_store` | Per minute, per store | fact_receipts | Sales velocity |
| `top_products_15m` | Rolling 15 min | fact_receipt_lines | Product rankings |
| `inventory_position_current` | Current snapshot | fact_store_inventory_txn | Current stock levels |
| `truck_dwell_daily` | Daily, per truck | fact_truck_moves | Logistics performance |
| `campaign_revenue_daily` | Daily, per campaign | fact_marketing + fact_receipts | Marketing ROI |
| `tender_mix_daily` | Daily | fact_receipts | Payment method distribution |
| `online_sales_daily` | Daily | fact_online_order_headers | Online revenue |
| `fulfillment_daily` | Daily | fact_online_order_lines | Fulfillment metrics |
| _(... more KPI tables)_ | | | |

**⏱️ Time Estimate**: 5-15 minutes

### Step 4.3: Create Silver → Gold Pipeline

1. **Create Pipeline**:
   - New → Data pipeline
   - Name: `pl_silver_to_gold`

2. **Add Notebook Activity**:
   - Activity: Notebook
   - Notebook: `03-silver-to-gold`
   - Parameters:
     ```json
     {
       "SILVER_DB": "ag",
       "GOLD_DB": "au"
     }
     ```

3. **Set Schedule**:
   - Trigger: Scheduled
   - Recurrence: Every 15 minutes
   - Start time: Current UTC time

**✅ Verification**:
```sql
SHOW TABLES IN au;
-- Should show 17+ aggregated tables

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

**Expected Gold Layer**:
- **Schema**: `au`
- **Tables**: 17+ aggregated KPI tables
- **Format**: Delta Lake
- **Refresh**: Every 15 minutes
- **Purpose**: Fast dashboard queries

---

## Phase 5: User-Facing Artifacts

### Step 5.1: Create Semantic Model

**Power BI Semantic Model** provides unified view of Gold layer + KQL Database.

1. **Import Semantic Model**:
   - New → Semantic model
   - Import: `fabric/semantic_model/model.tmdl`

2. **Configure Connections**:
   - **Gold Lakehouse Tables** (DirectLake):
     - Source: `retail_lakehouse`
     - Schema: `au`
     - Tables: All `gold_*` tables

   - **Dimension Tables** (DirectLake):
     - Source: `retail_lakehouse`
     - Schema: `ag`
     - Tables: `dim_stores`, `dim_products`

   - **Real-Time KQL** (DirectQuery):
     - Source: `kql_retail_db`
     - Tables: Materialized views (optional for 7-day hot data)

3. **Define Relationships**:
   - Already defined in model.tmdl
   - Verify relationships render correctly

4. **Publish Model**

### Step 5.2: Create Real-Time Dashboard

**KQL-based dashboard** for operational metrics (last 24 hours).

1. **Create Dashboard**:
   - New → Real-Time Dashboard
   - Name: `Retail Operations - Real-Time`

2. **Add Tiles** using KQL Querysets:
   - Import queries from: `fabric/kql_database/querysets/*.kql`
   - Example tiles:
     - Sales/min by Store (1h window)
     - Online Orders (15m window)
     - Fulfillment Pipeline (24h)
     - BLE Presence (30m)
     - Marketing Cost (24h)
     - Tender Mix (15m)
     - Top Products (15m)
     - Open Stockouts (24h)

3. **Configure Data Source**:
   - All tiles → Data source: `kql_retail_db`

4. **Set Auto-Refresh**:
   - Refresh interval: 30 seconds

**✅ Verification**: Dashboard should show live data updating every 30 seconds

### Step 5.3: Create Power BI Report

**Historical analytics report** using Semantic Model.

1. **Create Report**:
   - New → Power BI report
   - Connect to: Semantic model (from Step 5.1)

2. **Build Visualizations**:
   - Use Gold layer tables from `au` schema
   - Create pages for:
     - Sales trends (sales_minute_store)
     - Product performance (top_products_15m)
     - Inventory health (inventory_position_current)
     - Marketing ROI (campaign_revenue_daily)
     - Fulfillment metrics (fulfillment_daily)

3. **Publish Report**

### Step 5.4: Configure Alerts & Rules

**Real-time alerts** for business events (optional).

1. **Import Alert Definitions**:
   - Rules definitions: `fabric/kql_database/rules/definitions.kql`

2. **Create Alerts**:
   - **Stockout Alert**: When `stockout_detected` event fires
   - **High-Value Transaction**: Receipt total > $1000
   - **Truck Dwell Exceeded**: Dwell time > SLA threshold
   - **Marketing Budget Alert**: Campaign spend exceeds threshold

3. **Configure Actions**:
   - Email notifications
   - Teams channel messages
   - Power Automate flows

---

## Validation & Testing

### End-to-End Data Flow Test

**Objective**: Verify data flows from generator → Bronze → Silver → Gold → Dashboard

```bash
# 1. Start streaming data generator
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{"duration_seconds": 300, "burst": 100}'

# 2. Wait 1 minute, then check Bronze layer (Eventhouse)
# In KQL Database:
receipt_created | where ingest_timestamp > ago(1h) | count
# Expected: > 0 rows

# 3. Wait 5 minutes (for Bronze → Silver pipeline)
# In Lakehouse SQL:
SELECT COUNT(*) FROM ag.fact_receipts WHERE event_ts > CURRENT_TIMESTAMP - INTERVAL 1 HOUR;
# Expected: Matches KQL count (approximately)

# 4. Wait 15 minutes (for Silver → Gold pipeline)
# In Lakehouse SQL:
SELECT MAX(ts) FROM au.sales_minute_store;
# Expected: Within last 15 minutes

# 5. Check Dashboard
# Open Real-Time Dashboard
# Expected: Latest data visible, auto-refreshing every 30 seconds
```

### Schema Validation Test

```sql
-- Verify all Bronze shortcuts exist
SELECT COUNT(*) as bronze_table_count FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'cusn';
-- Expected: 42

-- Verify all Silver tables exist
SELECT COUNT(*) as silver_table_count FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ag';
-- Expected: 24

-- Verify all Gold tables exist
SELECT COUNT(*) as gold_table_count FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'au';
-- Expected: 17+

-- Check for schema mismatches (should be 0)
-- This is reported at end of 02-onelake-to-silver.ipynb execution
```

### Performance Validation

```sql
-- Check Gold aggregation freshness
SELECT
    'sales_minute_store' as table_name,
    MAX(ts) as latest_timestamp,
    TIMESTAMPDIFF(MINUTE, MAX(ts), CURRENT_TIMESTAMP) as minutes_lag
FROM au.sales_minute_store
UNION ALL
SELECT
    'top_products_15m',
    MAX(window_start),
    TIMESTAMPDIFF(MINUTE, MAX(window_start), CURRENT_TIMESTAMP)
FROM au.top_products_15m;
-- Expected: < 20 minutes lag (15 min pipeline + processing)
```

---

## Troubleshooting

### Issue: Bronze Layer Incomplete

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
   - Re-run `00-create-bronze-shortcuts.ipynb`
   - Check ADLSv2 connection string
   - Verify parquet files exist in storage account

2. **Missing streaming shortcuts** (< 18):
   - Eventhouse shortcuts must be created manually (see Step 2.4)
   - Verify Eventhouse URI is correct
   - Check KQL database has event tables

### Issue: Schema Mismatch Errors in Silver

**Symptom**: `02-onelake-to-silver.ipynb` fails with "Schema mismatch detected"

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
   - Update field mappings in `02-onelake-to-silver.ipynb` transform functions

### Issue: No Data in Silver/Gold

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
1. **Bronze empty**: Generate more data (see Phase 1)
2. **Pipeline not running**: Check pipeline schedule is active
3. **Pipeline failing**: Check execution logs for errors
4. **Transformation error**: Run notebook manually to see detailed errors

### Issue: Dashboard Shows No Data

**Symptom**: Real-Time Dashboard tiles are empty

**Diagnosis**:
```kql
// In KQL Database Query Editor
receipt_created | count
mv_store_sales_minute | count
```

**Solutions**:
1. **No streaming data**: Start data generator streaming (see Phase 1.5)
2. **Eventstream not running**: Check Eventstream status in Fabric Portal
3. **Materialized views not refreshing**: Check MV policies in KQL Database
4. **Dashboard query errors**: Test each KQL query individually

### Issue: Slow Dashboard Performance

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

## Configuration Reference

### Environment Variables Summary

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| **Data Generator** ||||
| `EVENTHUB_CONNECTION_STRING` | Yes | - | Event Hubs connection string |
| `EVENTHUB_NAME` | Yes | `retail-events` | Event Hub name |
| `AZURE_STORAGE_CONNECTION_STRING` | Yes | - | ADLSv2 connection string |
| `AZURE_STORAGE_CONTAINER` | Yes | `supermarket` | Storage container name |
| **Bronze Layer** ||||
| `ADLS_ACCOUNT` | Yes | `stdretail` | Storage account name |
| `ADLS_CONTAINER` | Yes | `supermarket` | Container name |
| `EVENTHOUSE_URI` | Yes | - | Eventhouse query URI |
| `EVENTHOUSE_DATABASE` | Yes | `kql_retail_db` | KQL database name |
| `BRONZE_SCHEMA` | No | `cusn` | Bronze schema name |
| `REQUIRE_EVENTHOUSE` | No | `false` | Fail if Eventhouse not configured |
| **Silver Layer** ||||
| `SILVER_DB` | No | `ag` | Silver database/schema name |
| `BRONZE_SCHEMA` | No | `cusn` | Source Bronze schema |
| `FAIL_ON_SCHEMA_MISMATCH` | No | `false` | Fail on schema incompatibility |
| **Gold Layer** ||||
| `SILVER_DB` | No | `ag` | Source Silver schema |
| `GOLD_DB` | No | `au` | Gold database/schema name |

### Pipeline Schedule Recommendations

| Pipeline | Frequency | Rationale |
|----------|-----------|-----------|
| `pl_bronze_to_silver` | 5 minutes | Balance freshness vs compute cost |
| `pl_silver_to_gold` | 15 minutes | Gold is for analytics, less time-sensitive |
| `pl_maintenance` | Daily (3 AM) | Optimize Delta tables during low usage |

---

## Next Steps

After completing this deployment:

1. **Customize for Your Use Case**:
   - Add industry-specific KPIs to Gold layer
   - Create custom dashboard pages
   - Configure alerts for your business rules

2. **Optimize Performance**:
   - Monitor pipeline execution times
   - Add indexes to frequently queried columns
   - Adjust materialized view refresh policies

3. **Enable AI Features** (Future):
   - Copilot integration for natural language queries
   - Anomaly detection models
   - Predictive analytics

4. **Scale Data Generation**:
   - Increase streaming rate for load testing
   - Generate multi-year historical data
   - Add more stores/products for larger scale

---

## Rollback & Disaster Recovery

If you need to rollback the medallion architecture deployment or recover from issues, follow these procedures:

### Rollback Bronze Layer

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
- Re-run `00-create-bronze-shortcuts.ipynb` to recreate shortcuts

### Rollback Silver Layer

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
- Re-run `02-onelake-to-silver.ipynb` or `pl_bronze_to_silver` pipeline

### Rollback Gold Layer

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
- Re-run `03-silver-to-gold.ipynb` or `pl_silver_to_gold` pipeline

### Rollback Pipelines

To remove deployed pipelines:

**Via Fabric Portal**:
1. Navigate to Data Factory in your Fabric workspace
2. Select Pipelines
3. Right-click each pipeline → Delete
   - `pl_bronze_to_silver`
   - `pl_silver_to_gold`
   - `pl_maintenance` (if deployed)
4. Confirm deletion

**Via PowerShell** (optional):
```powershell
# Requires Fabric PowerShell module
$workspaceId = "<your-workspace-id>"
$pipelines = @("pl_bronze_to_silver", "pl_silver_to_gold", "pl_maintenance")

foreach ($pipeline in $pipelines) {
    Remove-FabricPipeline -WorkspaceId $workspaceId -Name $pipeline
    Write-Host "Deleted pipeline: $pipeline"
}
```

**Recreate Pipelines**:
- Re-deploy using Fabric portal or PowerShell with template files

### Rollback Semantic Model

To remove or reset the semantic model:

**Via Fabric Portal**:
1. Navigate to your workspace
2. Find semantic model (e.g., `RetailDemoModel`)
3. Right-click → Delete
4. Confirm deletion

**Update Existing Model**:
If you need to revert table references:
1. Open semantic model in Power BI Desktop
2. Transform Data → Advanced Editor
3. Update table source paths:
   - Gold: `/Tables/au/*` → `/Tables/gold/*`
   - Silver: `/Tables/ag/*` → `/Tables/silver/*`
4. Refresh and re-publish

**Recreate Semantic Model**:
- Re-deploy using `fabric/semantic_model/model.tmdl`

### Disaster Recovery Scenarios

#### Scenario 1: Corrupted Silver Tables
**Symptoms**: Silver tables have incorrect data, schema mismatches, or duplicate records

**Recovery**:
```python
# Drop and recreate specific table
table_name = "fact_receipts"
spark.sql(f"DROP TABLE IF EXISTS ag.{table_name}")

# Re-run Silver transformation
# This will recreate the table from Bronze shortcuts
%run ./02-onelake-to-silver.ipynb
```

#### Scenario 2: Missing Bronze Shortcuts
**Symptoms**: Bronze shortcuts lost connection or deleted accidentally

**Recovery**:
1. Run validation notebook: `05-validate-bronze-shortcuts.ipynb`
2. Identify missing shortcuts from output
3. Re-run Bronze creation notebook: `00-create-bronze-shortcuts.ipynb`
4. Verify with validation script again

#### Scenario 3: Pipeline Failures
**Symptoms**: Pipelines failing with schema mismatch or connection errors

**Recovery**:
1. Check pipeline parameters (SILVER_DB, BRONZE_SCHEMA, GOLD_DB)
2. Verify environment variables:
   - Set `FAIL_ON_SCHEMA_MISMATCH=false` for testing
   - Set `REQUIRE_EVENTHOUSE=false` if Eventhouse unavailable
3. Re-run pipeline with corrected parameters
4. Check logs for specific error messages

#### Scenario 4: Complete Environment Reset
**Symptoms**: Need to start over completely

**Recovery** (in order):
1. Stop all running pipelines
2. Rollback Gold layer (drop `au` database)
3. Rollback Silver layer (drop `ag` database)
4. Rollback Bronze layer (drop `cusn` shortcuts)
5. Delete pipelines via Fabric portal
6. Delete semantic model
7. Re-run complete deployment from Phase 1

**Backup Recommendations**:
- Export Fabric workspace as JSON (for pipelines, notebooks)
- Document custom configuration values
- Maintain copy of connection strings in Azure Key Vault
- Version control all notebook and pipeline templates

**Data Preservation**:
- Bronze shortcuts reference source data (ADLSv2, Eventhouse) - no data loss on rollback
- Silver/Gold Delta tables can be backed up via:
  ```python
  # Export to Parquet before rollback
  df = spark.table("ag.fact_receipts")
  df.write.parquet("abfss://backup@storage.dfs.core.windows.net/silver_backup/fact_receipts")
  ```

---

## Fabric Capacity Planning

### Capacity SKU Recommendations

Microsoft Fabric capacity is measured in Capacity Units (CU). Choose appropriate SKU based on workload:

| Environment | SKU | CUs | vCores | RAM | Use Case |
|-------------|-----|-----|--------|-----|----------|
| **Development/POC** | F2 | 2 | 2 | 8 GB | Small-scale testing, single developer |
| **Development/Testing** | F4 | 4 | 4 | 16 GB | Team development, moderate data volumes |
| **Staging** | F8 | 8 | 8 | 32 GB | Pre-production validation, load testing |
| **Production (Small)** | F16 | 16 | 16 | 64 GB | <5M events/day, <50 concurrent users |
| **Production (Medium)** | F32 | 32 | 32 | 128 GB | 5-20M events/day, 50-200 concurrent users |
| **Production (Large)** | F64+ | 64+ | 64+ | 256+ GB | >20M events/day, >200 concurrent users |

### Workload Sizing Guidelines

**Retail Demo - Expected Resource Usage:**

| Component | Daily Data Volume | Processing Time | Capacity Impact |
|-----------|------------------|-----------------|-----------------|
| **Datagen → Event Hubs** | 1-10M events | Continuous | Minimal (external) |
| **Eventhouse Ingestion** | 1-10M events | Continuous | High (streaming) |
| **Bronze Shortcuts** | 0 GB (references only) | <1 min | Minimal |
| **Silver Transformation** | 500MB - 5GB | 5-10 min per run | Medium (every 5 min) |
| **Gold Aggregation** | 100MB - 1GB | 3-5 min per run | Low (every 15 min) |
| **Power BI DirectLake** | Varies by users | Real-time | Medium (concurrent queries) |

**Formula for Initial Sizing:**
```
Required CUs ≈ (Events per day / 1M) × 2 + (Concurrent users / 50) × 4
```

**Examples:**
- 2M events/day, 10 users: 2×2 + 0.2×4 = **~5 CUs** → F4 or F8
- 10M events/day, 100 users: 10×2 + 2×4 = **~28 CUs** → F32
- 50M events/day, 500 users: 50×2 + 10×4 = **~140 CUs** → F128 or F256

### Auto-Scale Considerations

Fabric capacity supports auto-scaling to handle spiky workloads:

**When to Enable Auto-Scale:**
- Unpredictable event volumes (promotions, seasonal spikes)
- Variable user concurrency (business hours vs. off-hours)
- Cost optimization (scale down overnight)

**Configuration:**
1. Navigate to Fabric capacity settings in Azure Portal
2. Enable **Auto-scale**
3. Configure:
   - Minimum capacity: Base SKU (e.g., F16)
   - Maximum capacity: Peak SKU (e.g., F64)
   - Scale triggers: CPU/memory thresholds

**Cost Impact:**
- Auto-scale charges per-second at higher SKU when scaled up
- Can reduce costs by 30-50% vs. fixed high SKU
- Monitor scaling patterns to optimize min/max settings

### Capacity Monitoring

Monitor capacity utilization to identify need for upgrades:

**Key Metrics:**
- **CU Utilization**: Target <70% average, <90% peak
- **Throttling Events**: Should be 0 in production
- **Query Queue Time**: Target <5 seconds
- **Pipeline Execution Time**: Should not increase over time

**View Metrics:**
1. Azure Portal → Fabric Capacity → **Monitoring**
2. View:
   - CU consumption over time
   - Top consuming workspaces
   - Throttling incidents
   - Performance trends

**Upgrade Triggers:**
- CU utilization consistently >80%
- Throttling events >5 per day
- Pipeline execution time increases >50%
- User-reported performance issues

### Cost Optimization Tips

1. **Right-size Capacity**: Start with F8, monitor, then adjust
2. **Use Auto-scale**: Enable for variable workloads
3. **Optimize Queries**: Reduce CU consumption via Z-ordering, partitioning
4. **Schedule Maintenance**: Run OPTIMIZE/VACUUM during off-hours
5. **Archive Old Data**: Move inactive data to cold storage (ADLS)
6. **Consolidate Workspaces**: Share capacity across multiple projects
7. **Monitor Idle Capacity**: Pause/scale down non-production during off-hours

### Production Deployment Checklist

Before deploying to production capacity:

- [ ] Load test with expected data volumes
- [ ] Verify auto-scale configuration
- [ ] Set up capacity monitoring and alerts
- [ ] Document baseline CU utilization
- [ ] Configure budget alerts in Azure Cost Management
- [ ] Plan capacity upgrade path for growth
- [ ] Test failover to backup capacity (disaster recovery)

### Capacity Region Considerations

- Choose region close to data sources (ADLS, Event Hubs) to minimize latency
- Ensure region supports Real-Time Intelligence features
- Consider multi-region deployment for high availability (advanced)

---

## Support & Resources

- **Documentation**: `/docs/` folder in repository
- **Architecture**: `docs/architecture.md`
- **Schema Reference**: `docs/schema-mapping-batch-streaming.md`
- **Security**: `docs/fabric-security.md`
- **Monitoring**: `docs/fabric-monitoring.md`
- **Issues**: [GitHub Issues](https://github.com/amattas/retail-demo/issues)

- **Microsoft Fabric Docs**: https://learn.microsoft.com/fabric/
- **KQL Reference**: https://learn.microsoft.com/azure/data-explorer/kusto/query/
- **Fabric Capacity Planning**: https://learn.microsoft.com/fabric/enterprise/licenses

---

**Document Version**: 1.1 (January 2026)
**Last Updated**: After PR #165 - Added Fabric capacity planning, security, and monitoring guidance
