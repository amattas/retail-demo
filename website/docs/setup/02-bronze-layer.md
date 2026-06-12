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

**Optional scripts** (run later if you deploy the corresponding features):

```bash
# 5. KQL-native anomaly detection functions (optional)
06-ml-anomaly-detection.kql

# 6. Dynamic pricing approval workflow tables (optional, see Phase 7)
07-pricing-approval-tables.kql
```

There is no `05-*.kql` script; the numbering gap is intentional.

**Tables Created**: 19 streaming tables — 18 event tables plus the `unknown_event` catch-all
- `receipt_created`, `receipt_line_added`, `payment_processed`
- `inventory_updated`, `stockout_detected`, `reorder_triggered`
- `customer_entered`, `customer_zone_changed`, `ble_ping_detected`
- `truck_arrived`, `truck_departed`, `store_opened`, `store_closed`
- `ad_impression`, `promotion_applied`
- `online_order_created`, `online_order_picked`, `online_order_shipped`
- `unknown_event` (dead-letter for unrecognized event types)

## Step 2.3: Enable OneLake Availability for Eventhouse

Before creating shortcuts, enable OneLake availability for the Eventhouse database:

1. **In Eventhouse** → Select `retail_eventhouse` database
2. Open **Database details** panel
3. Enable **OneLake availability** toggle
4. Wait for the setting to propagate (may take a few minutes)

This allows the Lakehouse to create shortcuts to Eventhouse tables.

## Step 2.4: Create Bronze Layer Shortcuts

Shortcuts are created **manually in the Fabric portal**. The `01-create-bronze-shortcuts.ipynb` notebook is a guided checklist: it creates the `cusn` schema, prints the full list of shortcuts to create, and verifies the result.

### Upload and Run the Notebook

1. In Lakehouse → Notebooks
2. Import: `fabric/lakehouse/01-create-bronze-shortcuts.ipynb`
3. Adjust the configuration cell if your names differ from the defaults:

```python
LAKEHOUSE_NAME = "retail_lakehouse"   # also settable via LAKEHOUSE_NAME env var
BRONZE_SCHEMA = "cusn"
ADLS_ACCOUNT = "stdretail"
ADLS_CONTAINER = "supermarket"
EVENTHOUSE_DATABASE = "retail_eventhouse"
```

### Create ADLS Shortcuts in Files/

For each of the 24 parquet folders (6 dimensions + 18 facts) printed by the notebook:

1. In Lakehouse Explorer → **Files** → Right-click → **New shortcut** → **Azure Data Lake Storage Gen2**
2. Connection: `https://stdretail.dfs.core.windows.net`
3. Path: `supermarket/<folder_name>` (e.g., `supermarket/fact_receipts`)

### Create Eventhouse Shortcuts in Tables/cusn/

For each of the 18 event tables printed by the notebook:

1. In Lakehouse Explorer → **Tables** → Right-click → **New shortcut** → **Microsoft OneLake**
2. Select your Eventhouse → `retail_eventhouse` database
3. Create the shortcut in the `cusn` schema

### Verify

Re-run the verification cells at the end of the notebook. They report `Files/` shortcut count (expected 24/24) and `Tables/cusn/` shortcut count (expected 18/18).

## Important Notes

- **Leave ADLS shortcuts as Parquet** - Do not convert to Delta format. The Silver and Gold layers handle Delta conversion automatically.
- ADLS parquet shortcuts go in `Files/` (not Tables/)
- Eventhouse shortcuts go in `Tables/cusn/`

## Verification

```sql
-- In Lakehouse SQL Analytics
SHOW TABLES IN cusn;
-- Should show 18 streaming shortcuts (Eventhouse event tables)

-- Test streaming shortcut
SELECT * FROM cusn.receipt_created LIMIT 10;
```

Batch parquet shortcuts live in `Files/` (not in a schema), so verify them in the Lakehouse Explorer or via Spark:

```python
# In a Fabric notebook
spark.read.parquet("Files/dim_stores").show(10)
```

## Expected Bronze Layer

| Component | Location | Count |
|-----------|----------|-------|
| **Dimensions** (batch parquet) | `Files/dim_*` | 6 |
| **Facts** (batch parquet) | `Files/fact_*` | 18 |
| **Events** (streaming from Eventhouse) | `Tables/cusn/` | 18 |
| **Total Shortcuts** | | 42 |

## Next Step

Continue to [Phase 3: Silver Layer](03-silver-layer.md)
