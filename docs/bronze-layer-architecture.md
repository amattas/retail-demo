# Bronze Layer Architecture

## Overview

The Bronze layer (`cusn` schema) serves as the unified data ingestion layer in the Medallion architecture. It creates Lakehouse shortcuts to both batch historical data (ADLSv2 parquet) and real-time streaming data (Eventhouse).

## Schema Name: `cusn`

The Bronze layer uses the `cusn` schema name for all shortcuts.

## Data Sources

### 1. ADLSv2 Parquet (Batch Historical Data)

**Source**: Azure Data Lake Storage Gen2
**Storage Account**: `stdretail`
**Container**: `supermarket`
**Format**: Parquet (monthly partitions for fact tables)

**Tables**: 24 tables (6 dimensions + 18 facts)

#### Dimension Tables (6)
- `cusn.dim_geographies`
- `cusn.dim_stores`
- `cusn.dim_distribution_centers`
- `cusn.dim_trucks`
- `cusn.dim_customers`
- `cusn.dim_products`

#### Fact Tables (18)
- `cusn.fact_receipts`
- `cusn.fact_receipt_lines`
- `cusn.fact_store_inventory_txn`
- `cusn.fact_dc_inventory_txn`
- `cusn.fact_truck_moves`
- `cusn.fact_truck_inventory`
- `cusn.fact_foot_traffic`
- `cusn.fact_ble_pings`
- `cusn.fact_customer_zone_changes`
- `cusn.fact_marketing`
- `cusn.fact_online_order_headers`
- `cusn.fact_online_order_lines`
- `cusn.fact_payments`
- `cusn.fact_store_ops`
- `cusn.fact_stockouts`
- `cusn.fact_promotions`
- `cusn.fact_promo_lines`
- `cusn.fact_reorders`

### 2. Eventhouse (Real-Time Streaming Data)

**Source**: Fabric Eventhouse KQL Database
**Database**: `kql_retail_db`
**Format**: KQL tables (streaming events)

**Event Tables**: 18 tables

#### Transaction Events (3)
- `cusn.receipt_created`
- `cusn.receipt_line_added`
- `cusn.payment_processed`

#### Inventory Events (3)
- `cusn.inventory_updated`
- `cusn.stockout_detected`
- `cusn.reorder_triggered`

#### Customer Events (3)
- `cusn.customer_entered`
- `cusn.customer_zone_changed`
- `cusn.ble_ping_detected`

#### Operational Events (4)
- `cusn.truck_arrived`
- `cusn.truck_departed`
- `cusn.store_opened`
- `cusn.store_closed`

#### Marketing Events (2)
- `cusn.ad_impression`
- `cusn.promotion_applied`

#### Omnichannel Events (3)
- `cusn.online_order_created`
- `cusn.online_order_picked`
- `cusn.online_order_shipped`

## Total Shortcuts: 42

- **Batch Parquet**: 24 shortcuts (6 dims + 18 facts)
- **Streaming Events**: 18 shortcuts
- **Total**: 42 Bronze layer shortcuts

## Implementation

The Bronze layer is created and managed through the notebook:
- **Notebook**: `fabric/lakehouse/00-create-bronze-shortcuts.ipynb`
- **Execution**: Run once during initial setup, or when new tables are added

### Shortcut Creation Process

1. **Create Bronze Schema**:
   ```sql
   CREATE SCHEMA IF NOT EXISTS cusn;
   ```

2. **Create ADLSv2 Shortcuts** (for each parquet table):
   ```sql
   CREATE TABLE IF NOT EXISTS cusn.{table_name}
   USING PARQUET
   LOCATION 'abfss://supermarket@stdretail.dfs.core.windows.net/{table_name}/';
   ```

3. **Create Eventhouse Shortcuts** (for each event table):
   - Use Fabric UI or REST API to create shortcuts to KQL tables
   - Target schema: `cusn`
   - Source: Eventhouse KQL database tables

## Schema Alignment

### Batch Parquet Schema
- **Field Names**: snake_case (e.g., `receipt_id_ext`, `product_id`, `event_ts`)
- **External IDs**: VARCHAR type (e.g., `receipt_id_ext`, `order_id_ext`)
- **Timestamps**: `event_ts` column for all fact tables
- **Monetary**: Both decimal and cents columns (e.g., `total_amount`, `total_cents`)

### Streaming Event Schema
- **Field Names**: snake_case (e.g., `receipt_id`, `product_id`, `ingest_timestamp`)
- **External IDs**: string type (matches batch VARCHAR)
- **Timestamps**: `ingest_timestamp` for event ingestion time
- **Monetary**: Decimal only (e.g., `total`, `subtotal`, `tax`)

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│ ADLSv2 Parquet (Historical)        Eventhouse (Real-Time)  │
│ - 6 Dimension Tables               - 18 Event Tables        │
│ - 18 Fact Tables                   - Streaming from APIs    │
│ - Monthly partitions               - Live events            │
└─────────────────┬───────────────────────────┬───────────────┘
                  │                           │
                  └───────────┬───────────────┘
                              │ (shortcuts)
                  ┌───────────▼───────────────┐
                  │ Bronze Layer (cusn)       │
                  │ - 24 batch shortcuts      │
                  │ - 18 streaming shortcuts  │
                  │ - Total: 42 tables        │
                  └───────────┬───────────────┘
                              │ (read & transform)
                  ┌───────────▼───────────────┐
                  │ Silver Layer (ag)         │
                  │ - Combine batch+streaming │
                  │ - Validate & transform    │
                  │ - Delta format            │
                  └───────────────────────────┘
```

## Benefits

1. **Unified Access**: Single interface to query both batch and streaming data
2. **No Data Movement**: Shortcuts reference data in-place (no copying)
3. **Real-Time + Historical**: Combine streaming events with batch historical data
4. **Schema Isolation**: Bronze layer preserves source schemas
5. **Flexibility**: Easy to add new sources or update existing ones

## Maintenance

### Adding New Tables
1. Add parquet export for new table in datagen
2. Upload parquet files to ADLSv2
3. Create shortcut in `cusn` schema via notebook
4. Add corresponding event table shortcut if streaming is enabled
5. Update Silver layer to consume new Bronze tables

### Updating Schemas
1. Schema changes in datagen automatically reflected in parquet
2. Shortcuts read latest schema from source
3. Silver layer transformations may need updates for new columns

### Monitoring
- Verify shortcut connectivity: `SELECT * FROM cusn.{table_name} LIMIT 10;`
- Check row counts: Monitor Bronze table row counts vs source
- Validate schemas: `DESCRIBE TABLE cusn.{table_name};`

## Related Documentation

- [Schema Mapping: Batch ↔ Streaming](./schema-mapping-batch-streaming.md)
- [Silver Layer Transformation](../fabric/lakehouse/02-onelake-to-silver.ipynb)
- [KQL Event Tables](../fabric/kql_database/02-create-tables.kql)
