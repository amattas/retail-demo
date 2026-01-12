# Schema Mapping: Batch Parquet ↔ Streaming Events

## Overview

This document maps batch parquet fact tables to their corresponding streaming event tables, documenting schema differences and transformation logic used in the Silver layer.

## Architecture Context

- **Batch Parquet** (`cusn.fact_*`): Historical data from datagen, uploaded to ADLSv2
- **Streaming Events** (`cusn.*_created`, etc.): Real-time events from Eventhouse
- **Silver Combination** (`ag.fact_*`): UNION ALL of batch + streaming with schema alignment

## Critical Design Decision

**No Overlap**: The data generator ensures batch historical and streaming real-time data never overlap in time, so simple UNION ALL without deduplication is safe.

---

## Complete Mapping Table

| Batch Parquet Table | Streaming Event Table(s) | Combined in Silver | Notes |
|---------------------|-------------------------|-------------------|-------|
| `fact_receipts` | `receipt_created` | `ag.fact_receipts` | Field mapping required |
| `fact_receipt_lines` | `receipt_line_added` | `ag.fact_receipt_lines` | Field mapping required |
| `fact_payments` | `payment_processed` | `ag.fact_payments` | Batch only currently |
| `fact_store_inventory_txn` | `inventory_updated` | `ag.fact_store_inventory_txn` | Batch only currently |
| `fact_dc_inventory_txn` | `inventory_updated` | `ag.fact_dc_inventory_txn` | Batch only currently |
| `fact_stockouts` | `stockout_detected` | `ag.fact_stockouts` | Batch only currently |
| `fact_reorders` | `reorder_triggered` | `ag.fact_reorders` | Batch only currently |
| `fact_foot_traffic` | `customer_entered` | `ag.fact_foot_traffic` | Batch only currently |
| `fact_customer_zone_changes` | `customer_zone_changed` | `ag.fact_customer_zone_changes` | Batch only currently |
| `fact_ble_pings` | `ble_ping_detected` | `ag.fact_ble_pings` | Batch only currently |
| `fact_truck_moves` | `truck_arrived`, `truck_departed` | `ag.fact_truck_moves` | Batch only currently |
| `fact_store_ops` | `store_opened`, `store_closed` | `ag.fact_store_ops` | Batch only currently |
| `fact_marketing` | `ad_impression` | `ag.fact_marketing` | Batch only currently |
| `fact_promotions` | `promotion_applied` | `ag.fact_promotions` | Batch only currently |
| `fact_online_order_headers` | `online_order_created` | `ag.fact_online_order_headers` | Batch only currently |
| `fact_online_order_lines` | `online_order_picked`, `online_order_shipped` | `ag.fact_online_order_lines` | Batch only currently |
| `fact_truck_inventory` | *(none)* | `ag.fact_truck_inventory` | Batch only (no streaming) |
| `fact_promo_lines` | *(none)* | `ag.fact_promo_lines` | Batch only (no streaming) |

---

## Detailed Schema Mappings

### 1. fact_receipts ↔ receipt_created

**Status**: ✅ Active - Streaming events combined with batch

#### Batch Schema (`cusn.fact_receipts`)
```
event_ts: TIMESTAMP
receipt_id_ext: VARCHAR
payment_method: VARCHAR
discount_amount: DECIMAL
tax: DECIMAL
tax_cents: BIGINT
subtotal: DECIMAL
subtotal_cents: BIGINT
total: DECIMAL
total_cents: BIGINT
receipt_type: VARCHAR
customer_id: BIGINT
store_id: BIGINT
return_for_receipt_id_ext: VARCHAR (nullable)
```

#### Streaming Schema (`cusn.receipt_created`)
```
ingest_timestamp: TIMESTAMP
receipt_id: string
tender_type: string
tax: decimal
subtotal: decimal
total: decimal
customer_id: long
store_id: long
```

#### Field Mapping (Streaming → Batch)
```python
df_stream.select(
    F.col("ingest_timestamp").alias("event_ts"),
    F.col("receipt_id").alias("receipt_id_ext"),
    F.col("tender_type").alias("payment_method"),
    F.lit(0.0).alias("discount_amount"),
    F.col("tax"),
    (F.col("tax") * 100).cast("bigint").alias("tax_cents"),
    F.col("subtotal"),
    (F.col("subtotal") * 100).cast("bigint").alias("subtotal_cents"),
    F.col("total"),
    (F.col("total") * 100).cast("bigint").alias("total_cents"),
    F.lit("SALE").alias("receipt_type"),
    F.col("customer_id"),
    F.col("store_id"),
    F.lit(None).cast("string").alias("return_for_receipt_id_ext")
)
```

**Key Differences**:
- Streaming lacks `*_cents` fields → calculated as `amount * 100`
- Streaming lacks `discount_amount` → defaults to 0.0
- Streaming lacks `receipt_type` → defaults to "SALE"
- Streaming uses `tender_type` → mapped to `payment_method`
- Streaming uses `receipt_id` → mapped to `receipt_id_ext`

---

### 2. fact_receipt_lines ↔ receipt_line_added

**Status**: ✅ Active - Streaming events combined with batch

#### Batch Schema (`cusn.fact_receipt_lines`)
```
event_ts: TIMESTAMP
receipt_id_ext: VARCHAR
line_num: INTEGER
product_id: BIGINT
quantity: INTEGER
unit_price: DECIMAL
unit_cents: BIGINT
ext_price: DECIMAL
ext_cents: BIGINT
promo_code: VARCHAR (nullable)
```

#### Streaming Schema (`cusn.receipt_line_added`)
```
ingest_timestamp: TIMESTAMP
receipt_id: string
line_number: int
product_id: long
quantity: int
unit_price: decimal
extended_price: decimal
promo_code: string (nullable)
```

#### Field Mapping (Streaming → Batch)
```python
df_stream.select(
    F.col("ingest_timestamp").alias("event_ts"),
    F.col("receipt_id").alias("receipt_id_ext"),
    F.col("line_number").alias("line_num"),
    F.col("product_id"),
    F.col("quantity"),
    F.col("unit_price"),
    (F.col("unit_price") * 100).cast("bigint").alias("unit_cents"),
    F.col("extended_price").alias("ext_price"),
    (F.col("extended_price") * 100).cast("bigint").alias("ext_cents"),
    F.col("promo_code")
)
```

**Key Differences**:
- Streaming lacks `*_cents` fields → calculated from decimal amounts
- Streaming uses `line_number` → mapped to `line_num`
- Streaming uses `extended_price` → mapped to `ext_price`
- Streaming uses `receipt_id` → mapped to `receipt_id_ext`

**Critical Note**:
- ✅ CORRECT: Only `receipt_id_ext VARCHAR` used for linkage
- ❌ REMOVED: Old `receipt_id BIGINT` column no longer exists

---

### 3. fact_payments ↔ payment_processed

**Status**: ⚠️ Batch Only - Streaming events exist but not yet combined

#### Batch Schema (`cusn.fact_payments`)
```
event_ts: TIMESTAMP
receipt_id_ext: VARCHAR (nullable)
order_id_ext: VARCHAR (nullable)
payment_method: VARCHAR
amount: DECIMAL
amount_cents: BIGINT
transaction_id: VARCHAR
processing_time_ms: INTEGER
status: VARCHAR
decline_reason: VARCHAR (nullable)
store_id: BIGINT (nullable)
customer_id: BIGINT (nullable)
```

#### Streaming Schema (`cusn.payment_processed`)
```
ingest_timestamp: TIMESTAMP
receipt_id: string (nullable)
order_id: string (nullable)
payment_method: string
amount: decimal
transaction_id: string
processing_time_ms: int
status: string
decline_reason: string (nullable)
store_id: long (nullable)
customer_id: long (nullable)
```

**Implementation Status**: Streaming events are generated but Silver layer currently loads batch only. Field mapping would be straightforward (similar to receipts).

---

### 4-18. Remaining Fact Tables

All remaining fact tables currently use **batch only**:

#### No Field Mapping Needed (Batch Only)
- `fact_truck_inventory` - No streaming equivalent
- `fact_promo_lines` - No streaming equivalent

#### Streaming Events Exist But Not Yet Combined
The following tables have corresponding streaming events defined, but Silver layer transformation currently loads batch only:

- `fact_store_inventory_txn` ↔ `inventory_updated`
- `fact_dc_inventory_txn` ↔ `inventory_updated`
- `fact_stockouts` ↔ `stockout_detected`
- `fact_reorders` ↔ `reorder_triggered`
- `fact_foot_traffic` ↔ `customer_entered`
- `fact_customer_zone_changes` ↔ `customer_zone_changed`
- `fact_ble_pings` ↔ `ble_ping_detected`
- `fact_truck_moves` ↔ `truck_arrived`, `truck_departed`
- `fact_store_ops` ↔ `store_opened`, `store_closed`
- `fact_marketing` ↔ `ad_impression`
- `fact_promotions` ↔ `promotion_applied`
- `fact_online_order_headers` ↔ `online_order_created`
- `fact_online_order_lines` ↔ `online_order_picked`, `online_order_shipped`

**Future Work**: Add transform functions in `02-onelake-to-silver.ipynb` to combine batch + streaming for these tables.

---

## General Transformation Patterns

### Pattern 1: Timestamp Mapping
```python
# Batch uses: event_ts
# Streaming uses: ingest_timestamp
F.col("ingest_timestamp").alias("event_ts")
```

### Pattern 2: External ID Mapping
```python
# Batch uses: receipt_id_ext, order_id_ext (VARCHAR)
# Streaming uses: receipt_id, order_id (string)
F.col("receipt_id").alias("receipt_id_ext")
F.col("order_id").alias("order_id_ext")
```

### Pattern 3: Cents Calculation
```python
# Batch has both: amount (DECIMAL) + amount_cents (BIGINT)
# Streaming has only: amount (decimal)
F.col("amount"),
(F.col("amount") * 100).cast("bigint").alias("amount_cents")
```

### Pattern 4: Field Name Normalization
```python
# Streaming field → Batch field
"extended_price" → "ext_price"
"line_number" → "line_num"
"tender_type" → "payment_method"
```

### Pattern 5: Default Values
```python
# Fields present in batch but not streaming
F.lit(0.0).alias("discount_amount")
F.lit("SALE").alias("receipt_type")
F.lit(None).cast("string").alias("return_for_receipt_id_ext")
```

---

## Implementation in Silver Layer

### Bronze to Silver Transformation
**Notebook**: `fabric/lakehouse/02-onelake-to-silver.ipynb`

### Current Implementation
```python
def transform_receipts(df_batch):
    """Combine batch fact_receipts with streaming receipt_created."""
    try:
        df_stream = read_bronze_table("receipt_created")
        df_stream_mapped = df_stream.select(
            # ... field mappings ...
        )
        return df_batch.unionAll(df_stream_mapped)
    except:
        return df_batch  # Fallback to batch only if streaming unavailable

load_table("fact_receipts", transform_receipts)
```

### Merge Strategy
- **Method**: UNION ALL (no deduplication)
- **Justification**: Data generator ensures no temporal overlap
- **Validation**: None needed for demo environment
- **Production**: Would add dedup on `(trace_id, event_ts)` if overlap possible

---

## Adding New Streaming Combinations

To add streaming support for additional fact tables:

1. **Verify Event Schema**: Check `fabric/kql_database/02-create-tables.kql`
2. **Create Transform Function**: Add to `02-onelake-to-silver.ipynb`
   ```python
   def transform_{table_name}(df_batch):
       df_stream = read_bronze_table("{event_name}")
       df_stream_mapped = df_stream.select(
           # Map fields from streaming to batch schema
       )
       return df_batch.unionAll(df_stream_mapped)
   ```
3. **Update Load Call**: `load_table("{table_name}", transform_{table_name})`
4. **Test**: Verify schema alignment and row counts

---

## Schema Evolution Guidance

### When Batch Schema Changes
1. Update datagen field mappings in `field_mapping.py`
2. Regenerate parquet files with new schema
3. Upload to ADLSv2 (overwrite or new partition)
4. Bronze shortcuts automatically reflect new schema
5. Update Silver transform functions if field names changed
6. Update streaming transforms if affected fields are mapped

### When Streaming Schema Changes
1. Update KQL event table definitions in `02-create-tables.kql`
2. Update Pydantic event models in `streaming/schemas.py`
3. Eventhouse shortcuts automatically reflect new schema
4. Update Silver transform functions for affected mappings
5. Test UNION ALL still works (schema compatibility)

### Breaking Changes to Avoid
- ❌ Removing required fields from batch schema
- ❌ Changing field types incompatibly (VARCHAR → BIGINT)
- ❌ Removing `*_cents` fields (used in downstream Gold aggregations)
- ❌ Changing external ID field names without updating transforms

---

## Related Documentation

- [Architecture](./architecture.md)
- [Field Mapping Reference](../datagen/src/retail_datagen/generators/fact_generators/field_mapping.py)
- [KQL Event Schemas](../fabric/kql_database/02-create-tables.kql)
- [Silver Transformation Notebook](../fabric/lakehouse/02-onelake-to-silver.ipynb)
