# Streaming Implementation Status

## Overview

This document tracks which fact tables have streaming integration implemented in the Silver layer (`ag` schema). The architecture supports combining batch historical data with real-time streaming events, but **not all fact tables have streaming integration implemented yet**.

## Current Status: 2 of 18 Fact Tables

| Status | Count | Percentage |
|--------|-------|------------|
| ✅ Streaming Implemented | 2 | 11% |
| 🔨 Batch Only (Planned) | 16 | 89% |
| **Total Fact Tables** | **18** | **100%** |

---

## ✅ Implemented (Batch + Streaming)

These fact tables combine both batch historical parquet and real-time streaming events:

### 1. fact_receipts
- **Batch Source**: `cusn.fact_receipts` (ADLSv2 parquet)
- **Streaming Source**: `cusn.receipt_created` (Eventhouse)
- **Silver Output**: `ag.fact_receipts`
- **Implementation**: `transform_receipts()` in `02-onelake-to-silver.ipynb`
- **Schema Mapping**:
  - `receipt_id` → `receipt_id_ext`
  - `tender_type` → `payment_method`
  - `ingest_timestamp` → `event_ts`
  - Calculated: `*_cents` fields (rounded from decimals)
  - Defaults: `discount_amount=0.0`, `receipt_type='SALE'`

### 2. fact_receipt_lines
- **Batch Source**: `cusn.fact_receipt_lines` (ADLSv2 parquet)
- **Streaming Source**: `cusn.receipt_line_added` (Eventhouse)
- **Silver Output**: `ag.fact_receipt_lines`
- **Implementation**: `transform_receipt_lines()` in `02-onelake-to-silver.ipynb`
- **Schema Mapping**:
  - `receipt_id` → `receipt_id_ext`
  - `line_number` → `line_num`
  - `extended_price` → `ext_price`
  - `ingest_timestamp` → `event_ts`
  - Calculated: `unit_cents`, `ext_cents` (rounded from decimals)

---

## 🔨 Batch Only (Streaming Planned)

These fact tables currently load **batch data only** from ADLSv2 parquet. Corresponding streaming events exist in Eventhouse but are not yet combined in the Silver layer.

### Inventory & Supply Chain (6 tables)

#### 3. fact_store_inventory_txn
- **Batch Source**: `cusn.fact_store_inventory_txn`
- **Streaming Source**: `cusn.inventory_updated` (not yet integrated)
- **Status**: Batch only
- **Future Work**: Filter `inventory_updated` by `location_type='STORE'`

#### 4. fact_dc_inventory_txn
- **Batch Source**: `cusn.fact_dc_inventory_txn`
- **Streaming Source**: `cusn.inventory_updated` (not yet integrated)
- **Status**: Batch only
- **Future Work**: Filter `inventory_updated` by `location_type='DC'`

#### 5. fact_stockouts
- **Batch Source**: `cusn.fact_stockouts`
- **Streaming Source**: `cusn.stockout_detected` (not yet integrated)
- **Status**: Batch only

#### 6. fact_reorders
- **Batch Source**: `cusn.fact_reorders`
- **Streaming Source**: `cusn.reorder_triggered` (not yet integrated)
- **Status**: Batch only

#### 7. fact_truck_inventory
- **Batch Source**: `cusn.fact_truck_inventory`
- **Streaming Source**: None (batch only by design)
- **Status**: Batch only - no streaming equivalent

#### 8. fact_truck_moves
- **Batch Source**: `cusn.fact_truck_moves`
- **Streaming Source**: `cusn.truck_arrived`, `cusn.truck_departed` (not yet integrated)
- **Status**: Batch only
- **Future Work**: UNION both arrival and departure events

### Customer & In-Store Events (3 tables)

#### 9. fact_foot_traffic
- **Batch Source**: `cusn.fact_foot_traffic`
- **Streaming Source**: `cusn.customer_entered` (not yet integrated)
- **Status**: Batch only

#### 10. fact_customer_zone_changes
- **Batch Source**: `cusn.fact_customer_zone_changes`
- **Streaming Source**: `cusn.customer_zone_changed` (not yet integrated)
- **Status**: Batch only

#### 11. fact_ble_pings
- **Batch Source**: `cusn.fact_ble_pings`
- **Streaming Source**: `cusn.ble_ping_detected` (not yet integrated)
- **Status**: Batch only

### Marketing & Promotions (2 tables)

#### 12. fact_marketing
- **Batch Source**: `cusn.fact_marketing`
- **Streaming Source**: `cusn.ad_impression` (not yet integrated)
- **Status**: Batch only

#### 13. fact_promotions
- **Batch Source**: `cusn.fact_promotions`
- **Streaming Source**: `cusn.promotion_applied` (not yet integrated)
- **Status**: Batch only

#### 14. fact_promo_lines
- **Batch Source**: `cusn.fact_promo_lines`
- **Streaming Source**: None (batch only by design)
- **Status**: Batch only - no streaming equivalent

### Payments & Orders (4 tables)

#### 15. fact_payments
- **Batch Source**: `cusn.fact_payments`
- **Streaming Source**: `cusn.payment_processed` (not yet integrated)
- **Status**: Batch only

#### 16. fact_online_order_headers
- **Batch Source**: `cusn.fact_online_order_headers`
- **Streaming Source**: `cusn.online_order_created` (not yet integrated)
- **Status**: Batch only

#### 17. fact_online_order_lines
- **Batch Source**: `cusn.fact_online_order_lines`
- **Streaming Source**: `cusn.online_order_picked`, `cusn.online_order_shipped` (not yet integrated)
- **Status**: Batch only
- **Future Work**: UNION multiple event types (picked, shipped, delivered)

### Store Operations (1 table)

#### 18. fact_store_ops
- **Batch Source**: `cusn.fact_store_ops`
- **Streaming Source**: `cusn.store_opened`, `cusn.store_closed` (not yet integrated)
- **Status**: Batch only
- **Future Work**: UNION both open and close events

---

## Implementation Pattern

When adding streaming integration for batch-only tables, follow this pattern from `fact_receipts`:

```python
def transform_{table_name}(df_batch):
    """Combine batch {table_name} with streaming {event_name}."""
    global schema_mismatch_count
    try:
        # Read streaming data
        df_stream = read_bronze_table("{event_name}")

        # Map streaming fields to batch schema
        df_stream_mapped = df_stream.select(
            # Map each streaming field to batch column name
            F.col("streaming_field").alias("batch_field"),
            # Use F.round() for all cents calculations
            F.round(F.col("amount") * 100).cast("bigint").alias("amount_cents"),
            # Add default values for batch-only fields
            F.lit(default_value).alias("batch_only_field")
        )

        # Validate schemas match before UNION
        batch_cols = set(df_batch.columns)
        stream_cols = set(df_stream_mapped.columns)
        if batch_cols != stream_cols:
            # Log detailed mismatch information
            print(f"⚠️  SCHEMA MISMATCH: ...")
            schema_mismatch_count += 1

            if FAIL_ON_SCHEMA_MISMATCH:
                raise ValueError("Production failure - schema mismatch")
            else:
                return df_batch  # Fallback to batch only

        # UNION batch + streaming (no dedup needed per user confirmation)
        return df_batch.unionAll(df_stream_mapped)

    except Exception as e:
        # Handle missing streaming table gracefully
        if "does not exist" in str(e).lower():
            print(f"  ℹ️  Streaming table not found, using batch only")
        else:
            print(f"  ⚠️  Could not combine streaming: {e}")
        return df_batch

# Use in load_table call
load_table("{table_name}", transform_{table_name})
```

### Key Requirements

1. **Column Order**: Must match batch schema exactly (Spark UNION is order-sensitive)
2. **Cents Calculation**: Always use `F.round()` before `cast("bigint")` to avoid truncation
3. **Schema Validation**: Pre-flight check before UNION to catch mismatches early
4. **Fail-Fast Mode**: Support `FAIL_ON_SCHEMA_MISMATCH=true` for production
5. **Graceful Fallback**: Return batch-only if streaming unavailable (dev/test environments)
6. **Metrics Tracking**: Increment `schema_mismatch_count` for monitoring

---

## Future Work

### Phase 1: High-Value Streaming (Recommended Next)
Add streaming integration for high-frequency, customer-facing events:
- `fact_payments` (transaction processing)
- `fact_online_order_headers` + `fact_online_order_lines` (e-commerce)
- `fact_foot_traffic` (real-time store analytics)

### Phase 2: Inventory & Supply Chain
- `fact_store_inventory_txn` + `fact_dc_inventory_txn` (inventory tracking)
- `fact_stockouts` + `fact_reorders` (supply chain alerts)
- `fact_truck_moves` (logistics tracking)

### Phase 3: Marketing & Customer Insights
- `fact_marketing` (campaign effectiveness)
- `fact_promotions` (promotion tracking)
- `fact_customer_zone_changes` + `fact_ble_pings` (in-store behavior)

### Phase 4: Operational Events
- `fact_store_ops` (store operations monitoring)

---

## Testing Streaming Integration

When adding streaming support to a new fact table:

### 1. Verify Bronze Shortcuts Exist
```bash
python fabric/lakehouse/validate-bronze-shortcuts.py
```

### 2. Test Schema Alignment
```python
# In Fabric notebook
df_batch = spark.table("cusn.{fact_table}")
df_stream = spark.table("cusn.{event_table}")

print("Batch columns:")
print(sorted(df_batch.columns))

print("\nStreaming columns (before mapping):")
print(sorted(df_stream.columns))

# After implementing transform function
df_stream_mapped = transform_{table_name}(df_batch)
print("\nStreaming columns (after mapping):")
print(sorted(df_stream_mapped.columns))
```

### 3. Validate UNION Compatibility
```python
# Ensure no schema mismatch errors
batch_cols = set(df_batch.columns)
stream_cols = set(df_stream_mapped.columns)
assert batch_cols == stream_cols, f"Mismatch: {batch_cols ^ stream_cols}"
```

### 4. Test Row Count
```python
# Verify combined data
df_combined = df_batch.unionAll(df_stream_mapped)
print(f"Batch rows: {df_batch.count()}")
print(f"Streaming rows: {df_stream_mapped.count()}")
print(f"Combined rows: {df_combined.count()}")
assert df_combined.count() == df_batch.count() + df_stream_mapped.count()
```

---

## Related Documentation

- [Schema Mapping: Batch ↔ Streaming](./schema-mapping-batch-streaming.md) - Detailed field mappings
- [Bronze Layer Architecture](./bronze-layer-architecture.md) - Bronze shortcut design
- [End-to-End Deployment](./end-to-end-deployment.md) - Complete deployment guide
- Notebook: `fabric/lakehouse/02-onelake-to-silver.ipynb` - Silver transformation logic
