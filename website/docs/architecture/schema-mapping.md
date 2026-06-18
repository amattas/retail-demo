# Schema Mapping: Batch Parquet ↔ Streaming Events

## Overview

This document maps batch parquet fact tables to their corresponding streaming event tables, documenting schema differences and the transformation logic used in the Silver layer (`ag` schema).

## Architecture Context

- **Batch Parquet** (`Files/fact_*`): Historical data from the data generator, uploaded to ADLSv2 and surfaced via Lakehouse `Files/` shortcuts
- **Streaming Events** (`cusn.receipt_created`, etc.): Real-time events from Eventhouse, surfaced via `Tables/cusn/` shortcuts
- **Silver** (`ag.fact_*`): Batch data is loaded once by `02-historical-data-load.ipynb`; streaming events are then appended incrementally by `03-streaming-to-silver.ipynb`

## Critical Design Decision

**No Overlap**: The data generator ensures batch historical and streaming real-time data never overlap in time, so appending streaming events without deduplication is safe. Incremental processing uses per-table watermarks stored in `ag._watermarks` (keyed on `ingest_timestamp`), so events are not processed twice.

---

## Complete Mapping Table

All streaming integrations below are implemented in `03-streaming-to-silver.ipynb`.

| Batch Parquet Table | Streaming Event Table(s) | Silver Target | Notes |
|---------------------|-------------------------|---------------|-------|
| `fact_receipts` | `receipt_created` | `ag.fact_receipts` | Field mapping required |
| `fact_receipt_lines` | `receipt_line_added` | `ag.fact_receipt_lines` | Field mapping required |
| `fact_payments` | `payment_processed` | `ag.fact_payments` | |
| `fact_store_inventory_txn` | `inventory_updated` | `ag.fact_store_inventory_txn` | All `inventory_updated` events land here |
| `fact_dc_inventory_txn` | *(none currently)* | `ag.fact_dc_inventory_txn` | Batch only; streaming `inventory_updated` is not split by location |
| `fact_stockouts` | `stockout_detected` | `ag.fact_stockouts` | |
| `fact_reorders` | `reorder_triggered` | `ag.fact_reorders` | |
| `fact_foot_traffic` | `customer_entered` | `ag.fact_foot_traffic` | |
| `fact_customer_zone_changes` | `customer_zone_changed` | `ag.fact_customer_zone_changes` | |
| `fact_ble_pings` | `ble_ping_detected` | `ag.fact_ble_pings` | |
| `fact_truck_moves` | `truck_arrived`, `truck_departed` | `ag.fact_truck_moves` | Both event types appended with `status` literal |
| `fact_store_ops` | `store_opened`, `store_closed` | `ag.fact_store_ops` | `operation_type` set to OPENED/CLOSED |
| `fact_marketing` | `ad_impression` | `ag.fact_marketing` | |
| `fact_promotions` | `promotion_applied` | `ag.fact_promotions` | |
| `fact_online_order_headers` | `online_order_created` | `ag.fact_online_order_headers` | |
| `fact_online_order_lines` | `online_order_picked`, `online_order_shipped` | `ag.fact_online_order_status` | Status events land in a separate table, not `fact_online_order_lines` |
| `fact_truck_inventory` | *(none)* | `ag.fact_truck_inventory` | Batch only (no streaming equivalent) |
| `fact_promo_lines` | *(none)* | `ag.fact_promo_lines` | Batch only (no streaming equivalent) |

---

## Detailed Schema Mappings

### 1. fact_receipts ↔ receipt_created

#### Batch Schema (`Files/fact_receipts`)
```
event_ts: TIMESTAMP
receipt_id_ext: VARCHAR
payment_method: VARCHAR
discount_amount: DECIMAL
tax_amount: DECIMAL
tax_cents: BIGINT
subtotal: DECIMAL
subtotal_cents: BIGINT
total_amount: DECIMAL
total_cents: BIGINT
receipt_type: VARCHAR
customer_id: BIGINT
store_id: BIGINT
return_for_receipt_id_ext: VARCHAR (nullable)
```

#### Streaming Schema (`cusn.receipt_created`)
```
ingest_timestamp: datetime
receipt_id: string
tender_type: string
tax: real
subtotal: real
total: real
item_count: long
campaign_id: string (nullable)
customer_id: long
store_id: long
```

#### Field Mapping (Streaming → Silver, from `transform_receipt_created`)
```python
df.select(
    F.col("ingest_timestamp").alias("event_ts"),
    F.to_date(F.col("ingest_timestamp")).alias("event_date"),
    F.col("receipt_id").alias("receipt_id_ext"),
    F.col("tender_type").alias("payment_method"),
    F.lit(0.0).cast("string").alias("discount_amount"),
    F.col("tax").cast("string").alias("tax_amount"),
    F.round(F.col("tax") * 100).cast("bigint").alias("tax_cents"),
    F.col("subtotal").cast("string").alias("subtotal"),
    F.col("total").cast("string").alias("total_amount"),
    F.round(F.col("total") * 100).cast("bigint").alias("total_cents"),
    F.lit("SALE").alias("receipt_type"),
    F.round(F.col("subtotal") * 100).cast("bigint").alias("subtotal_cents"),
    F.col("customer_id").cast("long"),
    F.col("store_id").cast("long"),
    F.lit(None).cast("string").alias("return_for_receipt_id_ext")
)
```

**Key Differences**:
- Streaming lacks `*_cents` fields → calculated with `F.round(amount * 100)`
- Streaming lacks `discount_amount` → defaults to 0.0
- Streaming lacks `receipt_type` → defaults to "SALE"
- Streaming uses `tender_type` → mapped to `payment_method`
- Streaming uses `receipt_id` → mapped to `receipt_id_ext`
- An `event_date` column is derived for `dim_date` relationships

---

### 2. fact_receipt_lines ↔ receipt_line_added

#### Streaming Schema (`cusn.receipt_line_added`)
```
ingest_timestamp: datetime
receipt_id: string
line_number: long
product_id: long
quantity: long
unit_price: real
extended_price: real
promo_code: string (nullable)
```

#### Field Mapping (Streaming → Silver, from `transform_receipt_line_added`)
```python
df.select(
    F.col("receipt_id").alias("receipt_id_ext"),
    F.col("ingest_timestamp").alias("event_ts"),
    F.to_date(F.col("ingest_timestamp")).alias("event_date"),
    F.col("product_id").cast("long"),
    F.col("line_number").cast("int").alias("line_num"),
    F.col("quantity").cast("int"),
    F.col("unit_price"),
    F.col("extended_price").alias("ext_price"),
    F.round(F.col("unit_price") * 100).cast("bigint").alias("unit_cents"),
    F.round(F.col("extended_price") * 100).cast("bigint").alias("ext_cents"),
    F.col("promo_code")
)
```

**Key Differences**:
- Streaming uses `line_number` → mapped to `line_num`
- Streaming uses `extended_price` → mapped to `ext_price`
- Streaming uses `receipt_id` → mapped to `receipt_id_ext`
- `*_cents` fields calculated from decimal amounts

**Critical Note**:
- Only `receipt_id_ext` (string) is used for linkage; there is no numeric `receipt_id` FK in Silver

---

### 3. fact_payments ↔ payment_processed

#### Streaming Schema (`cusn.payment_processed`)
```
ingest_timestamp: datetime
receipt_id: string (nullable)
order_id: string (nullable)
payment_method: string
amount: real
amount_cents: long
transaction_id: string
processing_time: datetime
processing_time_ms: int
status: string
decline_reason: string (nullable)
store_id: long (nullable)
customer_id: long
```

#### Field Mapping (Streaming → Silver, from `transform_payment_processed`)
```python
df.select(
    F.col("ingest_timestamp").alias("event_ts"),
    F.to_date(F.col("ingest_timestamp")).alias("event_date"),
    F.col("transaction_id").alias("payment_id"),
    F.col("receipt_id").alias("receipt_id_ext"),
    F.col("order_id").alias("order_id_ext"),
    F.col("payment_method"),
    F.col("amount").cast("string"),
    F.col("amount_cents").cast("bigint"),
    F.col("transaction_id"),
    F.col("processing_time_ms").cast("long"),
    F.col("status"),
    F.col("decline_reason"),
    F.col("store_id").cast("long"),
    F.col("customer_id").cast("long")
)
```

---

### 4. Other Mappings of Note

- **`inventory_updated`**: `reason` → `txn_type`, `quantity_delta` → `quantity`; `balance` is left null (calculated downstream). All events currently land in `ag.fact_store_inventory_txn` (no store/DC split in the streaming path).
- **`truck_arrived` / `truck_departed`**: `arrival_time` → `eta`, `departure_time` → `etd`; a `status` literal (`ARRIVED`/`DEPARTED`) distinguishes the two event types in `ag.fact_truck_moves`.
- **`customer_entered`**: `dwell_time` → `dwell_seconds`, `customer_count` → `count`; `customer_id` left null.
- **`ad_impression`**: `impression_id` → `impression_id_ext`, `device_type` → `device`; `cost_cents` computed.
- **`promotion_applied`**: `receipt_id` → `receipt_id_ext`; `product_ids` (dynamic) cast to string.
- **`online_order_created`**: `order_id` → `order_id_ext`, `tender_type` → `payment_method`, `subtotal`/`tax`/`total` → `*_amount` plus computed `*_cents`.
- **`online_order_picked` / `online_order_shipped`**: appended to `ag.fact_online_order_status` with a `status` literal (`PICKED`/`SHIPPED`); not merged into `fact_online_order_lines`.

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
# Batch uses: receipt_id_ext, order_id_ext
# Streaming uses: receipt_id, order_id
F.col("receipt_id").alias("receipt_id_ext")
F.col("order_id").alias("order_id_ext")
```

### Pattern 3: Cents Calculation
```python
# Batch has both: amount + amount_cents
# Streaming (mostly) has only: amount
F.round(F.col("amount") * 100).cast("bigint").alias("amount_cents")
```

### Pattern 4: Field Name Normalization
```python
# Streaming field → Batch field
"extended_price" → "ext_price"
"line_number" → "line_num"
"tender_type" → "payment_method"
"device_type" → "device"
"quantity_delta" → "quantity"
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

**Notebook**: `fabric/lakehouse/03-streaming-to-silver.ipynb`

Each event type is processed by `process_events(source_table, target_table, transform_fn)`, which:

1. Reads the Eventhouse shortcut (`cusn.<event_table>`)
2. Filters to rows newer than the stored watermark (`ag._watermarks`)
3. Applies the transform function to map streaming fields to the Silver schema
4. Appends to `ag.<fact_table>` with `mergeSchema=true`
5. Advances the watermark to the max `ingest_timestamp` processed

### Merge Strategy
- **Method**: Incremental append with watermarks (no deduplication)
- **Justification**: Data generator ensures no temporal overlap between batch history and streaming
- **Production note**: Would add dedup on `(trace_id, event_ts)` if overlap were possible

---

## Adding New Streaming Combinations

To add streaming support for a new event type:

1. **Verify Event Schema**: Check `fabric/kql_database/01-create-tables.kql` and the event payload definition in `utility/notebooks/templates/driver-05-stream.py`
2. **Create Transform Function**: Add to `03-streaming-to-silver.ipynb`
   ```python
   def transform_{event_name}(df):
       return df.select(
           F.col("ingest_timestamp").alias("event_ts"),
           # Map remaining streaming fields to Silver column names
       )
   ```
3. **Register the Mapping**: `process_events("{event_name}", "{fact_table}", transform_{event_name})`
4. **Test**: Verify schema alignment and row counts

---

## Schema Evolution Guidance

### When Batch Schema Changes
1. Update the Lakehouse table contract in `utility/src/retail_setup/generation/schemas.py`
2. Regenerate parquet files with new schema
3. Upload to ADLSv2 (overwrite or new partition)
4. Bronze shortcuts automatically reflect new schema
5. Update Silver transform functions if field names changed

### When Streaming Schema Changes
1. Update the event payload definitions in `utility/notebooks/templates/driver-05-stream.py`
2. Update KQL event table definitions in `fabric/kql_database/01-create-tables.kql` and mappings in `02-create-ingestion-mappings.kql`
3. Eventhouse shortcuts automatically reflect new schema
4. Update transform functions in `03-streaming-to-silver.ipynb` for affected mappings

### Breaking Changes to Avoid
- ❌ Removing required fields from batch schema
- ❌ Changing field types incompatibly (VARCHAR → BIGINT)
- ❌ Removing `*_cents` fields (used in downstream Gold aggregations)
- ❌ Changing external ID field names without updating transforms

---

## Related Documentation

- [Architecture](./index.md)
- [Streaming Implementation Status](./streaming-implementation-status.md)
- [Table Contract Reference](https://github.com/amattas/retail-demo/blob/main/utility/src/retail_setup/generation/schemas.py)
- [KQL Event Schemas](https://github.com/amattas/retail-demo/blob/main/fabric/kql_database/01-create-tables.kql)
- [Silver Transformation Notebook](https://github.com/amattas/retail-demo/blob/main/fabric/lakehouse/03-streaming-to-silver.ipynb)
