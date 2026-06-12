# Streaming Implementation Status

## Overview

This document tracks which fact tables have streaming integration implemented in the Silver layer (`ag` schema). The architecture combines batch historical data (loaded once from ADLS parquet by `02-historical-data-load.ipynb`) with real-time streaming events (appended incrementally from Eventhouse by `03-streaming-to-silver.ipynb`).

## Current Status: All Event Types Integrated

All 18 streaming event types defined in `datagen/src/retail_datagen/streaming/schemas.py` are processed into Silver by `03-streaming-to-silver.ipynb`.

| Status | Count |
|--------|-------|
| Streaming implemented (event → Silver transform) | 18 of 18 event types |
| Batch-only fact tables (no streaming equivalent by design) | 2 (`fact_truck_inventory`, `fact_promo_lines`) |

:::note Deployment-pending items
Per `STATUS.md`, the code is complete but two steps remain manual deployment tasks in a Fabric workspace:

1. **Eventstream configuration** — wiring routes from Event Hubs to the KQL event tables
2. **Semantic model deployment** — publishing the Power BI semantic model (`fabric/powerbi/retail_model.pbip`)

Until the Eventstream is configured in the workspace, no streaming events arrive in Eventhouse, so the Silver streaming path processes zero rows.
:::

---

## Implemented Event → Silver Mappings

All mappings below live in `03-streaming-to-silver.ipynb` and run via the `streaming-data-load` pipeline (notebook 03 followed by notebook 04).

### Transactions
| Event | Silver target | Transform |
|-------|---------------|-----------|
| `receipt_created` | `ag.fact_receipts` | `transform_receipt_created` |
| `receipt_line_added` | `ag.fact_receipt_lines` | `transform_receipt_line_added` |
| `payment_processed` | `ag.fact_payments` | `transform_payment_processed` |

### Inventory & Supply Chain
| Event | Silver target | Transform |
|-------|---------------|-----------|
| `inventory_updated` | `ag.fact_store_inventory_txn` | `transform_inventory_updated` |
| `stockout_detected` | `ag.fact_stockouts` | `transform_stockout_detected` |
| `reorder_triggered` | `ag.fact_reorders` | `transform_reorder_triggered` |
| `truck_arrived` | `ag.fact_truck_moves` | `transform_truck_arrived` |
| `truck_departed` | `ag.fact_truck_moves` | `transform_truck_departed` |

### Customer & In-Store
| Event | Silver target | Transform |
|-------|---------------|-----------|
| `customer_entered` | `ag.fact_foot_traffic` | `transform_customer_entered` |
| `customer_zone_changed` | `ag.fact_customer_zone_changes` | `transform_customer_zone_changed` |
| `ble_ping_detected` | `ag.fact_ble_pings` | `transform_ble_ping` |

### Store Operations
| Event | Silver target | Transform |
|-------|---------------|-----------|
| `store_opened` | `ag.fact_store_ops` | `transform_store_operation(df, "OPENED")` |
| `store_closed` | `ag.fact_store_ops` | `transform_store_operation(df, "CLOSED")` |

### Marketing & Promotions
| Event | Silver target | Transform |
|-------|---------------|-----------|
| `ad_impression` | `ag.fact_marketing` | `transform_ad_impression` |
| `promotion_applied` | `ag.fact_promotions` | `transform_promotion_applied` |

### Omnichannel
| Event | Silver target | Transform |
|-------|---------------|-----------|
| `online_order_created` | `ag.fact_online_order_headers` | `transform_online_order_created` |
| `online_order_picked` | `ag.fact_online_order_status` | `transform_online_order_picked` |
| `online_order_shipped` | `ag.fact_online_order_status` | `transform_online_order_shipped` |

---

## Known Limitations

- **DC inventory**: All streaming `inventory_updated` events land in `ag.fact_store_inventory_txn`; the streaming path does not currently split store vs. DC transactions into `fact_dc_inventory_txn` (which is populated by batch only).
- **Online order lines**: Streaming pick/ship status events go to `ag.fact_online_order_status` rather than merging into `ag.fact_online_order_lines`.
- **Inventory balance**: The streaming transform leaves `balance` null (it must be derived downstream); batch data carries the running balance.
- **Batch-only tables**: `fact_truck_inventory` and `fact_promo_lines` have no streaming event equivalent by design.

---

## Implementation Pattern

`03-streaming-to-silver.ipynb` uses watermark-based incremental processing rather than batch+stream UNIONs:

```python
def process_events(source_table, target_table, transform_fn, ts_col="ingest_timestamp"):
    """Process new events from Eventhouse and append to Silver."""
    # 1. Skip if the Eventhouse shortcut (cusn.<source_table>) does not exist
    # 2. Read rows where ingest_timestamp > stored watermark (ag._watermarks)
    # 3. Apply transform_fn to map streaming fields to the Silver schema
    # 4. Append to ag.<target_table> with mergeSchema=true
    # 5. Advance the watermark

process_events("receipt_created", "fact_receipts", transform_receipt_created)
```

### Key Requirements for New Transforms

1. **Timestamp mapping**: `ingest_timestamp` → `event_ts` (plus derived `event_date` where the batch table has it)
2. **Cents calculation**: Always use `F.round()` before `cast("bigint")` to avoid truncation
3. **External IDs**: Map streaming `receipt_id`/`order_id` to `receipt_id_ext`/`order_id_ext`
4. **ID types**: Cast ID columns to `long`/`int` to match batch schemas (see `cast_id_columns`)
5. **Graceful fallback**: Missing Eventhouse shortcuts are skipped (dev/test environments)

---

## Testing Streaming Integration

### 1. Verify Bronze Shortcuts Exist
```bash
python fabric/lakehouse/validate-bronze-shortcuts.py
```

### 2. Test Schema Alignment
```python
# In a Fabric notebook
df_silver = spark.table("ag.fact_receipts")
df_stream = spark.table("cusn.receipt_created")

print("Silver columns:")
print(sorted(df_silver.columns))

print("\nStreaming columns (before mapping):")
print(sorted(df_stream.columns))

df_stream_mapped = transform_receipt_created(df_stream)
print("\nStreaming columns (after mapping):")
print(sorted(df_stream_mapped.columns))
```

### 3. Verify Watermarks Advance
```python
spark.table("retail_lakehouse.ag._watermarks").show(truncate=False)
```

### 4. Verify Row Counts Grow
```python
# Run before and after a streaming burst
spark.table("ag.fact_receipts").count()
```

---

## Related Documentation

- [Schema Mapping: Batch ↔ Streaming](./schema-mapping.md) - Detailed field mappings
- [Architecture](./index.md) - System architecture including Bronze layer design
- [Setup Guide](../setup/index.md) - Complete deployment guide
- Notebook: `fabric/lakehouse/03-streaming-to-silver.ipynb` - Silver transformation logic
