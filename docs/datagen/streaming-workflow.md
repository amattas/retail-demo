# Streaming Workflow Documentation

## Design

The streaming system uses an **outbox pattern** to ensure all fact table events are streamed in chronological order.

## How It Works

### 1. Historical Data Generation (Manual)
```python
# Generate historical data for analysis
# Does NOT write to streaming_outbox
await fact_generator.generate_historical_data(
    start_date, end_date,
    publish_to_outbox=False  # ← Does not populate outbox
)
```

**Purpose**: Bulk generation for historical analysis
**Outbox**: NOT populated
**Use Case**: Initial data load, backfilling historical data

### 2. Streaming Mode (Automatic)
```python
# Streaming task manages outbox automatically
while streaming:
    # Try to drain outbox
    item = outbox_lease_next(conn)

    if item:
        # Send event to Azure Event Hub
        await client.send_event(event)
        outbox_ack_sent(conn, item['outbox_id'])
    else:
        # Outbox empty - generate next day
        next_day = last_generated_timestamp + 1 day
        await fact_generator.generate_historical_data(
            next_day_start, next_day_end,
            publish_to_outbox=True  # ← Populates outbox
        )
```

**Purpose**: Real-time streaming to Azure Event Hub
**Outbox**: Populated ONLY when empty
**Use Case**: Continuous event streaming

## Key Points

✅ **Historical generation does NOT write to outbox**
- Prevents massive backlogs
- Outbox only used for streaming

✅ **Streaming generates one day at a time**
- Only when outbox is empty
- Maintains chronological order
- Prevents runaway generation

✅ **Events sent in timestamp order**
- Oldest event first
- Matches real-world event sequences

## Preventing 96M Event Backlog

The previous 96M event backlog was caused by:
1. Streaming task generated multiple days of data
2. Azure connection issues prevented sending
3. Events accumulated in outbox
4. Streaming kept generating more days (bug?)

**Prevention**:
- Historical generation: `publish_to_outbox=False` (already correct)
- Streaming generates ONLY when outbox empty (already correct)
- Monitor outbox size: `GET /api/stream/outbox/status`

## API Endpoints

### Start Streaming
```bash
POST /api/stream/start
{
  "duration_minutes": 60  # Optional
}
```

### Check Outbox Status
```bash
GET /api/stream/outbox/status
```

### Clear Outbox (if needed)
```sql
-- In DuckDB
DROP TABLE streaming_outbox;
-- Table will be recreated automatically
```

## Event Coverage

All 18 fact tables generate streaming events:
- receipt_created, receipt_line_added, payment_processed
- inventory_updated, stockout_detected, reorder_triggered
- customer_entered, customer_zone_changed, ble_ping_detected
- truck_arrived, truck_departed
- store_opened, store_closed
- ad_impression, promotion_applied
- online_order_created, online_order_picked, online_order_shipped
