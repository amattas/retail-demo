# Phase 6: Streaming Setup

Configure Eventstream for real-time event ingestion from Event Hubs.

## Step 6.1: Create Eventstream

**Create Eventstream for real-time ingestion**:

1. New → Eventstream
   - Name: `retail_events_stream`

2. **Add Source**: Azure Event Hubs
   - Connection: Your Event Hubs namespace
   - Hub: `retail-events`
   - Consumer Group: `$Default`

3. **Add Destinations** (2 destinations):

### Destination 1: KQL Database

| Setting | Value |
|---------|-------|
| Target | `retail_eventhouse` (from Step 2.1) |
| Input data format | JSON |
| Routing | Route by `event_type` field; unrecognized types land in `unknown_event` |
| Ingestion mapping | Use the `EventMapping` mappings from Step 2.2 |

:::note
A Lakehouse destination is **not** required. The Lakehouse Bronze layer reads streaming data through OneLake shortcuts to the Eventhouse tables (`Tables/cusn/`, created in Phase 2), not through a separate Eventstream sink.
:::

4. **Start Eventstream**

## Step 6.2: Start Data Generator Streaming

**Send test events via data generator**:

```bash
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{"duration_minutes": 5, "burst_override": 100}'

# Monitor and stop
curl http://localhost:8000/api/stream/status
curl -X POST http://localhost:8000/api/stream/stop
```

Omit `duration_minutes` to stream indefinitely. Streaming requires that historical fact data has been generated first (Phase 1) and `AZURE_EVENTHUB_CONNECTION_STRING` is set.

## Verification

### Check KQL Database

```kql
receipt_created | take 10
```

### Check Pipeline Execution

- Verify `pl_streaming_silver` runs successfully after 5 minutes
- Verify `pl_streaming_gold` runs successfully after 15 minutes

## Eventstream Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────────┐
│   Datagen   │────▶│  Event Hubs  │────▶│ Eventstream  │────▶│ KQL Database  │
└─────────────┘     └──────────────┘     └──────────────┘     │ (Eventhouse)  │
                                                              └───────┬───────┘
                                                                      │ OneLake shortcuts
                                                                      ▼
                                                              ┌───────────────┐
                                                              │  Lakehouse    │
                                                              │ Tables/cusn/  │
                                                              └───────────────┘
```

## Next Step

Continue to [Phase 7: Dashboards](07-dashboards.md)
