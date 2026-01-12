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
| Routing | Route by `event_type` field |
| Auto-create tables | ✅ Enabled |
| Ingestion mapping | Use mappings from Step 2.2 |

### Destination 2: Lakehouse Files (optional)

For raw event backup:

| Setting | Value |
|---------|-------|
| Target | `retail_lakehouse` |
| Folder | `/Files/bronze/raw_events/` |
| Partitioning | By `event_type` and `date` |

4. **Start Eventstream**

## Step 6.2: Start Data Generator Streaming

**Send test events via data generator**:

```bash
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{"duration_seconds": 300, "burst": 100}'
```

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
┌─────────────┐     ┌──────────────┐     ┌─────────────────────┐
│   Datagen   │────▶│  Event Hubs  │────▶│    Eventstream      │
└─────────────┘     └──────────────┘     │                     │
                                         │  ┌───────────────┐  │
                                         │  │ KQL Database  │  │
                                         │  │ (streaming)   │  │
                                         │  └───────────────┘  │
                                         │                     │
                                         │  ┌───────────────┐  │
                                         │  │ Lakehouse     │  │
                                         │  │ (backup)      │  │
                                         │  └───────────────┘  │
                                         └─────────────────────┘
```

## Next Step

Continue to [Phase 7: Dashboards](07-dashboards.md)
