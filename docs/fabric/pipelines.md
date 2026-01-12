# Pipelines

Data Pipelines orchestrating medallion flows and scheduled processing.

## Pipeline Summary

| Pipeline | Schedule | Notebook | Description |
|----------|----------|----------|-------------|
| `pl_historical_load` | Once (manual) | `02-historical-data-load` | Initial load of Files/ parquet to Silver and Gold |
| `pl_streaming_silver` | Every 5 min | `03-streaming-to-silver` | Eventhouse events to Silver (incremental) |
| `pl_streaming_gold` | Every 15 min | `04-streaming-to-gold` | Silver to Gold aggregations |
| `pl_maintenance` | Daily 3 AM UTC | `05-maintain-delta-tables` | Delta OPTIMIZE/VACUUM routines |

## Pipeline Configurations

### pl_historical_load

**Purpose**: Initial one-time load of historical batch data from Files/ parquet shortcuts through the complete medallion pipeline.

| Setting | Value |
|---------|-------|
| **Notebook** | `02-historical-data-load` |
| **Schedule** | Manual (run once) |
| **Timeout** | 1 hour |
| **Retries** | 3 |
| **Retry Interval** | 30 seconds |

**Parameters**:
```
SILVER_DB = "ag"
GOLD_DB = "au"
```

---

### pl_streaming_silver

**Purpose**: Incrementally processes real-time events from Eventhouse (cusn schema) to Silver Delta tables using watermark-based tracking.

| Setting | Value |
|---------|-------|
| **Notebook** | `03-streaming-to-silver` |
| **Schedule** | Every 5 minutes |
| **Timeout** | 1 hour |
| **Retries** | 3 |
| **Retry Interval** | 30 seconds |

**Parameters**:
```
SILVER_DB = "ag"
BRONZE_SCHEMA = "cusn"
```

---

### pl_streaming_gold

**Purpose**: Aggregates Silver Delta tables into Gold layer KPIs for dashboards.

| Setting | Value |
|---------|-------|
| **Notebook** | `04-streaming-to-gold` |
| **Schedule** | Every 15 minutes |
| **Timeout** | 30 minutes |
| **Retries** | 3 |
| **Retry Interval** | 30 seconds |

**Parameters**:
```
SILVER_DB = "ag"
GOLD_DB = "au"
```

---

### pl_maintenance

**Purpose**: Runs Delta table optimization (OPTIMIZE and VACUUM) to maintain query performance and manage storage.

| Setting | Value |
|---------|-------|
| **Notebook** | `05-maintain-delta-tables` |
| **Schedule** | Daily at 3:00 AM UTC |
| **Timeout** | 1 hour |
| **Retries** | 0 |

**Parameters**:
```
SILVER_DB = "ag"
GOLD_DB = "au"
```

---

## Creating Pipelines in Fabric

Pipelines must be created manually in Microsoft Fabric. For each pipeline:

1. **Navigate to Data Factory**:
   - In your Fabric workspace → New → Data pipeline

2. **Add Notebook Activity**:
   - Drag "Notebook" activity onto canvas
   - Select the corresponding notebook
   - Configure parameters as shown above

3. **Configure Policy**:
   - Set timeout, retries, and retry interval

4. **Add Trigger**:
   - Click "Add trigger" → "New/Edit"
   - Select "Schedule" trigger
   - Configure recurrence as shown above

5. **Save and Activate**:
   - Save pipeline
   - Toggle trigger to "Started"

## Monitoring

Monitor pipeline runs via:
- **Fabric Portal** → Data Factory → Pipelines → View runs
- Check execution status, duration, and error logs
- Set up alerts for pipeline failures

## Deprecated Pipelines

The following pipelines from earlier versions are no longer needed:

| Old Pipeline | Replacement |
|--------------|-------------|
| `pl_bronze_to_silver` | `pl_streaming_silver` |
| `pl_silver_to_gold` | `pl_streaming_gold` |
| `pl_adls_parquet_to_lakehouse` | Bronze shortcuts (no copy needed) |
| `pl_compaction_and_optimize` | `pl_maintenance` |
