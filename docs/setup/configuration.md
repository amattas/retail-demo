# Configuration Reference

Complete reference for environment variables and pipeline settings.

## Environment Variables

### Data Generator

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `EVENTHUB_CONNECTION_STRING` | Yes | - | Event Hubs connection string |
| `EVENTHUB_NAME` | Yes | `retail-events` | Event Hub name |
| `AZURE_STORAGE_CONNECTION_STRING` | Yes | - | ADLSv2 connection string |
| `AZURE_STORAGE_CONTAINER` | Yes | `supermarket` | Storage container name |

### Bronze Layer

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ADLS_ACCOUNT` | Yes | `stdretail` | Storage account name |
| `ADLS_CONTAINER` | Yes | `supermarket` | Container name |
| `EVENTHOUSE_URI` | Yes | - | Eventhouse query URI |
| `EVENTHOUSE_DATABASE` | Yes | `retail_eventhouse` | KQL database name |
| `BRONZE_SCHEMA` | No | `cusn` | Bronze schema name |
| `REQUIRE_EVENTHOUSE` | No | `false` | Fail if Eventhouse not configured |

### Silver Layer

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SILVER_DB` | No | `ag` | Silver database/schema name |
| `BRONZE_SCHEMA` | No | `cusn` | Source Bronze schema |
| `FAIL_ON_SCHEMA_MISMATCH` | No | `false` | Fail on schema incompatibility |

### Gold Layer

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SILVER_DB` | No | `ag` | Source Silver schema |
| `GOLD_DB` | No | `au` | Gold database/schema name |

## Pipeline Settings

### Schedule Recommendations

| Pipeline | Frequency | Rationale |
|----------|-----------|-----------|
| `pl_historical_load` | Once (manual) | Initial data load only |
| `pl_streaming_silver` | 5 minutes | Balance freshness vs compute cost |
| `pl_streaming_gold` | 15 minutes | Gold is for analytics, less time-sensitive |
| `pl_maintenance` | Daily (3 AM) | Optimize Delta tables during low usage |

### Retry Configuration

| Setting | Value |
|---------|-------|
| **Retries** | 3 |
| **Retry Interval** | 30 seconds |
| **Timeout** | 1 hour (30 min for Gold) |

## Schema Naming Convention

| Schema | Layer | Purpose |
|--------|-------|---------|
| `cusn` | Bronze | Eventhouse event table shortcuts |
| `ag` | Silver | Cleaned, deduplicated Delta tables |
| `au` | Gold | Pre-aggregated KPIs |

## Notebook Parameters

### 02-historical-data-load.ipynb

```python
SILVER_DB = "ag"     # Target Silver schema
GOLD_DB = "au"       # Target Gold schema
```

### 03-streaming-to-silver.ipynb

```python
SILVER_DB = "ag"           # Target Silver schema
BRONZE_SCHEMA = "cusn"     # Source Bronze schema
```

### 04-streaming-to-gold.ipynb

```python
SILVER_DB = "ag"     # Source Silver schema
GOLD_DB = "au"       # Target Gold schema
```

### 05-maintain-delta-tables.ipynb

```python
SILVER_DB = "ag"     # Silver schema to optimize
GOLD_DB = "au"       # Gold schema to optimize
```
