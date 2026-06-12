# Configuration Reference

Complete reference for environment variables and pipeline settings.

## Environment Variables

### Data Generator

Non-secret settings live in `datagen/config.json` (copy from `config.example.json`); the Event Hub name is `stream.hub` (default `retail-events`). Secrets are supplied via environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `AZURE_EVENTHUB_CONNECTION_STRING` | For streaming | Event Hubs connection string |
| `AZURE_STORAGE_ACCOUNT_URI` (or `AZURE_STORAGE_ACCOUNT_URL`) | For upload | Storage account URI, may include container/prefix (e.g., `https://stdretail.blob.core.windows.net/supermarket`) |
| `AZURE_STORAGE_ACCOUNT_KEY` | For upload | Storage account key |

See the [Datagen Configuration Reference](../datagen/configuration.md) for the full `config.json` schema.

### Bronze Layer (01-create-bronze-shortcuts.ipynb)

Settings are constants in the notebook's configuration cell (edit before running):

| Setting | Default | Description |
|---------|---------|-------------|
| `LAKEHOUSE_NAME` | `retail_lakehouse` | Lakehouse name (also settable via env var) |
| `BRONZE_SCHEMA` | `cusn` | Bronze schema for Eventhouse shortcuts |
| `ADLS_ACCOUNT` | `stdretail` | Storage account name |
| `ADLS_CONTAINER` | `supermarket` | Container name |
| `EVENTHOUSE_DATABASE` | `retail_eventhouse` | KQL database name |

### Silver Layer (03-streaming-to-silver.ipynb)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LAKEHOUSE_NAME` | No | `retail_lakehouse` | Lakehouse name |
| `SILVER_DB` | No | `ag` | Silver database/schema name |
| `BRONZE_SCHEMA` | No | `cusn` | Source Bronze schema |

### Gold Layer (04-streaming-to-gold.ipynb)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LAKEHOUSE_NAME` | No | `retail_lakehouse` | Lakehouse name |
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
LAKEHOUSE_NAME = "retail_lakehouse"  # Lakehouse containing ag/au schemas
```
