# Configuration Guide

Complete reference for configuring the retail data generator.

## Configuration File

The generator uses a `config.json` file. Load via:

```python
from retail_datagen.config.models import RetailConfig

config = RetailConfig.from_file("config.json")
```

## Environment Variables

Sensitive values should use environment variables instead of config file:

| Variable | Purpose |
|----------|---------|
| `AZURE_EVENTHUB_CONNECTION_STRING` | Event Hub connection string |
| `AZURE_STORAGE_ACCOUNT_URI` | Storage account URI |
| `AZURE_STORAGE_ACCOUNT_KEY` | Storage account key |
| `RETAIL_DATAGEN_TEST_MODE` | Set to `true` for test mode |

---

## Configuration Sections

### seed (required)

Random seed for reproducible data generation.

```json
{
  "seed": 42
}
```

- **Type**: integer (0 to 2^32-1)
- **Required**: Yes

---

### volume (required)

Controls data generation scale and volume.

```json
{
  "volume": {
    "stores": 250,
    "dcs": 12,
    "customers_per_day": 1500,
    "items_per_ticket_mean": 8.5,
    "total_customers": 500000,
    "total_products": 10000,
    "total_geographies": 100,
    "online_orders_per_day": 2500,
    "marketing_impressions_per_day": 10000,
    "refrigerated_trucks": 8,
    "non_refrigerated_trucks": 12,
    "supplier_refrigerated_trucks": 12,
    "supplier_non_refrigerated_trucks": 18,
    "truck_capacity": 15000,
    "truck_dc_assignment_rate": 0.85,
    "dc_initial_inventory_min": 500,
    "dc_initial_inventory_max": 5000,
    "store_initial_inventory_min": 20,
    "store_initial_inventory_max": 200,
    "dc_reorder_point_min": 50,
    "dc_reorder_point_max": 500,
    "store_reorder_point_min": 5,
    "store_reorder_point_max": 50
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `stores` | int | required | Number of stores |
| `dcs` | int | required | Number of distribution centers |
| `customers_per_day` | int | required | Avg customers per day per store |
| `items_per_ticket_mean` | float | required | Avg items per receipt |
| `total_customers` | int | 500,000 | Total customer pool |
| `total_products` | int | 10,000 | Total product catalog |
| `total_geographies` | int | 100 | Number of geographies |
| `online_orders_per_day` | int | 2,500 | Daily online orders |
| `marketing_impressions_per_day` | int | 10,000 | Daily ad impressions cap |
| `refrigerated_trucks` | int | 8 | Refrigerated trucks |
| `non_refrigerated_trucks` | int | 12 | Non-refrigerated trucks |
| `truck_capacity` | int | 15,000 | Max items per truck |
| `truck_dc_assignment_rate` | float | 0.85 | % trucks assigned to DCs |

---

### realtime (required)

Controls streaming to Azure Event Hub.

```json
{
  "realtime": {
    "emit_interval_ms": 500,
    "burst": 100,
    "azure_connection_string": "",
    "max_batch_size": 256,
    "batch_timeout_ms": 1000,
    "retry_attempts": 3,
    "backoff_multiplier": 2.0,
    "circuit_breaker_enabled": true,
    "circuit_breaker_failure_threshold": 5,
    "circuit_breaker_recovery_timeout": 60,
    "monitoring_interval": 30,
    "max_buffer_size": 10000,
    "enable_dead_letter_queue": true
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `emit_interval_ms` | int | required | Ms between event bursts |
| `burst` | int | required | Events per burst |
| `azure_connection_string` | string | "" | Event Hub connection (prefer env var) |
| `max_batch_size` | int | 256 | Max events per batch |
| `batch_timeout_ms` | int | 1,000 | Batch timeout in ms |
| `retry_attempts` | int | 3 | Retry attempts on failure |
| `backoff_multiplier` | float | 2.0 | Exponential backoff multiplier |
| `circuit_breaker_enabled` | bool | true | Enable circuit breaker |
| `circuit_breaker_failure_threshold` | int | 5 | Failures before circuit opens |
| `circuit_breaker_recovery_timeout` | int | 60 | Seconds before retry after open |
| `monitoring_interval` | int | 30 | Stats update interval (seconds) |
| `max_buffer_size` | int | 10,000 | Internal event buffer size |
| `enable_dead_letter_queue` | bool | true | Enable DLQ for failed events |

---

### paths (required)

File system paths for data files.

```json
{
  "paths": {
    "dict": "data/dictionaries",
    "master": "data/master",
    "facts": "data/facts"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `dict` | string | Path to dictionary CSV files |
| `master` | string | Path to master data output |
| `facts` | string | Path to fact data output |

---

### stream (required)

Event Hub name configuration.

```json
{
  "stream": {
    "hub": "retail-events"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `hub` | string | Event Hub name |

---

### historical (optional)

Historical data generation settings.

```json
{
  "historical": {
    "start_date": "2024-01-01"
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start_date` | string | "2024-01-01" | Start date (YYYY-MM-DD) |

---

### performance (optional)

Resource usage controls.

```json
{
  "performance": {
    "max_cpu_percent": 100.0,
    "max_workers": null,
    "batch_hours": 1
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_cpu_percent` | float | 100.0 | Max CPU usage (0-100) |
| `max_workers` | int\|null | null | Override parallel workers |
| `batch_hours` | int | 1 | Hours to batch before DB insert |

---

### storage (optional)

Azure Storage configuration for uploads.

```json
{
  "storage": {
    "account_uri": null,
    "account_key": null
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `account_uri` | string\|null | Storage account URI |
| `account_key` | string\|null | Storage account key (prefer env var) |

---

### marketing_cost (optional)

Marketing impression cost configuration.

```json
{
  "marketing_cost": {
    "email_cost_min": 0.10,
    "email_cost_max": 0.25,
    "display_cost_min": 0.50,
    "display_cost_max": 2.00,
    "social_cost_min": 0.75,
    "social_cost_max": 3.00,
    "search_cost_min": 1.00,
    "search_cost_max": 5.00,
    "video_cost_min": 1.50,
    "video_cost_max": 5.25,
    "mobile_multiplier": 1.0,
    "tablet_multiplier": 1.2,
    "desktop_multiplier": 1.5
  }
}
```

---

## Example Configurations

### Development (Small Scale)

```json
{
  "seed": 42,
  "volume": {
    "stores": 10,
    "dcs": 2,
    "customers_per_day": 100,
    "items_per_ticket_mean": 5.0,
    "total_customers": 10000,
    "total_products": 1000,
    "online_orders_per_day": 50
  },
  "realtime": {
    "emit_interval_ms": 1000,
    "burst": 10
  },
  "paths": {
    "dict": "data/dictionaries",
    "master": "data/master",
    "facts": "data/facts"
  },
  "stream": {
    "hub": "retail-events-dev"
  }
}
```

### Production (Full Scale)

```json
{
  "seed": 12345,
  "volume": {
    "stores": 250,
    "dcs": 12,
    "customers_per_day": 1500,
    "items_per_ticket_mean": 8.5,
    "total_customers": 500000,
    "total_products": 10000,
    "online_orders_per_day": 2500,
    "marketing_impressions_per_day": 10000
  },
  "realtime": {
    "emit_interval_ms": 100,
    "burst": 500,
    "max_batch_size": 512,
    "circuit_breaker_enabled": true
  },
  "paths": {
    "dict": "data/dictionaries",
    "master": "data/master",
    "facts": "data/facts"
  },
  "stream": {
    "hub": "retail-events"
  },
  "performance": {
    "max_cpu_percent": 80.0,
    "batch_hours": 4
  }
}
```

### High-Throughput Streaming

```json
{
  "seed": 99999,
  "volume": {
    "stores": 250,
    "dcs": 12,
    "customers_per_day": 2000,
    "items_per_ticket_mean": 10.0
  },
  "realtime": {
    "emit_interval_ms": 50,
    "burst": 1000,
    "max_batch_size": 1024,
    "batch_timeout_ms": 500,
    "retry_attempts": 5,
    "circuit_breaker_failure_threshold": 10,
    "max_buffer_size": 50000
  },
  "paths": {
    "dict": "data/dictionaries",
    "master": "data/master",
    "facts": "data/facts"
  },
  "stream": {
    "hub": "retail-events-high-throughput"
  }
}
```

---

## Connection String Format

Azure Event Hub connection strings follow this format:

```
Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<name>;SharedAccessKey=<key>;EntityPath=<hub>
```

For Fabric Real-Time Intelligence (RTI):

```
Endpoint=sb://eventstream-<id>.servicebus.fabric.microsoft.com/;SharedAccessKeyName=<name>;SharedAccessKey=<key>;EntityPath=<stream>
```

---

## Security Best Practices

1. **Never commit connection strings** - Use environment variables
2. **Use minimal permissions** - Send-only SAS keys for streaming
3. **Rotate keys regularly** - Update environment variables, not config files
4. **Test with mock connections** - Use `mock://` prefix in development
