# Streaming Setup Guide

Legacy guide for the deprecated local generator's Event Hub streaming mode. The current Fabric live path uses `stream-events.ipynb` to write directly to Eventhouse/KQL with the Fabric Spark connector for Kusto; see [Phase 6: Optional Live Streaming](../setup/06-streaming.md).

## Prerequisites

- **Python 3.11+** (strict requirement)
- Legacy Azure Event Hub target, if you are running the deprecated local generator
- Connection string with send permissions for that legacy target
- **Historical data** must be generated first (streaming requires existing base data)

## Installation

### 1. Install Azure Event Hub SDK

The streaming system requires the Azure Event Hubs SDK:

```bash
pip install azure-eventhub>=5.11.0
```

This dependency is included in `requirements.txt` and automatically installed with:

```bash
pip install -e .
```

### 2. Verify Installation

Start the FastAPI server from the `datagen/` directory:

```bash
./launch.sh
```

Or run uvicorn directly:

```bash
python -m uvicorn retail_datagen.main:app --app-dir src --host 0.0.0.0 --port 8000 --reload
```

Access the application at http://localhost:8000 (Swagger docs at http://localhost:8000/docs).

## Legacy Azure Event Hub Configuration

### Option A: Environment Variable (Recommended)

Set the connection string as an environment variable for maximum security:

```bash
export AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key;EntityPath=retail-events"
```

**Advantages:**
- No credentials in code or config files
- Easy to rotate credentials
- Works across different environments
- Prevents accidental commits of secrets

**For persistent configuration**, add to your shell profile:

```bash
# Add to ~/.bashrc, ~/.zshrc, or equivalent
echo 'export AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://..."' >> ~/.bashrc
source ~/.bashrc
```

### Option B: Configuration File

Edit `config.json` in the project root:

```json
{
  "realtime": {
    "azure_connection_string": "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key;EntityPath=retail-events",
    "emit_interval_ms": 500,
    "burst": 100
  },
  "stream": {
    "hub": "retail-events"
  }
}
```

**Warning:** Never commit `config.json` with real credentials to version control. Add to `.gitignore`:

```bash
echo 'config.json' >> .gitignore
```

### Option C: Azure Key Vault (Enterprise)

The generator has no built-in Key Vault integration; store the connection string in Key Vault and inject it as the environment variable at startup:

```bash
export AZURE_EVENTHUB_CONNECTION_STRING="$(az keyvault secret show \
  --vault-name your-vault \
  --name eventhub-connection-string \
  --query value -o tsv)"
./launch.sh
```

**Requirements:**
- Azure Key Vault instance
- Managed identity or service principal with secret read access

## Current Microsoft Fabric RTI Setup

Do not create a Fabric Eventstream for this demo. For current Fabric RTI live
ingestion, import and run `utility/notebooks/stream-events.ipynb` with:

| Parameter | Value |
| --- | --- |
| `sink` | `eventhouse` |
| `kusto_uri` | KQL database Query URI from the database details card |
| `kql_database` | `retail_eventhouse` |

The notebook uses Structured Streaming `foreachBatch` to route by `event_type`
and append to KQL tables through the Fabric Spark connector for Kusto. See
[Phase 6: Optional Live Streaming](../setup/06-streaming.md) and Microsoft's
[Spark connector tutorial](https://learn.microsoft.com/fabric/real-time-intelligence/spark-connector).

## Connection String Format

Azure Event Hub connection strings have this format:

```
Endpoint=sb://<namespace>.servicebus.windows.net/;
SharedAccessKeyName=<policy-name>;
SharedAccessKey=<key>;
EntityPath=<event-hub-name>
```

**Components:**
- **Endpoint**: Service Bus namespace URL
- **SharedAccessKeyName**: Access policy name (usually "RootManageSharedAccessKey")
- **SharedAccessKey**: Secret key for authentication
- **EntityPath**: Event Hub name (optional, can be set in config.json)

**Example:**
```
Endpoint=sb://retail-analytics.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123xyz789==;EntityPath=retail-events
```

## Verification

### Test Connection

For the legacy generator, use the API to test the Azure Event Hub connection:

```bash
curl -X POST http://localhost:8000/api/stream/test
```

**Expected success response:**
```json
{
  "success": true,
  "message": "Connection test successful",
  "response_time_ms": 45.2
}
```

**Expected failure response:**
```json
{
  "success": false,
  "message": "Connection test failed: Invalid connection string format",
  "details": {
    "exception_type": "ValueError"
  }
}
```

### Verify Prerequisites

Check that fact data exists (required for streaming):

```bash
curl http://localhost:8000/api/generation/status
```

Expected response:
```json
{
  "has_fact_data": true,
  "last_generated_timestamp": "2024-01-15T10:30:00",
  "fact_start_date": "2024-01-01T00:00:00",
  "last_fact_run": "2024-01-15T10:30:00",
  "last_realtime_run": null,
  "can_start_realtime": true
}
```

If `has_fact_data` is `false`, generate fact data first:

```bash
curl -X POST http://localhost:8000/api/generate/fact \
  -H "Content-Type: application/json" -d '{}'
```

### Test Streaming

Start a short streaming session (1 minute):

```bash
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{"duration_minutes": 1, "emit_interval_override": 1000, "burst_override": 10}'
```

Monitor status:

```bash
curl http://localhost:8000/api/stream/status
```

Check statistics:

```bash
curl http://localhost:8000/api/stream/statistics
```

## Configuration Parameters

Key configuration options in `config.json`:

```json
{
  "realtime": {
    "emit_interval_ms": 500,           // Time between event bursts (milliseconds)
    "burst": 100,                       // Events per burst
    "max_batch_size": 256,              // Max events per Azure batch
    "batch_timeout_ms": 1000,           // Batch timeout
    "retry_attempts": 3,                // Retry failed sends
    "backoff_multiplier": 2.0,          // Exponential backoff multiplier
    "circuit_breaker_enabled": true,    // Enable circuit breaker pattern
    "monitoring_interval": 30,          // Monitoring interval (seconds)
    "max_buffer_size": 10000,           // Max events in buffer
    "enable_dead_letter_queue": true    // Enable DLQ for failed events
  }
}
```

**Performance tuning:**
- **Low throughput**: `emit_interval_ms: 2000`, `burst: 50`
- **Medium throughput**: `emit_interval_ms: 500`, `burst: 100` (default)
- **High throughput**: `emit_interval_ms: 100`, `burst: 500`

**Warning:** High throughput may hit legacy Event Hub throttling limits. Monitor failure rates.

## Troubleshooting

### Connection String Issues

**Problem:** "Invalid connection string format"
**Solution:** Ensure connection string includes all required components (Endpoint, SharedAccessKeyName, SharedAccessKey)

**Problem:** "Authentication failed"
**Solution:** Verify SharedAccessKey is correct and policy has Send permissions

### Prerequisite Errors

**Problem:** "Historical data must be generated before starting real-time streaming"
**Solution:** Generate fact data before starting streaming:

```bash
# Generate dimension data
curl -X POST http://localhost:8000/api/generate/dimensions \
  -H "Content-Type: application/json" -d '{}'

# Generate fact data
curl -X POST http://localhost:8000/api/generate/fact \
  -H "Content-Type: application/json" -d '{}'
```

### Network Issues

**Problem:** Connection timeouts
**Solution:**
- Check firewall rules allow outbound connections to `*.servicebus.windows.net`
- Verify DNS resolution: `nslookup <namespace>.servicebus.windows.net`
- Test network connectivity: `telnet <namespace>.servicebus.windows.net 5671`

### Throttling

**Problem:** High failure rates, circuit breaker trips
**Solution:**
- Reduce `burst` size
- Increase `emit_interval_ms`
- Upgrade Event Hub tier (Standard → Premium)
- Check Azure Event Hub metrics for throttling

## Next Steps

- **API Usage**: See [streaming-api.md](streaming-api.md) for endpoint documentation
- **Operations**: See [streaming-operations.md](streaming-operations.md) for monitoring and troubleshooting
- **Security**: See [auth-setup.md](auth-setup.md) for credential management best practices
- **Web UI**: Access http://localhost:8000 for browser-based control

## Security Best Practices

1. **Never commit credentials** to version control
2. **Use environment variables** for connection strings in development
3. **Use Azure Key Vault** for production environments
4. **Rotate keys regularly** (quarterly recommended)
5. **Use least privilege** access policies (Send-only for streaming)
6. **Enable TLS 1.2+** for all connections (default)
7. **Audit access logs** in Azure Portal regularly

## Support

For issues or questions:
- Check the server's terminal output (the app logs to stdout) or the `logs/` directory
- Review [streaming-operations.md](streaming-operations.md) troubleshooting section
- Check the target streaming service metrics in Azure Portal
