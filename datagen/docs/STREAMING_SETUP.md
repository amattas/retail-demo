# Streaming Setup Guide

Complete guide for setting up real-time event streaming to Azure Event Hub or Microsoft Fabric Real-Time Intelligence.

## Prerequisites

- **Python 3.11+** (strict requirement)
- **Azure Event Hub** or **Microsoft Fabric RTI** workspace
- **Connection string** with send permissions
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

Check that retail-datagen is properly installed:

```bash
python -m retail_datagen.main --help
```

Start the FastAPI server:

```bash
python -m retail_datagen.main
```

Access the application at http://localhost:8000

## Azure Event Hub Configuration

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

For production environments, use Azure Key Vault integration:

```json
{
  "realtime": {
    "use_keyvault": true,
    "keyvault_url": "https://your-vault.vault.azure.net/",
    "keyvault_secret_name": "eventhub-connection-string"
  }
}
```

**Requirements:**
- Azure Key Vault instance
- Managed identity or service principal with secret read access
- Azure SDK for Key Vault: `pip install azure-keyvault-secrets azure-identity`

## Microsoft Fabric RTI Setup

Microsoft Fabric Real-Time Intelligence uses Event Hubs as its streaming backend.

### Step 1: Create Event Stream in Fabric

1. Navigate to your Fabric workspace
2. Click **New** → **Eventstream**
3. Name your event stream (e.g., "retail-events")
4. Wait for provisioning to complete

### Step 2: Get Connection String

1. Open your Event Stream
2. Navigate to the **Keys** or **Settings** section
3. Copy the **Connection string-primary key**
4. Format will be: `Endpoint=sb://eventstream-xxx.servicebus.windows.net/;SharedAccessKeyName=...`

### Step 3: Configure retail-datagen

Set the connection string using environment variable:

```bash
export AZURE_EVENTHUB_CONNECTION_STRING="<your-fabric-eventstream-connection-string>"
```

Or add to `config.json`:

```json
{
  "realtime": {
    "azure_connection_string": "<your-fabric-eventstream-connection-string>"
  },
  "stream": {
    "hub": "retail-events"
  }
}
```

### Step 4: Configure Event Stream Destination

In Fabric, configure where events should flow:

1. **Lakehouse**: Direct ingestion to Delta tables
2. **KQL Database**: Real-time analytics with Kusto Query Language
3. **Eventhouse**: Advanced real-time intelligence scenarios
4. **Custom endpoint**: Webhook, Function, Logic App

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

Use the API to test your Azure Event Hub connection:

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

Check that historical data exists (required for streaming):

```bash
curl http://localhost:8000/api/generators/state
```

Expected response:
```json
{
  "has_historical_data": true,
  "last_generated_timestamp": "2024-01-15T10:30:00",
  ...
}
```

If `has_historical_data` is `false`, generate historical data first:

```bash
curl -X POST http://localhost:8000/api/generators/historical/start
```

### Test Streaming

Start a short streaming session (1 minute):

```bash
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{"duration_minutes": 1, "emit_interval_ms": 1000, "burst": 10}'
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

**Warning:** High throughput may hit Event Hub throttling limits. Monitor failure rates.

## Troubleshooting

### Connection String Issues

**Problem:** "Invalid connection string format"
**Solution:** Ensure connection string includes all required components (Endpoint, SharedAccessKeyName, SharedAccessKey)

**Problem:** "Authentication failed"
**Solution:** Verify SharedAccessKey is correct and policy has Send permissions

### Prerequisite Errors

**Problem:** "Historical data must be generated first"
**Solution:** Generate historical data before starting streaming:

```bash
# Generate master data
curl -X POST http://localhost:8000/api/generators/master/start

# Generate historical data
curl -X POST http://localhost:8000/api/generators/historical/start
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

- **API Usage**: See [STREAMING_API.md](STREAMING_API.md) for endpoint documentation
- **Operations**: See [STREAMING_OPERATIONS.md](STREAMING_OPERATIONS.md) for monitoring and troubleshooting
- **Security**: See [CREDENTIALS.md](CREDENTIALS.md) for credential management best practices
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
- Check logs: `tail -f retail_datagen.log`
- Enable debug logging: Set `LOG_LEVEL=DEBUG` environment variable
- Review [STREAMING_OPERATIONS.md](STREAMING_OPERATIONS.md) troubleshooting section
- Check Azure Event Hub metrics in Azure Portal
