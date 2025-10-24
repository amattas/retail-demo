# Connection String Validation Guide

## Overview

The Retail Data Generator includes comprehensive validation for Azure Event Hub and Microsoft Fabric RTI connection strings. This ensures configuration errors are caught early with helpful, actionable error messages.

## Quick Start

### Validate Your Connection String

Before starting streaming, validate your connection string:

```bash
curl -X POST http://localhost:8000/api/stream/validate-connection \
  -H "Content-Type: application/json" \
  -d '{"connection_string": "YOUR_CONNECTION_STRING_HERE"}'
```

**Response:**
```json
{
  "valid": true,
  "error": null,
  "message": "Connection string is valid",
  "metadata": {
    "endpoint": "sb://eventstream-xxx.servicebus.windows.net",
    "namespace": "eventstream-xxx",
    "key_name": "key_123456",
    "entity_path": "es_mystream",
    "has_key": true,
    "is_fabric_rti": true
  },
  "sanitized": "Endpoint=sb://...;SharedAccessKeyName=key_123456;SharedAccessKey=***REDACTED***;EntityPath=es_mystream"
}
```

### Validate Complete Configuration

Validate your entire streaming configuration including connection string and parameters:

```bash
curl -X POST http://localhost:8000/api/config/validate
```

**Response:**
```json
{
  "valid": true,
  "errors": [],
  "warnings": [],
  "connection_metadata": {
    "endpoint": "sb://eventstream-xxx.servicebus.windows.net",
    "namespace": "eventstream-xxx",
    "entity_path": "es_mystream",
    "is_fabric_rti": true
  },
  "recommendations": [
    "Detected Fabric RTI connection - ensure workspace has proper permissions",
    "Fabric RTI automatically scales - monitor usage in Fabric portal"
  ]
}
```

## Connection String Formats

### Azure Event Hub (Standard)

```
Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key-here==
```

**Optional:** Add `EntityPath=your-hub-name` if not specified in code.

### Microsoft Fabric RTI (Event Stream)

```
Endpoint=sb://eventstream-xxxxxx.servicebus.windows.net/;SharedAccessKeyName=key_123456;SharedAccessKey=your-key-here==;EntityPath=es_your_stream_name
```

**Required Components:**
- Namespace starts with `eventstream-`
- EntityPath starts with `es_`
- SharedAccessKeyName typically starts with `key_`

## Validation Rules

### Required Components

| Component | Description | Example |
|-----------|-------------|---------|
| `Endpoint` | Service Bus endpoint with `sb://` protocol | `sb://namespace.servicebus.windows.net/` |
| `SharedAccessKeyName` | Name of the access key | `RootManageSharedAccessKey` or `key_123456` |
| `SharedAccessKey` | Access key value (base64-encoded) | `abc123xyz789==` |
| `EntityPath` | Hub or stream name (required for Fabric RTI) | `es_mystream` |

### Format Requirements

✅ **Must Have:**
- Connection string length ≥ 50 characters
- Endpoint domain ends with `.servicebus.windows.net` or `.servicebus.chinacloudapi.cn`
- SharedAccessKey length ≥ 20 characters
- No empty values (no `SharedAccessKeyName=;`)

✅ **Fabric RTI Specific:**
- Must include `EntityPath` parameter
- EntityPath must start with `es_`
- Namespace should start with `eventstream-`

## Common Errors and Solutions

### Error: "Connection string is too short (likely incomplete)"

**Cause:** Connection string is less than 50 characters or missing components.

**Solution:**
1. Ensure you copied the entire connection string
2. Verify it includes all three required components (Endpoint, SharedAccessKeyName, SharedAccessKey)

```bash
# Incomplete ❌
Endpoint=sb://test

# Complete ✓
Endpoint=sb://test-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123xyz789==
```

### Error: "Missing required part: Event Hub endpoint (Endpoint=sb://...)"

**Cause:** Connection string is missing the `Endpoint=sb://` component.

**Solution:** Add the endpoint at the beginning:

```bash
# Missing Endpoint ❌
SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123xyz789==

# With Endpoint ✓
Endpoint=sb://test-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123xyz789==
```

### Error: "Invalid Event Hub endpoint domain"

**Cause:** Endpoint doesn't end with a valid Azure domain.

**Solution:** Ensure endpoint ends with `.servicebus.windows.net` (global) or `.servicebus.chinacloudapi.cn` (China):

```bash
# Invalid Domain ❌
Endpoint=sb://test.invalid-domain.com/

# Valid Domain ✓
Endpoint=sb://test-namespace.servicebus.windows.net/
```

### Error: "SharedAccessKey appears invalid (too short)"

**Cause:** The key is suspiciously short (less than 20 characters).

**Solution:**
1. Copy the full key from Azure Portal or Fabric workspace
2. Ensure you didn't accidentally truncate the key

```bash
# Too Short ❌
SharedAccessKey=short

# Valid Length ✓
SharedAccessKey=abc123xyz789longenoughkey==
```

### Error: "Fabric RTI connection strings require EntityPath"

**Cause:** Detected a Fabric RTI connection (namespace starts with `eventstream-`) but missing EntityPath.

**Solution:** Add EntityPath parameter with your Event Stream name:

```bash
# Missing EntityPath ❌
Endpoint=sb://eventstream-xxx.servicebus.windows.net/;SharedAccessKeyName=key_123;SharedAccessKey=abc123==

# With EntityPath ✓
Endpoint=sb://eventstream-xxx.servicebus.windows.net/;SharedAccessKeyName=key_123;SharedAccessKey=abc123==;EntityPath=es_mystream
```

### Error: "Fabric RTI EntityPath should start with 'es_' (Event Stream)"

**Cause:** EntityPath doesn't follow Fabric RTI naming convention.

**Solution:** Ensure EntityPath starts with `es_`:

```bash
# Wrong Prefix ❌
EntityPath=mystream

# Correct Prefix ✓
EntityPath=es_mystream
```

## Configuration Parameter Warnings

### Warning: "Very low emit_interval (50ms) may cause high CPU usage"

**Cause:** `emit_interval_ms` is set below 100ms.

**Impact:** May cause high CPU usage due to tight event generation loop.

**Recommendation:**
- For testing: Use 500-1000ms
- For production: Use 100-500ms
- Only use <100ms if you need extremely high throughput

```json
{
  "realtime": {
    "emit_interval_ms": 500  // Recommended for most use cases
  }
}
```

### Warning: "Large burst size (1500) may exceed Event Hub limits"

**Cause:** `burst` is set above 1000 events.

**Impact:** May hit Event Hub throughput limits or cause throttling.

**Recommendation:**
- Standard Event Hub: Use 50-500 per burst
- Fabric RTI: Can handle larger bursts, but test incrementally

```json
{
  "realtime": {
    "burst": 100  // Safe for most scenarios
  }
}
```

### Recommendation: "Batch sizes > 256 may hit Event Hub message size limits"

**Cause:** `max_batch_size` is set above 256.

**Impact:** Large batches may exceed 1MB message size limit.

**Recommendation:**
- Keep batch size at or below 256 events
- Monitor for "Message too large" errors if using higher values

```json
{
  "realtime": {
    "max_batch_size": 256  // Maximum recommended
  }
}
```

## Best Practices

### 1. Validate Before Deploying

Always validate your connection string before deploying to production:

```bash
# Test connection string format
curl -X POST http://localhost:8000/api/stream/validate-connection \
  -H "Content-Type: application/json" \
  -d '{"connection_string": "YOUR_CONNECTION_STRING"}'

# Test actual connectivity
curl -X POST http://localhost:8000/api/stream/test
```

### 2. Use Environment Variables

Store connection strings in environment variables, not in `config.json`:

```bash
export AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://..."
```

**Benefits:**
- Keeps secrets out of version control
- Easy to rotate credentials
- Works across environments (dev, staging, prod)

### 3. Monitor Validation on Startup

Check application logs on startup for validation status:

```
INFO - ✓ Event Hub connection string validated
```

or

```
WARNING - ⚠ Event Hub connection string validation failed: Missing required part: SharedAccessKey
```

### 4. Test Configuration Changes

When changing configuration, validate before restarting:

```bash
curl -X POST http://localhost:8000/api/config/validate \
  -H "Content-Type: application/json" \
  -d @new-config.json
```

### 5. Fabric RTI Specific

For Fabric RTI connections:
1. Ensure workspace has proper permissions
2. Verify Event Stream is running
3. Monitor usage in Fabric portal (capacity units)
4. Test with small burst sizes first

## Getting Connection Strings

### Azure Event Hub

1. Go to Azure Portal → Event Hubs
2. Select your namespace
3. Go to "Shared access policies"
4. Select or create a policy
5. Copy "Connection string-primary key"

### Microsoft Fabric RTI

1. Go to Fabric workspace → Real-Time hub
2. Select your Event Stream
3. Click "Get connection string"
4. Copy the connection string (includes EntityPath)

## Troubleshooting

### Connection String Works in Azure but Fails Validation

**Check:**
1. Did you copy the entire string? (Look for trailing `...`)
2. Is there whitespace at the beginning or end?
3. Are there any line breaks in the string?

**Solution:**
```bash
# Remove any whitespace and line breaks
echo "YOUR_CONNECTION_STRING" | tr -d '\n\r '
```

### Validation Passes but Connection Fails

**Check:**
1. Is the Event Hub or Event Stream actually running?
2. Do you have network connectivity to Azure?
3. Is the access key still valid? (Keys can be rotated)

**Solution:**
```bash
# Test actual connection
curl -X POST http://localhost:8000/api/stream/test
```

### Getting "Invalid configuration" errors

**Check:**
1. Is your JSON valid? Use a JSON validator
2. Are all required fields present?
3. Do the values match expected types (numbers vs strings)?

**Solution:**
```bash
# Validate JSON syntax first
cat config.json | python -m json.tool
```

## API Reference

### POST /api/stream/validate-connection

Validate connection string format without attempting connection.

**Request:**
```json
{
  "connection_string": "Endpoint=sb://..."
}
```

**Response:**
```json
{
  "valid": true,
  "error": null,
  "message": "Connection string is valid",
  "metadata": { ... },
  "sanitized": "Endpoint=sb://...;SharedAccessKey=***REDACTED***"
}
```

### POST /api/config/validate

Validate complete streaming configuration.

**Request (optional body):**
```json
{
  "realtime": {
    "azure_connection_string": "Endpoint=sb://...",
    "emit_interval_ms": 500,
    "burst": 100
  }
}
```

**Response:**
```json
{
  "valid": true,
  "errors": [],
  "warnings": [],
  "connection_metadata": { ... },
  "recommendations": []
}
```

### POST /api/stream/test

Test actual connection to Event Hub (attempts to connect).

**Response:**
```json
{
  "success": true,
  "message": "Connection successful",
  "response_time_ms": 234.5,
  "details": { ... }
}
```

## Support

For additional help:
- Check application logs for detailed error messages
- See `VALIDATION_IMPLEMENTATION_SUMMARY.md` for technical details
- Review `AGENTS.md` for data contracts and specifications
