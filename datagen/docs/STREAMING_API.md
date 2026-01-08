# Streaming API Reference

Complete REST API reference for real-time event streaming endpoints.

## Base URL

```
http://localhost:8000/api
```

## Authentication

Currently no authentication required for local development. For production deployments, implement authentication middleware.

---

## Streaming Control Endpoints

### Start Streaming

Start real-time event streaming to Azure Event Hub.

```http
POST /stream/start
```

**Request Body:**

```json
{
  "emit_interval_ms": 500,
  "burst": 100,
  "duration_minutes": null,
  "event_types": ["receipt_created", "inventory_updated"]
}
```

**Parameters:**
- `emit_interval_ms` (integer, optional): Milliseconds between event bursts. Default: 500
- `burst` (integer, optional): Number of events per burst. Default: 100
- `duration_minutes` (integer, optional): Auto-stop after N minutes. Null = run indefinitely
- `event_types` (array, optional): Filter specific event types. Null = all types

**Response (200 OK):**

```json
{
  "success": true,
  "message": "Event streaming started",
  "operation_id": "streaming_a1b2c3d4",
  "started_at": "2024-01-15T10:30:00Z"
}
```

**Error Responses:**

- **409 Conflict**: Streaming already active
- **400 Bad Request**: Invalid event types or missing fact data
- **400 Bad Request**: Azure connection string not configured

**Example:**

```bash
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{
    "emit_interval_ms": 1000,
    "burst": 50,
    "duration_minutes": 10
  }'
```

---

### Stop Streaming

Stop the currently active streaming session.

```http
POST /stream/stop
```

**Response (200 OK):**

```json
{
  "success": true,
  "message": "Event streaming stopped",
  "operation_id": "streaming_a1b2c3d4"
}
```

**Error Responses:**

- **400 Bad Request**: No active streaming session
- **404 Not Found**: Streaming session not found

**Example:**

```bash
curl -X POST http://localhost:8000/api/stream/stop
```

---

### Get Streaming Status

Get current streaming status and basic statistics.

```http
GET /stream/status
```

**Response (200 OK):**

```json
{
  "is_streaming": true,
  "status": "running",
  "uptime_seconds": 450.5,
  "events_sent": 15234,
  "events_per_second": 33.8,
  "last_event_time": "2024-01-15T10:37:30Z"
}
```

**Status Values:**
- `running`: Actively streaming events
- `stopped`: No active streaming session
- `error`: Streaming encountered errors

**Example:**

```bash
curl http://localhost:8000/api/stream/status
```

---

## Statistics & Monitoring Endpoints

### Get Detailed Statistics

Get comprehensive streaming statistics and metrics.

```http
GET /stream/statistics
```

**Response (200 OK):**

```json
{
  "events_generated": 25000,
  "events_sent_successfully": 24897,
  "events_failed": 103,
  "batches_sent": 250,
  "total_streaming_time": 600.5,
  "events_per_second": 41.6,
  "bytes_sent": 15728640,
  "last_event_time": "2024-01-15T10:40:00Z",
  "event_type_counts": {
    "receipt_created": 5000,
    "receipt_line_added": 15000,
    "inventory_updated": 3000,
    "customer_entered": 2000
  },
  "error_counts": {
    "connection_timeout": 85,
    "throttling": 18
  },
  "connection_failures": 12,
  "circuit_breaker_trips": 2
}
```

**Example:**

```bash
curl http://localhost:8000/api/stream/statistics
```

---

### Get Recent Events

Retrieve the most recent streaming events (last 100 events buffered).

```http
GET /stream/events/recent?limit=50
```

**Query Parameters:**
- `limit` (integer): Number of events to return (1-100). Default: 100

**Response (200 OK):**

```json
{
  "events": [
    {
      "timestamp": "2024-01-15T10:40:15Z",
      "event_type": "receipt_created",
      "trace_id": "TR_20240115_abc123",
      "payload": {
        "store_id": 42,
        "customer_id": 15678,
        "receipt_id": "RCT_20240115_xyz789",
        "total": 85.42,
        "item_count": 8
      }
    }
  ],
  "count": 50,
  "timestamp": "2024-01-15T10:40:30Z"
}
```

**Example:**

```bash
curl "http://localhost:8000/api/stream/events/recent?limit=20"
```

---

### Stream Health Check

Check health of streaming components.

```http
GET /stream/health
```

**Response (200 OK):**

```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:40:00Z",
  "checks": {
    "streaming_task": {
      "status": "active",
      "task_status": "running",
      "uptime_seconds": 600
    },
    "azure_config": {
      "status": "configured",
      "hub_name": "retail-events"
    },
    "statistics": {
      "status": "healthy",
      "events_generated": 25000,
      "events_per_second": 41.6,
      "failure_rate": 0.004
    }
  }
}
```

**Status Values:**
- `healthy`: All components operational
- `degraded`: Some components not configured or have issues
- `unhealthy`: Critical failures detected

**Example:**

```bash
curl http://localhost:8000/api/stream/health
```

---

## Configuration Endpoints

### Get Streaming Configuration

Get current streaming and real-time configuration.

```http
GET /stream/config
```

**Response (200 OK):**

```json
{
  "realtime": {
    "emit_interval_ms": 500,
    "burst": 100,
    "azure_connection_string": "Endpoint=sb://...",
    "max_batch_size": 256,
    "circuit_breaker_enabled": true
  },
  "stream": {
    "hub": "retail-events"
  },
  "available_event_types": [
    "receipt_created",
    "receipt_line_added",
    "payment_processed",
    "inventory_updated",
    "stockout_detected",
    "reorder_triggered",
    "customer_entered",
    "customer_zone_changed",
    "ble_ping_detected",
    "truck_arrived",
    "truck_departed",
    "store_opened",
    "store_closed",
    "ad_impression",
    "promotion_applied"
  ]
}
```

**Example:**

```bash
curl http://localhost:8000/api/stream/config
```

---

### Update Streaming Configuration

Update streaming configuration settings.

```http
PUT /stream/config
```

**Request Body:**

```json
{
  "emit_interval_ms": 1000,
  "burst": 50,
  "max_batch_size": 128,
  "circuit_breaker_enabled": true
}
```

**Parameters (all optional):**
- `emit_interval_ms` (integer): Time between bursts
- `burst` (integer): Events per burst
- `max_batch_size` (integer): Max events per batch
- `batch_timeout_ms` (integer): Batch timeout
- `retry_attempts` (integer): Send retry attempts
- `circuit_breaker_enabled` (boolean): Enable circuit breaker
- `monitoring_interval` (integer): Monitoring interval seconds

**Response (200 OK):**

```json
{
  "success": true,
  "message": "Streaming configuration updated successfully"
}
```

**Error Responses:**

- **409 Conflict**: Cannot update while streaming is active
- **400 Bad Request**: Invalid configuration values

**Example:**

```bash
curl -X PUT http://localhost:8000/api/stream/config \
  -H "Content-Type: application/json" \
  -d '{"emit_interval_ms": 2000, "burst": 25}'
```

---

### Test Azure Connection

Test connection to Azure Event Hub.

```http
POST /stream/test
```

**Response (200 OK - Success):**

```json
{
  "success": true,
  "message": "Connection test successful",
  "response_time_ms": 45.2,
  "details": {
    "namespace": "retail-analytics",
    "event_hub": "retail-events"
  }
}
```

**Response (200 OK - Failure):**

```json
{
  "success": false,
  "message": "Connection test failed: Authentication failed",
  "response_time_ms": 125.8,
  "details": {
    "exception_type": "AuthenticationError"
  }
}
```

**Example:**

```bash
curl -X POST http://localhost:8000/api/stream/test
```

---

## Event Type Management

### List Available Event Types

Get list of all available event types for streaming.

```http
GET /stream/event-types
```

**Response (200 OK):**

```json
{
  "event_types": [
    "receipt_created",
    "receipt_line_added",
    "payment_processed",
    "inventory_updated",
    "stockout_detected",
    "reorder_triggered",
    "customer_entered",
    "customer_zone_changed",
    "ble_ping_detected",
    "truck_arrived",
    "truck_departed",
    "store_opened",
    "store_closed",
    "ad_impression",
    "promotion_applied"
  ],
  "count": 15,
  "description": "Available event types for real-time streaming"
}
```

**Example:**

```bash
curl http://localhost:8000/api/stream/event-types
```

---

## Supply Chain Disruption Endpoints

Simulate supply chain disruptions that affect streaming events.

### Create Disruption

Create a supply chain disruption simulation.

```http
POST /disruption/create
```

**Request Body:**

```json
{
  "disruption_type": "truck_breakdown",
  "target_id": 5,
  "severity": 0.7,
  "duration_minutes": 30,
  "product_ids": [101, 102, 103]
}
```

**Parameters:**
- `disruption_type` (string): Type of disruption
  - `dc_outage`: Distribution center outage
  - `inventory_shortage`: Inventory shortage
  - `truck_breakdown`: Truck breakdown
  - `weather_delay`: Weather-related delays
- `target_id` (integer): Target entity ID (DC, store, or truck)
- `severity` (float): Disruption severity (0.0-1.0)
- `duration_minutes` (integer): Duration in minutes
- `product_ids` (array, optional): Affected product IDs

**Response (200 OK):**

```json
{
  "success": true,
  "disruption_id": "disruption_x7y8z9",
  "message": "Created truck_breakdown disruption for target 5",
  "active_until": "2024-01-15T11:10:00Z"
}
```

**Example:**

```bash
curl -X POST http://localhost:8000/api/disruption/create \
  -H "Content-Type: application/json" \
  -d '{
    "disruption_type": "inventory_shortage",
    "target_id": 10,
    "severity": 0.5,
    "duration_minutes": 15
  }'
```

---

### List Active Disruptions

Get all currently active disruptions.

```http
GET /disruption/list
```

**Response (200 OK):**

```json
{
  "disruptions": [
    {
      "disruption_id": "disruption_x7y8z9",
      "type": "truck_breakdown",
      "target_id": 5,
      "severity": 0.7,
      "created_at": "2024-01-15T10:40:00Z",
      "active_until": "2024-01-15T11:10:00Z",
      "time_remaining_minutes": 12.5,
      "events_affected": 45,
      "status": "active"
    }
  ],
  "count": 1,
  "timestamp": "2024-01-15T10:57:30Z"
}
```

**Example:**

```bash
curl http://localhost:8000/api/disruption/list
```

---

### Cancel Disruption

Cancel a specific active disruption.

```http
DELETE /disruption/{disruption_id}
```

**Response (200 OK):**

```json
{
  "success": true,
  "message": "Cancelled disruption disruption_x7y8z9",
  "operation_id": "disruption_x7y8z9"
}
```

**Error Response:**

- **404 Not Found**: Disruption not found or already expired

**Example:**

```bash
curl -X DELETE http://localhost:8000/api/disruption/disruption_x7y8z9
```

---

### Clear All Disruptions

Cancel all active disruptions.

```http
POST /disruption/clear-all
```

**Response (200 OK):**

```json
{
  "success": true,
  "message": "Cleared 3 active disruptions"
}
```

**Example:**

```bash
curl -X POST http://localhost:8000/api/disruption/clear-all
```

---

## Event Envelope Format

All events use a standard envelope format for consistency and traceability.

### Envelope Schema

```json
{
  "event_type": "receipt_created",
  "payload": {
    "store_id": 42,
    "customer_id": 15678,
    "receipt_id": "RCT_20240115_xyz789",
    "subtotal": 78.50,
    "tax": 6.92,
    "total": 85.42,
    "tender_type": "CREDIT_CARD",
    "item_count": 8
  },
  "trace_id": "TR_20240115_abc123xyz",
  "ingest_timestamp": "2024-01-15T10:40:15.123Z",
  "schema_version": "1.0",
  "source": "retail-datagen",
  "correlation_id": "SESSION_xyz789",
  "partition_key": "store_42"
}
```

**Envelope Fields:**
- `event_type` (string): Event type identifier (see Event Types section)
- `payload` (object): Event-specific data
- `trace_id` (string): Unique trace identifier for tracking
- `ingest_timestamp` (string): ISO-8601 timestamp when event was created
- `schema_version` (string): Event schema version (default: "1.0")
- `source` (string): Source system (default: "retail-datagen")
- `correlation_id` (string, optional): Links related events
- `partition_key` (string, optional): Event Hub partition key

---

## Event Types & Payloads

### Transaction Events

#### receipt_created

```json
{
  "store_id": 42,
  "customer_id": 15678,
  "receipt_id": "RCT_20240115_xyz789",
  "subtotal": 78.50,
  "tax": 6.92,
  "total": 85.42,
  "tender_type": "CREDIT_CARD",
  "item_count": 8
}
```

#### receipt_line_added

```json
{
  "receipt_id": "RCT_20240115_xyz789",
  "line_number": 1,
  "product_id": 1523,
  "quantity": 2,
  "unit_price": 12.99,
  "extended_price": 25.98,
  "promo_code": "SAVE10"
}
```

#### payment_processed

```json
{
  "receipt_id": "RCT_20240115_xyz789",
  "payment_method": "CREDIT_CARD",
  "amount": 85.42,
  "transaction_id": "TXN_abc123",
  "processing_time": "2024-01-15T10:40:15Z",
  "status": "APPROVED"
}
```

### Inventory Events

#### inventory_updated

```json
{
  "store_id": 42,
  "dc_id": null,
  "product_id": 1523,
  "quantity_delta": -2,
  "reason": "SALE",
  "source": "POS"
}
```

#### stockout_detected

```json
{
  "store_id": 42,
  "dc_id": null,
  "product_id": 1523,
  "last_known_quantity": 0,
  "detection_time": "2024-01-15T10:40:00Z"
}
```

#### reorder_triggered

```json
{
  "store_id": 42,
  "dc_id": null,
  "product_id": 1523,
  "current_quantity": 5,
  "reorder_quantity": 50,
  "reorder_point": 10,
  "priority": "HIGH"
}
```

### Customer Events

#### customer_entered

```json
{
  "store_id": 42,
  "sensor_id": "SENSOR_ENTRANCE_A",
  "zone": "ENTRANCE",
  "customer_count": 1,
  "dwell_time": 0
}
```

#### customer_zone_changed

```json
{
  "store_id": 42,
  "customer_ble_id": "BLE_abc123",
  "from_zone": "ENTRANCE",
  "to_zone": "PRODUCE",
  "timestamp": "2024-01-15T10:40:15Z"
}
```

#### ble_ping_detected

```json
{
  "store_id": 42,
  "beacon_id": "BEACON_PRODUCE_1",
  "customer_ble_id": "BLE_abc123",
  "rssi": -65,
  "zone": "PRODUCE"
}
```

### Operational Events

#### truck_arrived

```json
{
  "truck_id": "TRK_12345",
  "dc_id": null,
  "store_id": 42,
  "shipment_id": "SHIP_xyz789",
  "arrival_time": "2024-01-15T10:40:00Z",
  "estimated_unload_duration": 45
}
```

#### truck_departed

```json
{
  "truck_id": "TRK_12345",
  "dc_id": 5,
  "store_id": null,
  "shipment_id": "SHIP_xyz789",
  "departure_time": "2024-01-15T11:25:00Z",
  "actual_unload_duration": 48
}
```

#### store_opened / store_closed

```json
{
  "store_id": 42,
  "operation_time": "2024-01-15T08:00:00Z",
  "operation_type": "opened"
}
```

### Marketing Events

#### ad_impression

```json
{
  "channel": "SOCIAL_MEDIA",
  "campaign_id": "CAMP_WINTER_2024",
  "creative_id": "CREATIVE_001",
  "customer_ad_id": "AD_xyz789",
  "impression_id": "IMP_abc123",
  "cost": 0.15,
  "device_type": "MOBILE"
}
```

#### promotion_applied

```json
{
  "receipt_id": "RCT_20240115_xyz789",
  "promo_code": "SAVE10",
  "discount_amount": 8.50,
  "discount_type": "PERCENTAGE",
  "product_ids": [1523, 1524, 1525]
}
```

---

## Rate Limiting

Some endpoints have rate limiting to prevent abuse:

- `/stream/start`: 5 requests per 60 seconds
- `/stream/test`: 10 requests per 60 seconds
- `/disruption/create`: 20 requests per 60 seconds

**Rate limit exceeded response (429):**

```json
{
  "detail": "Rate limit exceeded. Try again later."
}
```

---

## Error Responses

Standard error response format:

```json
{
  "detail": "Error message describing the issue"
}
```

**Common HTTP Status Codes:**
- `200 OK`: Request successful
- `400 Bad Request`: Invalid request parameters
- `404 Not Found`: Resource not found
- `409 Conflict`: Conflicting state (e.g., streaming already active)
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: Server-side error
- `501 Not Implemented`: Feature not yet implemented

---

## WebSocket Support (Future)

WebSocket support for real-time event streaming is planned for future releases:

```
ws://localhost:8000/ws/stream
```

This will enable browser-based real-time event monitoring without polling.

---

## OpenAPI Documentation

Interactive API documentation available at:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

---

## SDK Examples

### Python

```python
import requests

# Start streaming
response = requests.post('http://localhost:8000/api/stream/start', json={
    'emit_interval_ms': 1000,
    'burst': 50,
    'duration_minutes': 10
})
print(response.json())

# Get status
status = requests.get('http://localhost:8000/api/stream/status')
print(status.json())

# Stop streaming
stop = requests.post('http://localhost:8000/api/stream/stop')
print(stop.json())
```

### JavaScript/Node.js

```javascript
const axios = require('axios');

const baseURL = 'http://localhost:8000/api';

// Start streaming
async function startStreaming() {
  const response = await axios.post(`${baseURL}/stream/start`, {
    emit_interval_ms: 1000,
    burst: 50,
    duration_minutes: 10
  });
  console.log(response.data);
}

// Get status
async function getStatus() {
  const response = await axios.get(`${baseURL}/stream/status`);
  console.log(response.data);
}

// Stop streaming
async function stopStreaming() {
  const response = await axios.post(`${baseURL}/stream/stop`);
  console.log(response.data);
}
```

### cURL

```bash
# Start streaming
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{"emit_interval_ms": 1000, "burst": 50, "duration_minutes": 10}'

# Get status
curl http://localhost:8000/api/stream/status

# Stop streaming
curl -X POST http://localhost:8000/api/stream/stop
```

---

## Next Steps

- **Setup**: See [STREAMING_SETUP.md](STREAMING_SETUP.md) for configuration
- **Operations**: See [STREAMING_OPERATIONS.md](STREAMING_OPERATIONS.md) for monitoring
- **Security**: See [CREDENTIALS.md](CREDENTIALS.md) for credential management
