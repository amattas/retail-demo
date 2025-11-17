# Streaming Operations Guide

Comprehensive guide for monitoring, troubleshooting, and operating real-time event streaming in production.

## Table of Contents

- [Monitoring](#monitoring)
- [Key Metrics](#key-metrics)
- [Health Checks](#health-checks)
- [Troubleshooting](#troubleshooting)
- [Circuit Breaker](#circuit-breaker)
- [Dead Letter Queue](#dead-letter-queue)
- [Performance Tuning](#performance-tuning)
- [Production Deployment](#production-deployment)
- [Best Practices](#best-practices)

---

## Monitoring

### Dashboard Overview

The web UI provides real-time monitoring at http://localhost:8000:

**Key Sections:**
- **Streaming Status**: Active/stopped, uptime, events sent
- **Event Rate**: Events per second graph
- **Event Type Distribution**: Breakdown by event type
- **Error Rate**: Failed events and circuit breaker status
- **Recent Events**: Last 100 events with payloads

### API Monitoring

Use the REST API for programmatic monitoring and alerting.

#### Get Current Status

```bash
curl http://localhost:8000/api/stream/status
```

**Response:**
```json
{
  "is_streaming": true,
  "status": "running",
  "uptime_seconds": 3600,
  "events_sent": 120000,
  "events_per_second": 33.3,
  "last_event_time": "2024-01-15T11:30:00Z"
}
```

#### Get Detailed Statistics

```bash
curl http://localhost:8000/api/stream/statistics
```

**Response:**
```json
{
  "events_generated": 125000,
  "events_sent_successfully": 124500,
  "events_failed": 500,
  "batches_sent": 1250,
  "total_streaming_time": 3600,
  "events_per_second": 34.7,
  "bytes_sent": 62500000,
  "event_type_counts": {
    "receipt_created": 25000,
    "receipt_line_added": 75000,
    "inventory_updated": 15000,
    "customer_entered": 10000
  },
  "error_counts": {
    "connection_timeout": 350,
    "throttling": 150
  },
  "connection_failures": 50,
  "circuit_breaker_trips": 5
}
```

#### Health Check

```bash
curl http://localhost:8000/api/stream/health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T11:30:00Z",
  "checks": {
    "streaming_task": {
      "status": "active",
      "task_status": "running",
      "uptime_seconds": 3600
    },
    "azure_config": {
      "status": "configured",
      "hub_name": "retail-events"
    },
    "statistics": {
      "status": "healthy",
      "events_generated": 125000,
      "events_per_second": 34.7,
      "failure_rate": 0.004
    }
  }
}
```

---

## Key Metrics

### Essential Metrics to Monitor

#### 1. Event Throughput

**Metric**: `events_per_second`
**Target**: 20-50 events/sec (configurable based on needs)
**Alert**: < 10 events/sec when streaming is active

**How to check:**
```bash
curl http://localhost:8000/api/stream/status | jq '.events_per_second'
```

#### 2. Success Rate

**Metric**: `(events_sent_successfully / events_generated) * 100`
**Target**: > 99%
**Alert**: < 95%

**Calculate:**
```bash
curl http://localhost:8000/api/stream/statistics | jq '(.events_sent_successfully / .events_generated) * 100'
```

#### 3. Failure Rate

**Metric**: `(events_failed / events_generated) * 100`
**Target**: < 1%
**Alert**: > 5%

**Calculate:**
```bash
curl http://localhost:8000/api/stream/statistics | jq '(.events_failed / .events_generated) * 100'
```

#### 4. Circuit Breaker Trips

**Metric**: `circuit_breaker_trips`
**Target**: 0
**Alert**: > 3 trips in 1 hour

**How to check:**
```bash
curl http://localhost:8000/api/stream/statistics | jq '.circuit_breaker_trips'
```

#### 5. Connection Failures

**Metric**: `connection_failures`
**Target**: 0
**Alert**: > 10 failures in 10 minutes

**How to check:**
```bash
curl http://localhost:8000/api/stream/statistics | jq '.connection_failures'
```

### Azure Event Hub Metrics

Monitor these in Azure Portal:

- **Incoming Messages**: Should match `events_sent_successfully`
- **Throttled Requests**: Should be 0 or minimal
- **Server Errors**: Should be 0
- **User Errors**: Check for 400/403/413 errors
- **Throughput Units Used**: Monitor for capacity

**Azure CLI:**
```bash
az monitor metrics list \
  --resource "/subscriptions/{subscription-id}/resourceGroups/{rg}/providers/Microsoft.EventHub/namespaces/{namespace}" \
  --metric "IncomingMessages" \
  --start-time 2024-01-15T10:00:00Z \
  --end-time 2024-01-15T11:00:00Z
```

---

## Health Checks

### Application Health

**Endpoint**: `GET /api/stream/health`

**Status Levels:**
- `healthy`: All systems operational
- `degraded`: Some issues but still functional
- `unhealthy`: Critical failures

**Integration with monitoring tools:**

```bash
# Prometheus scrape config
scrape_configs:
  - job_name: 'retail-datagen'
    metrics_path: '/api/stream/health'
    static_configs:
      - targets: ['localhost:8000']
```

### Kubernetes Liveness Probe

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 30
  periodSeconds: 10
  failureThreshold: 3
```

### Kubernetes Readiness Probe

```yaml
readinessProbe:
  httpGet:
    path: /api/stream/health
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 5
  failureThreshold: 2
```

---

## Troubleshooting

### Common Issues & Solutions

#### Issue 1: Connection Failures

**Symptoms:**
- High `connection_failures` count
- Circuit breaker opens immediately
- Events not reaching Azure Event Hub

**Diagnosis:**
```bash
# Test connection
curl -X POST http://localhost:8000/api/stream/test

# Check Azure config
curl http://localhost:8000/api/stream/config | jq '.realtime.azure_connection_string'
```

**Solutions:**

1. **Verify connection string format:**
   ```bash
   # Should include: Endpoint, SharedAccessKeyName, SharedAccessKey
   echo $AZURE_EVENTHUB_CONNECTION_STRING
   ```

2. **Check network connectivity:**
   ```bash
   # Test DNS resolution
   nslookup your-namespace.servicebus.windows.net

   # Test connectivity (port 5671 for AMQP)
   telnet your-namespace.servicebus.windows.net 5671
   ```

3. **Verify Azure credentials:**
   - Check SharedAccessKey is correct
   - Verify policy has Send permissions
   - Check key hasn't expired

4. **Check firewall rules:**
   - Allow outbound connections to `*.servicebus.windows.net`
   - Ports: 5671 (AMQP), 443 (HTTPS)

#### Issue 2: High Failure Rate

**Symptoms:**
- `failure_rate` > 5%
- Many events in dead letter queue
- Throttling errors in statistics

**Diagnosis:**
```bash
# Check error breakdown
curl http://localhost:8000/api/stream/statistics | jq '.error_counts'

# Check DLQ size
curl http://localhost:8000/api/stream/dlq | jq '.count'
```

**Solutions:**

1. **Azure Event Hub throttling:**
   - Check Azure metrics for throttled requests
   - Reduce `burst` size: `curl -X PUT http://localhost:8000/api/stream/config -d '{"burst": 50}'`
   - Increase `emit_interval_ms`: `curl -X PUT http://localhost:8000/api/stream/config -d '{"emit_interval_ms": 1000}'`
   - Upgrade Event Hub tier (Standard → Premium)

2. **Network congestion:**
   - Increase `batch_timeout_ms` to allow more time for sends
   - Reduce `max_batch_size` to smaller batches
   - Check network latency: `ping your-namespace.servicebus.windows.net`

3. **Application issues:**
   - Check application logs for exceptions
   - Verify Python version (requires 3.11+)
   - Check memory usage: `ps aux | grep python`

#### Issue 3: Circuit Breaker Opens Frequently

**Symptoms:**
- `circuit_breaker_trips` increasing
- Streaming stops automatically
- Status shows "error"

**Diagnosis:**
```bash
# Check circuit breaker status
curl http://localhost:8000/api/stream/statistics | jq '.circuit_breaker_trips'

# Check failure pattern
curl http://localhost:8000/api/stream/statistics | jq '.error_counts'
```

**Solutions:**

1. **Increase failure threshold:**
   ```json
   {
     "realtime": {
       "failure_threshold": 10,
       "circuit_breaker_enabled": true
     }
   }
   ```

2. **Increase timeout:**
   ```json
   {
     "realtime": {
       "timeout_seconds": 120
     }
   }
   ```

3. **Fix underlying connection issues** (see Issue 1)

#### Issue 4: Low Throughput

**Symptoms:**
- `events_per_second` < 10
- Slow event generation
- High uptime but low event count

**Diagnosis:**
```bash
# Check current rate
curl http://localhost:8000/api/stream/status | jq '.events_per_second'

# Check configuration
curl http://localhost:8000/api/stream/config | jq '.realtime.emit_interval_ms, .realtime.burst'
```

**Solutions:**

1. **Increase burst size:**
   ```bash
   curl -X PUT http://localhost:8000/api/stream/config \
     -H "Content-Type: application/json" \
     -d '{"burst": 200}'
   ```

2. **Decrease emit interval:**
   ```bash
   curl -X PUT http://localhost:8000/api/stream/config \
     -H "Content-Type: application/json" \
     -d '{"emit_interval_ms": 250}'
   ```

3. **Check system resources:**
   ```bash
   # CPU usage
   top -p $(pgrep -f retail_datagen)

   # Memory usage
   ps aux | grep retail_datagen
   ```

#### Issue 5: Events Not Appearing in Azure

**Symptoms:**
- `events_sent_successfully` shows high counts
- Azure Event Hub shows no incoming messages
- No errors reported

**Diagnosis:**
```bash
# Verify Event Hub name
curl http://localhost:8000/api/stream/config | jq '.stream.hub'

# Check recent events
curl http://localhost:8000/api/stream/events/recent?limit=5
```

**Solutions:**

1. **Verify Event Hub name matches:**
   - Check `stream.hub` in config
   - Verify Event Hub exists in Azure Portal
   - Check EntityPath in connection string

2. **Check Azure Event Hub metrics:**
   - Navigate to Azure Portal → Event Hub → Metrics
   - Look at "Incoming Messages" (should match events_sent)
   - Check "Throttled Requests" (should be 0)

3. **Verify connection string:**
   ```bash
   # Connection string should include EntityPath or hub name in config
   echo $AZURE_EVENTHUB_CONNECTION_STRING | grep EntityPath
   ```

---

## Circuit Breaker

### How It Works

The circuit breaker prevents cascading failures by stopping send attempts when error rate is too high.

**States:**
- **Closed**: Normal operation, all events sent
- **Open**: Too many failures, reject all sends immediately
- **Half-Open**: Testing if service recovered, allow limited sends

**Configuration:**
```json
{
  "realtime": {
    "circuit_breaker_enabled": true,
    "failure_threshold": 5,
    "timeout_seconds": 60,
    "half_open_max_calls": 3
  }
}
```

**Parameters:**
- `failure_threshold`: Consecutive failures before opening (default: 5)
- `timeout_seconds`: Time to wait before trying half-open (default: 60)
- `half_open_max_calls`: Test calls in half-open state (default: 3)

### Monitoring Circuit Breaker

```bash
# Check trips
curl http://localhost:8000/api/stream/statistics | jq '.circuit_breaker_trips'

# Current state (check failure pattern)
curl http://localhost:8000/api/stream/statistics | jq '.error_counts'
```

### Manual Reset

Stop and restart streaming to reset circuit breaker:

```bash
# Stop streaming
curl -X POST http://localhost:8000/api/stream/stop

# Fix underlying issues

# Restart streaming
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{"emit_interval_ms": 1000, "burst": 50}'
```

---

## Dead Letter Queue

### Overview

Failed events are stored in a dead letter queue (DLQ) for analysis and retry.

**Maximum size**: 10,000 events (configurable)
**Retention**: In-memory only, cleared on restart

### View DLQ

```bash
curl http://localhost:8000/api/stream/dlq
```

**Response:**
```json
{
  "events": [
    {
      "event": { /* event envelope */ },
      "error": "Connection timeout",
      "timestamp": "2024-01-15T11:30:00Z",
      "retry_count": 3
    }
  ],
  "count": 15
}
```

### Analyze DLQ

```bash
# Count events by error type
curl http://localhost:8000/api/stream/dlq | jq '.events | group_by(.error) | map({error: .[0].error, count: length})'
```

### Clear DLQ

```bash
curl -X DELETE http://localhost:8000/api/stream/dlq
```

### Retry Failed Events

**Manual retry** (future feature):
```bash
# Coming soon
curl -X POST http://localhost:8000/api/stream/dlq/retry
```

**Current workaround**: Stop and restart streaming, events will be regenerated.

---

## Performance Tuning

### Configuration Profiles

#### Low Throughput (Development)

```json
{
  "realtime": {
    "emit_interval_ms": 2000,
    "burst": 50,
    "max_batch_size": 128
  }
}
```

**Throughput**: ~25 events/sec

#### Medium Throughput (Default)

```json
{
  "realtime": {
    "emit_interval_ms": 500,
    "burst": 100,
    "max_batch_size": 256
  }
}
```

**Throughput**: ~200 events/sec

#### High Throughput (Production)

```json
{
  "realtime": {
    "emit_interval_ms": 100,
    "burst": 500,
    "max_batch_size": 1000
  }
}
```

**Throughput**: ~5000 events/sec

**Warning**: High throughput may hit Event Hub throttling limits. Monitor Azure metrics.

### Azure Event Hub Limits

**Standard Tier:**
- Ingress: 1 MB/sec or 1000 events/sec
- Egress: 2 MB/sec
- Throughput Units: 1-20 (auto-inflate available)

**Premium Tier:**
- Ingress: Higher throughput
- Dedicated capacity
- Better latency

**Calculate event size:**
```bash
# Average event size
curl http://localhost:8000/api/stream/statistics | jq '(.bytes_sent / .events_sent_successfully) / 1024'
```

### Batch Optimization

**Rule of thumb:**
- Small events (<1KB): `max_batch_size: 500-1000`
- Medium events (1-10KB): `max_batch_size: 100-500`
- Large events (>10KB): `max_batch_size: 50-100`

**Test batch performance:**
```bash
# Monitor batches sent
watch -n 1 'curl -s http://localhost:8000/api/stream/statistics | jq ".batches_sent, .events_sent_successfully"'
```

---

## Production Deployment

### Recommended Configuration

```json
{
  "realtime": {
    "emit_interval_ms": 1000,
    "burst": 100,
    "max_batch_size": 256,
    "batch_timeout_ms": 1000,
    "retry_attempts": 3,
    "backoff_multiplier": 2.0,
    "circuit_breaker_enabled": true,
    "monitoring_interval": 30,
    "max_buffer_size": 10000,
    "enable_dead_letter_queue": true
  }
}
```

### Environment Variables

```bash
# Required
export AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://..."

# Optional
export LOG_LEVEL="INFO"
export ALLOWED_ORIGINS="https://your-domain.com"
export MAX_WORKERS="4"
```

### Docker Deployment

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -e .

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "src.retail_datagen.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Run container:**
```bash
docker build -t retail-datagen .

docker run -d \
  --name retail-datagen \
  -p 8000:8000 \
  -e AZURE_EVENTHUB_CONNECTION_STRING="$AZURE_EVENTHUB_CONNECTION_STRING" \
  retail-datagen
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: retail-datagen
spec:
  replicas: 1
  selector:
    matchLabels:
      app: retail-datagen
  template:
    metadata:
      labels:
        app: retail-datagen
    spec:
      containers:
      - name: retail-datagen
        image: retail-datagen:latest
        ports:
        - containerPort: 8000
        env:
        - name: AZURE_EVENTHUB_CONNECTION_STRING
          valueFrom:
            secretKeyRef:
              name: azure-credentials
              key: connection-string
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /api/stream/health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
```

### Scaling Considerations

**Single Instance:**
- Simpler deployment
- State managed in-memory
- Suitable for most use cases

**Multiple Instances:**
- Not currently supported (stateful application)
- Future: Shared state via Redis/Database
- Event Hub partitioning required

---

## Best Practices

### Operations

1. **Start Small, Scale Up**
   - Begin with `burst: 50`, `emit_interval_ms: 2000`
   - Monitor success rate and Azure metrics
   - Gradually increase throughput

2. **Monitor Continuously**
   - Set up alerts for key metrics
   - Check DLQ regularly for patterns
   - Monitor Azure Event Hub metrics

3. **Enable Circuit Breaker**
   - Prevents cascading failures
   - Allows automatic recovery
   - Protects Azure Event Hub from overload

4. **Use Dead Letter Queue**
   - Analyze failed events for patterns
   - Identify systemic issues
   - Improve event generation logic

5. **Test Connection Regularly**
   - Run `/stream/test` endpoint periodically
   - Monitor response times
   - Detect configuration issues early

### Security

1. **Credential Management**
   - Use environment variables (never hardcode)
   - Rotate keys quarterly
   - Use Azure Key Vault for production

2. **Network Security**
   - Enable TLS 1.2+ (default)
   - Use private endpoints if available
   - Restrict firewall rules to necessary ports

3. **Access Control**
   - Use least privilege access policies
   - Separate read/write permissions
   - Audit access logs regularly

### Maintenance

1. **Regular Updates**
   - Keep Azure SDK updated
   - Monitor for security patches
   - Test updates in non-production first

2. **Log Management**
   - Rotate logs regularly
   - Set appropriate log levels (INFO for prod)
   - Ship logs to centralized logging system

3. **Backup State**
   - Export generation state periodically
   - Store configuration in version control
   - Document custom configurations

---

## Alerting Examples

### Prometheus Rules

```yaml
groups:
  - name: retail_datagen_streaming
    rules:
      - alert: StreamingHighFailureRate
        expr: (streaming_events_failed / streaming_events_generated) > 0.05
        for: 5m
        annotations:
          summary: "High streaming failure rate"
          description: "Failure rate is {{ $value }}%"

      - alert: StreamingCircuitBreakerOpen
        expr: streaming_circuit_breaker_trips > 3
        for: 10m
        annotations:
          summary: "Circuit breaker tripped multiple times"

      - alert: StreamingLowThroughput
        expr: streaming_events_per_second < 10
        for: 10m
        annotations:
          summary: "Streaming throughput is low"
          description: "Only {{ $value }} events/sec"
```

### Bash Monitoring Script

```bash
#!/bin/bash

# Monitor streaming health
while true; do
  STATUS=$(curl -s http://localhost:8000/api/stream/health | jq -r '.status')
  FAILURE_RATE=$(curl -s http://localhost:8000/api/stream/statistics | jq '(.events_failed / .events_generated) * 100')

  if [ "$STATUS" != "healthy" ]; then
    echo "ALERT: Streaming health is $STATUS"
  fi

  if (( $(echo "$FAILURE_RATE > 5" | bc -l) )); then
    echo "ALERT: Failure rate is ${FAILURE_RATE}%"
  fi

  sleep 60
done
```

---

## Support & Debugging

### Enable Debug Logging

```bash
export LOG_LEVEL="DEBUG"
python -m retail_datagen.main
```

### View Logs

```bash
# Follow application logs
tail -f retail_datagen.log

# Search for errors
grep -i error retail_datagen.log

# Search for specific event type
grep "receipt_created" retail_datagen.log
```

### Generate Diagnostic Report

```bash
#!/bin/bash
echo "=== Streaming Diagnostic Report ===" > diagnostic.txt
echo "Generated: $(date)" >> diagnostic.txt
echo "" >> diagnostic.txt

echo "=== Status ===" >> diagnostic.txt
curl -s http://localhost:8000/api/stream/status >> diagnostic.txt
echo "" >> diagnostic.txt

echo "=== Statistics ===" >> diagnostic.txt
curl -s http://localhost:8000/api/stream/statistics >> diagnostic.txt
echo "" >> diagnostic.txt

echo "=== Health ===" >> diagnostic.txt
curl -s http://localhost:8000/api/stream/health >> diagnostic.txt
echo "" >> diagnostic.txt

echo "=== DLQ ===" >> diagnostic.txt
curl -s http://localhost:8000/api/stream/dlq >> diagnostic.txt

echo "Diagnostic report saved to diagnostic.txt"
```

---

## Next Steps

- **Setup**: See [STREAMING_SETUP.md](STREAMING_SETUP.md) for initial configuration
- **API Reference**: See [STREAMING_API.md](STREAMING_API.md) for endpoint documentation
- **Security**: See [CREDENTIALS.md](CREDENTIALS.md) for credential management
- **Streaming Outbox**

  The outbox holds pending events for the realtime drain. It should only contain the daily slice generated by the outbox streaming path — not the entire dataset.

  - `GET /api/stream/outbox/status` — counts by status and oldest pending timestamp
  - `POST /api/stream/outbox/drain` — drains pending items with pacing
  - `DELETE /api/stream/outbox/clear` — fast reset (drop/recreate)

  Historical generation does not populate the outbox; only `/api/stream/start` (outbox mode) adds new pending items when it generates the next day.
