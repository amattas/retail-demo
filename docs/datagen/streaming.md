# Real-Time Event Streaming System

The retail data generator includes a comprehensive real-time event streaming system that generates and streams synthetic retail events to Azure Event Hub. This system simulates live retail operations with realistic patterns, correlations, and timing.

## 🚀 Quick Start

```python
import asyncio
from datetime import timedelta
from retail_datagen.config.models import RetailConfig
from retail_datagen.streaming import EventStreamer

# Load configuration
config = RetailConfig.from_file("config.json")

# Create and initialize streamer
streamer = EventStreamer(config, azure_connection_string="your_connection_string")

# Stream events for 5 minutes
async def stream_events():
    async with streamer.streaming_session(duration=timedelta(minutes=5)):
        # Streaming runs in background
        stats = await streamer.get_statistics()
        print(f"Generated {stats['events_generated']} events")

asyncio.run(stream_events())
```

## 📋 System Architecture

### Core Components

1. **EventStreamer** - Main orchestration engine
2. **EventFactory** - Generates realistic retail events
3. **AzureEventHubClient** - Azure integration with resilience
4. **Event Schemas** - Type-safe event definitions

### Event Flow

```
EventFactory → EventBuffer → BatchProcessor → AzureEventHub
     ↓              ↓             ↓              ↓
  Statistics → DeadLetterQueue → CircuitBreaker → Monitoring
```

## 🎯 Event Types

The system generates these retail event types:

### Transaction Events
- `receipt_created` - New customer purchases
- `receipt_line_added` - Individual items scanned
- `payment_processed` - Payment completion

### Inventory Events  
- `inventory_updated` - Stock level changes
- `stockout_detected` - Out-of-stock conditions
- `reorder_triggered` - Automatic reordering

### Customer Events
- `customer_entered` - Foot traffic entry
- `customer_zone_changed` - Movement between store zones
- `ble_ping_detected` - Bluetooth beacon interactions

### Operational Events
- `truck_arrived` - Delivery events
- `truck_departed` - Shipment completion
- `store_opened` - Daily operations start
- `store_closed` - Daily operations end

### Marketing Events
- `ad_impression` - Marketing campaign exposures
- `promotion_applied` - Discount code usage

## 📦 Event Envelope Format

All events use a standardized envelope:

```json
{
  "event_type": "receipt_created",
  "payload": { /* event-specific data */ },
  "trace_id": "TR_1704067200_00001",
  "ingest_timestamp": "2024-01-01T12:00:00.000Z",
  "schema_version": "1.0",
  "source": "retail-datagen",
  "correlation_id": "optional_correlation_id",
  "partition_key": "store_123"
}
```

## ⚙️ Configuration

### Basic Configuration (config.json)

```json
{
  "realtime": {
    "emit_interval_ms": 500,
    "burst": 100,
    "azure_connection_string": "Endpoint=sb://...",
    "max_batch_size": 256,
    "batch_timeout_ms": 1000,
    "retry_attempts": 3,
    "backoff_multiplier": 2.0,
    "circuit_breaker_enabled": true,
    "monitoring_interval": 30,
    "max_buffer_size": 10000,
    "enable_dead_letter_queue": true
  },
  "stream": {
    "hub": "retail-events"
  }
}
```

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `emit_interval_ms` | Time between event bursts | 500 |
| `burst` | Events per burst | 100 |
| `max_batch_size` | Events per Azure batch | 256 |
| `batch_timeout_ms` | Max batch wait time | 1000 |
| `retry_attempts` | Retry count for failures | 3 |
| `circuit_breaker_enabled` | Enable failure protection | true |
| `monitoring_interval` | Stats update frequency | 30 |

## 🔧 Advanced Usage

### Custom Event Generation

```python
from retail_datagen.streaming import EventFactory, EventType

factory = EventFactory(stores, customers, products, dcs, seed=42)

# Generate specific event types
receipt_event = factory.generate_event(EventType.RECEIPT_CREATED, timestamp)

# Generate mixed events with custom weights
event_weights = {
    EventType.RECEIPT_CREATED: 0.3,
    EventType.CUSTOMER_ENTERED: 0.4,
    EventType.INVENTORY_UPDATED: 0.3
}
events = factory.generate_mixed_events(100, timestamp, event_weights)
```

### Event Hooks and Monitoring

```python
streamer = EventStreamer(config)

# Add event hooks
def log_high_value_receipts(event):
    if event.event_type == EventType.RECEIPT_CREATED:
        if event.payload.get('total', 0) > 100:
            print(f"High-value receipt: ${event.payload['total']}")

def log_batch_stats(events):
    print(f"Sent batch of {len(events)} events")

def handle_errors(error, context):
    print(f"Error in {context}: {error}")

streamer.add_event_generated_hook(log_high_value_receipts)
streamer.add_batch_sent_hook(log_batch_stats)
streamer.add_error_hook(handle_errors)
```

### Health Monitoring

```python
# Get real-time statistics
stats = await streamer.get_statistics()
print(f"Events/sec: {stats['events_per_second']}")
print(f"Success rate: {stats['events_sent_successfully'] / stats['events_generated']}")

# Get health status
health = await streamer.get_health_status()
print(f"System healthy: {health['overall_healthy']}")
print(f"Azure connection: {health['components']['azure_event_hub']['healthy']}")
```

## 🛡️ Resilience Features

### Circuit Breaker Pattern
Automatically opens circuit after 5 consecutive failures, preventing cascade failures.

### Retry Logic
Exponential backoff with configurable retry attempts for transient failures.

### Dead Letter Queue
Failed events are queued for later processing or analysis.

### Buffer Management
Internal buffering prevents data loss during temporary outages.

### Graceful Shutdown
Proper cleanup ensures no events are lost during shutdown.

## 📊 Performance Characteristics

### Throughput
- **Target**: 1000+ events/second
- **Burst capacity**: 10,000 events/burst
- **Azure batching**: Up to 256 events/batch

### Latency
- **Event generation**: <1ms per event
- **Azure delivery**: 100-500ms (network dependent)
- **End-to-end**: <1 second typical

### Resource Usage
- **Memory**: ~50MB base + 1KB per buffered event
- **CPU**: 5-10% of single core at 1000 events/sec
- **Network**: ~100KB/sec at 1000 events/sec

## 🔍 Realistic Event Patterns

### Time-based Distribution
- **Business hours**: Peak activity 9 AM - 8 PM
- **Off-hours**: Minimal activity with maintenance events
- **Weekends**: Reduced but consistent activity

### Store-based Variation
- **Large stores**: More frequent, higher-value transactions
- **Small stores**: Fewer, smaller transactions
- **Geographic factors**: Regional shopping patterns

### Event Correlations
- Receipt creation triggers inventory updates
- Low inventory triggers reorder events
- Customer entry correlates with BLE pings
- Marketing impressions lead to promotional purchases

### Seasonal Effects
- Holiday shopping spikes
- Weather-related patterns
- Promotional calendar alignment

## 🧪 Testing

### Unit Tests
```bash
python -m pytest tests/unit/test_streaming.py
```

### Integration Tests
```bash
python -m pytest tests/integration/test_streaming.py
```

### Manual Testing
```bash
# Test without Azure connection
python test_streaming_implementation.py

# Example usage
python example_streaming_usage.py
```

## 🚨 Troubleshooting

### Common Issues

**Connection Failures**
- Verify Azure connection string
- Check network connectivity
- Validate Event Hub name

**High Memory Usage**
- Reduce `max_buffer_size`
- Increase `batch_timeout_ms`
- Check for buffer flush failures

**Low Throughput**
- Decrease `emit_interval_ms`
- Increase `burst` size
- Optimize `max_batch_size`

**Event Delivery Failures**
- Check Azure Event Hub quotas
- Verify authentication
- Monitor circuit breaker state

### Debug Mode
Enable debug logging for detailed troubleshooting:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Now run streaming system
```

## 📈 Monitoring and Metrics

### Key Metrics
- `events_generated` - Total events created
- `events_sent_successfully` - Events delivered to Azure
- `events_failed` - Failed delivery attempts
- `events_per_second` - Current throughput
- `batches_sent` - Azure batches delivered
- `connection_failures` - Azure connectivity issues

### Health Checks
- Azure Event Hub connectivity
- Circuit breaker state
- Buffer utilization
- Error rates
- Resource usage

## 🔐 Security Considerations

### Data Privacy
- All generated data is synthetic
- No real customer information
- GDPR/CCPA compliant by design

### Azure Integration
- Connection string encryption recommended
- Network security group restrictions
- Azure Active Directory integration supported

### Audit Logging
- All events include trace IDs
- Comprehensive error logging
- Security event monitoring

## 🎯 Production Deployment

### Azure Event Hub Setup
1. Create Event Hub namespace
2. Create retail-events hub
3. Configure access policies
4. Set up monitoring alerts

### Configuration Management
- Store connection strings securely
- Use environment variables
- Implement configuration validation

### Monitoring Setup
- Azure Monitor integration
- Custom metric dashboards
- Alert rules for failures

### Scaling Considerations
- Multiple streamer instances
- Event Hub partition strategy
- Load balancing across regions

## 📚 API Reference

### EventStreamer Class

#### Methods
- `initialize()` - Initialize streaming components
- `start(duration)` - Begin streaming
- `stop()` - Graceful shutdown
- `get_statistics()` - Current metrics
- `get_health_status()` - Health information

#### Context Manager
```python
async with streamer.streaming_session(duration):
    # Streaming active
    pass
# Automatically cleaned up
```

### EventFactory Class

#### Methods
- `generate_event(type, timestamp)` - Single event
- `generate_mixed_events(count, timestamp, weights)` - Mixed batch
- `should_generate_event(type, timestamp)` - Pattern check

### AzureEventHubClient Class

#### Methods
- `connect()` - Establish connection
- `disconnect()` - Close connection
- `send_events(events)` - Batch send
- `health_check()` - Connection status
- `get_statistics()` - Client metrics

For complete API documentation, see the inline docstrings and type hints in the source code.