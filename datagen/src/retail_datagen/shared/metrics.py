"""Prometheus metrics for streaming system."""
import time

from prometheus_client import REGISTRY, Counter, Gauge, Histogram, Summary


def _get_or_create_metric(metric_class, name: str, doc: str, labelnames=None, **kwargs):
    """Get existing metric or create new one to avoid duplication errors in tests."""
    # Check if already exists
    for collector in list(REGISTRY._collector_to_names.keys()):
        if hasattr(collector, "_name") and collector._name == name:
            return collector
    # Create new
    try:
        if labelnames is not None:
            kwargs["labelnames"] = labelnames
        return metric_class(name, doc, registry=REGISTRY, **kwargs)
    except ValueError as e:
        if "Duplicated timeseries" in str(e):
            # Race condition - try to find it again
            for collector in list(REGISTRY._collector_to_names.keys()):
                if hasattr(collector, "_name") and collector._name == name:
                    return collector
        raise


# Event metrics
events_generated_total = _get_or_create_metric(
    Counter,
    "streaming_events_generated_total",
    "Total number of events generated",
    ["event_type"],
)

events_sent_total = _get_or_create_metric(
    Counter,
    "streaming_events_sent_total",
    "Total number of events successfully sent to Event Hub",
    ["event_type"],
)

events_failed_total = _get_or_create_metric(
    Counter,
    "streaming_events_failed_total",
    "Total number of events that failed to send",
    ["event_type", "error_type"],
)

# Batch metrics
batches_sent_total = _get_or_create_metric(
    Counter,
    "streaming_batches_sent_total",
    "Total number of batches sent to Event Hub",
)

batches_failed_total = _get_or_create_metric(
    Counter,
    "streaming_batches_failed_total",
    "Total number of batches that failed to send",
    ["error_type"],
)

batch_size_bytes = _get_or_create_metric(
    Histogram,
    "streaming_batch_size_bytes",
    "Size of event batches in bytes",
    buckets=[1024, 10240, 51200, 102400, 256000, 512000, 1048576],
)

batch_send_duration_seconds = _get_or_create_metric(
    Histogram,
    "streaming_batch_send_duration_seconds",
    "Time taken to send a batch to Event Hub",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

# Streaming state metrics
streaming_active = _get_or_create_metric(
    Gauge, "streaming_active", "Whether streaming is currently active (1) or not (0)"
)

streaming_paused = _get_or_create_metric(
    Gauge, "streaming_paused", "Whether streaming is currently paused (1) or not (0)"
)

# Circuit breaker metrics
circuit_breaker_state = _get_or_create_metric(
    Gauge,
    "circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half-open)",
)

circuit_breaker_failures = _get_or_create_metric(
    Counter, "circuit_breaker_failures_total", "Total number of circuit breaker failures"
)

circuit_breaker_trips = _get_or_create_metric(
    Counter,
    "circuit_breaker_trips_total",
    "Total number of times circuit breaker has opened",
)

# Dead letter queue metrics
dlq_size = _get_or_create_metric(
    Gauge, "dlq_size", "Current number of events in dead letter queue"
)

dlq_events_added_total = _get_or_create_metric(
    Counter, "dlq_events_added_total", "Total number of events added to DLQ"
)

# Connection metrics
eventhub_connected = _get_or_create_metric(
    Gauge, "eventhub_connected", "Whether connected to Event Hub (1) or not (0)"
)

eventhub_connection_failures_total = _get_or_create_metric(
    Counter,
    "eventhub_connection_failures_total",
    "Total number of Event Hub connection failures",
)

# Performance metrics
event_generation_duration_seconds = _get_or_create_metric(
    Summary, "event_generation_duration_seconds", "Time taken to generate events"
)

streaming_uptime_seconds = _get_or_create_metric(
    Gauge, "streaming_uptime_seconds", "How long streaming has been active (seconds)"
)

# Throughput metrics
events_per_second = _get_or_create_metric(
    Gauge, "streaming_events_per_second", "Current event generation rate (events/sec)"
)

bytes_per_second = _get_or_create_metric(
    Gauge, "streaming_bytes_per_second", "Current throughput in bytes/sec"
)


class MetricsCollector:
    """Helper class for collecting and updating metrics."""

    def __init__(self):
        self.start_time = None

    def start_streaming(self):
        """Mark streaming as started."""
        streaming_active.set(1)
        self.start_time = time.time()

    def stop_streaming(self):
        """Mark streaming as stopped."""
        streaming_active.set(0)
        streaming_paused.set(0)
        self.start_time = None

    def pause_streaming(self):
        """Mark streaming as paused."""
        streaming_paused.set(1)

    def resume_streaming(self):
        """Mark streaming as resumed."""
        streaming_paused.set(0)

    def record_event_generated(self, event_type: str):
        """Record an event generation."""
        events_generated_total.labels(event_type=event_type).inc()

    def record_event_sent(self, event_type: str):
        """Record a successful event send."""
        events_sent_total.labels(event_type=event_type).inc()

    def record_event_failed(self, event_type: str, error_type: str):
        """Record a failed event send."""
        events_failed_total.labels(event_type=event_type, error_type=error_type).inc()

    def record_batch_sent(self, size_bytes: int, duration: float):
        """Record a successful batch send."""
        batches_sent_total.inc()
        batch_size_bytes.observe(size_bytes)
        batch_send_duration_seconds.observe(duration)

    def record_batch_failed(self, error_type: str):
        """Record a failed batch send."""
        batches_failed_total.labels(error_type=error_type).inc()

    def update_circuit_breaker_state(self, state: str):
        """Update circuit breaker state metric."""
        state_map = {"CLOSED": 0, "OPEN": 1, "HALF_OPEN": 2}
        circuit_breaker_state.set(state_map.get(state, 0))

    def record_circuit_breaker_failure(self):
        """Record a circuit breaker failure."""
        circuit_breaker_failures.inc()

    def record_circuit_breaker_trip(self):
        """Record a circuit breaker trip."""
        circuit_breaker_trips.inc()

    def update_dlq_size(self, size: int):
        """Update DLQ size metric."""
        dlq_size.set(size)

    def record_dlq_event_added(self):
        """Record event added to DLQ."""
        dlq_events_added_total.inc()

    def update_connection_status(self, connected: bool):
        """Update Event Hub connection status."""
        eventhub_connected.set(1 if connected else 0)

    def record_connection_failure(self):
        """Record Event Hub connection failure."""
        eventhub_connection_failures_total.inc()

    def update_throughput(
        self, events_count: int, bytes_count: int, duration: float
    ):
        """Update throughput metrics."""
        if duration > 0:
            events_per_second.set(events_count / duration)
            bytes_per_second.set(bytes_count / duration)

    def update_uptime(self):
        """Update streaming uptime."""
        if self.start_time:
            uptime = time.time() - self.start_time
            streaming_uptime_seconds.set(uptime)


# Global metrics collector instance
metrics_collector = MetricsCollector()
