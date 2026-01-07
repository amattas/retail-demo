"""
Configuration and data structures for event streaming.

This module provides configuration models and data structures used
throughout the event streaming system.
"""

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps
from typing import Any

from ...config.models import RetailConfig
from ...shared.metrics import event_generation_duration_seconds
from ..schemas import EventEnvelope, EventType


def event_generation_pipeline(method: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to measure and post-process event generation."""

    @wraps(method)
    async def wrapper(self, *args: Any, **kwargs: Any):
        events: list[EventEnvelope] = []
        try:
            with event_generation_duration_seconds.time():
                events = await method(self, *args, **kwargs)
        except Exception as exc:  # pragma: no cover - defensive logging
            self.log.error(
                "Error generating event burst",
                session_id=self._session_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return []

        self._process_generated_events(events)
        return events

    return wrapper


@dataclass
class StreamingStatistics:
    """Statistics and metrics for event streaming."""

    events_generated: int = 0
    events_sent_successfully: int = 0
    events_failed: int = 0
    batches_sent: int = 0
    total_streaming_time: float = 0.0
    events_per_second: float = 0.0
    bytes_sent: int = 0
    last_event_time: datetime | None = None
    event_type_counts: dict[EventType, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    error_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    connection_failures: int = 0
    circuit_breaker_trips: int = 0


@dataclass
class DLQEntry:
    """Dead letter queue entry with metadata."""

    event: EventEnvelope
    error_message: str
    error_category: str
    error_severity: str
    timestamp: str
    retry_count: int = 0
    last_retry_timestamp: str | None = None


@dataclass
class StreamingConfig:
    """Extended configuration for streaming operations."""

    emit_interval_ms: int = 500
    burst: int = 100
    azure_connection_string: str | None = None
    hub_name: str = "retail-events"
    max_batch_size: int = 256
    batch_timeout_ms: int = 1000
    retry_attempts: int = 3
    backoff_multiplier: float = 2.0
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: int = 60
    monitoring_interval: int = 30  # seconds
    max_buffer_size: int = 10000
    enable_dead_letter_queue: bool = True
    dlq_max_size: int = 10000
    dlq_retry_enabled: bool = True
    dlq_retry_max_attempts: int = 3

    @classmethod
    def from_retail_config(cls, config: RetailConfig) -> "StreamingConfig":
        """Create streaming config from main retail config."""
        streaming_config = cls()
        streaming_config.emit_interval_ms = config.realtime.emit_interval_ms
        streaming_config.burst = config.realtime.burst
        streaming_config.hub_name = config.stream.hub

        # Get connection string using secure method (env var, Key Vault, or config)
        if hasattr(config.realtime, "get_connection_string"):
            streaming_config.azure_connection_string = (
                config.realtime.get_connection_string()
            )
        elif hasattr(config.realtime, "azure_connection_string"):
            streaming_config.azure_connection_string = (
                config.realtime.azure_connection_string
            )

        if hasattr(config.realtime, "max_batch_size"):
            streaming_config.max_batch_size = config.realtime.max_batch_size
        if hasattr(config.realtime, "batch_timeout_ms"):
            streaming_config.batch_timeout_ms = config.realtime.batch_timeout_ms
        if hasattr(config.realtime, "retry_attempts"):
            streaming_config.retry_attempts = config.realtime.retry_attempts
        if hasattr(config.realtime, "backoff_multiplier"):
            streaming_config.backoff_multiplier = config.realtime.backoff_multiplier

        # Circuit breaker fields have defaults in RealtimeConfig, so hasattr() is unnecessary
        streaming_config.circuit_breaker_enabled = config.realtime.circuit_breaker_enabled
        streaming_config.circuit_breaker_failure_threshold = (
            config.realtime.circuit_breaker_failure_threshold
        )
        streaming_config.circuit_breaker_recovery_timeout = (
            config.realtime.circuit_breaker_recovery_timeout
        )
        if hasattr(config.realtime, "monitoring_interval"):
            streaming_config.monitoring_interval = config.realtime.monitoring_interval

        # DLQ configuration
        if hasattr(config.realtime, "dlq_max_size"):
            streaming_config.dlq_max_size = config.realtime.dlq_max_size
        if hasattr(config.realtime, "dlq_retry_enabled"):
            streaming_config.dlq_retry_enabled = config.realtime.dlq_retry_enabled
        if hasattr(config.realtime, "dlq_retry_max_attempts"):
            streaming_config.dlq_retry_max_attempts = (
                config.realtime.dlq_retry_max_attempts
            )

        return streaming_config
