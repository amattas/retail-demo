"""
Unit tests for DLQ error recovery and error classification.
"""
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from retail_datagen.config.models import RetailConfig
from retail_datagen.streaming.errors import (
    ErrorCategory,
    ErrorSeverity,
    classify_error,
)
from retail_datagen.streaming.event_streaming import DLQEntry, EventStreamer
from retail_datagen.streaming.schemas import EventEnvelope, EventType


class TestErrorClassification:
    """Test error classification system."""

    def test_classify_network_error(self):
        """Test classification of network errors."""
        exception = TimeoutError("Connection timeout")
        error = classify_error(exception)

        assert error.severity == ErrorSeverity.TRANSIENT
        assert error.category == ErrorCategory.NETWORK
        assert error.retryable is True
        assert "Network error" in error.message

    def test_classify_authentication_error(self):
        """Test classification of authentication errors."""
        exception = PermissionError("Unauthorized access")
        error = classify_error(exception)

        assert error.severity == ErrorSeverity.PERMANENT
        assert error.category == ErrorCategory.AUTHENTICATION
        assert error.retryable is False
        assert "Authentication error" in error.message

    def test_classify_throttling_error(self):
        """Test classification of throttling errors."""
        exception = Exception("Rate limit exceeded")
        error = classify_error(exception)

        assert error.severity == ErrorSeverity.TRANSIENT
        assert error.category == ErrorCategory.THROTTLING
        assert error.retryable is True
        assert "Throttling error" in error.message

    def test_classify_serialization_error(self):
        """Test classification of serialization errors."""
        exception = ValueError("JSON encoding error")
        error = classify_error(exception)

        assert error.severity == ErrorSeverity.PERMANENT
        assert error.category == ErrorCategory.SERIALIZATION
        assert error.retryable is False
        assert "Serialization error" in error.message

    def test_classify_unknown_error(self):
        """Test classification of unknown errors."""
        exception = RuntimeError("Unknown runtime error")
        error = classify_error(exception)

        assert error.severity == ErrorSeverity.TRANSIENT
        assert error.category == ErrorCategory.UNKNOWN
        assert error.retryable is True
        assert "Unknown error" in error.message


class TestDLQEntry:
    """Test DLQEntry dataclass."""

    def test_dlq_entry_creation(self):
        """Test DLQEntry creation with all fields."""
        event = EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={"test": "data"},
            trace_id="test-trace-123",
            ingest_timestamp=datetime.now(UTC),
        )

        entry = DLQEntry(
            event=event,
            error_message="Test error",
            error_category="network",
            error_severity="transient",
            timestamp="2025-01-01T00:00:00Z",
            retry_count=0,
        )

        assert entry.event == event
        assert entry.error_message == "Test error"
        assert entry.error_category == "network"
        assert entry.error_severity == "transient"
        assert entry.retry_count == 0
        assert entry.last_retry_timestamp is None

    def test_dlq_entry_with_retry(self):
        """Test DLQEntry with retry metadata."""
        event = EventEnvelope(
            event_type=EventType.INVENTORY_UPDATED,
            payload={"store_id": 1, "product_id": 100},
            trace_id="test-trace-456",
            ingest_timestamp=datetime.now(UTC),
        )

        entry = DLQEntry(
            event=event,
            error_message="Retry test",
            error_category="throttling",
            error_severity="transient",
            timestamp="2025-01-01T00:00:00Z",
            retry_count=2,
            last_retry_timestamp="2025-01-01T00:05:00Z",
        )

        assert entry.retry_count == 2
        assert entry.last_retry_timestamp == "2025-01-01T00:05:00Z"


@pytest.fixture
def mock_config():
    """Create a mock RetailConfig for testing."""
    config_dict = {
        "seed": 42,
        "volume": {"stores": 2, "dcs": 1, "total_customers": 10, "customers_per_day": 5, "items_per_ticket_mean": 4.2},
        "paths": {
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
        "historical": {"start_date": "2024-01-01"},
        "realtime": {
            "emit_interval_ms": 500,
            "burst": 10,
            "azure_connection_string": "mock://test",
            "dlq_max_size": 100,
            "dlq_retry_enabled": True,
            "dlq_retry_max_attempts": 3,
        },
        "stream": {"hub": "test-hub"},
    }
    return RetailConfig(**config_dict)


@pytest.fixture
def event_streamer(mock_config):
    """Create an EventStreamer instance for testing."""
    streamer = EventStreamer(
        config=mock_config,
        stores=[],
        customers=[],
        products=[],
        distribution_centers=[],
    )
    return streamer


class TestEventStreamerDLQ:
    """Test EventStreamer DLQ functionality."""

    def test_get_dlq_summary_empty(self, event_streamer):
        """Test DLQ summary with empty queue."""
        summary = event_streamer.get_dlq_summary()

        assert summary["size"] == 0
        assert summary["by_category"] == {}
        assert summary["by_severity"] == {}
        assert summary["oldest_entry"] is None
        assert summary["newest_entry"] is None

    @pytest.mark.asyncio
    async def test_retry_dlq_events_max_retries_exceeded(self, event_streamer):
        """Test that events exceeding max retries are not retried."""
        # Add event with retry count already at max
        event = EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={"test": "max_retries"},
            trace_id="test-max-retries",
            ingest_timestamp=datetime.now(UTC),
        )

        dlq_entry = DLQEntry(
            event=event,
            error_message="Max retries test",
            error_category="network",
            error_severity="transient",
            timestamp=datetime.now(UTC).isoformat(),
            retry_count=5,  # Already exceeded
        )
        event_streamer._dlq.append(dlq_entry)

        event_streamer._azure_client = Mock()
        event_streamer._azure_client.send_events = AsyncMock(return_value=True)

        result = await event_streamer.retry_dlq_events(max_retries=3)

        assert result["total_attempted"] == 1
        assert result["succeeded"] == 0
        assert result["failed"] == 1
        assert result["still_in_dlq"] == 1
        # Verify send was not called
        event_streamer._azure_client.send_events.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_dlq_events_empty_queue(self, event_streamer):
        """Test retry with empty DLQ."""
        result = await event_streamer.retry_dlq_events()

        assert result["total_attempted"] == 0
        assert result["succeeded"] == 0
        assert result["failed"] == 0
        assert result["still_in_dlq"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
