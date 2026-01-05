"""
Integration tests for DLQ error recovery with full streaming system.
"""
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from src.retail_datagen.config.models import RetailConfig
from src.retail_datagen.streaming.event_streamer import EventStreamer
from src.retail_datagen.streaming.schemas import EventEnvelope, EventType


@pytest.fixture
def test_config():
    """Create test configuration."""
    config_dict = {
        "seed": 42,
        "volume": {
            "stores": 2,
            "dcs": 1,
            "total_customers": 10,
            "customers_per_day": 5,
            "items_per_ticket_mean": 4.2,
        },
        "paths": {
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
        "historical": {"start_date": "2024-01-01"},
        "realtime": {
            "emit_interval_ms": 100,  # Fast for testing
            "burst": 5,
            "azure_connection_string": "mock://test",
            "max_batch_size": 10,
            "dlq_max_size": 100,
            "dlq_retry_enabled": True,
            "dlq_retry_max_attempts": 3,
        },
        "stream": {"hub": "test-hub"},
    }
    return RetailConfig(**config_dict)


@pytest.mark.asyncio
async def test_dlq_integration_failure_and_retry(test_config):
    """Test DLQ integration with event failure and successful retry."""
    streamer = EventStreamer(
        config=test_config,
        stores=[],
        customers=[],
        products=[],
        distribution_centers=[],
    )

    # Initialize streamer without starting full streaming
    await streamer.initialize()

    # Create test events
    events = [
        EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={"test": f"event_{i}"},
            trace_id=f"trace-{i}",
            ingest_timestamp=datetime.now(UTC),
        )
        for i in range(5)
    ]

    # Simulate send failure
    mock_exception = TimeoutError("Simulated network timeout")
    await streamer._handle_send_failure(events, mock_exception)

    # Verify events were added to DLQ
    assert len(streamer._dlq) == 5

    # Verify DLQ summary
    summary = streamer.get_dlq_summary()
    assert summary["size"] == 5
    assert summary["by_category"]["network"] == 5
    assert summary["by_severity"]["transient"] == 5

    # Mock successful retry
    streamer._azure_client.send_events = AsyncMock(return_value=True)

    # Retry DLQ events
    result = await streamer.retry_dlq_events(max_retries=3)

    # Verify all succeeded
    assert result["total_attempted"] == 5
    assert result["succeeded"] == 5
    assert result["failed"] == 0
    assert result["still_in_dlq"] == 0

    # Verify DLQ is now empty
    assert len(streamer._dlq) == 0


@pytest.mark.asyncio
async def test_dlq_integration_mixed_retry_results(test_config):
    """Test DLQ with mixed retry results (some succeed, some fail)."""
    streamer = EventStreamer(
        config=test_config,
        stores=[],
        customers=[],
        products=[],
        distribution_centers=[],
    )

    await streamer.initialize()

    # Add 10 events to DLQ
    events = [
        EventEnvelope(
            event_type=EventType.INVENTORY_UPDATED,
            payload={"id": i},
            trace_id=f"trace-{i}",
            ingest_timestamp=datetime.now(UTC),
        )
        for i in range(10)
    ]

    await streamer._handle_send_failure(events, Exception("Test error"))

    # Mock mixed results: first 5 succeed, last 5 fail
    call_count = 0

    async def mock_send(events):
        nonlocal call_count
        result = call_count < 5
        call_count += 1
        return result

    streamer._azure_client.send_events = AsyncMock(side_effect=mock_send)

    # Retry
    result = await streamer.retry_dlq_events(max_retries=3)

    # Verify mixed results
    assert result["total_attempted"] == 10
    assert result["succeeded"] == 5
    assert result["failed"] == 5
    assert result["still_in_dlq"] == 5

    # Verify remaining events have incremented retry count
    for entry in streamer._dlq:
        assert entry.retry_count == 1


@pytest.mark.asyncio
async def test_dlq_integration_max_retries_enforcement(test_config):
    """Test that max retries are properly enforced."""
    streamer = EventStreamer(
        config=test_config,
        stores=[],
        customers=[],
        products=[],
        distribution_centers=[],
    )

    await streamer.initialize()

    # Add event to DLQ
    event = EventEnvelope(
        event_type=EventType.CUSTOMER_ENTERED,
        payload={"test": "max_retry"},
        trace_id="trace-max-retry",
        ingest_timestamp=datetime.now(UTC),
    )

    await streamer._handle_send_failure([event], Exception("Test error"))

    # Mock always failing send
    streamer._azure_client.send_events = AsyncMock(return_value=False)

    # Retry multiple times with max_retries=2
    for i in range(5):
        result = await streamer.retry_dlq_events(max_retries=2)

        if i < 2:
            # Should still be retrying
            assert result["total_attempted"] == 1
            assert result["failed"] == 1
            assert all(entry.retry_count == i + 1 for entry in streamer._dlq)
        else:
            # Max retries exceeded, should not retry
            assert result["total_attempted"] == 1
            assert result["succeeded"] == 0
            assert result["failed"] == 1
            assert result["still_in_dlq"] == 1
            # Verify retry count didn't increase
            assert streamer._dlq[0].retry_count == 2


@pytest.mark.asyncio
async def test_dlq_statistics_integration(test_config):
    """Test that DLQ statistics are properly integrated into overall stats."""
    streamer = EventStreamer(
        config=test_config,
        stores=[],
        customers=[],
        products=[],
        distribution_centers=[],
    )

    await streamer.initialize()

    # Add events with different error types
    test_errors = [
        (EventType.RECEIPT_CREATED, TimeoutError("Network timeout")),
        (EventType.INVENTORY_UPDATED, PermissionError("Auth failed")),
        (EventType.CUSTOMER_ENTERED, Exception("Rate limit exceeded")),
    ]

    for event_type, exception in test_errors:
        event = EventEnvelope(
            event_type=event_type,
            payload={"test": event_type.value},
            trace_id=f"trace-{event_type.value}",
            ingest_timestamp=datetime.now(UTC),
        )
        await streamer._handle_send_failure([event], exception)

    # Get overall statistics
    stats = await streamer.get_statistics()

    # Verify DLQ info is included
    assert "dlq_summary" in stats
    assert stats["dead_letter_queue_size"] == 3

    # Verify DLQ summary details
    dlq_summary = stats["dlq_summary"]
    assert dlq_summary["size"] == 3
    assert "network" in dlq_summary["by_category"]
    assert "authentication" in dlq_summary["by_category"]
    assert "throttling" in dlq_summary["by_category"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
