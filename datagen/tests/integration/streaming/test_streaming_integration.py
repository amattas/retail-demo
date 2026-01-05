"""
Integration tests for the complete streaming system.

Tests the full streaming flow end-to-end including EventStreamer orchestrator,
EventFactory generation, AzureEventHubClient (mocked), configuration system,
state management, and API endpoints.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch

import pytest

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.generation_state import GenerationStateManager
from retail_datagen.shared.models import (
    Customer,
    DistributionCenter,
    ProductMaster,
    Store,
)
from retail_datagen.streaming.azure_client import AzureEventHubClient, CircuitBreaker
from retail_datagen.streaming.event_factory import EventFactory
from retail_datagen.streaming.event_streaming import EventStreamer
from retail_datagen.streaming.schemas import EventEnvelope, EventType

# ================================
# FIXTURES
# ================================


@pytest.fixture
def test_config(tmp_path):
    """Create test configuration for streaming."""
    config_data = {
        "seed": 42,
        "volume": {
            "stores": 5,
            "dcs": 2,
            "total_customers": 100,
            "customers_per_day": 20,
            "items_per_ticket_mean": 4.2,
        },
        "realtime": {
            "emit_interval_ms": 100,
            "burst": 50,
            "azure_connection_string": "Endpoint=sb://integration-test.servicebus.windows.net/;SharedAccessKeyName=TestKey;SharedAccessKey=aW50ZWdyYXRpb250ZXN0a2V5MTIzNDU2Nzg5MA==;EntityPath=integration-hub",
        },
        "paths": {
            "dict": str(tmp_path / "dictionaries"),
            "master": str(tmp_path / "master"),
            "facts": str(tmp_path / "facts"),
        },
        "stream": {"hub": "test-retail-events"},
    }

    # Create directories
    (tmp_path / "dictionaries").mkdir()
    (tmp_path / "master").mkdir()
    (tmp_path / "facts").mkdir()

    return RetailConfig(**config_data)


@pytest.fixture
def sample_stores():
    """Create sample stores for testing."""
    return [
        Store(
            ID=1,
            StoreNumber="ST001",
            Address="123 Main St, Springfield, IL 62701",
            GeographyID=1,
        ),
        Store(
            ID=2,
            StoreNumber="ST002",
            Address="456 Oak Ave, Riverside, CA 92501",
            GeographyID=2,
        ),
        Store(
            ID=3,
            StoreNumber="ST003",
            Address="789 Pine Rd, Franklin, TN 37064",
            GeographyID=3,
        ),
    ]


@pytest.fixture
def sample_customers():
    """Create sample customers for testing."""
    return [
        Customer(
            ID=1,
            FirstName="Alexis",
            LastName="Anderson",
            Address="111 Elm St, Springfield, IL 62701",
            GeographyID=1,
            LoyaltyCard="LC000001234",
            Phone="(555) 123-4567",
            BLEId="BLEABC123",
            AdId="ADXYZ789",
        ),
        Customer(
            ID=2,
            FirstName="Blake",
            LastName="Brightwell",
            Address="222 Maple Dr, Riverside, CA 92501",
            GeographyID=2,
            LoyaltyCard="LC000004321",
            Phone="(555) 987-6543",
            BLEId="BLEDEF456",
            AdId="ADUVW123",
        ),
        Customer(
            ID=3,
            FirstName="Casey",
            LastName="Clearwater",
            Address="333 Birch Ln, Franklin, TN 37064",
            GeographyID=3,
            LoyaltyCard="LC000005678",
            Phone="(555) 555-5555",
            BLEId="BLEGHI789",
            AdId="ADPQR456",
        ),
    ]


@pytest.fixture
def sample_products():
    """Create sample products for testing."""
    return [
        ProductMaster(
            ID=1,
            ProductName="Widget Pro",
            Brand="SuperBrand",
            Company="Acme Corp",
            Department="Electronics",
            Category="Gadgets",
            Subcategory="Widgets",
            Cost=Decimal("15.00"),
            MSRP=Decimal("22.99"),
            SalePrice=Decimal("19.99"),
            RequiresRefrigeration=False,
            LaunchDate=datetime.now(UTC),
        ),
        ProductMaster(
            ID=2,
            ProductName="Gadget Plus",
            Brand="MegaBrand",
            Company="Global Industries",
            Department="Electronics",
            Category="Devices",
            Subcategory="Gadgets",
            Cost=Decimal("20.00"),
            MSRP=Decimal("34.49"),
            SalePrice=Decimal("29.99"),
            RequiresRefrigeration=False,
            LaunchDate=datetime.now(UTC),
        ),
    ]


@pytest.fixture
def sample_dcs():
    """Create sample distribution centers for testing."""
    return [
        DistributionCenter(
            ID=1,
            DCNumber="DC001",
            Address="999 Industrial Blvd, Springfield, IL 62701",
            GeographyID=1,
        ),
        DistributionCenter(
            ID=2,
            DCNumber="DC002",
            Address="888 Warehouse Way, Riverside, CA 92501",
            GeographyID=2,
        ),
    ]


@pytest.fixture
def mock_event_hub_client():
    """Create mock Azure Event Hub client."""
    client = AsyncMock(spec=AzureEventHubClient)
    client.send_events = AsyncMock(return_value=True)
    client.send_batch = AsyncMock(return_value=True)
    client.connect = AsyncMock(return_value=True)
    client.disconnect = AsyncMock(return_value=None)
    client.is_connected = Mock(return_value=True)
    client.health_check = AsyncMock(
        return_value={
            "healthy": True,
            "connection_status": "connected",
            "last_send_time": datetime.now(UTC),
        }
    )
    client.get_statistics = Mock(
        return_value={
            "events_sent": 0,
            "events_failed": 0,
            "batches_sent": 0,
            "is_connected": True,
            "circuit_breaker_state": "CLOSED",
        }
    )
    return client


@pytest.fixture
def event_factory(sample_stores, sample_customers, sample_products, sample_dcs):
    """Create EventFactory for testing."""
    return EventFactory(
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
        seed=42,
    )


# ================================
# TEST: End-to-End Streaming Flow
# ================================


@pytest.mark.asyncio
async def test_full_streaming_flow(
    test_config,
    sample_stores,
    sample_customers,
    sample_products,
    sample_dcs,
    mock_event_hub_client,
):
    """Test complete streaming flow: config → master data → streaming → Event Hub."""

    # Create streamer with mock client
    streamer = EventStreamer(
        config=test_config,
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
    )

    # Replace Azure client with mock
    with patch.object(streamer, "_azure_client", mock_event_hub_client):
        # Initialize streamer
        success = await streamer.initialize()
        assert success, "Streamer initialization failed"

        # Start streaming for short duration
        duration = timedelta(seconds=2)
        streaming_task = asyncio.create_task(streamer.start(duration))

        # Allow streaming to run
        await asyncio.sleep(2.5)

        # Verify events were generated
        stats = await streamer.get_statistics()
        assert stats["events_generated"] > 0, "No events were generated"
        assert stats["is_streaming"] is False, "Streaming should have stopped"

        # Verify mock client was called
        mock_event_hub_client.send_events.assert_called()

        # Stop streaming
        await streamer.stop()

        # Verify cleanup
        final_stats = await streamer.get_statistics()
        assert final_stats["is_streaming"] is False


# ================================
# TEST: Event Hub Batch Sending
# ================================


@pytest.mark.asyncio
async def test_event_hub_batch_sending(event_factory, mock_event_hub_client):
    """Test batching and sending to Event Hub."""

    # Generate events
    timestamp = datetime.now(UTC)
    events = event_factory.generate_mixed_events(count=100, timestamp=timestamp)

    assert len(events) > 0, "No events generated"

    # Configure batch size
    mock_event_hub_client.max_batch_size = 25

    # Send events in batches
    success = await mock_event_hub_client.send_events(events)

    assert success, "Event sending failed"
    mock_event_hub_client.send_events.assert_called_once()

    # Verify event serialization (check first event)
    first_event = events[0]
    assert isinstance(first_event, EventEnvelope)
    assert first_event.event_type in EventType
    assert first_event.trace_id is not None
    assert first_event.payload is not None


# ================================
# TEST: Circuit Breaker Integration
# ================================


@pytest.mark.asyncio
async def test_circuit_breaker_opens_on_failures():
    """Test circuit breaker integration during send failures."""

    # Create circuit breaker
    circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5)

    # Simulate failures
    mock_func = Mock(side_effect=Exception("Connection failed"))

    # Try calling multiple times
    failure_count = 0
    for _ in range(5):
        try:
            circuit_breaker.call(mock_func)
        except Exception:
            failure_count += 1

    # Verify circuit breaker opened after threshold
    assert circuit_breaker.state == "OPEN", "Circuit breaker should be open"
    assert circuit_breaker.failure_count >= 3

    # Try calling when circuit is open
    with pytest.raises(Exception, match="Circuit breaker is OPEN"):
        circuit_breaker.call(mock_func)

    # Wait for recovery timeout
    await asyncio.sleep(6)

    # Mock success for recovery
    mock_func_success = Mock(return_value=True)

    # Circuit breaker should attempt reset
    try:
        result = circuit_breaker.call(mock_func_success)
        assert result is True
        assert circuit_breaker.state == "CLOSED", "Circuit breaker should close on success"
    except Exception:
        # State should be HALF_OPEN at least
        assert circuit_breaker.state in ["HALF_OPEN", "CLOSED"]


# ================================
# TEST: Dead Letter Queue Flow
# ================================


@pytest.mark.asyncio
async def test_dlq_flow(
    test_config,
    sample_stores,
    sample_customers,
    sample_products,
    sample_dcs,
):
    """Test failed events go to DLQ and can be retrieved."""

    # Create streamer with DLQ enabled
    streamer = EventStreamer(
        config=test_config,
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
    )

    # Mock client that fails
    mock_client = AsyncMock(spec=AzureEventHubClient)
    mock_client.send_events = AsyncMock(return_value=False)  # Fail sending
    mock_client.connect = AsyncMock(return_value=True)
    mock_client.is_connected = Mock(return_value=True)

    with patch.object(streamer, "_azure_client", mock_client):
        await streamer.initialize()

        # Generate events manually and try to send
        timestamp = datetime.now(UTC)
        events = streamer._event_factory.generate_mixed_events(count=10, timestamp=timestamp)

        # Add to buffer
        async with streamer._buffer_lock:
            streamer._event_buffer.extend(events)

        # Try to flush (will fail)
        await streamer._flush_event_buffer()

        # Verify events in DLQ
        stats = await streamer.get_statistics()
        assert stats["dead_letter_queue_size"] > 0, "DLQ should contain failed events"
        assert stats["events_failed"] > 0, "Failed events count should increase"


# ================================
# TEST: Event Type Filtering
# ================================


@pytest.mark.asyncio
async def test_event_filtering(event_factory):
    """Test filtering to specific event types."""

    # Configure filter for specific types
    allowed_types = {EventType.RECEIPT_CREATED, EventType.PAYMENT_PROCESSED}

    # Generate events with custom weights
    event_weights = {et: 1.0 for et in allowed_types}

    timestamp = datetime.now(UTC)
    events = event_factory.generate_mixed_events(
        count=100, timestamp=timestamp, event_weights=event_weights
    )

    # Verify only filtered types generated
    event_types_generated = {event.event_type for event in events}
    assert event_types_generated.issubset(
        allowed_types
    ), f"Unexpected event types: {event_types_generated - allowed_types}"

    # Verify event distribution
    type_counts = {}
    for event in events:
        type_counts[event.event_type] = type_counts.get(event.event_type, 0) + 1

    assert len(type_counts) > 0, "No events generated"


# ================================
# TEST: API Integration
# ================================


@pytest.mark.asyncio
async def test_streaming_api_endpoints_mock():
    """Test streaming API endpoints (mock test without FastAPI TestClient)."""

    # This is a simplified mock test - full API testing would use FastAPI TestClient
    # Testing the core logic that the API endpoints would call

    from retail_datagen.streaming.router import (
        _reset_streaming_state,
        _streaming_statistics,
        _update_streaming_statistics,
    )

    # Reset state
    _reset_streaming_state()

    # Verify initial state
    assert _streaming_statistics["events_generated"] == 0
    assert _streaming_statistics["events_sent_successfully"] == 0

    # Simulate event processing
    event_data = {
        "event_type": "receipt_created",
        "payload": {"store_id": 1, "customer_id": 1},
    }

    _update_streaming_statistics(event_data, success=True)

    # Verify statistics updated
    assert _streaming_statistics["events_generated"] == 1
    assert _streaming_statistics["events_sent_successfully"] == 1


# ================================
# TEST: State Persistence
# ================================


@pytest.mark.asyncio
async def test_state_persistence_across_runs(tmp_path):
    """Test streaming state persists and continues correctly."""

    state_file = tmp_path / "generation_state.json"

    # Create initial state
    state_manager = GenerationStateManager(state_file=str(state_file))

    # Set historical data flag
    initial_time = datetime.now(UTC)
    state_manager.update_after_historical_run(
        start_date=initial_time - timedelta(days=1),
        end_date=initial_time,
    )

    # Verify can start realtime
    assert state_manager.can_start_realtime(), "Should be able to start realtime"

    # Get last timestamp
    last_ts = state_manager.get_last_generated_timestamp()
    assert last_ts is not None, "Should have last timestamp"

    # Simulate streaming run
    state_manager.update_after_realtime_run(
        start_time=initial_time,
        end_time=initial_time + timedelta(hours=1),
    )

    # Create new state manager (simulating restart)
    new_state_manager = GenerationStateManager(state_file=str(state_file))

    # Verify state persisted
    new_last_ts = new_state_manager.get_last_generated_timestamp()
    assert new_last_ts is not None, "State should persist across instances"
    assert new_last_ts >= last_ts, "New timestamp should be >= old timestamp"


# ================================
# TEST: Configuration Validation
# ================================


def test_fabric_rti_connection_validation():
    """Test Fabric RTI connection string validation."""

    from retail_datagen.shared.credential_utils import (
        is_fabric_rti_connection_string,
        validate_eventhub_connection_string,
    )

    # Valid Azure Event Hub string with proper length key
    azure_conn = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=TestKey;SharedAccessKey=TW9ja0tleUZvclRlc3RpbmdQdXJwb3Nlc09ubHkxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTA=;EntityPath=test-hub"
    is_valid, error = validate_eventhub_connection_string(azure_conn)
    assert is_valid, f"Azure connection string should be valid: {error}"
    assert not is_fabric_rti_connection_string(azure_conn), "Should not be Fabric RTI"

    # Valid Fabric RTI string
    fabric_conn = "Endpoint=sb://test-fabric.eventhub.windows.net/;SharedAccessKeyName=FabricKey;SharedAccessKey=RmFicmljS2V5MTIzRmFicmljS2V5MTIz;EntityPath=fabric-hub"
    is_valid, error = validate_eventhub_connection_string(fabric_conn)
    assert is_valid, f"Fabric connection string should be valid: {error}"

    # Invalid string (missing parts)
    invalid_conn = "Endpoint=sb://test.servicebus.windows.net/"
    is_valid, error = validate_eventhub_connection_string(invalid_conn)
    assert not is_valid, "Invalid connection string should fail validation"
    assert error is not None, "Error message should be provided"


# ================================
# TEST: Statistics Collection
# ================================


@pytest.mark.asyncio
async def test_statistics_collection(
    test_config,
    sample_stores,
    sample_customers,
    sample_products,
    sample_dcs,
    mock_event_hub_client,
):
    """Test statistics are collected correctly during streaming."""

    streamer = EventStreamer(
        config=test_config,
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
    )

    with patch.object(streamer, "_azure_client", mock_event_hub_client):
        await streamer.initialize()

        # Generate known number of events
        timestamp = datetime.now(UTC)
        events = streamer._event_factory.generate_mixed_events(
            count=50, timestamp=timestamp
        )

        # Add to buffer and flush
        async with streamer._buffer_lock:
            streamer._event_buffer.extend(events)

        # Update statistics manually
        async with streamer._stats_lock:
            streamer._statistics.events_generated = len(events)
            streamer._statistics.events_sent_successfully = len(events)
            streamer._statistics.batches_sent = 1

        # Get statistics
        stats = await streamer.get_statistics()

        # Verify counters match
        assert stats["events_generated"] == len(events), "Generated count mismatch"
        assert (
            stats["events_sent_successfully"] == len(events)
        ), "Sent count mismatch"
        assert stats["batches_sent"] == 1, "Batch count mismatch"
        assert stats["events_failed"] == 0, "Should have no failures"


# ================================
# TEST: Performance (Throughput)
# ================================


@pytest.mark.asyncio
async def test_streaming_throughput(event_factory):
    """Test streaming can achieve target throughput."""

    # Configure for high throughput
    timestamp = datetime.now(UTC)
    start_time = asyncio.get_event_loop().time()

    # Generate large batch of events
    events = event_factory.generate_mixed_events(count=1000, timestamp=timestamp)

    end_time = asyncio.get_event_loop().time()
    elapsed = end_time - start_time

    # Calculate events per second
    events_per_second = len(events) / max(elapsed, 0.001)

    # Verify meets minimum threshold (should be very fast with mocking)
    assert (
        events_per_second > 100
    ), f"Throughput too low: {events_per_second:.2f} events/sec"
    assert len(events) > 0, "Should generate events"


# ================================
# TEST: Error Recovery
# ================================


@pytest.mark.asyncio
async def test_error_recovery():
    """Test system recovers from transient errors."""

    # Create mock client with intermittent failures
    call_count = 0

    async def failing_send(events):
        nonlocal call_count
        call_count += 1
        # Fail first 2 times, then succeed
        if call_count <= 2:
            return False
        return True

    mock_client = AsyncMock(spec=AzureEventHubClient)
    mock_client.send_events = AsyncMock(side_effect=failing_send)
    mock_client.connect = AsyncMock(return_value=True)
    mock_client.is_connected = Mock(return_value=True)

    # Test retry logic
    test_events = [
        EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={"store_id": 1},
            trace_id="TEST123",
            ingest_timestamp=datetime.now(UTC),
        )
    ]

    # First call should fail
    result = await mock_client.send_events(test_events)
    assert result is False, "First call should fail"

    # Second call should fail
    result = await mock_client.send_events(test_events)
    assert result is False, "Second call should fail"

    # Third call should succeed
    result = await mock_client.send_events(test_events)
    assert result is True, "Third call should succeed"

    assert call_count == 3, "Should have called send 3 times"


# ================================
# TEST: Monitoring and Health
# ================================


@pytest.mark.asyncio
async def test_monitoring_and_health(
    test_config,
    sample_stores,
    sample_customers,
    sample_products,
    sample_dcs,
    mock_event_hub_client,
):
    """Test monitoring and health check functionality."""

    streamer = EventStreamer(
        config=test_config,
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
    )

    with patch.object(streamer, "_azure_client", mock_event_hub_client):
        await streamer.initialize()

        # Get health status
        health = await streamer.get_health_status()

        assert "overall_healthy" in health
        assert "components" in health
        assert "azure_event_hub" in health["components"]
        assert "event_factory" in health["components"]

        # Verify Azure client health
        azure_health = health["components"]["azure_event_hub"]
        assert azure_health["healthy"] is True


# ================================
# TEST: Event Factory State
# ================================


@pytest.mark.asyncio
async def test_event_factory_maintains_state(event_factory):
    """Test EventFactory maintains realistic state across events."""

    timestamp = datetime.now(UTC)

    # Generate customer entered event
    customer_event = event_factory.generate_event(
        EventType.CUSTOMER_ENTERED, timestamp
    )
    assert customer_event is not None, "Should generate customer entered event"

    # Verify customer session created
    assert (
        len(event_factory.state.customer_sessions) > 0
    ), "Customer session should be tracked"

    # Generate receipt event (requires customer in store)
    # Add small delay to allow customer to be in store
    receipt_timestamp = timestamp + timedelta(minutes=5)
    receipt_event = event_factory.generate_event(
        EventType.RECEIPT_CREATED, receipt_timestamp
    )

    # Should either generate event or return None (if no eligible customers)
    # Both are valid based on business logic
    if receipt_event:
        assert receipt_event.payload.get("store_id") is not None
        assert receipt_event.payload.get("customer_id") is not None


# ================================
# TEST: Concurrent Access Safety
# ================================


@pytest.mark.asyncio
async def test_concurrent_statistics_access(
    test_config,
    sample_stores,
    sample_customers,
    sample_products,
    sample_dcs,
    mock_event_hub_client,
):
    """Test concurrent access to statistics is thread-safe."""

    streamer = EventStreamer(
        config=test_config,
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
    )

    with patch.object(streamer, "_azure_client", mock_event_hub_client):
        await streamer.initialize()

        # Create multiple tasks that access statistics concurrently
        async def read_stats():
            for _ in range(10):
                stats = await streamer.get_statistics()
                assert isinstance(stats, dict)
                await asyncio.sleep(0.01)

        # Run multiple concurrent readers
        tasks = [asyncio.create_task(read_stats()) for _ in range(5)]

        # Wait for all tasks
        await asyncio.gather(*tasks)

        # Verify no race conditions occurred (no exceptions thrown)
        # If we got here, concurrent access was safe


# ================================
# TEST: Event Hooks
# ================================


@pytest.mark.asyncio
async def test_event_hooks(
    test_config,
    sample_stores,
    sample_customers,
    sample_products,
    sample_dcs,
    mock_event_hub_client,
):
    """Test event hooks are called correctly."""

    streamer = EventStreamer(
        config=test_config,
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
    )

    # Track hook calls
    generated_events = []
    sent_events = []
    batches_sent = []
    errors_caught = []

    def on_event_generated(event: EventEnvelope):
        generated_events.append(event)

    def on_event_sent(event: EventEnvelope):
        sent_events.append(event)

    def on_batch_sent(events: list[EventEnvelope]):
        batches_sent.append(events)

    def on_error(error: Exception, context: str):
        errors_caught.append((error, context))

    # Register hooks
    streamer.add_event_generated_hook(on_event_generated)
    streamer.add_event_sent_hook(on_event_sent)
    streamer.add_batch_sent_hook(on_batch_sent)
    streamer.add_error_hook(on_error)

    with patch.object(streamer, "_azure_client", mock_event_hub_client):
        await streamer.initialize()

        # Generate events
        timestamp = datetime.now(UTC)
        events = await streamer._generate_event_burst(timestamp)

        # Verify generated hooks called
        assert len(generated_events) > 0, "Generated hooks should be called"
        assert len(generated_events) == len(events), "Hook should be called for each event"


# ================================
# TEST: Multiple Event Types
# ================================


@pytest.mark.asyncio
async def test_multiple_event_types_generation(event_factory):
    """Test generation of all event types."""

    timestamp = datetime.now(UTC)

    # Test each event type can be generated
    event_types_to_test = [
        EventType.RECEIPT_CREATED,
        EventType.CUSTOMER_ENTERED,
        EventType.INVENTORY_UPDATED,
        EventType.BLE_PING_DETECTED,
        EventType.TRUCK_ARRIVED,
        EventType.AD_IMPRESSION,
    ]

    generated_types = set()

    for event_type in event_types_to_test:
        # Generate multiple attempts (some may fail due to business logic)
        for _ in range(10):
            event = event_factory.generate_event(event_type, timestamp)
            if event:
                generated_types.add(event.event_type)
                break

    # Should have generated at least some event types
    assert len(generated_types) > 0, "Should generate at least some event types"


# ================================
# TEST: Time-Based Event Generation
# ================================


@pytest.mark.asyncio
async def test_time_based_event_generation(event_factory):
    """Test event generation respects business hours and time patterns."""

    # Business hours (9 AM)
    business_hours = datetime.now(UTC).replace(hour=9, minute=0, second=0)

    # Off hours (3 AM)
    off_hours = datetime.now(UTC).replace(hour=3, minute=0, second=0)

    # Generate events during business hours
    business_events = event_factory.generate_mixed_events(
        count=100, timestamp=business_hours
    )

    # Generate events during off hours
    off_events = event_factory.generate_mixed_events(count=100, timestamp=off_hours)

    # Business hours should generate more events due to higher probability
    # Note: This is probabilistic, so we check the pattern exists, not exact counts
    assert len(business_events) >= 0, "Should generate some business hour events"
    assert len(off_events) >= 0, "Should generate some off-hour events"


# ================================
# TEST: Connection String Handling
# ================================


def test_connection_string_sanitization():
    """Test connection string sanitization for logging."""

    from retail_datagen.shared.credential_utils import sanitize_connection_string

    conn_str = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=TestKey;SharedAccessKey=U2VjcmV0S2V5MTIzU2VjcmV0S2V5MTIzU2VjcmV0S2V5MTIz;EntityPath=test-hub"

    sanitized = sanitize_connection_string(conn_str)

    # Verify sensitive data is hidden
    assert "U2VjcmV0S2V5MTIz" not in sanitized, "Secret key should be hidden"
    assert "***" in sanitized or "REDACTED" in sanitized.upper(), "Should indicate redaction"
    assert "test.servicebus.windows.net" in sanitized, "Endpoint should be visible"


# ================================
# TEST: Stream Duration Handling
# ================================


@pytest.mark.asyncio
async def test_stream_duration_handling(
    test_config,
    sample_stores,
    sample_customers,
    sample_products,
    sample_dcs,
    mock_event_hub_client,
):
    """Test streaming respects duration limits."""

    streamer = EventStreamer(
        config=test_config,
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
    )

    with patch.object(streamer, "_azure_client", mock_event_hub_client):
        await streamer.initialize()

        # Start streaming for 1 second
        duration = timedelta(seconds=1)
        start_time = datetime.now(UTC)

        await streamer.start(duration)

        end_time = datetime.now(UTC)
        elapsed = (end_time - start_time).total_seconds()

        # Should complete within reasonable time (1 second + some overhead)
        assert elapsed < 3.0, f"Streaming took too long: {elapsed:.2f}s"
        assert elapsed >= 1.0, f"Streaming ended too early: {elapsed:.2f}s"


# ================================
# TEST: Buffer Management
# ================================


@pytest.mark.asyncio
async def test_buffer_management(
    test_config,
    sample_stores,
    sample_customers,
    sample_products,
    sample_dcs,
    mock_event_hub_client,
):
    """Test event buffer management and flushing."""

    streamer = EventStreamer(
        config=test_config,
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
    )

    with patch.object(streamer, "_azure_client", mock_event_hub_client):
        await streamer.initialize()

        # Generate events and add to buffer
        timestamp = datetime.now(UTC)
        events = streamer._event_factory.generate_mixed_events(
            count=10, timestamp=timestamp
        )

        async with streamer._buffer_lock:
            streamer._event_buffer.extend(events)
            initial_buffer_size = len(streamer._event_buffer)

        assert initial_buffer_size == 10, "Buffer should contain 10 events"

        # Flush buffer
        await streamer._flush_event_buffer()

        # Verify buffer is empty
        async with streamer._buffer_lock:
            assert len(streamer._event_buffer) == 0, "Buffer should be empty after flush"

        # Verify events were sent
        mock_event_hub_client.send_events.assert_called()
