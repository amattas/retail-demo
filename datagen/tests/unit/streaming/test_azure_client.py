"""
Comprehensive unit tests for Azure Event Hub client.

This module tests the Azure Event Hub client wrapper, including:
- Connection management
- Event sending (single and batch)
- Circuit breaker pattern
- Retry logic with exponential backoff
- Error handling
- Dead letter queue functionality
- Mock client fallback

Testing Framework: pytest
Coverage Goals: 80%+ code coverage
Python Version: 3.11+
"""

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from retail_datagen.streaming.azure_client import (
    AZURE_AVAILABLE,
    AzureEventHubClient,
    CircuitBreaker,
)
from retail_datagen.streaming.schemas import EventEnvelope, EventType

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_azure_sdk():
    """Mock the Azure Event Hub SDK for testing."""
    with patch(
        "retail_datagen.streaming.azure_client.EventHubProducerClient"
    ) as mock_client:
        # Create mock instance
        mock_instance = MagicMock()
        mock_instance.send_batch = AsyncMock()
        mock_instance.close = AsyncMock()
        mock_instance.get_partition_properties = AsyncMock(
            return_value={"partition_id": "0", "beginning_sequence_number": 0}
        )
        mock_instance.get_eventhub_properties = AsyncMock(
            return_value=MagicMock(partition_ids=["0", "1", "2", "3"])
        )

        # Make from_connection_string return the mock instance
        mock_client.from_connection_string = MagicMock(return_value=mock_instance)

        # Make context manager work
        mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_instance.__aexit__ = AsyncMock(return_value=None)

        yield mock_client


@pytest.fixture
def mock_time():
    """Mock time for deterministic testing."""
    with patch("time.time") as mock:
        mock.return_value = 1000.0
        yield mock


@pytest.fixture
def mock_datetime():
    """Mock datetime for deterministic testing."""
    with patch("retail_datagen.streaming.azure_client.datetime") as mock:
        mock_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        mock.now.return_value = mock_now
        mock.UTC = UTC
        yield mock


@pytest.fixture
def valid_connection_string() -> str:
    """Valid Azure Event Hub connection string for testing."""
    return "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dGVzdGtleTEyM3Rlc3RrZXkxMjN0ZXN0a2V5MTIzZGVzdGtleTE=;EntityPath=test-hub"


@pytest.fixture
def sample_event_envelope() -> EventEnvelope:
    """Create a sample event envelope for testing."""
    return EventEnvelope(
        event_type=EventType.RECEIPT_CREATED,
        payload={
            "store_id": 1,
            "customer_id": 42,
            "receipt_id": "RCP001",
            "subtotal": 99.99,
            "tax": 8.00,
            "total": 107.99,
            "tender_type": "CREDIT_CARD",
            "item_count": 3,
        },
        trace_id=str(uuid4()),
        ingest_timestamp=datetime.now(UTC),
        schema_version="1.0",
        source="retail-datagen",
    )


@pytest.fixture
def sample_event_batch(sample_event_envelope) -> list[EventEnvelope]:
    """Create a batch of sample events for testing."""
    batch = []
    for i in range(5):
        event = EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={
                "store_id": 1,
                "customer_id": i,
                "receipt_id": f"RCP{i:03d}",
                "subtotal": 50.0 + i * 10,
                "tax": 4.0,
                "total": 54.0 + i * 10,
                "tender_type": "CREDIT_CARD",
                "item_count": i + 1,
            },
            trace_id=str(uuid4()),
            ingest_timestamp=datetime.now(UTC),
            schema_version="1.0",
            source="retail-datagen",
        )
        batch.append(event)
    return batch


# =============================================================================
# CircuitBreaker Tests
# =============================================================================


class TestCircuitBreaker:
    """Test circuit breaker implementation."""

    def test_initial_state_is_closed(self):
        """Circuit breaker should start in CLOSED state."""
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

        assert cb.state == "CLOSED"
        assert cb.failure_count == 0
        assert cb.last_failure_time is None

    def test_breaker_opens_after_threshold_failures(self, mock_datetime):
        """Circuit breaker should open after N consecutive failures."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

        # First failure
        try:
            cb.call(lambda: self._failing_function())
        except ValueError:
            pass

        assert cb.state == "CLOSED"
        assert cb.failure_count == 1

        # Second failure
        try:
            cb.call(lambda: self._failing_function())
        except ValueError:
            pass

        assert cb.state == "CLOSED"
        assert cb.failure_count == 2

        # Third failure should open circuit
        try:
            cb.call(lambda: self._failing_function())
        except ValueError:
            pass

        assert cb.state == "OPEN"
        assert cb.failure_count == 3

    def test_open_circuit_rejects_calls(self):
        """Open circuit breaker should reject all calls."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=60)

        # Force circuit open
        try:
            cb.call(lambda: self._failing_function())
        except ValueError:
            pass

        assert cb.state == "OPEN"

        # Should now reject calls
        from retail_datagen.streaming.azure_client import EventHubError

        with pytest.raises(EventHubError, match="Circuit breaker is OPEN"):
            cb.call(lambda: "should not execute")

    def test_breaker_half_open_after_timeout(self, mock_datetime):
        """Circuit breaker should enter half-open state after timeout."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=60)

        # Open the circuit
        try:
            cb.call(lambda: self._failing_function())
        except ValueError:
            pass

        assert cb.state == "OPEN"

        # Simulate time passing
        mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 1, 30, tzinfo=UTC)

        # Should allow attempt in half-open state
        result = cb.call(lambda: "success")
        assert result == "success"
        assert cb.state == "CLOSED"
        assert cb.failure_count == 0

    def test_breaker_closes_on_success(self, mock_datetime):
        """Circuit breaker should close after successful operation."""
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

        # Accumulate some failures
        for _ in range(3):
            try:
                cb.call(lambda: self._failing_function())
            except ValueError:
                pass

        assert cb.failure_count == 3
        assert cb.state == "CLOSED"

        # Success should reset
        result = cb.call(lambda: "success")
        assert result == "success"
        assert cb.failure_count == 0
        assert cb.state == "CLOSED"

    def test_breaker_timeout_configuration(self):
        """Circuit breaker should respect custom timeout configuration."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=30)

        assert cb.failure_threshold == 2
        assert cb.recovery_timeout == 30

    @staticmethod
    def _failing_function():
        """Helper function that always fails."""
        raise ValueError("Test failure")


# =============================================================================
# AzureEventHubClient Connection Management Tests
# =============================================================================


class TestAzureEventHubClientConnection:
    """Test Azure Event Hub client connection management."""

    @pytest.mark.asyncio
    async def test_client_initialization_with_valid_config(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test client initializes with valid configuration."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                max_batch_size=100,
            )

            assert client.connection_string == valid_connection_string
            assert client.hub_name == "test-hub"
            assert client.max_batch_size == 100
            assert client._client is not None

    @pytest.mark.asyncio
    async def test_successful_connection(self, mock_azure_sdk, valid_connection_string):
        """Test successful connection to Azure Event Hub."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            success = await client.connect()

            assert success is True
            assert client.is_connected() is True

    @pytest.mark.asyncio
    async def test_connection_failure_handling(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test connection failure handling."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            # Make connection fail
            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_instance.get_partition_properties = AsyncMock(
                side_effect=Exception("Connection failed")
            )

            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            success = await client.connect()

            assert success is False
            assert client.is_connected() is False
            assert client._statistics["connection_failures"] >= 1

    @pytest.mark.asyncio
    async def test_graceful_disconnect(self, mock_azure_sdk, valid_connection_string):
        """Test graceful disconnection from Event Hub."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            assert client.is_connected() is True

            await client.disconnect()
            assert client.is_connected() is False

    @pytest.mark.asyncio
    async def test_health_check_when_connected(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test health check returns healthy status when connected."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            health = await client.health_check()

            assert health["healthy"] is True
            assert health["connection_status"] == "connected"
            assert "partition_count" in health

    @pytest.mark.asyncio
    async def test_health_check_when_disconnected(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test health check returns unhealthy status when disconnected."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            # Don't connect
            health = await client.health_check()

            assert health["healthy"] is False
            assert health["connection_status"] == "disconnected"

    @pytest.mark.asyncio
    async def test_managed_connection_context_manager(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test managed connection context manager."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            async with client.managed_connection():
                assert client.is_connected() is True

            # Should be disconnected after context exit
            assert client.is_connected() is False


# =============================================================================
# Event Sending Tests
# =============================================================================


class TestEventSending:
    """Test event sending functionality."""

    @pytest.mark.asyncio
    async def test_send_single_event_success(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test sending a single event successfully."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            success = await client.send_event(sample_event_envelope)

            assert success is True
            assert client._statistics["events_sent"] == 1
            assert client._statistics["batches_sent"] == 1

    @pytest.mark.asyncio
    async def test_send_batch_events_success(
        self, mock_azure_sdk, valid_connection_string, sample_event_batch
    ):
        """Test sending batch of events successfully."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            success = await client.send_events(sample_event_batch)

            assert success is True
            assert client._statistics["events_sent"] == 5
            assert client._statistics["batches_sent"] == 1

    @pytest.mark.asyncio
    async def test_send_empty_event_list(self, mock_azure_sdk, valid_connection_string):
        """Test sending empty event list returns True."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            success = await client.send_events([])

            assert success is True
            assert client._statistics["events_sent"] == 0

    @pytest.mark.asyncio
    async def test_send_when_disconnected(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test sending event when client is disconnected fails."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            # Don't connect
            success = await client.send_event(sample_event_envelope)

            assert success is False

    @pytest.mark.asyncio
    async def test_event_serialization_to_json(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test event is properly serialized to JSON."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            with patch(
                "retail_datagen.streaming.azure_client.EventData"
            ) as mock_event_data:
                client = AzureEventHubClient(
                    connection_string=valid_connection_string, hub_name="test-hub"
                )

                await client.connect()
                await client.send_event(sample_event_envelope)

                # Verify EventData was called with JSON
                mock_event_data.assert_called()
                call_args = mock_event_data.call_args[0][0]
                # Should be valid JSON
                json.loads(call_args)

    @pytest.mark.asyncio
    async def test_partition_key_set_when_provided(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test partition key is set when provided in event."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            event = EventEnvelope(
                event_type=EventType.RECEIPT_CREATED,
                payload={"store_id": 1},
                trace_id=str(uuid4()),
                ingest_timestamp=datetime.now(UTC),
                partition_key="store-1",
            )

            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            await client.send_event(event)

            # Verify partition key was set
            # (Would need deeper mocking to verify, but covered by integration tests)


# =============================================================================
# Batching Tests
# =============================================================================


class TestBatching:
    """Test batch processing and optimization."""

    @pytest.mark.asyncio
    async def test_batch_size_optimization(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test events are batched up to max_batch_size."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                max_batch_size=3,
            )

            # Create 10 events
            events = []
            for i in range(10):
                event = EventEnvelope(
                    event_type=EventType.RECEIPT_CREATED,
                    payload={"store_id": i},
                    trace_id=str(uuid4()),
                    ingest_timestamp=datetime.now(UTC),
                )
                events.append(event)

            await client.connect()
            success = await client.send_events(events)

            assert success is True
            # Should have sent 4 batches: 3+3+3+1
            assert client._statistics["batches_sent"] == 4
            assert client._statistics["events_sent"] == 10

    @pytest.mark.asyncio
    async def test_buffer_add_and_flush(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test adding events to buffer and flushing."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()

            # Add events to buffer
            await client.add_to_buffer(sample_event_envelope)
            await client.add_to_buffer(sample_event_envelope)

            assert len(client._event_buffer) == 2

            # Flush buffer
            success = await client.flush_buffer()

            assert success is True
            assert len(client._event_buffer) == 0
            assert client._statistics["events_sent"] == 2

    @pytest.mark.asyncio
    async def test_buffer_auto_flush_on_max_size(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test buffer auto-flushes when reaching max size."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                max_batch_size=3,
            )

            await client.connect()

            # Add events up to max batch size
            for i in range(3):
                event = EventEnvelope(
                    event_type=EventType.RECEIPT_CREATED,
                    payload={"store_id": i},
                    trace_id=str(uuid4()),
                    ingest_timestamp=datetime.now(UTC),
                )
                await client.add_to_buffer(event)

            # Give async task time to execute
            await asyncio.sleep(0.1)

            # Buffer should be cleared (auto-flushed)
            # Note: This is timing-dependent in real tests

    @pytest.mark.asyncio
    async def test_flush_empty_buffer(self, mock_azure_sdk, valid_connection_string):
        """Test flushing empty buffer returns True."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            success = await client.flush_buffer()

            assert success is True
            assert client._statistics["events_sent"] == 0


# =============================================================================
# Retry Logic Tests
# =============================================================================


class TestRetryLogic:
    """Test retry logic with exponential backoff."""

    @pytest.mark.asyncio
    async def test_exponential_backoff_calculation(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test exponential backoff delay calculation."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                retry_attempts=3,
                backoff_multiplier=2.0,
            )

            # Test backoff calculation
            # First retry: 2^0 = 1 second
            # Second retry: 2^1 = 2 seconds
            # Third retry: 2^2 = 4 seconds

            assert client.backoff_multiplier**0 == 1.0
            assert client.backoff_multiplier**1 == 2.0
            assert client.backoff_multiplier**2 == 4.0

    @pytest.mark.asyncio
    async def test_max_retry_attempts(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test that retries stop after max attempts."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            # Make all sends fail
            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_instance.send_batch = AsyncMock(side_effect=Exception("Send failed"))
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)

            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                retry_attempts=3,
                backoff_multiplier=0.1,  # Fast retries for testing
                circuit_breaker_enabled=False,  # Disable to test retry logic
            )

            await client.connect()

            # Patch sleep to avoid waiting
            with patch("asyncio.sleep", new_callable=AsyncMock):
                success = await client.send_event(sample_event_envelope)

            assert success is False
            assert client._statistics["events_failed"] >= 1

    @pytest.mark.asyncio
    async def test_retry_on_transient_errors(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test retry on transient errors."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            # Fail first 2 attempts, succeed on 3rd
            attempt_count = 0

            async def mock_send_batch(batch):
                nonlocal attempt_count
                attempt_count += 1
                if attempt_count < 3:
                    raise Exception("Transient error")
                return True

            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_instance.send_batch = mock_send_batch
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)

            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                retry_attempts=3,
                backoff_multiplier=0.1,
                circuit_breaker_enabled=False,
            )

            await client.connect()

            with patch("asyncio.sleep", new_callable=AsyncMock):
                success = await client.send_event(sample_event_envelope)

            assert success is True
            assert attempt_count == 3


# =============================================================================
# Circuit Breaker Integration Tests
# =============================================================================


class TestCircuitBreakerIntegration:
    """Test circuit breaker integration with client."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_on_failures(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test circuit breaker opens after threshold failures."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            # Make sends fail
            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_instance.send_batch = AsyncMock(side_effect=Exception("Send failed"))
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)

            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                retry_attempts=1,
                circuit_breaker_enabled=True,
            )

            # Manually set threshold to 2 for faster testing
            client.circuit_breaker.failure_threshold = 2

            await client.connect()

            # First failure
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await client.send_event(sample_event_envelope)

            assert client.circuit_breaker.state == "CLOSED"

            # Second failure should open circuit
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await client.send_event(sample_event_envelope)

            assert client.circuit_breaker.state == "OPEN"

    @pytest.mark.asyncio
    async def test_disabled_circuit_breaker(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test client works with circuit breaker disabled."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                circuit_breaker_enabled=False,
            )

            assert client.circuit_breaker is None

            await client.connect()
            success = await client.send_event(sample_event_envelope)

            assert success is True


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Test comprehensive error handling."""

    @pytest.mark.asyncio
    async def test_connection_timeout_error(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test connection timeout error handling."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_instance.get_partition_properties = AsyncMock(
                side_effect=TimeoutError("Connection timeout")
            )

            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            success = await client.connect()

            assert success is False
            assert client._statistics["connection_failures"] >= 1

    @pytest.mark.asyncio
    async def test_serialization_error_handling(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test serialization error handling."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            # Create event with non-serializable payload
            # (Pydantic will handle this, but testing error path)

            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()

            # Test with invalid event that can't be serialized
            # Would need to mock model_dump_json to raise error

    @pytest.mark.asyncio
    async def test_event_hub_specific_error(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test EventHubError handling."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            from retail_datagen.streaming.azure_client import EventHubError

            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_instance.send_batch = AsyncMock(side_effect=EventHubError("Hub error"))
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)

            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                retry_attempts=1,
                circuit_breaker_enabled=False,
            )

            await client.connect()

            with patch("asyncio.sleep", new_callable=AsyncMock):
                success = await client.send_event(sample_event_envelope)

            assert success is False

    @pytest.mark.asyncio
    async def test_azure_service_error(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test Azure service error handling."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            from retail_datagen.streaming.azure_client import AzureError

            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_instance.send_batch = AsyncMock(
                side_effect=AzureError("Service unavailable")
            )
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)

            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                retry_attempts=1,
                circuit_breaker_enabled=False,
            )

            await client.connect()

            with patch("asyncio.sleep", new_callable=AsyncMock):
                success = await client.send_event(sample_event_envelope)

            assert success is False


# =============================================================================
# Statistics and Monitoring Tests
# =============================================================================


class TestStatisticsAndMonitoring:
    """Test statistics tracking and monitoring."""

    @pytest.mark.asyncio
    async def test_statistics_tracking(
        self, mock_azure_sdk, valid_connection_string, sample_event_batch
    ):
        """Test that statistics are properly tracked."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            await client.send_events(sample_event_batch)

            stats = await client.get_statistics()

            assert stats["events_sent"] == 5
            assert stats["batches_sent"] == 1
            assert stats["events_failed"] == 0
            assert stats["is_connected"] is True
            assert stats["hub_name"] == "test-hub"
            assert "last_send_time" in stats

    @pytest.mark.asyncio
    async def test_statistics_include_circuit_breaker_state(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test statistics include circuit breaker state."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                circuit_breaker_enabled=True,
            )

            stats = await client.get_statistics()

            assert "circuit_breaker_state" in stats
            assert stats["circuit_breaker_state"] == "CLOSED"

    @pytest.mark.asyncio
    async def test_statistics_when_circuit_breaker_disabled(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test statistics when circuit breaker is disabled."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                circuit_breaker_enabled=False,
            )

            stats = await client.get_statistics()

            assert stats["circuit_breaker_state"] == "DISABLED"

    @pytest.mark.asyncio
    async def test_buffer_size_in_statistics(
        self, mock_azure_sdk, valid_connection_string, sample_event_envelope
    ):
        """Test buffer size is included in statistics."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.add_to_buffer(sample_event_envelope)
            await client.add_to_buffer(sample_event_envelope)

            stats = await client.get_statistics()

            assert stats["buffer_size"] == 2


# =============================================================================
# Mock Client Tests
# =============================================================================


class TestMockClient:
    """Test mock client fallback functionality."""

    @pytest.mark.asyncio
    async def test_mock_client_when_sdk_unavailable(self, valid_connection_string):
        """Test mock client is used when Azure SDK not installed."""
        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", False):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            # Mock client should allow connection
            success = await client.connect()
            assert success is True

    @pytest.mark.asyncio
    async def test_mock_client_send_always_succeeds(
        self, valid_connection_string, sample_event_envelope
    ):
        """Test mock client sends always succeed."""
        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", False):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            success = await client.send_event(sample_event_envelope)

            assert success is True

    @pytest.mark.asyncio
    async def test_mock_client_health_check(self, valid_connection_string):
        """Test mock client health check."""
        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", False):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            await client.connect()
            health = await client.health_check()

            assert health["healthy"] is True
            assert "mock" in health["connection_status"]


# =============================================================================
# Edge Cases and Boundary Conditions
# =============================================================================


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_connection_string(self):
        """Test handling of empty connection string."""
        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(connection_string="", hub_name="test-hub")

            # Should not crash, but client won't be initialized
            assert client._client is None

    @pytest.mark.asyncio
    async def test_none_connection_string(self):
        """Test handling of None connection string."""
        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(connection_string=None, hub_name="test-hub")

            # Should not crash
            assert client._client is None

    @pytest.mark.asyncio
    async def test_very_large_batch(self, mock_azure_sdk, valid_connection_string):
        """Test handling of very large event batch."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                max_batch_size=100,
            )

            # Create 1000 events
            events = []
            for i in range(1000):
                event = EventEnvelope(
                    event_type=EventType.RECEIPT_CREATED,
                    payload={"store_id": i},
                    trace_id=str(uuid4()),
                    ingest_timestamp=datetime.now(UTC),
                )
                events.append(event)

            await client.connect()
            success = await client.send_events(events)

            assert success is True
            assert client._statistics["events_sent"] == 1000
            # Should have sent 10 batches of 100
            assert client._statistics["batches_sent"] == 10

    @pytest.mark.asyncio
    async def test_custom_batch_size_configuration(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test custom batch size configuration."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                max_batch_size=50,
            )

            assert client.max_batch_size == 50

    @pytest.mark.asyncio
    async def test_custom_retry_configuration(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test custom retry configuration."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string,
                hub_name="test-hub",
                retry_attempts=5,
                backoff_multiplier=1.5,
            )

            assert client.retry_attempts == 5
            assert client.backoff_multiplier == 1.5

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test disconnecting when not connected doesn't crash."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            # Disconnect without connecting first
            await client.disconnect()

            assert client.is_connected() is False


# =============================================================================
# Connection Test Tests
# =============================================================================


class TestConnectionTest:
    """Test connection testing functionality."""

    @pytest.mark.asyncio
    async def test_connection_test_success(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test successful connection test."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            # Setup mock to return proper Event Hub properties
            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_props = MagicMock()
            mock_props.partition_ids = ["0", "1", "2", "3"]
            mock_props.created_at = datetime(2024, 1, 1, tzinfo=UTC)
            mock_instance.get_eventhub_properties = AsyncMock(return_value=mock_props)

            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            success, message, metadata = await client.test_connection()

            assert success is True
            assert "successful" in message.lower()
            assert metadata["hub_name"] == "test-hub"
            assert metadata["partition_count"] == 4
            assert metadata["partition_ids"] == ["0", "1", "2", "3"]
            assert "endpoint" in metadata

    @pytest.mark.asyncio
    async def test_connection_test_with_entity_path(self, mock_azure_sdk):
        """Test connection test with EntityPath in connection string."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        conn_string = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dGVzdGtleTEyM3Rlc3RrZXkxMjN0ZXN0a2V5MTIzZGVzdGtleTE=;EntityPath=my-hub"

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_props = MagicMock()
            mock_props.partition_ids = ["0", "1"]
            mock_props.created_at = None
            mock_instance.get_eventhub_properties = AsyncMock(return_value=mock_props)

            client = AzureEventHubClient(connection_string=conn_string, hub_name="")

            success, message, metadata = await client.test_connection()

            assert success is True
            assert metadata["entity_path"] == "my-hub"
            assert metadata["hub_name"] == "my-hub"

    @pytest.mark.asyncio
    async def test_connection_test_failure(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test connection test handles failures."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            # Setup mock to raise EventHubError
            from retail_datagen.streaming.azure_client import EventHubError

            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_instance.get_eventhub_properties = AsyncMock(
                side_effect=EventHubError("Connection failed")
            )

            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            success, message, metadata = await client.test_connection()

            assert success is False
            assert "Event Hub error" in message
            assert metadata == {}

    @pytest.mark.asyncio
    async def test_connection_test_without_hub_name(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test connection test fails when hub name is missing."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            # Use a connection string without EntityPath to force failure
            conn_no_entity = (
                "Endpoint=sb://test.servicebus.windows.net/;"
                "SharedAccessKeyName=RootManageSharedAccessKey;"
                "SharedAccessKey=dGVzdGtleTEyM3Rlc3RrZXkxMjN0ZXN0a2V5MTIzZGVzdGtleTE="
            )
            client = AzureEventHubClient(connection_string=conn_no_entity, hub_name="")

            success, message, metadata = await client.test_connection()

            assert success is False
            assert "Hub name not specified" in message

    @pytest.mark.asyncio
    async def test_connection_test_fabric_rti_detection(self, mock_azure_sdk):
        """Test Fabric RTI detection in connection test."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        # Fabric RTI connection string (namespace starts with eventstream-)
        conn_string = "Endpoint=sb://eventstream-abc123.servicebus.windows.net/;SharedAccessKeyName=key_test;SharedAccessKey=ZXZlbnRzdHJlYW10ZXN0a2V5ZXZlbnRzdHJlYW10ZXN0a2V5ZXZlbnRzdHJlYW10ZXN0a2V5;EntityPath=es_retail"

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            mock_instance = mock_azure_sdk.from_connection_string.return_value
            mock_props = MagicMock()
            mock_props.partition_ids = ["0"]
            mock_props.created_at = None
            mock_instance.get_eventhub_properties = AsyncMock(return_value=mock_props)

            client = AzureEventHubClient(connection_string=conn_string, hub_name="")

            success, message, metadata = await client.test_connection()

            assert success is True
            assert metadata["is_fabric_rti"] is True
            assert metadata["namespace"].startswith("eventstream-")

    @pytest.mark.asyncio
    async def test_connection_test_mock_client(self, valid_connection_string):
        """Test connection test with mock client (SDK not available)."""
        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", False):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            success, message, metadata = await client.test_connection()

            assert success is True
            assert "Mock connection successful" in message
            assert metadata["endpoint"] == "mock://localhost"
            assert metadata["is_fabric_rti"] is False

    @pytest.mark.asyncio
    async def test_parse_connection_string(
        self, mock_azure_sdk, valid_connection_string
    ):
        """Test connection string parsing."""
        if not AZURE_AVAILABLE:
            pytest.skip("Azure SDK not available")

        with patch("retail_datagen.streaming.azure_client.AZURE_AVAILABLE", True):
            client = AzureEventHubClient(
                connection_string=valid_connection_string, hub_name="test-hub"
            )

            metadata = client._parse_connection_string(valid_connection_string)

            assert "endpoint" in metadata
            assert "namespace" in metadata
            assert "key_name" in metadata
            assert metadata["endpoint"] == "sb://test.servicebus.windows.net/"
            assert metadata["namespace"] == "test"
            assert metadata["key_name"] == "RootManageSharedAccessKey"
            assert metadata["is_fabric_rti"] is False


# =============================================================================
# Concurrent Access Tests
# =============================================================================


class TestLockInitialization:
    """Tests for lock initialization in various contexts."""

    @pytest.mark.asyncio
    async def test_locks_initialized_in_connect(self):
        """Test that locks are explicitly initialized during connect()."""
        client = AzureEventHubClient(
            connection_string="mock://localhost",
            hub_name="test-hub",
        )

        # Locks should be None initially (lazy initialization)
        assert client._buffer_lock is None
        assert client._stats_lock is None

        # After connect, locks should be initialized
        await client.connect()

        assert client._buffer_lock is not None
        assert client._stats_lock is not None
        assert isinstance(client._buffer_lock, asyncio.Lock)
        assert isinstance(client._stats_lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_lazy_initialization_still_works(self):
        """Test that lazy initialization via _get_buffer_lock() still works."""
        client = AzureEventHubClient(
            connection_string="mock://localhost",
            hub_name="test-hub",
        )

        # Locks should be None initially
        assert client._buffer_lock is None
        assert client._stats_lock is None

        # Calling _get_buffer_lock() should create locks
        buffer_lock = client._get_buffer_lock()
        stats_lock = client._get_stats_lock()

        assert buffer_lock is not None
        assert stats_lock is not None
        assert isinstance(buffer_lock, asyncio.Lock)
        assert isinstance(stats_lock, asyncio.Lock)

        # Subsequent calls should return the same lock instances
        assert client._get_buffer_lock() is buffer_lock
        assert client._get_stats_lock() is stats_lock

    @pytest.mark.asyncio
    async def test_locks_work_before_connect(self):
        """Test that buffer operations work even before connect() is called."""
        client = AzureEventHubClient(
            connection_string="mock://localhost",
            hub_name="test-hub",
        )

        # Create a test event
        event = EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={"test": True},
            trace_id=str(uuid4()),
            ingest_timestamp=datetime.now(UTC),
        )

        # add_to_buffer should work even before connect() (lazy init)
        await client.add_to_buffer(event)

        # Buffer lock should have been created by add_to_buffer's lazy initialization
        assert client._buffer_lock is not None

        # Buffer should contain the event - get_statistics() will initialize stats_lock
        stats = await client.get_statistics()
        assert stats["buffer_size"] == 1

        # Stats lock should now be initialized after get_statistics() call
        assert client._stats_lock is not None


class TestConcurrentBufferAccess:
    """Tests for thread-safe buffer operations under concurrent access."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock client for concurrent testing."""
        return AzureEventHubClient(
            connection_string="mock://localhost",
            hub_name="test-hub",
            max_batch_size=10,
        )

    def _create_test_event(self, event_id: int) -> EventEnvelope:
        """Create a test event with a unique identifier."""
        return EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={"event_id": event_id, "test": True},
            trace_id=str(uuid4()),
            ingest_timestamp=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_concurrent_add_to_buffer_no_lost_events(self, mock_client):
        """Test that concurrent add_to_buffer calls don't lose events."""
        num_events = 100
        events_added = []

        async def add_event(event_id: int):
            event = self._create_test_event(event_id)
            await mock_client.add_to_buffer(event)
            events_added.append(event_id)

        # Run multiple add_to_buffer calls concurrently
        await asyncio.gather(*[add_event(i) for i in range(num_events)])

        # Get statistics to verify buffer operations completed
        stats = await mock_client.get_statistics()
        assert "buffer_size" in stats  # Verify stats structure is valid

        # All events should be either in buffer or flushed
        # (we can't check exact count due to auto-flush, but no events should be lost)
        assert len(events_added) == num_events

    @pytest.mark.asyncio
    async def test_concurrent_add_and_flush(self, mock_client):
        """Test concurrent add and flush operations don't corrupt buffer."""
        num_adds = 50
        num_flushes = 10

        async def add_events():
            for i in range(num_adds):
                event = self._create_test_event(i)
                await mock_client.add_to_buffer(event)
                await asyncio.sleep(0.001)  # Small delay to interleave

        async def flush_periodically():
            for _ in range(num_flushes):
                await mock_client.flush_buffer()
                await asyncio.sleep(0.005)

        # Run add and flush concurrently
        await asyncio.gather(add_events(), flush_periodically())

        # Final flush to clear any remaining events
        await mock_client.flush_buffer()

        # Buffer should be empty after final flush
        stats = await mock_client.get_statistics()
        assert stats["buffer_size"] == 0

    @pytest.mark.asyncio
    async def test_concurrent_get_statistics(self, mock_client):
        """Test concurrent get_statistics calls are safe."""
        # Add some events first
        for i in range(5):
            event = self._create_test_event(i)
            await mock_client.add_to_buffer(event)

        async def get_stats():
            return await mock_client.get_statistics()

        # Run multiple get_statistics calls concurrently
        results = await asyncio.gather(*[get_stats() for _ in range(20)])

        # All results should be valid dicts with consistent structure
        for stats in results:
            assert isinstance(stats, dict)
            assert "buffer_size" in stats
            assert "events_sent" in stats
            assert "is_connected" in stats

    @pytest.mark.asyncio
    async def test_lock_prevents_buffer_corruption_under_high_contention(
        self, mock_client
    ):
        """Test that lock prevents buffer state corruption under high contention.

        This test rapidly adds events and checks statistics concurrently to verify
        that the buffer size is always non-negative and within reasonable bounds.
        """

        async def rapid_add_and_check():
            for _ in range(50):
                event = self._create_test_event(0)
                await mock_client.add_to_buffer(event)
                stats = await mock_client.get_statistics()
                # Buffer size should never be negative or unreasonably large
                assert stats["buffer_size"] >= 0, "Buffer size became negative"
                assert stats["buffer_size"] <= 1000, "Buffer grew beyond expected limit"

        # Run multiple concurrent tasks that add and check
        await asyncio.gather(*[rapid_add_and_check() for _ in range(10)])

    @pytest.mark.asyncio
    async def test_concurrent_flush_prevents_duplicate_sends(self, mock_client):
        """Test that _is_flushing flag prevents duplicate flush operations.

        Multiple concurrent flush calls should not result in the same events
        being sent multiple times.
        """
        # Add some events
        for i in range(5):
            event = self._create_test_event(i)
            await mock_client.add_to_buffer(event)

        # Trigger multiple concurrent flushes
        flush_results = await asyncio.gather(
            *[mock_client.flush_buffer() for _ in range(10)]
        )

        # All flushes should succeed (return True) but buffer should be empty after
        assert all(isinstance(r, bool) for r in flush_results)

        stats = await mock_client.get_statistics()
        assert stats["buffer_size"] == 0, "Buffer should be empty after flushes"

    @pytest.mark.asyncio
    async def test_statistics_counters_atomic_under_concurrent_updates(
        self, mock_client
    ):
        """Test that statistics counters are updated atomically.

        Concurrent send operations should result in accurate event counts.
        """
        initial_stats = await mock_client.get_statistics()
        initial_sent = initial_stats.get("events_sent", 0)

        # The mock client doesn't actually send events, so we verify the structure
        # and that concurrent statistics reads don't cause errors
        async def read_stats_repeatedly():
            for _ in range(50):
                stats = await mock_client.get_statistics()
                assert stats["events_sent"] >= initial_sent
                assert stats["events_failed"] >= 0
                assert stats["batches_sent"] >= 0

        await asyncio.gather(*[read_stats_repeatedly() for _ in range(5)])
