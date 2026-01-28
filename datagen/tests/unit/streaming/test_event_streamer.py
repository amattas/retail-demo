"""
Unit tests for EventStreamer class (batch streaming mode).

Tests cover:
- Initialization and configuration
- Batch streaming from DuckDB
- Error handling
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from retail_datagen.config.models import RetailConfig
from retail_datagen.streaming.event_streaming import EventStreamer, StreamingConfig


# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def mock_config():
    """Create a minimal RetailConfig for testing."""
    return RetailConfig(
        seed=42,
        volume={
            "stores": 10,
            "dcs": 2,
            "customers_per_day": 100,
            "items_per_ticket_mean": 3.0,
        },
        realtime={
            "emit_interval_ms": 100,
            "burst": 10,
            "azure_connection_string": "Endpoint=sb://testnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleQ==;EntityPath=test-eventhub",
            "max_batch_size": 50,
            "batch_timeout_ms": 500,
            "retry_attempts": 2,
            "circuit_breaker_enabled": True,
            "monitoring_interval": 1,
        },
        paths={
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
        stream={"hub": "test-retail-events"},
    )


# ============================================================================
# Test Class: Initialization
# ============================================================================


class TestEventStreamerInitialization:
    """Test EventStreamer initialization."""

    def test_initialization_with_config(self, mock_config):
        """Test streamer initializes with minimal config."""
        streamer = EventStreamer(mock_config)

        assert streamer is not None
        assert streamer.config == mock_config
        assert isinstance(streamer.streaming_config, StreamingConfig)
        assert streamer.streaming_config.hub_name == "test-retail-events"

    def test_initialization_with_connection_string_override(self, mock_config):
        """Test connection string can be overridden at initialization."""
        custom_connection = "Endpoint=sb://customnamespace.servicebus.windows.net/;SharedAccessKeyName=CustomKey;SharedAccessKey=Y3VzdG9ta2V5Y3VzdG9ta2V5Y3VzdG9ta2V5Y3VzdG9taw==;EntityPath=custom-hub"
        streamer = EventStreamer(mock_config, azure_connection_string=custom_connection)

        assert streamer.streaming_config.azure_connection_string == custom_connection

    def test_streaming_config_from_retail_config(self, mock_config):
        """Test StreamingConfig properly extracts values from RetailConfig."""
        streaming_config = StreamingConfig.from_retail_config(mock_config)

        assert streaming_config.hub_name == "test-retail-events"
        assert (
            streaming_config.azure_connection_string
            == "Endpoint=sb://testnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleQ==;EntityPath=test-eventhub"
        )
        assert streaming_config.max_batch_size == 50
        assert streaming_config.batch_timeout_ms == 500
        assert streaming_config.retry_attempts == 2
        assert streaming_config.circuit_breaker_enabled is True

    def test_duckdb_connection_initialized(self, mock_config):
        """Test DuckDB connection is initialized."""
        with patch(
            "retail_datagen.db.duckdb_engine.get_duckdb_conn"
        ) as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn

            streamer = EventStreamer(mock_config)

            assert streamer._duckdb_conn == mock_conn
            mock_get_conn.assert_called_once()

    def test_duckdb_connection_failure_handled(self, mock_config):
        """Test DuckDB connection failure is handled gracefully."""
        with patch(
            "retail_datagen.db.duckdb_engine.get_duckdb_conn",
            side_effect=Exception("Connection failed"),
        ):
            streamer = EventStreamer(mock_config)

            # Should not raise, connection should be None
            assert streamer._duckdb_conn is None


# ============================================================================
# Test Class: Batch Streaming
# ============================================================================


class TestBatchStreaming:
    """Test batch streaming functionality."""

    @pytest.mark.asyncio
    async def test_start_calls_batch_streaming(self, mock_config):
        """Test that start() delegates to batch streaming."""
        streamer = EventStreamer(mock_config)

        # Mock the batch streaming manager
        streamer._batch_streaming_manager.start_batch_streaming_duckdb = AsyncMock(
            return_value=True
        )

        result = await streamer.start()

        assert result is True
        streamer._batch_streaming_manager.start_batch_streaming_duckdb.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_batch_streaming_duckdb_success(self, mock_config):
        """Test successful batch streaming from DuckDB."""
        streamer = EventStreamer(mock_config)

        # Mock the batch streaming manager
        streamer._batch_streaming_manager.start_batch_streaming_duckdb = AsyncMock(
            return_value=True
        )

        result = await streamer.start_batch_streaming_duckdb()

        assert result is True
        streamer._batch_streaming_manager.start_batch_streaming_duckdb.assert_called_once_with(
            streamer._duckdb_conn, streamer.streaming_config.azure_connection_string
        )

    @pytest.mark.asyncio
    async def test_start_batch_streaming_duckdb_failure(self, mock_config):
        """Test batch streaming handles failures gracefully."""
        streamer = EventStreamer(mock_config)

        # Mock the batch streaming manager to return failure
        streamer._batch_streaming_manager.start_batch_streaming_duckdb = AsyncMock(
            return_value=False
        )

        result = await streamer.start_batch_streaming_duckdb()

        assert result is False

    @pytest.mark.asyncio
    async def test_batch_streaming_with_no_connection(self, mock_config):
        """Test batch streaming when DuckDB connection is None."""
        streamer = EventStreamer(mock_config)
        streamer._duckdb_conn = None

        # Mock the batch streaming manager
        streamer._batch_streaming_manager.start_batch_streaming_duckdb = AsyncMock(
            return_value=False
        )

        result = await streamer.start_batch_streaming_duckdb()

        # Should call the manager with None connection
        assert result is False
        streamer._batch_streaming_manager.start_batch_streaming_duckdb.assert_called_once_with(
            None, streamer.streaming_config.azure_connection_string
        )


# ============================================================================
# Test Class: Configuration
# ============================================================================


class TestConfiguration:
    """Test configuration handling."""

    def test_session_id_generated(self, mock_config):
        """Test that a session ID is generated during initialization."""
        streamer = EventStreamer(mock_config)

        assert streamer._session_id is not None
        assert len(streamer._session_id) > 0

    def test_logger_initialized(self, mock_config):
        """Test that logger is initialized."""
        streamer = EventStreamer(mock_config)

        assert streamer.log is not None

    def test_batch_streaming_manager_initialized(self, mock_config):
        """Test that batch streaming manager is initialized."""
        streamer = EventStreamer(mock_config)

        assert streamer._batch_streaming_manager is not None


# ============================================================================
# Summary of Test Coverage
# ============================================================================
"""
Test Coverage Summary:

1. Initialization (5 tests)
   - Basic initialization with config
   - Connection string override
   - StreamingConfig extraction
   - DuckDB connection initialization
   - DuckDB connection failure handling

2. Batch Streaming (4 tests)
   - start() delegates to batch streaming
   - Successful batch streaming
   - Batch streaming failure handling
   - Batch streaming with no connection

3. Configuration (3 tests)
   - Session ID generation
   - Logger initialization
   - Batch streaming manager initialization

Total: 12 tests covering batch streaming mode
"""
