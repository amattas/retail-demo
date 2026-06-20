"""Unit tests for structured logging functionality."""

import json
import logging

from retail_datagen.shared.logging_utils import (
    StructuredLogger,
    get_structured_logger,
)


class TestStructuredLogger:
    """Test structured logging functionality."""

    def test_generate_correlation_id(self):
        """Test correlation ID generation."""
        logger = get_structured_logger("test")
        corr_id = logger.generate_correlation_id()

        assert corr_id.startswith("CORR_")
        assert len(corr_id) == 17  # CORR_ + 12 hex chars

    def test_set_and_clear_correlation_id(self):
        """Test setting and clearing correlation IDs."""
        logger = get_structured_logger("test")

        # Initially None
        assert logger._correlation_id is None

        # Set correlation ID
        test_id = "TEST_123"
        logger.set_correlation_id(test_id)
        assert logger._correlation_id == test_id

        # Clear correlation ID
        logger.clear_correlation_id()
        assert logger._correlation_id is None

    def test_structured_log_format(self, caplog):
        """Test that logs are formatted as JSON with correct fields."""
        logger = get_structured_logger("test.module")
        logger.set_correlation_id("TEST_CORR_123")

        with caplog.at_level(logging.INFO):
            logger.info("Test message", key1="value1", key2=42)

        # Check that log was created
        assert len(caplog.records) == 1

        # Parse JSON from log message
        log_record = caplog.records[0]
        log_data = json.loads(log_record.message)

        # Verify structure
        assert log_data["level"] == "INFO"
        assert log_data["message"] == "Test message"
        assert log_data["correlation_id"] == "TEST_CORR_123"
        assert "timestamp" in log_data
        assert "context" in log_data
        assert log_data["context"]["key1"] == "value1"
        assert log_data["context"]["key2"] == 42

    def test_log_without_correlation_id(self, caplog):
        """Test logging without a correlation ID set."""
        logger = get_structured_logger("test.module")

        with caplog.at_level(logging.INFO):
            logger.info("Test message without correlation")

        log_record = caplog.records[0]
        log_data = json.loads(log_record.message)

        assert log_data["correlation_id"] == "none"

    def test_different_log_levels(self, caplog):
        """Test all log levels."""
        logger = get_structured_logger("test.module")

        with caplog.at_level(logging.DEBUG):
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")

        assert len(caplog.records) == 4

        levels = [json.loads(r.message)["level"] for r in caplog.records]
        assert levels == ["DEBUG", "INFO", "WARNING", "ERROR"]

    def test_logging_with_complex_context(self, caplog):
        """Test logging with complex context data."""
        logger = get_structured_logger("test.module")

        complex_data = {
            "list": [1, 2, 3],
            "nested": {"key": "value"},
            "number": 123.45,
            "boolean": True,
        }

        with caplog.at_level(logging.INFO):
            logger.info("Complex context test", **complex_data)

        log_record = caplog.records[0]
        log_data = json.loads(log_record.message)

        assert log_data["context"]["list"] == [1, 2, 3]
        assert log_data["context"]["nested"] == {"key": "value"}
        assert log_data["context"]["number"] == 123.45
        assert log_data["context"]["boolean"] is True

    def test_multiple_loggers_independent(self):
        """Test that multiple loggers have independent correlation IDs."""
        logger1 = get_structured_logger("module1")
        logger2 = get_structured_logger("module2")

        logger1.set_correlation_id("CORR_1")
        logger2.set_correlation_id("CORR_2")

        assert logger1._correlation_id == "CORR_1"
        assert logger2._correlation_id == "CORR_2"

    def test_logger_name_preserved(self):
        """Test that logger name is preserved."""
        logger = get_structured_logger("my.custom.module")
        assert logger.logger.name == "my.custom.module"


class TestEventEnvelopeCorrelationFields:
    """Test that EventEnvelope supports correlation tracking fields."""

    def test_event_envelope_has_correlation_fields(self):
        """Test EventEnvelope has session_id and parent_event_id fields."""
        from datetime import UTC, datetime

        from retail_datagen.streaming.schemas import EventEnvelope, EventType

        # Create event envelope with correlation fields
        event = EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={"test": "data"},
            trace_id="TRACE_123",
            ingest_timestamp=datetime.now(UTC),
            correlation_id="CORR_ABC",
            session_id="SESSION_XYZ",
            parent_event_id="PARENT_456",
        )

        assert event.correlation_id == "CORR_ABC"
        assert event.session_id == "SESSION_XYZ"
        assert event.parent_event_id == "PARENT_456"

    def test_event_envelope_correlation_fields_optional(self):
        """Test that correlation fields are optional."""
        from datetime import UTC, datetime

        from retail_datagen.streaming.schemas import EventEnvelope, EventType

        # Create event without correlation fields
        event = EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={"test": "data"},
            trace_id="TRACE_123",
            ingest_timestamp=datetime.now(UTC),
        )

        assert event.correlation_id is None
        assert event.session_id is None
        assert event.parent_event_id is None


class TestAzureClientStructuredLogging:
    """Test Azure client uses structured logging."""

    def test_azure_client_has_structured_logger(self):
        """Test AzureEventHubClient initializes structured logger."""
        from retail_datagen.streaming.azure_client import AzureEventHubClient

        client = AzureEventHubClient(
            connection_string="mock://test", hub_name="test-hub"
        )

        assert hasattr(client, "log")
        assert isinstance(client.log, StructuredLogger)

    def test_azure_client_logs_initialization(self, caplog):
        """Test that Azure client logs initialization with structured logging."""
        from retail_datagen.streaming.azure_client import AzureEventHubClient

        with caplog.at_level(logging.INFO):
            AzureEventHubClient(connection_string="mock://test", hub_name="test-hub")

        # Should have logged initialization
        assert len(caplog.records) > 0

        # Check one of the log records is JSON structured
        json_logs = [r for r in caplog.records if r.message.startswith("{")]
        assert len(json_logs) > 0

        # Parse first JSON log
        log_data = json.loads(json_logs[0].message)
        assert "timestamp" in log_data
        assert "level" in log_data
        assert "message" in log_data


class TestLoggingConfiguration:
    """Test logging configuration."""

    def test_configure_structured_logging(self):
        """Test logging configuration function."""
        from retail_datagen.shared.logging_config import (
            configure_structured_logging,
        )

        # Should not raise an error
        configure_structured_logging(level="INFO")

        # Verify root logger level
        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO

    def test_configure_with_different_levels(self):
        """Test configuration with different log levels."""
        from retail_datagen.shared.logging_config import (
            configure_structured_logging,
        )

        configure_structured_logging(level="DEBUG")
        assert logging.getLogger().level == logging.DEBUG

        configure_structured_logging(level="WARNING")
        assert logging.getLogger().level == logging.WARNING
