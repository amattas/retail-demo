"""
Unit tests for flexible connection string validation.

Tests the relaxed validation mode for test scenarios.
"""

import os

from retail_datagen.config.models import RetailConfig
from retail_datagen.shared.credential_utils import (
    validate_eventhub_connection_string,
)
from tests.test_utils import (
    FABRIC_RTI_CONNECTION_STRING,
    MOCK_CONNECTION_STRING,
    TEST_CONNECTION_STRING,
    TEST_PROTOCOL_STRING,
    create_test_connection_string,
)


class TestFlexibleValidation:
    """Test flexible validation modes for connection strings."""

    def test_strict_mode_rejects_short_connection_string(self):
        """Strict mode should reject connection strings that are too short."""
        short_conn = (
            "Endpoint=sb://test.servicebus.windows.net/;"
            "SharedAccessKeyName=key;SharedAccessKey=short"
        )

        is_valid, error = validate_eventhub_connection_string(
            short_conn, strict=True
        )

        assert not is_valid
        assert "too short" in error.lower() or "invalid" in error.lower()

    def test_non_strict_mode_allows_shorter_keys(self):
        """Non-strict mode should allow shorter keys for testing."""
        short_key_conn = (
            "Endpoint=sb://test.servicebus.windows.net/;"
            "SharedAccessKeyName=TestKey;"
            "SharedAccessKey=shortkey12;"
            "EntityPath=test-hub"
        )

        is_valid, error = validate_eventhub_connection_string(
            short_key_conn, strict=False, allow_mock=True
        )

        # Should pass in non-strict mode
        assert is_valid, f"Expected valid in non-strict mode, got error: {error}"

    def test_mock_protocol_allowed_with_allow_mock(self):
        """Mock protocol should be allowed when allow_mock=True."""
        is_valid, error = validate_eventhub_connection_string(
            MOCK_CONNECTION_STRING, strict=False, allow_mock=True
        )

        assert is_valid
        assert error == ""

    def test_test_protocol_allowed_with_allow_mock(self):
        """Test protocol should be allowed when allow_mock=True."""
        is_valid, error = validate_eventhub_connection_string(
            TEST_PROTOCOL_STRING, strict=False, allow_mock=True
        )

        assert is_valid
        assert error == ""

    def test_mock_protocol_rejected_without_allow_mock(self):
        """Mock protocol should be rejected when allow_mock=False."""
        is_valid, error = validate_eventhub_connection_string(
            MOCK_CONNECTION_STRING, strict=True, allow_mock=False
        )

        assert not is_valid
        # Mock string "mock://localhost/test-hub" is too short and lacks
        # proper structure
        assert any(
            phrase in error.lower()
            for phrase in ["missing required part", "endpoint", "too short"]
        )

    def test_valid_test_connection_string(self):
        """Test helper should create valid connection strings."""
        conn_str = TEST_CONNECTION_STRING

        is_valid, error = validate_eventhub_connection_string(
            conn_str, strict=False, allow_mock=True
        )

        assert is_valid, f"Expected valid connection string, got error: {error}"

    def test_fabric_rti_test_connection_string(self):
        """Test helper should create valid Fabric RTI connection strings."""
        conn_str = FABRIC_RTI_CONNECTION_STRING

        is_valid, error = validate_eventhub_connection_string(
            conn_str, strict=False, allow_mock=True
        )

        assert is_valid, (
            f"Expected valid Fabric RTI connection string, got error: {error}"
        )
        assert "eventstream-" in conn_str
        assert "es_" in conn_str

    def test_config_accepts_test_connection_in_test_mode(self):
        """RetailConfig should accept test connection strings in test mode."""
        # Test mode should already be set by conftest.py fixture
        assert os.getenv("RETAIL_DATAGEN_TEST_MODE") == "true"

        config_data = {
            "seed": 42,
            "volume": {
                "stores": 10,
                "dcs": 2,
                "customers_per_day": 100,
                "items_per_ticket_mean": 3.0,
            },
            "realtime": {
                "emit_interval_ms": 500,
                "burst": 100,
                "azure_connection_string": TEST_CONNECTION_STRING,
            },
            "paths": {
                "dict": "data/dictionaries",
                "master": "data/master",
                "facts": "data/facts",
            },
            "stream": {"hub": "test-hub"},
        }

        # Should not raise validation error in test mode
        config = RetailConfig(**config_data)
        assert config.realtime.azure_connection_string == TEST_CONNECTION_STRING

    def test_config_accepts_mock_connection_in_test_mode(self):
        """RetailConfig should accept mock connection strings in test mode."""
        config_data = {
            "seed": 42,
            "volume": {
                "stores": 10,
                "dcs": 2,
                "customers_per_day": 100,
                "items_per_ticket_mean": 3.0,
            },
            "realtime": {
                "emit_interval_ms": 500,
                "burst": 100,
                "azure_connection_string": MOCK_CONNECTION_STRING,
            },
            "paths": {
                "dict": "data/dictionaries",
                "master": "data/master",
                "facts": "data/facts",
            },
            "stream": {"hub": "test-hub"},
        }

        # Should not raise validation error
        config = RetailConfig(**config_data)
        assert config.realtime.azure_connection_string == MOCK_CONNECTION_STRING

    def test_create_custom_test_connection_string(self):
        """Test creating custom connection strings with the helper."""
        custom_conn = create_test_connection_string(
            namespace="mytest",
            key_name="MyKey",
            key="customkey123",
            entity_path="my-hub",
        )

        assert "mytest.servicebus.windows.net" in custom_conn
        assert "SharedAccessKeyName=MyKey" in custom_conn
        assert "SharedAccessKey=customkey123" in custom_conn
        assert "EntityPath=my-hub" in custom_conn

        # Should be valid in non-strict mode
        is_valid, error = validate_eventhub_connection_string(
            custom_conn, strict=False, allow_mock=True
        )
        assert is_valid, (
            f"Custom connection string should be valid, got error: {error}"
        )

    def test_non_strict_allows_non_standard_domains(self):
        """Non-strict mode should allow non-standard servicebus domains."""
        custom_domain_conn = (
            "Endpoint=sb://test.servicebus.custom.net/;"
            "SharedAccessKeyName=TestKey;"
            "SharedAccessKey=testkey12345;"
            "EntityPath=test-hub"
        )

        # Strict mode should reject
        is_valid_strict, _ = validate_eventhub_connection_string(
            custom_domain_conn, strict=True, allow_mock=False
        )
        assert not is_valid_strict

        # Non-strict mode should allow (as long as it has .servicebus.)
        is_valid_relaxed, error = validate_eventhub_connection_string(
            custom_domain_conn, strict=False, allow_mock=True
        )
        assert is_valid_relaxed, (
            f"Non-strict should allow custom domains, got: {error}"
        )

    def test_empty_connection_string_always_invalid(self):
        """Empty connection strings should always be invalid."""
        is_valid_strict, error_strict = validate_eventhub_connection_string(
            "", strict=True, allow_mock=False
        )
        is_valid_relaxed, error_relaxed = validate_eventhub_connection_string(
            "", strict=False, allow_mock=True
        )

        assert not is_valid_strict
        assert not is_valid_relaxed
        assert "empty" in error_strict.lower()
        assert "empty" in error_relaxed.lower()

    def test_whitespace_only_connection_string_invalid(self):
        """Whitespace-only connection strings should always be invalid."""
        is_valid, error = validate_eventhub_connection_string(
            "   ", strict=False, allow_mock=True
        )

        assert not is_valid
        assert "whitespace" in error.lower()
