"""
Unit tests for credential utility functions.

Tests validation, sanitization, and metadata extraction for
Event Hub connection strings.
"""

from retail_datagen.shared.credential_utils import (
    get_connection_string_metadata,
    is_fabric_rti_connection_string,
    sanitize_connection_string,
    validate_eventhub_connection_string,
    validate_fabric_rti_specific,
)


class TestValidateEventHubConnectionString:
    """Tests for connection string validation."""

    def test_valid_standard_connection_string(self):
        """Test validation of a standard Azure Event Hub connection string."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789LongEnoughKey=="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is True
        assert error == ""

    def test_valid_connection_string_with_entity_path(self):
        """Test validation with EntityPath component."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789LongEnoughKey==;"
            "EntityPath=retail-events"
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is True
        assert error == ""

    def test_valid_fabric_rti_connection_string(self):
        """Test validation of Microsoft Fabric RTI connection string."""
        conn_str = (
            "Endpoint=sb://eventstream-abcd1234.servicebus.windows.net/;"
            "SharedAccessKeyName=key_123456;"
            "SharedAccessKey=xyz789abc123LongEnoughKey==;"
            "EntityPath=es_fabric_stream"
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is True
        assert error == ""

    def test_valid_china_cloud_connection_string(self):
        """Test validation for Azure China cloud."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.chinacloudapi.cn/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789LongEnoughKey=="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is True
        assert error == ""

    def test_empty_connection_string(self):
        """Test validation fails for empty string."""
        is_valid, error = validate_eventhub_connection_string("")
        assert is_valid is False
        assert "empty" in error.lower()

    def test_whitespace_only_connection_string(self):
        """Test validation fails for whitespace-only string."""
        is_valid, error = validate_eventhub_connection_string("   ")
        assert is_valid is False
        assert "whitespace" in error.lower()

    def test_missing_endpoint(self):
        """Test validation fails when Endpoint is missing."""
        conn_str = (
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789=="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "Endpoint=sb://" in error

    def test_missing_key_name(self):
        """Test validation fails when SharedAccessKeyName is missing."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKey=abc123xyz789LongEnoughKey=="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "Shared access key name" in error

    def test_missing_key(self):
        """Test validation fails when SharedAccessKey is missing."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey"
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "Shared access key" in error

    def test_invalid_endpoint_domain(self):
        """Test validation fails with invalid endpoint domain."""
        conn_str = (
            "Endpoint=sb://test-namespace.invalid-domain.com/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789=="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "endpoint domain" in error.lower()

    def test_invalid_endpoint_protocol(self):
        """Test validation fails when endpoint doesn't use sb:// protocol."""
        conn_str = (
            "Endpoint=https://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789=="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "sb://" in error

    def test_empty_key_name(self):
        """Test validation fails when key name is empty."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=;"
            "SharedAccessKey=abc123xyz789=="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "SharedAccessKeyName cannot be empty" in error

    def test_empty_key(self):
        """Test validation fails when key is empty."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "SharedAccessKey cannot be empty" in error

    def test_empty_entity_path(self):
        """Test validation fails when EntityPath is empty."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789LongEnoughKey==;"
            "EntityPath="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "EntityPath cannot be empty" in error


class TestSanitizeConnectionString:
    """Tests for connection string sanitization."""

    def test_sanitize_standard_connection_string(self):
        """Test sanitization redacts SharedAccessKey."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=SecretKeyValue123"
        )
        sanitized = sanitize_connection_string(conn_str)

        assert "***REDACTED***" in sanitized
        assert "SecretKeyValue123" not in sanitized
        assert "test-namespace.servicebus.windows.net" in sanitized
        assert "RootManageSharedAccessKey" in sanitized

    def test_sanitize_preserves_structure(self):
        """Test sanitization preserves connection string structure."""
        conn_str = (
            "Endpoint=sb://test.servicebus.windows.net/;"
            "SharedAccessKeyName=TestKey;"
            "SharedAccessKey=Secret123;"
            "EntityPath=test-hub"
        )
        sanitized = sanitize_connection_string(conn_str)

        assert sanitized.count(";") == conn_str.count(";")
        assert "Endpoint=" in sanitized
        assert "SharedAccessKeyName=" in sanitized
        assert "EntityPath=" in sanitized

    def test_sanitize_empty_string(self):
        """Test sanitization of empty string."""
        sanitized = sanitize_connection_string("")
        assert sanitized == "[empty]"

    def test_sanitize_whitespace_only(self):
        """Test sanitization of whitespace-only string."""
        sanitized = sanitize_connection_string("   ")
        assert sanitized == "[whitespace-only]"

    def test_sanitize_case_insensitive(self):
        """Test sanitization works with different casing."""
        conn_str = (
            "Endpoint=sb://test.servicebus.windows.net/;"
            "SharedAccessKeyName=TestKey;"
            "sharedaccesskey=Secret123"  # lowercase
        )
        sanitized = sanitize_connection_string(conn_str)
        assert "Secret123" not in sanitized
        assert "***REDACTED***" in sanitized


class TestGetConnectionStringMetadata:
    """Tests for connection string metadata extraction."""

    def test_extract_standard_metadata(self):
        """Test metadata extraction from standard connection string."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789LongEnoughKey==;"
            "EntityPath=retail-events"
        )
        metadata = get_connection_string_metadata(conn_str)

        assert metadata["endpoint"] == "sb://test-namespace.servicebus.windows.net/"
        assert metadata["key_name"] == "RootManageSharedAccessKey"
        assert metadata["entity_path"] == "retail-events"
        assert metadata["has_key"] is True
        assert metadata["is_valid"] is True

    def test_extract_metadata_without_entity_path(self):
        """Test metadata extraction when EntityPath is not present."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789LongEnoughKey=="
        )
        metadata = get_connection_string_metadata(conn_str)

        assert metadata["endpoint"] is not None
        assert metadata["key_name"] is not None
        assert metadata["entity_path"] is None
        assert metadata["has_key"] is True
        assert metadata["is_valid"] is True

    def test_extract_metadata_empty_string(self):
        """Test metadata extraction from empty string."""
        metadata = get_connection_string_metadata("")

        assert metadata["endpoint"] is None
        assert metadata["key_name"] is None
        assert metadata["entity_path"] is None
        assert metadata["has_key"] is False
        assert metadata["is_valid"] is False

    def test_extract_metadata_invalid_string(self):
        """Test metadata extraction from invalid connection string."""
        conn_str = "InvalidConnectionString"
        metadata = get_connection_string_metadata(conn_str)

        assert metadata["is_valid"] is False

    def test_metadata_no_key_exposure(self):
        """Test that metadata never exposes the actual key value."""
        conn_str = (
            "Endpoint=sb://test.servicebus.windows.net/;"
            "SharedAccessKeyName=TestKey;"
            "SharedAccessKey=SuperSecretValue123"
        )
        metadata = get_connection_string_metadata(conn_str)

        # Ensure the actual key value is not in any metadata field
        metadata_str = str(metadata)
        assert "SuperSecretValue123" not in metadata_str
        assert metadata["has_key"] is True


class TestIsFabricRTIConnectionString:
    """Tests for Fabric RTI connection string detection."""

    def test_detect_fabric_rti_connection_string(self):
        """Test detection of Fabric RTI connection string."""
        conn_str = (
            "Endpoint=sb://eventstream-abcd1234.servicebus.windows.net/;"
            "SharedAccessKeyName=key_123456;"
            "SharedAccessKey=xyz789abc123==;"
            "EntityPath=es_fabric_stream"
        )
        is_fabric = is_fabric_rti_connection_string(conn_str)
        assert is_fabric is True

    def test_detect_standard_azure_connection_string(self):
        """Test that standard Azure connection string is not detected as Fabric."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789==;"
            "EntityPath=retail-events"
        )
        is_fabric = is_fabric_rti_connection_string(conn_str)
        assert is_fabric is False

    def test_detect_fabric_with_eventstream_subdomain(self):
        """Test detection based on eventstream subdomain."""
        conn_str = (
            "Endpoint=sb://eventstream-xyz.servicebus.windows.net/;"
            "SharedAccessKeyName=key_abc;"
            "SharedAccessKey=secret123"
        )
        is_fabric = is_fabric_rti_connection_string(conn_str)
        assert is_fabric is True

    def test_detect_fabric_with_es_prefix(self):
        """Test detection based on es_ entity path prefix."""
        conn_str = (
            "Endpoint=sb://eventstream-xyz.servicebus.windows.net/;"
            "SharedAccessKeyName=TestKey;"
            "SharedAccessKey=secret123;"
            "EntityPath=es_mystream"
        )
        is_fabric = is_fabric_rti_connection_string(conn_str)
        assert is_fabric is True

    def test_empty_string_not_fabric(self):
        """Test that empty string is not detected as Fabric."""
        is_fabric = is_fabric_rti_connection_string("")
        assert is_fabric is False

    def test_partial_match_not_fabric(self):
        """Test that partial matches (only 1 indicator) don't trigger detection."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789==;"
            "EntityPath=es_test"  # Only 1 indicator
        )
        is_fabric = is_fabric_rti_connection_string(conn_str)
        assert is_fabric is False


class TestValidateFabricRTISpecific:
    """Tests for Fabric RTI specific validation."""

    def test_valid_fabric_rti_connection(self):
        """Test validation of valid Fabric RTI connection string."""
        conn_str = (
            "Endpoint=sb://eventstream-abcd1234.servicebus.windows.net/;"
            "SharedAccessKeyName=key_123456;"
            "SharedAccessKey=xyz789abc123longenoughkey==;"
            "EntityPath=es_fabric_stream"
        )
        is_valid, message, metadata = validate_fabric_rti_specific(conn_str)
        assert is_valid is True
        assert "Valid Fabric RTI" in message
        assert metadata["is_fabric_rti"] is True
        assert metadata["entity_path"] == "es_fabric_stream"
        assert metadata["namespace"] == "eventstream-abcd1234"

    def test_standard_eventhub_not_fabric(self):
        """Test that standard Event Hub connection is not flagged as Fabric RTI."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789longenoughkey=="
        )
        is_valid, message, metadata = validate_fabric_rti_specific(conn_str)
        assert is_valid is True
        assert "Not a Fabric RTI" in message
        assert metadata["is_fabric_rti"] is False

    def test_fabric_rti_missing_entity_path(self):
        """Test Fabric RTI validation fails without EntityPath."""
        conn_str = (
            "Endpoint=sb://eventstream-abcd1234.servicebus.windows.net/;"
            "SharedAccessKeyName=key_123456;"
            "SharedAccessKey=xyz789abc123longenoughkey=="
        )
        is_valid, message, metadata = validate_fabric_rti_specific(conn_str)
        assert is_valid is False
        assert "require EntityPath" in message

    def test_fabric_rti_invalid_entity_path_prefix(self):
        """Test Fabric RTI validation fails with wrong EntityPath prefix."""
        conn_str = (
            "Endpoint=sb://eventstream-abcd1234.servicebus.windows.net/;"
            "SharedAccessKeyName=key_123456;"
            "SharedAccessKey=xyz789abc123longenoughkey==;"
            "EntityPath=wrong_prefix_stream"
        )
        is_valid, message, metadata = validate_fabric_rti_specific(conn_str)
        assert is_valid is False
        assert "should start with 'es_'" in message

    def test_fabric_rti_invalid_namespace(self):
        """Test Fabric RTI validation warns about incorrect namespace format."""
        conn_str = (
            "Endpoint=sb://wrong-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=key_123456;"
            "SharedAccessKey=xyz789abc123longenoughkey==;"
            "EntityPath=es_fabric_stream"
        )
        is_valid, message, metadata = validate_fabric_rti_specific(conn_str)
        assert is_valid is False
        assert "should start with 'eventstream-'" in message


class TestConnectionStringMetadataEnhancements:
    """Tests for enhanced metadata extraction."""

    def test_metadata_includes_namespace(self):
        """Test that metadata extraction includes namespace."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789longenoughkey=="
        )
        metadata = get_connection_string_metadata(conn_str)
        assert metadata["namespace"] == "test-namespace"

    def test_metadata_includes_is_fabric_rti(self):
        """Test that metadata includes is_fabric_rti flag."""
        conn_str = (
            "Endpoint=sb://eventstream-abcd1234.servicebus.windows.net/;"
            "SharedAccessKeyName=key_123456;"
            "SharedAccessKey=xyz789abc123longenoughkey==;"
            "EntityPath=es_fabric_stream"
        )
        metadata = get_connection_string_metadata(conn_str)
        assert metadata["is_fabric_rti"] is True

    def test_metadata_fabric_rti_false_for_standard(self):
        """Test that is_fabric_rti is False for standard Event Hub."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc123xyz789longenoughkey=="
        )
        metadata = get_connection_string_metadata(conn_str)
        assert metadata["is_fabric_rti"] is False


class TestValidationEnhancements:
    """Tests for enhanced validation features."""

    def test_validation_checks_minimum_length(self):
        """Test that validation rejects very short connection strings."""
        conn_str = "Endpoint=sb://test"
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "too short" in error.lower()

    def test_validation_checks_key_length(self):
        """Test that validation checks SharedAccessKey length."""
        conn_str = (
            "Endpoint=sb://test-namespace.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=short"
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "too short" in error.lower()

    def test_fabric_rti_requires_entity_path(self):
        """Test that Fabric RTI connections require EntityPath."""
        # This connection string looks like Fabric RTI (eventstream- prefix)
        # but is missing EntityPath
        conn_str = (
            "Endpoint=sb://eventstream-abcd1234.servicebus.windows.net/;"
            "SharedAccessKeyName=key_123456;"
            "SharedAccessKey=xyz789abc123longenoughkey=="
        )
        is_valid, error = validate_eventhub_connection_string(conn_str)
        assert is_valid is False
        assert "Fabric RTI" in error
        assert "EntityPath" in error
