"""
Credential utility functions for secure connection string handling.

This module provides utilities for validating, sanitizing, and managing
Azure Event Hub connection strings and other credentials.
"""

import re


def validate_eventhub_connection_string(
    conn_str: str, strict: bool = True, allow_mock: bool = False
) -> tuple[bool, str]:
    """
    Validate Event Hub or Microsoft Fabric RTI connection string format.

    Supports both formats:
    - Azure Event Hub: Endpoint=sb://namespace.servicebus.windows.net/;...
    - Microsoft Fabric RTI: Endpoint=sb://eventstream-xxx.servicebus.windows.net/;...

    Expected format:
    Endpoint=sb://xxx.servicebus.windows.net/;SharedAccessKeyName=xxx;SharedAccessKey=xxx[;EntityPath=xxx]

    Args:
        conn_str: Connection string to validate
        strict: If True, enforce all validation rules. If False, allow shorter/simpler strings.
        allow_mock: If True, allow mock:// and test:// prefixes

    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if connection string is valid
        - error_message: Empty string if valid, error description if invalid
    """
    if not conn_str or not isinstance(conn_str, str):
        return False, "Connection string is empty or not a string"

    # Check for whitespace-only before stripping
    if conn_str and not conn_str.strip():
        return False, "Connection string contains only whitespace"

    conn_str = conn_str.strip()

    # Allow mock/test strings in non-strict mode
    if allow_mock and conn_str.startswith(("mock://", "test://")):
        return True, ""

    # Minimum length check (relaxed in non-strict mode)
    min_length = 50 if strict else 20
    if len(conn_str) < min_length:
        if strict:
            return False, "Connection string is too short (likely incomplete)"
        # In non-strict mode, just continue with validation

    # Required components for Event Hub connection string
    required_parts = {
        "Endpoint=sb://": "Event Hub endpoint (Endpoint=sb://...)",
        "SharedAccessKeyName=": "Shared access key name",
        "SharedAccessKey=": "Shared access key",
    }

    for part, description in required_parts.items():
        if part not in conn_str:
            return False, f"Missing required part: {description}"

    # Validate Endpoint format - support both Azure public cloud and China cloud
    valid_domains = [".servicebus.windows.net", ".servicebus.chinacloudapi.cn"]
    has_valid_domain = any(domain in conn_str for domain in valid_domains)

    if not has_valid_domain:
        if strict:
            return (
                False,
                f"Invalid Event Hub endpoint domain. Must end with {' or '.join(valid_domains)}",
            )
        # In non-strict mode, check for at least .servicebus. pattern
        elif ".servicebus." not in conn_str:
            return False, "Invalid Event Hub endpoint domain. Must contain .servicebus."

    # Validate that the Endpoint starts with sb:// protocol
    endpoint_match = re.search(r"Endpoint=(sb://[^;]+)", conn_str)
    if not endpoint_match:
        return False, "Invalid Endpoint format. Must start with sb://"

    # Extract and validate endpoint domain
    endpoint_part = endpoint_match.group(1)
    if "eventstream-" in endpoint_part:
        # Fabric RTI specific validation
        if strict and "EntityPath=" not in conn_str:
            return False, "Fabric RTI connection strings require EntityPath"

    # Validate that SharedAccessKeyName is not empty
    keyname_match = re.search(r"SharedAccessKeyName=([^;]+)", conn_str)
    if not keyname_match or not keyname_match.group(1).strip():
        return False, "SharedAccessKeyName cannot be empty"

    # Validate that SharedAccessKey is not empty and looks valid
    key_match = re.search(r"SharedAccessKey=([^;]+)", conn_str)
    if not key_match or not key_match.group(1).strip():
        return False, "SharedAccessKey cannot be empty"

    # Validate key format (should be base64-like)
    key_part = key_match.group(1).strip()
    min_key_length = 20 if strict else 10
    if len(key_part) < min_key_length:
        if strict:
            return False, "SharedAccessKey appears invalid (too short)"
        # In non-strict mode, allow shorter keys for testing

    # EntityPath is optional but should be non-empty if present
    if "EntityPath=" in conn_str:
        entitypath_match = re.search(r"EntityPath=([^;]+)", conn_str)
        if not entitypath_match or not entitypath_match.group(1).strip():
            return False, "EntityPath cannot be empty if specified"

    return True, ""


def sanitize_connection_string(conn_str: str) -> str:
    """
    Sanitize connection string for safe logging by redacting sensitive keys.

    This function redacts the SharedAccessKey value while preserving other
    components for debugging purposes.

    Args:
        conn_str: Connection string to sanitize

    Returns:
        Sanitized connection string with keys redacted

    Example:
        Input:  "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123"
        Output: "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=***REDACTED***"
    """
    if not conn_str:
        return "[empty]"

    if not conn_str.strip():
        return "[whitespace-only]"

    # Redact SharedAccessKey value
    sanitized = re.sub(
        r"(SharedAccessKey=)[^;]+", r"\1***REDACTED***", conn_str, flags=re.IGNORECASE
    )

    return sanitized


def get_connection_string_metadata(conn_str: str) -> dict:
    """
    Extract metadata from Event Hub connection string without exposing secrets.

    Args:
        conn_str: Connection string to analyze

    Returns:
        Dictionary containing non-sensitive metadata:
        - endpoint: Event Hub namespace endpoint
        - namespace: Event Hub namespace (extracted from endpoint)
        - key_name: SharedAccessKeyName
        - entity_path: EntityPath (if present)
        - has_key: Whether SharedAccessKey is present
        - is_valid: Whether the connection string is valid
        - is_fabric_rti: Whether this appears to be a Fabric RTI connection
    """
    metadata = {
        "endpoint": None,
        "namespace": None,
        "key_name": None,
        "entity_path": None,
        "has_key": False,
        "is_valid": False,
        "is_fabric_rti": False,
    }

    if not conn_str:
        return metadata

    # Extract endpoint
    endpoint_match = re.search(r"Endpoint=(sb://[^;]+)", conn_str)
    if endpoint_match:
        metadata["endpoint"] = endpoint_match.group(1)
        # Extract namespace from endpoint (e.g., sb://namespace.servicebus.windows.net/)
        namespace_match = re.search(r"sb://([^.]+)", endpoint_match.group(1))
        if namespace_match:
            metadata["namespace"] = namespace_match.group(1)

    # Extract key name
    keyname_match = re.search(r"SharedAccessKeyName=([^;]+)", conn_str)
    if keyname_match:
        metadata["key_name"] = keyname_match.group(1)

    # Extract entity path (optional)
    entitypath_match = re.search(r"EntityPath=([^;]+)", conn_str)
    if entitypath_match:
        metadata["entity_path"] = entitypath_match.group(1)

    # Check if key is present (but don't extract it)
    metadata["has_key"] = "SharedAccessKey=" in conn_str

    # Validate overall format
    is_valid, _ = validate_eventhub_connection_string(conn_str)
    metadata["is_valid"] = is_valid

    # Detect if this is Fabric RTI
    metadata["is_fabric_rti"] = is_fabric_rti_connection_string(conn_str)

    return metadata


def is_fabric_rti_connection_string(conn_str: str) -> bool:
    """
    Detect if the connection string is for Microsoft Fabric Real-Time Intelligence.

    Fabric RTI connection strings typically have entity paths with 'es_' prefix
    and key names with 'key_' prefix.

    Args:
        conn_str: Connection string to check

    Returns:
        True if connection string appears to be for Fabric RTI
    """
    if not conn_str:
        return False

    # Fabric RTI indicators
    fabric_indicators = [
        re.search(r"EntityPath=es_", conn_str),  # Event stream prefix
        re.search(r"SharedAccessKeyName=key_", conn_str),  # Key prefix
        re.search(
            r"eventstream-[a-z0-9]+\.servicebus\.windows\.net", conn_str
        ),  # Eventstream subdomain
    ]

    # If at least 2 indicators match, likely Fabric RTI
    matches = sum(1 for indicator in fabric_indicators if indicator)
    return matches >= 2


def validate_fabric_rti_specific(conn_str: str) -> tuple[bool, str, dict]:
    """
    Validate Fabric RTI specific requirements and extract metadata.

    Args:
        conn_str: Connection string to validate

    Returns:
        Tuple of (is_valid, message, metadata)
        - is_valid: True if valid or not a Fabric RTI connection
        - message: Validation message or warning
        - metadata: Connection string metadata
    """
    metadata = get_connection_string_metadata(conn_str)

    if not metadata.get("is_fabric_rti"):
        return True, "Not a Fabric RTI connection (standard Event Hub)", metadata

    # Fabric RTI specific checks
    if not metadata.get("entity_path"):
        return False, "Fabric RTI connections require EntityPath parameter", metadata

    if not metadata["entity_path"].startswith("es_"):
        return (
            False,
            "Fabric RTI EntityPath should start with 'es_' (Event Stream)",
            metadata,
        )

    # Check namespace format
    namespace = metadata.get("namespace", "")
    if not namespace.startswith("eventstream-"):
        return (
            False,
            "Fabric RTI namespace should start with 'eventstream-'",
            metadata,
        )

    return True, "Valid Fabric RTI connection string", metadata
