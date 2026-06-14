"""Test utilities for creating valid test data."""


def create_test_connection_string(
    namespace: str = "testnamespace",
    key_name: str = "TestKey",
    key: str = "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3RrZXl0ZXN0a2V5",
    entity_path: str = "test-hub",
    fabric_rti: bool = False,
) -> str:
    """
    Create a valid test connection string.

    Args:
        namespace: Event Hub namespace (use 'eventstream-xxx' for Fabric RTI)
        key_name: Shared access key name
        key: Shared access key (base64)
        entity_path: Event Hub or Event Stream name
        fabric_rti: If True, format as Fabric RTI connection

    Returns:
        Valid connection string for testing
    """
    if fabric_rti and not namespace.startswith("eventstream-"):
        namespace = f"eventstream-{namespace}"

    if fabric_rti and not entity_path.startswith("es_"):
        entity_path = f"es_{entity_path}"

    return (
        f"Endpoint=sb://{namespace}.servicebus.windows.net/;"
        f"SharedAccessKeyName={key_name};"
        f"SharedAccessKey={key};"
        f"EntityPath={entity_path}"
    )


# Pre-defined test connection strings
TEST_CONNECTION_STRING = create_test_connection_string()
FABRIC_RTI_CONNECTION_STRING = create_test_connection_string(fabric_rti=True)
MOCK_CONNECTION_STRING = "mock://localhost/test-hub"
TEST_PROTOCOL_STRING = "test://localhost/test-hub"
