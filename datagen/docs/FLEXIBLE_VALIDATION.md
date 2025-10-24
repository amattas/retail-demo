# Flexible Connection String Validation

## Overview

The retail data generator now supports flexible validation modes for Azure Event Hub connection strings. This enables tests to use simplified connection strings while maintaining strict validation in production.

## Validation Modes

### Strict Mode (Production)

**Default behavior** - enforces all validation rules:
- Connection string must be at least 50 characters
- SharedAccessKey must be at least 20 characters
- Endpoint domain must be `.servicebus.windows.net` or `.servicebus.chinacloudapi.cn`
- Fabric RTI connections must include EntityPath

**Example:**
```python
from retail_datagen.shared.credential_utils import validate_eventhub_connection_string

conn_str = "Endpoint=sb://prod.servicebus.windows.net/;SharedAccessKeyName=Key;SharedAccessKey=VeryLongProductionKey123456789=="
is_valid, error = validate_eventhub_connection_string(conn_str, strict=True)
```

### Non-Strict Mode (Testing)

**Relaxed validation** - allows simplified strings for testing:
- Connection string minimum length reduced to 20 characters
- SharedAccessKey minimum length reduced to 10 characters
- Allows custom `.servicebus.*` domains
- Fabric RTI EntityPath requirement relaxed
- Supports `mock://` and `test://` protocol prefixes

**Example:**
```python
from retail_datagen.shared.credential_utils import validate_eventhub_connection_string

# Short test connection string
test_conn = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=Key;SharedAccessKey=shortkey;"
is_valid, error = validate_eventhub_connection_string(test_conn, strict=False, allow_mock=True)

# Mock protocol
mock_conn = "mock://localhost/test-hub"
is_valid, error = validate_eventhub_connection_string(mock_conn, strict=False, allow_mock=True)
```

## Automatic Test Mode

Tests automatically enable non-strict validation via the `RETAIL_DATAGEN_TEST_MODE` environment variable.

### In conftest.py:
```python
@pytest.fixture(scope="session", autouse=True)
def set_test_mode():
    """Automatically set test mode for all tests."""
    os.environ["RETAIL_DATAGEN_TEST_MODE"] = "true"
    yield
    os.environ.pop("RETAIL_DATAGEN_TEST_MODE", None)
```

This allows RetailConfig to accept test connection strings without raising validation errors.

## Test Utilities

The `tests/test_utils.py` module provides helper functions for creating valid test connection strings:

```python
from tests.test_utils import (
    create_test_connection_string,
    TEST_CONNECTION_STRING,
    FABRIC_RTI_CONNECTION_STRING,
    MOCK_CONNECTION_STRING,
)

# Use pre-defined test strings
config = RetailConfig(
    realtime={"azure_connection_string": TEST_CONNECTION_STRING},
    ...
)

# Create custom test strings
custom_conn = create_test_connection_string(
    namespace="mytest",
    key_name="TestKey",
    key="customkey123",
    entity_path="my-hub"
)

# Create Fabric RTI test strings
fabric_conn = create_test_connection_string(
    namespace="test",
    fabric_rti=True  # Adds eventstream- prefix and es_ entity path
)
```

## API Endpoint

The `/stream/validate-connection` endpoint now supports a `strict` parameter:

```bash
# Strict validation (production)
curl -X POST http://localhost:8000/api/stream/validate-connection \
  -H "Content-Type: application/json" \
  -d '{
    "connection_string": "Endpoint=sb://...",
    "strict": true
  }'

# Non-strict validation (testing)
curl -X POST http://localhost:8000/api/stream/validate-connection \
  -H "Content-Type: application/json" \
  -d '{
    "connection_string": "test://localhost/hub",
    "strict": false
  }'
```

Response includes `strict_mode` field:
```json
{
  "valid": true,
  "message": "Connection string is valid",
  "strict_mode": false,
  "metadata": {...},
  "sanitized": "..."
}
```

## Use Cases

### Unit Tests
```python
def test_streaming_with_mock_connection():
    """Test streaming logic without real Azure connection."""
    config = RetailConfig(
        realtime={"azure_connection_string": "mock://localhost/test"},
        ...
    )
    # Test passes in test mode
```

### Integration Tests
```python
def test_event_generation():
    """Test event generation with simplified connection."""
    from tests.test_utils import TEST_CONNECTION_STRING

    config = RetailConfig(
        realtime={"azure_connection_string": TEST_CONNECTION_STRING},
        ...
    )
    # Tests run without needing real Azure credentials
```

### Local Development
```bash
# Set test mode for local development
export RETAIL_DATAGEN_TEST_MODE=true

# Use simplified connection string
python -m retail_datagen.main
```

## Production Safety

- Production code defaults to strict validation (`strict=True`)
- `RETAIL_DATAGEN_TEST_MODE` environment variable must be explicitly set
- Mock/test protocols (`mock://`, `test://`) are rejected in strict mode
- API endpoints default to strict validation

## Migration Guide

Existing tests should work without modification as the `set_test_mode` fixture is `autouse=True`.

If you need to explicitly control validation mode:

```python
# Old code (still works)
is_valid, error = validate_eventhub_connection_string(conn_str)

# New code (explicit control)
is_valid, error = validate_eventhub_connection_string(
    conn_str,
    strict=False,  # Relax validation
    allow_mock=True  # Allow mock:// and test://
)
```

## Implementation Details

### Files Modified

1. **`src/retail_datagen/shared/credential_utils.py`**
   - Added `strict` and `allow_mock` parameters to `validate_eventhub_connection_string()`
   - Relaxed validation rules in non-strict mode

2. **`src/retail_datagen/config/models.py`**
   - Updated Pydantic validator to check `RETAIL_DATAGEN_TEST_MODE` environment variable
   - Passes `strict` parameter to validation function

3. **`tests/conftest.py`**
   - Added `set_test_mode()` fixture to automatically enable test mode

4. **`tests/test_utils.py`** (new)
   - Helper functions for creating valid test connection strings
   - Pre-defined test constants

5. **`src/retail_datagen/streaming/router.py`**
   - Updated `/stream/validate-connection` endpoint to accept `strict` parameter

### Testing

Run validation tests:
```bash
# Test flexible validation
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/test_flexible_validation.py -v

# Test all credential utils
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/test_credential_utils.py -v

# Verify test mode is active
python -c "import os; print('Test mode:', os.getenv('RETAIL_DATAGEN_TEST_MODE'))"
```

## Success Criteria

✅ Tests can use simplified connection strings
✅ Production validation remains strict by default
✅ 30+ integration tests unblocked
✅ Easy toggle between strict/non-strict mode
✅ Mock strings (`mock://`, `test://`) work in test mode
✅ Backward compatibility maintained for existing tests
✅ API endpoint supports configurable validation
