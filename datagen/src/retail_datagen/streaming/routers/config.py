"""
Streaming configuration endpoints.

This module handles configuration management and connection validation.
"""

import logging
from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException, status

from ...api.models import (
    ConnectionTestResponse,
    OperationResult,
    StreamingConfigUpdate,
)
from ...config.models import RetailConfig
from ...shared.credential_utils import (
    get_connection_string_metadata,
    is_fabric_rti_connection_string,
    sanitize_connection_string,
    validate_eventhub_connection_string,
    validate_fabric_rti_specific,
)
from ...shared.dependencies import (
    get_config,
    get_task_status,
    rate_limit,
    update_config,
)
from ...streaming.azure_client import AzureEventHubClient
from ...streaming.schemas import EventType
from .state import get_session_id

logger = logging.getLogger(__name__)

router = APIRouter()

AVAILABLE_EVENT_TYPES = [e.value for e in EventType]


@router.get(
    "/stream/config",
    summary="Get streaming configuration",
    description="Get current streaming and real-time configuration",
)
async def get_streaming_config(config: RetailConfig = Depends(get_config)):
    """Get the current streaming configuration."""
    return {
        "realtime": config.realtime.model_dump(),
        "stream": config.stream.model_dump(),
        "available_event_types": AVAILABLE_EVENT_TYPES,
    }


@router.put(
    "/stream/config",
    response_model=OperationResult,
    summary="Update streaming configuration",
    description="Update streaming configuration settings",
)
async def update_streaming_config(
    updates: StreamingConfigUpdate, config: RetailConfig = Depends(get_config)
):
    """Update streaming configuration."""
    # Check if streaming is active
    session_id = get_session_id()
    if session_id:
        task_status = get_task_status(session_id)
        if task_status and task_status["status"] == "running":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "Cannot update configuration while streaming is active. "
                    "Stop streaming first."
                ),
            )

    try:
        # Create updated configuration
        realtime_config = config.realtime.model_copy()

        # Apply updates
        update_dict = updates.model_dump(exclude_none=True)
        for field, value in update_dict.items():
            setattr(realtime_config, field, value)

        # Update the configuration
        new_config = config.model_copy()
        new_config.realtime = realtime_config

        await update_config(new_config)

        # Save to file
        config_path = Path("config.json")
        new_config.to_file(config_path)

        return OperationResult(
            success=True, message="Streaming configuration updated successfully"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update configuration: {str(e)}",
        )


@router.post(
    "/stream/test",
    response_model=ConnectionTestResponse,
    summary="Test Azure Event Hub connection",
    description="""
    Test connection to Azure Event Hub or Fabric RTI without sending events.

    This endpoint validates:
    - Connection string format
    - Network connectivity
    - Authentication credentials
    - Event Hub exists and is accessible

    Returns metadata including partition count, hub name, and Fabric RTI detection.
    """,
)
@rate_limit(max_requests=10, window_seconds=60)
async def test_azure_connection(
    config: RetailConfig = Depends(get_config),
):
    """Test Azure Event Hub connection without sending events."""
    # Get connection string from config (respects env var priority)
    connection_string = config.realtime.get_connection_string()

    if not connection_string:
        return ConnectionTestResponse(
            success=False,
            message=(
                "No connection string configured. "
                "Set AZURE_EVENTHUB_CONNECTION_STRING env var or update config.json"
            ),
            response_time_ms=0.0,
            details={},
        )

    # Validate connection string format first
    is_valid, error = validate_eventhub_connection_string(connection_string)
    if not is_valid:
        return ConnectionTestResponse(
            success=False,
            message=f"Invalid connection string: {error}",
            response_time_ms=0.0,
            details={"validation_error": error},
        )

    try:
        start_time = datetime.now(UTC)

        # Create temporary client for testing
        test_client = AzureEventHubClient(
            connection_string=connection_string,
            hub_name=config.stream.hub,
            max_batch_size=1,
        )

        # Test connection (creates producer and fetches properties)
        success, message, metadata = await test_client.test_connection()

        end_time = datetime.now(UTC)
        response_time = (end_time - start_time).total_seconds() * 1000

        # Sanitize connection string for logging
        sanitized_conn = sanitize_connection_string(connection_string)
        logger.info(
            "Connection test %s: %s (connection: %s)",
            "succeeded" if success else "failed",
            message,
            sanitized_conn,
        )

        return ConnectionTestResponse(
            success=success,
            message=message,
            response_time_ms=response_time,
            details={
                "connection_metadata": metadata,
                "hub_configured": config.stream.hub,
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )

    except Exception as e:
        logger.error(f"Connection test error: {e}", exc_info=True)
        end_time = datetime.now(UTC)
        response_time = (end_time - start_time).total_seconds() * 1000

        return ConnectionTestResponse(
            success=False,
            message=f"Connection test error: {str(e)}",
            response_time_ms=response_time,
            details={
                "exception_type": type(e).__name__,
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )


@router.post(
    "/stream/validate-connection",
    summary="Validate connection string format",
    description=(
        "Validate Event Hub connection string format without testing connection"
    ),
)
@rate_limit(max_requests=20, window_seconds=60)
async def validate_connection(
    connection_string: str = Body(
        ..., description="Event Hub connection string to validate"
    ),
    strict: bool = Body(
        default=True,
        description="Enable strict validation (disable for testing)",
    ),
):
    """Validate Event Hub connection string format."""
    is_valid, error = validate_eventhub_connection_string(
        connection_string, strict=strict, allow_mock=not strict
    )

    # Get metadata without exposing secrets
    metadata = get_connection_string_metadata(connection_string)

    # Detect if this is a Fabric RTI connection string
    is_fabric = is_fabric_rti_connection_string(connection_string)

    # Validate Fabric RTI specific requirements
    fabric_valid, fabric_message, fabric_metadata = validate_fabric_rti_specific(
        connection_string
    )

    response = {
        "valid": is_valid and fabric_valid,
        "error": (
            error if not is_valid else (None if fabric_valid else fabric_message)
        ),
        "message": (
            "Connection string is valid"
            if (is_valid and fabric_valid)
            else "Invalid connection string"
        ),
        "strict_mode": strict,
        "metadata": {
            "endpoint": metadata.get("endpoint"),
            "namespace": metadata.get("namespace"),
            "key_name": metadata.get("key_name"),
            "entity_path": metadata.get("entity_path"),
            "has_key": metadata.get("has_key"),
            "is_fabric_rti": is_fabric,
        },
        "sanitized": sanitize_connection_string(connection_string),
    }

    return response


@router.post(
    "/config/validate",
    tags=["configuration"],
    summary="Validate streaming configuration",
    description="Validate streaming configuration including connection string",
)
@rate_limit(max_requests=20, window_seconds=60)
async def validate_streaming_config(
    config_override: dict | None = Body(
        None, description="Optional config to validate"
    ),
    config: RetailConfig = Depends(get_config),
) -> dict:
    """Validate streaming configuration including connection string."""
    errors = []
    warnings = []
    recommendations = []
    metadata = {}

    # Use override config if provided, otherwise current config
    test_config_obj = config
    if config_override:
        try:
            test_config_obj = RetailConfig(**config_override)
            conn_str = test_config_obj.realtime.get_connection_string()
        except Exception as e:
            errors.append(f"Invalid configuration: {str(e)}")
            conn_str = None
    else:
        conn_str = config.realtime.get_connection_string()

    # Validate connection string
    if not conn_str:
        errors.append(
            "No connection string configured. "
            "Set AZURE_EVENTHUB_CONNECTION_STRING or update config.json"
        )
    elif not conn_str.startswith(("mock://", "test://")):
        # Format validation
        is_valid, error = validate_eventhub_connection_string(conn_str)
        if not is_valid:
            errors.append(f"Connection string validation failed: {error}")
        else:
            # Extract metadata
            metadata = get_connection_string_metadata(conn_str)

            # Fabric RTI specific validation
            is_fabric_valid, fabric_msg, fabric_metadata = validate_fabric_rti_specific(
                conn_str
            )
            metadata.update(fabric_metadata)

            if not is_fabric_valid:
                warnings.append(fabric_msg)

            # Add recommendations based on metadata
            if metadata.get("is_fabric_rti"):
                recommendations.append(
                    "Detected Fabric RTI connection - "
                    "ensure workspace has proper permissions"
                )
                recommendations.append(
                    "Fabric RTI automatically scales - monitor usage in Fabric portal"
                )
            else:
                recommendations.append(
                    "Standard Event Hub detected - "
                    "monitor partition count and throughput"
                )

    # Validate configuration parameters
    if test_config_obj:
        # Check emit_interval
        if test_config_obj.realtime.emit_interval_ms < 100:
            warnings.append(
                f"Very low emit_interval "
                f"({test_config_obj.realtime.emit_interval_ms}ms) "
                "may cause high CPU usage"
            )

        # Check burst size
        if test_config_obj.realtime.burst > 1000:
            warnings.append(
                f"Large burst size ({test_config_obj.realtime.burst}) "
                "may exceed Event Hub limits"
            )

        # Check batch size
        if test_config_obj.realtime.max_batch_size > 256:
            recommendations.append(
                "Batch sizes > 256 may hit Event Hub message size limits"
            )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "connection_metadata": metadata,
        "recommendations": recommendations,
    }
