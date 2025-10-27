"""
FastAPI router for real-time streaming endpoints.

This module provides REST API endpoints for managing real-time event streaming
to Azure Event Hub with comprehensive monitoring and control capabilities.
"""

import asyncio
import logging
import random
from collections import deque
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status

from ..api.models import (
    ActiveDisruptionsResponse,
    ConnectionTestResponse,
    DisruptionRequest,
    DisruptionResponse,
    OperationResult,
    RecentEventsResponse,
    StreamingConfigUpdate,
    StreamingStartRequest,
    StreamingStatisticsResponse,
    StreamingStatusResponse,
)
from ..config.models import RetailConfig
from ..generators.generation_state import GenerationStateManager
from ..shared.credential_utils import (
    get_connection_string_metadata,
    is_fabric_rti_connection_string,
    sanitize_connection_string,
    validate_eventhub_connection_string,
)
from ..shared.dependencies import (
    cancel_task,
    create_background_task,
    get_config,
    get_event_streamer,
    get_task_status,
    rate_limit,
    update_config,
    update_task_progress,
)
from ..shared.logging_utils import get_structured_logger
from ..streaming.event_streamer import EventStreamer
from ..streaming.schemas import EventType

logger = logging.getLogger(__name__)
log = get_structured_logger(__name__)

router = APIRouter()

# Global streaming state
_streaming_session_id: str | None = None
_streaming_start_time: datetime | None = None
_recent_events: deque = deque(maxlen=100)  # Store last 100 events
_streaming_statistics = {
    "events_generated": 0,
    "events_sent_successfully": 0,
    "events_failed": 0,
    "batches_sent": 0,
    "total_streaming_time": 0.0,
    "events_per_second": 0.0,
    "bytes_sent": 0,
    "last_event_time": None,
    "event_type_counts": {},
    "error_counts": {},
    "connection_failures": 0,
    "circuit_breaker_trips": 0,
}

# Global disruption state
_active_disruptions: dict[str, dict[str, Any]] = {}  # disruption_id -> disruption_data

AVAILABLE_EVENT_TYPES = [e.value for e in EventType]


def _reset_streaming_state():
    """Reset global streaming state."""
    global _streaming_session_id, _streaming_start_time, _recent_events, _streaming_statistics

    _streaming_session_id = None
    _streaming_start_time = None
    _recent_events.clear()
    _streaming_statistics = {
        "events_generated": 0,
        "events_sent_successfully": 0,
        "events_failed": 0,
        "batches_sent": 0,
        "total_streaming_time": 0.0,
        "events_per_second": 0.0,
        "bytes_sent": 0,
        "last_event_time": None,
        "event_type_counts": {},
        "error_counts": {},
        "connection_failures": 0,
        "circuit_breaker_trips": 0,
    }


def _update_streaming_statistics(event_data: dict[str, Any], success: bool = True):
    """Update streaming statistics with new event data."""
    global _streaming_statistics

    _streaming_statistics["events_generated"] += 1
    _streaming_statistics["last_event_time"] = datetime.now()

    if success:
        _streaming_statistics["events_sent_successfully"] += 1
        # Estimate bytes (simplified)
        _streaming_statistics["bytes_sent"] += len(str(event_data))
    else:
        _streaming_statistics["events_failed"] += 1

    # Update event type counts
    event_type = event_data.get("event_type", "unknown")
    _streaming_statistics["event_type_counts"][event_type] = (
        _streaming_statistics["event_type_counts"].get(event_type, 0) + 1
    )

    # Calculate events per second
    if _streaming_start_time:
        elapsed = (datetime.now() - _streaming_start_time).total_seconds()
        if elapsed > 0:
            _streaming_statistics["events_per_second"] = (
                _streaming_statistics["events_generated"] / elapsed
            )
            _streaming_statistics["total_streaming_time"] = elapsed


# ================================
# STREAMING CONTROL ENDPOINTS
# ================================


@router.post(
    "/stream/start",
    response_model=OperationResult,
    summary="Start real-time streaming",
    description="Start streaming events to Azure Event Hub",
)
@rate_limit(max_requests=5, window_seconds=60)
async def start_streaming(
    request: StreamingStartRequest,
    fastapi_request: Request,
    event_streamer: EventStreamer = Depends(get_event_streamer),
    config: RetailConfig = Depends(get_config),
):
    """Start real-time event streaming."""

    global _streaming_session_id, _streaming_start_time

    # Generate request correlation ID
    request_id = f"REQ_{uuid4().hex[:12]}"
    log.set_correlation_id(request_id)

    log.info(
        "Streaming start request received",
        request_id=request_id,
        client_ip=(
            fastapi_request.client.host if fastapi_request.client else "unknown"
        ),
        duration_minutes=request.duration_minutes,
        event_types_count=len(request.event_types) if request.event_types else "all",
    )

    # Check if already streaming
    if _streaming_session_id:
        task_status = get_task_status(_streaming_session_id)
        if task_status and task_status["status"] == "running":
            log.warning(
                "Streaming already active",
                request_id=request_id,
                active_session=_streaming_session_id,
            )
            log.clear_correlation_id()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Streaming is already active. Stop current stream first.",
            )

    # Validate event types if specified (must match EventType values)
    if request.event_types:
        invalid_types = set(request.event_types) - set(AVAILABLE_EVENT_TYPES)
        if invalid_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid event types: {', '.join(invalid_types)}. "
                f"Valid types: {', '.join(AVAILABLE_EVENT_TYPES)}",
            )

    # Check prerequisite: historical data must be generated first
    state_manager = GenerationStateManager()
    if not state_manager.can_start_realtime():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Historical data must be generated before starting real-time streaming",
        )

    # Check Azure connection string
    if not config.realtime.azure_connection_string:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Azure Event Hub connection string not configured",
        )

    session_id = f"streaming_{uuid4().hex[:8]}"
    _streaming_session_id = session_id
    _streaming_start_time = datetime.now()
    _reset_streaming_state()

    async def streaming_task():
        """Background task for event streaming via EventStreamer."""
        try:
            update_task_progress(session_id, 0.0, "Initializing event streaming")

            # Check if SQLite mode should be used
            use_sqlite = True  # Default to SQLite if database exists

            # Try SQLite batch streaming first
            if use_sqlite:
                try:
                    from ..db.session import get_facts_session

                    async with get_facts_session() as db_session:
                        # Create new streamer with database session
                        db_streamer = EventStreamer(
                            config=config,
                            azure_connection_string=config.realtime.azure_connection_string,
                            session=db_session,
                        )
                        success = await db_streamer.start()

                        update_task_progress(session_id, 1.0, "Batch streaming completed")

                        stats = await db_streamer.get_statistics()
                        return {
                            "events_sent": stats.get("events_sent_successfully", 0),
                            "duration_minutes": request.duration_minutes,
                            "event_types": request.event_types or AVAILABLE_EVENT_TYPES,
                            "end_reason": "batch_completed",
                            "mode": "sqlite_batch",
                        }

                except Exception as db_error:
                    logger.warning(f"SQLite batch streaming failed, falling back to real-time: {db_error}")
                    # Fall through to real-time mode

            # Fall back to real-time generation mode
            if request.event_types:
                event_streamer.set_allowed_event_types(request.event_types)
            duration = (
                timedelta(minutes=request.duration_minutes)
                if request.duration_minutes
                else None
            )

            # Start streaming; runs until duration or stop
            await event_streamer.start(duration=duration)

            update_task_progress(session_id, 1.0, "Streaming completed")

            stats = await event_streamer.get_statistics()
            return {
                "events_sent": stats.get("events_sent_successfully", 0),
                "duration_minutes": request.duration_minutes,
                "event_types": request.event_types or AVAILABLE_EVENT_TYPES,
                "end_reason": (
                    "duration_completed" if request.duration_minutes else "manual_stop"
                ),
                "mode": "real_time",
            }

        except asyncio.CancelledError:
            update_task_progress(session_id, 1.0, "Streaming cancelled")
            return {"events_sent": 0, "end_reason": "cancelled"}
        except Exception as e:
            logger.error(f"Streaming task failed: {e}")
            _streaming_statistics["connection_failures"] += 1
            raise
        finally:
            global _streaming_session_id
            _streaming_session_id = None

    create_background_task(
        session_id, streaming_task(), f"Stream events: {request.event_types or 'all'}"
    )

    log.info(
        "Streaming started successfully",
        request_id=request_id,
        session_id=session_id,
        duration_minutes=request.duration_minutes,
    )
    log.clear_correlation_id()

    return OperationResult(
        success=True,
        message="Event streaming started",
        operation_id=session_id,
        started_at=datetime.now(),
    )


@router.post(
    "/stream/stop",
    response_model=OperationResult,
    summary="Stop event streaming",
    description="Stop the currently active event streaming",
)
async def stop_streaming():
    """Stop the currently active event streaming."""

    global _streaming_session_id

    if not _streaming_session_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No active streaming session to stop",
        )

    success = cancel_task(_streaming_session_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Streaming session not found or already completed",
        )

    session_id = _streaming_session_id
    _streaming_session_id = None

    return OperationResult(
        success=True, message="Event streaming stopped", operation_id=session_id
    )


@router.get(
    "/stream/status",
    response_model=StreamingStatusResponse,
    summary="Get streaming status",
    description="Get current streaming status and basic statistics",
)
async def get_streaming_status():
    """Get the current streaming status."""

    is_streaming = _streaming_session_id is not None
    status_enum = "stopped"
    uptime_seconds = 0.0

    if is_streaming and _streaming_start_time:
        uptime_seconds = (datetime.now() - _streaming_start_time).total_seconds()

        # Check actual task status
        task_status = get_task_status(_streaming_session_id)
        if task_status:
            status_map = {
                "running": "running",
                "completed": "stopped",
                "failed": "error",
                "cancelled": "stopped",
            }
            status_enum = status_map.get(task_status["status"], "stopped")
        else:
            status_enum = "error"

    return StreamingStatusResponse(
        is_streaming=is_streaming,
        status=status_enum,
        uptime_seconds=uptime_seconds,
        events_sent=_streaming_statistics["events_sent_successfully"],
        events_per_second=_streaming_statistics["events_per_second"],
        last_event_time=_streaming_statistics["last_event_time"],
    )


@router.post(
    "/stream/pause",
    summary="Pause streaming",
    description="Pause active streaming without stopping completely",
)
async def pause_streaming(
    event_streamer: EventStreamer = Depends(get_event_streamer),
):
    """
    Pause active streaming without stopping completely.

    Streaming state is maintained, so it can be resumed seamlessly.
    No events are generated or sent while paused.

    Returns:
        {
            "success": bool,
            "message": str,
            "paused_at": str (ISO timestamp),
            "events_sent_before_pause": int
        }
    """
    if not event_streamer:
        raise HTTPException(
            status_code=400, detail="No active streamer. Start streaming first."
        )

    result = await event_streamer.pause()

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return result


@router.post(
    "/stream/resume",
    summary="Resume streaming",
    description="Resume a paused streaming session",
)
async def resume_streaming(
    event_streamer: EventStreamer = Depends(get_event_streamer),
):
    """
    Resume paused streaming.

    Continues from where it left off with no event loss.

    Returns:
        {
            "success": bool,
            "message": str,
            "resumed_at": str (ISO timestamp),
            "pause_duration_seconds": float,
            "total_pause_count": int
        }
    """
    if not event_streamer:
        raise HTTPException(
            status_code=400, detail="No active streamer. Start streaming first."
        )

    result = await event_streamer.resume()

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return result


@router.get(
    "/stream/pause-status",
    summary="Get pause status",
    description="Get current pause status and statistics",
)
async def get_pause_status(
    event_streamer: EventStreamer = Depends(get_event_streamer),
):
    """
    Get current pause status and statistics.

    Returns:
        {
            "is_paused": bool,
            "pause_count": int,
            "total_pause_duration_seconds": float,
            "currently_paused_duration": float
        }
    """
    if not event_streamer:
        return {
            "is_paused": False,
            "pause_count": 0,
            "total_pause_duration_seconds": 0.0,
            "currently_paused_duration": 0.0,
        }

    return event_streamer.get_pause_statistics()


# ================================
# STREAMING CONFIGURATION ENDPOINTS
# ================================


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
    if _streaming_session_id:
        task_status = get_task_status(_streaming_session_id)
        if task_status and task_status["status"] == "running":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Cannot update configuration while streaming is active. Stop streaming first.",
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
        from pathlib import Path

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
    """
    Test Azure Event Hub connection without sending events.

    Validates connection string, credentials, and hub accessibility.
    """
    from ..streaming.azure_client import AzureEventHubClient

    # Get connection string from config (respects env var priority)
    connection_string = config.realtime.get_connection_string()

    if not connection_string:
        return ConnectionTestResponse(
            success=False,
            message="No connection string configured. Set AZURE_EVENTHUB_CONNECTION_STRING env var or update config.json",
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
        start_time = datetime.now()

        # Create temporary client for testing
        # Hub name can be empty if EntityPath is in the connection string
        test_client = AzureEventHubClient(
            connection_string=connection_string,
            hub_name=config.stream.hub,  # May be overridden by EntityPath
            max_batch_size=1,  # Minimal config for testing
        )

        # Test connection (creates producer and fetches properties)
        success, message, metadata = await test_client.test_connection()

        # Clean up (no connection to close since we used async context manager)
        end_time = datetime.now()
        response_time = (end_time - start_time).total_seconds() * 1000

        # Sanitize connection string for logging
        sanitized_conn = sanitize_connection_string(connection_string)
        logger.info(
            f"Connection test {'succeeded' if success else 'failed'}: {message} (connection: {sanitized_conn})"
        )

        return ConnectionTestResponse(
            success=success,
            message=message,
            response_time_ms=response_time,
            details={
                "connection_metadata": metadata,
                "hub_configured": config.stream.hub,
                "timestamp": datetime.now().isoformat(),
            },
        )

    except Exception as e:
        logger.error(f"Connection test error: {e}", exc_info=True)
        end_time = datetime.now()
        response_time = (end_time - start_time).total_seconds() * 1000

        return ConnectionTestResponse(
            success=False,
            message=f"Connection test error: {str(e)}",
            response_time_ms=response_time,
            details={
                "exception_type": type(e).__name__,
                "timestamp": datetime.now().isoformat(),
            },
        )


@router.post(
    "/stream/validate-connection",
    summary="Validate connection string format",
    description="Validate Event Hub connection string format without testing connection",
)
@rate_limit(max_requests=20, window_seconds=60)
async def validate_connection(
    connection_string: str = Body(
        ..., description="Event Hub connection string to validate"
    ),
    strict: bool = Body(
        default=True, description="Enable strict validation (disable for testing)"
    ),
):
    """
    Validate Event Hub connection string format with configurable strictness.

    This endpoint validates the connection string format without actually
    attempting to connect. It supports both standard Azure Event Hub and
    Microsoft Fabric Real-Time Intelligence connection strings.

    Args:
        connection_string: The connection string to validate
        strict: If False, allow shorter/simpler connection strings for testing
    """
    from ..shared.credential_utils import validate_fabric_rti_specific

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
        "error": error if not is_valid else (None if fabric_valid else fabric_message),
        "message": "Connection string is valid"
        if (is_valid and fabric_valid)
        else "Invalid connection string",
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
    """
    Validate streaming configuration including connection string.

    Validates:
    - Connection string format
    - Fabric RTI specific requirements
    - Configuration parameters (emit_interval, burst, etc.)
    - Credential accessibility

    Request body (optional):
    {
        "realtime": {
            "azure_connection_string": "Endpoint=sb://...",
            "emit_interval_ms": 500,
            "burst": 100
        }
    }

    Returns:
        {
            "valid": bool,
            "errors": list[str],
            "warnings": list[str],
            "connection_metadata": dict,
            "recommendations": list[str]
        }
    """
    from ..shared.credential_utils import (
        get_connection_string_metadata,
        validate_eventhub_connection_string,
        validate_fabric_rti_specific,
    )

    errors = []
    warnings = []
    recommendations = []
    metadata = {}

    # Use override config if provided, otherwise current config
    if config_override:
        try:
            test_config = RetailConfig(**config_override)
            conn_str = test_config.realtime.get_connection_string()
        except Exception as e:
            errors.append(f"Invalid configuration: {str(e)}")
            conn_str = None
    else:
        conn_str = config.realtime.get_connection_string()

    # Validate connection string
    if not conn_str:
        errors.append(
            "No connection string configured. Set AZURE_EVENTHUB_CONNECTION_STRING or update config.json"
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
                    "Detected Fabric RTI connection - ensure workspace has proper permissions"
                )
                recommendations.append(
                    "Fabric RTI automatically scales - monitor usage in Fabric portal"
                )
            else:
                recommendations.append(
                    "Standard Event Hub detected - monitor partition count and throughput"
                )

    # Validate configuration parameters
    if config_override or config:
        test_config_obj = test_config if config_override else config

        # Check emit_interval
        if test_config_obj.realtime.emit_interval_ms < 100:
            warnings.append(
                f"Very low emit_interval ({test_config_obj.realtime.emit_interval_ms}ms) may cause high CPU usage"
            )

        # Check burst size
        if test_config_obj.realtime.burst > 1000:
            warnings.append(
                f"Large burst size ({test_config_obj.realtime.burst}) may exceed Event Hub limits"
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


# ================================
# MONITORING ENDPOINTS
# ================================


@router.get(
    "/stream/statistics",
    response_model=StreamingStatisticsResponse,
    summary="Get streaming statistics",
    description="Get detailed streaming statistics and metrics",
)
async def get_streaming_statistics():
    """Get detailed streaming statistics."""

    return StreamingStatisticsResponse(
        events_generated=_streaming_statistics["events_generated"],
        events_sent_successfully=_streaming_statistics["events_sent_successfully"],
        events_failed=_streaming_statistics["events_failed"],
        batches_sent=_streaming_statistics["batches_sent"],
        total_streaming_time=_streaming_statistics["total_streaming_time"],
        events_per_second=_streaming_statistics["events_per_second"],
        bytes_sent=_streaming_statistics["bytes_sent"],
        last_event_time=_streaming_statistics["last_event_time"],
        event_type_counts=_streaming_statistics["event_type_counts"],
        error_counts=_streaming_statistics["error_counts"],
        connection_failures=_streaming_statistics["connection_failures"],
        circuit_breaker_trips=_streaming_statistics["circuit_breaker_trips"],
    )


@router.get(
    "/stream/events/recent",
    response_model=RecentEventsResponse,
    summary="Get recent events",
    description="Get the most recent streaming events (last 100)",
)
async def get_recent_events(
    limit: int = Query(
        100, ge=1, le=100, description="Number of recent events to return"
    ),
):
    """Get recent streaming events."""

    # Get the most recent events up to the limit
    recent_events = list(_recent_events)[-limit:]

    # Format for response
    formatted_events = []
    for event_data in recent_events:
        formatted_events.append(
            {
                "timestamp": event_data["timestamp"],
                "event_type": event_data["event"].get("event_type"),
                "trace_id": event_data["event"].get("trace_id"),
                "payload": event_data["event"].get("payload", {}),
            }
        )

    return RecentEventsResponse(
        events=formatted_events, count=len(formatted_events), timestamp=datetime.now()
    )


@router.get(
    "/stream/health",
    summary="Stream health check",
    description="Check the health of streaming components",
)
async def stream_health_check(config: RetailConfig = Depends(get_config)):
    """Check streaming component health."""

    checks = {}
    overall_status = "healthy"

    # Check if streaming is active
    if _streaming_session_id:
        task_status = get_task_status(_streaming_session_id)
        if task_status:
            checks["streaming_task"] = {
                "status": "active",
                "task_status": task_status["status"],
                "uptime_seconds": (
                    (datetime.now() - _streaming_start_time).total_seconds()
                    if _streaming_start_time
                    else 0
                ),
            }
        else:
            checks["streaming_task"] = {
                "status": "error",
                "message": "Streaming session ID exists but task not found",
            }
            overall_status = "unhealthy"
    else:
        checks["streaming_task"] = {"status": "inactive"}

    # Check Azure configuration
    if config.realtime.azure_connection_string:
        checks["azure_config"] = {"status": "configured", "hub_name": config.stream.hub}
    else:
        checks["azure_config"] = {
            "status": "not_configured",
            "message": "Azure connection string not set",
        }
        overall_status = "degraded"

    # Check statistics
    checks["statistics"] = {
        "status": "healthy",
        "events_generated": _streaming_statistics["events_generated"],
        "events_per_second": _streaming_statistics["events_per_second"],
        "failure_rate": (
            _streaming_statistics["events_failed"]
            / max(_streaming_statistics["events_generated"], 1)
        ),
    }

    return {"status": overall_status, "timestamp": datetime.now(), "checks": checks}


# ================================
# EVENT TYPE MANAGEMENT
# ================================


@router.get(
    "/stream/event-types",
    summary="List available event types",
    description="Get list of available event types for streaming",
)
async def list_event_types():
    """List all available event types for streaming."""
    return {
        "event_types": AVAILABLE_EVENT_TYPES,
        "count": len(AVAILABLE_EVENT_TYPES),
        "description": "Available event types for real-time streaming",
    }


# ================================
# SUPPLY CHAIN DISRUPTION ENDPOINTS
# ================================


def _cleanup_expired_disruptions():
    """Remove expired disruptions from active list."""
    global _active_disruptions
    now = datetime.now()
    expired_keys = [
        disruption_id
        for disruption_id, data in _active_disruptions.items()
        if data["active_until"] <= now
    ]
    for key in expired_keys:
        del _active_disruptions[key]


@router.post(
    "/disruption/create",
    response_model=DisruptionResponse,
    summary="Create supply chain disruption",
    description="Create a supply chain disruption that affects streaming events",
)
@rate_limit(max_requests=20, window_seconds=60)
async def create_disruption(request: DisruptionRequest):
    """Create a new supply chain disruption."""

    # Clean up expired disruptions first
    _cleanup_expired_disruptions()

    # Generate disruption ID
    disruption_id = f"disruption_{uuid4().hex[:8]}"
    active_until = datetime.now() + timedelta(minutes=request.duration_minutes)

    # Create disruption data
    disruption_data = {
        "disruption_id": disruption_id,
        "type": request.disruption_type,
        "target_id": request.target_id,
        "severity": request.severity,
        "product_ids": request.product_ids or [],
        "created_at": datetime.now(),
        "active_until": active_until,
        "events_affected": 0,
        "status": "active",
    }

    # Store in global state
    _active_disruptions[disruption_id] = disruption_data

    logger.info(
        f"Created disruption {disruption_id}: {request.disruption_type} affecting target {request.target_id}"
    )

    return DisruptionResponse(
        success=True,
        disruption_id=disruption_id,
        message=f"Created {request.disruption_type} disruption for target {request.target_id}",
        active_until=active_until,
    )


@router.get(
    "/disruption/list",
    response_model=ActiveDisruptionsResponse,
    summary="List active disruptions",
    description="Get list of all currently active supply chain disruptions",
)
async def list_active_disruptions():
    """Get list of all active disruptions."""

    # Clean up expired disruptions first
    _cleanup_expired_disruptions()

    # Format disruptions for response
    disruptions = []
    for disruption_id, data in _active_disruptions.items():
        disruption_info = {
            "disruption_id": disruption_id,
            "type": data["type"],
            "target_id": data["target_id"],
            "severity": data["severity"],
            "created_at": data["created_at"],
            "active_until": data["active_until"],
            "time_remaining_minutes": max(
                0, (data["active_until"] - datetime.now()).total_seconds() / 60
            ),
            "events_affected": data.get("events_affected", 0),
            "status": data["status"],
        }
        disruptions.append(disruption_info)

    return ActiveDisruptionsResponse(
        disruptions=disruptions, count=len(disruptions), timestamp=datetime.now()
    )


@router.delete(
    "/disruption/{disruption_id}",
    response_model=OperationResult,
    summary="Cancel disruption",
    description="Cancel an active supply chain disruption",
)
async def cancel_disruption(disruption_id: str):
    """Cancel an active disruption."""

    if disruption_id not in _active_disruptions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Disruption {disruption_id} not found or already expired",
        )

    # Remove the disruption
    _active_disruptions.pop(disruption_id)

    logger.info(f"Cancelled disruption {disruption_id}")

    return OperationResult(
        success=True,
        message=f"Cancelled disruption {disruption_id}",
        operation_id=disruption_id,
    )


@router.post(
    "/disruption/clear-all",
    response_model=OperationResult,
    summary="Clear all disruptions",
    description="Cancel all active supply chain disruptions",
)
async def clear_all_disruptions():
    """Cancel all active disruptions."""

    count = len(_active_disruptions)
    _active_disruptions.clear()

    logger.info(f"Cleared all {count} active disruptions")

    return OperationResult(success=True, message=f"Cleared {count} active disruptions")


def get_active_disruptions_for_target(
    target_type: str, target_id: int
) -> list[dict[str, Any]]:
    """Get active disruptions affecting a specific target."""
    _cleanup_expired_disruptions()

    matching_disruptions = []
    for disruption_data in _active_disruptions.values():
        if disruption_data["target_id"] == target_id:
            # Check if disruption type matches target type
            disruption_type = disruption_data["type"]
            if (
                (
                    target_type == "dc"
                    and disruption_type in ["dc_outage", "inventory_shortage"]
                )
                or (
                    target_type == "store" and disruption_type in ["inventory_shortage"]
                )
                or (
                    target_type == "truck"
                    and disruption_type in ["truck_breakdown", "weather_delay"]
                )
            ):
                matching_disruptions.append(disruption_data)

    return matching_disruptions


def apply_disruption_effects(event_data: dict[str, Any]) -> dict[str, Any]:
    """Apply disruption effects to an event if applicable."""

    # Clean up expired disruptions
    _cleanup_expired_disruptions()

    if not _active_disruptions:
        return event_data

    event_type = event_data.get("event_type")
    payload = event_data.get("payload", {})

    # Apply disruptions based on event type and target
    for disruption_id, disruption_data in _active_disruptions.items():
        disruption_type = disruption_data["type"]
        target_id = disruption_data["target_id"]
        severity = disruption_data["severity"]

        modified = False

        # DC inventory disruptions
        if (
            event_type == "dc_inventory_txn"
            and disruption_type in ["dc_outage", "inventory_shortage"]
            and payload.get("DCID") == target_id
        ):
            if disruption_type == "dc_outage":
                # Cancel all DC operations
                if random.random() < severity:
                    payload["QtyDelta"] = 0
                    payload["Reason"] = "OUTAGE"
                    modified = True

            elif disruption_type == "inventory_shortage":
                # Reduce inventory quantities
                if payload.get("QtyDelta", 0) > 0:  # Only affect positive inventory
                    reduction_factor = severity * random.uniform(0.5, 1.0)
                    payload["QtyDelta"] = max(
                        0, int(payload["QtyDelta"] * (1 - reduction_factor))
                    )
                    modified = True

        # Store inventory disruptions
        elif (
            event_type == "store_inventory_txn"
            and disruption_type == "inventory_shortage"
            and payload.get("StoreID") == target_id
        ):
            # Reduce store deliveries
            if (
                payload.get("QtyDelta", 0) > 0
                and payload.get("Reason") == "INBOUND_SHIPMENT"
            ):
                reduction_factor = severity * random.uniform(0.3, 0.8)
                payload["QtyDelta"] = max(
                    0, int(payload["QtyDelta"] * (1 - reduction_factor))
                )
                modified = True

        # Truck movement disruptions
        elif (
            event_type == "truck_move"
            and disruption_type in ["truck_breakdown", "weather_delay"]
            and payload.get("TruckId") == str(target_id)
        ):
            if disruption_type == "truck_breakdown":
                # Set truck to delayed status
                if random.random() < severity:
                    payload["Status"] = "DELAYED"
                    # Extend ETA
                    if "ETA" in payload:
                        original_eta = datetime.fromisoformat(
                            payload["ETA"].replace("Z", "+00:00")
                        )
                        delay_hours = int(severity * 8)  # Up to 8 hour delay
                        payload["ETA"] = (
                            original_eta + timedelta(hours=delay_hours)
                        ).isoformat()
                    modified = True

            elif disruption_type == "weather_delay":
                # Weather affects all trucks in area - add delays
                if random.random() < severity * 0.7:  # 70% of severity probability
                    payload["Status"] = "DELAYED"
                    delay_hours = int(severity * 4)  # Up to 4 hour delay
                    if "ETA" in payload:
                        original_eta = datetime.fromisoformat(
                            payload["ETA"].replace("Z", "+00:00")
                        )
                        payload["ETA"] = (
                            original_eta + timedelta(hours=delay_hours)
                        ).isoformat()
                    modified = True

        if modified:
            # Increment events affected counter
            disruption_data["events_affected"] += 1
            # Add disruption marker to event
            event_data["disruption_applied"] = {
                "disruption_id": disruption_id,
                "type": disruption_type,
                "severity": severity,
            }

    return event_data


# ================================
# DLQ MANAGEMENT ENDPOINTS
# ================================


@router.get("/stream/dlq/summary", tags=["streaming"])
async def get_dlq_summary(
    event_streamer: EventStreamer = Depends(get_event_streamer),
) -> dict:
    """
    Get DLQ summary statistics.

    Returns breakdown by error category and severity.
    """
    if not event_streamer:
        return {
            "size": 0,
            "by_category": {},
            "by_severity": {},
            "oldest_entry": None,
            "newest_entry": None,
        }

    return event_streamer.get_dlq_summary()


@router.post("/stream/dlq/retry", tags=["streaming"])
async def retry_dlq_events(
    max_retries: int = Body(3, description="Max retry attempts per event"),
    event_streamer: EventStreamer = Depends(get_event_streamer),
) -> dict:
    """
    Retry events from dead letter queue.

    Returns statistics about retry operation.
    """
    if not event_streamer:
        raise HTTPException(
            status_code=400,
            detail="No active streamer",
        )

    result = await event_streamer.retry_dlq_events(max_retries)
    return result


@router.get("/stream/dlq/events", tags=["streaming"])
async def get_dlq_events(
    limit: int = Query(100, ge=1, le=1000, description="Max events to return"),
    category: str | None = Query(None, description="Filter by error category"),
    event_streamer: EventStreamer = Depends(get_event_streamer),
) -> dict:
    """Get events from DLQ with optional filtering."""
    if not event_streamer:
        return {"events": [], "total": 0}

    # Access DLQ directly (thread-safe via lock in streamer)
    dlq = event_streamer._dlq

    # Filter by category if specified
    if category:
        filtered = [e for e in dlq if e.error_category == category]
    else:
        filtered = dlq

    # Limit results
    events = filtered[:limit]

    return {
        "events": [
            {
                "trace_id": e.event.trace_id,
                "event_type": e.event.event_type.value,
                "error_message": e.error_message,
                "error_category": e.error_category,
                "error_severity": e.error_severity,
                "timestamp": e.timestamp,
                "retry_count": e.retry_count,
                "last_retry_timestamp": e.last_retry_timestamp,
            }
            for e in events
        ],
        "total": len(filtered),
        "returned": len(events),
    }
