"""
Streaming monitoring endpoints.

This module handles statistics, health checks, and event monitoring.
"""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Query

from ...api.models import RecentEventsResponse, StreamingStatisticsResponse
from ...config.models import RetailConfig
from ...shared.dependencies import get_config, get_task_status
from ...streaming.schemas import EventType
from .state import (
    get_session_id,
    get_start_time,
    recent_events,
    streaming_statistics,
)

router = APIRouter()

AVAILABLE_EVENT_TYPES = [e.value for e in EventType]


@router.get(
    "/stream/statistics",
    response_model=StreamingStatisticsResponse,
    summary="Get streaming statistics",
    description="Get detailed streaming statistics and metrics",
)
async def get_streaming_statistics():
    """Get detailed streaming statistics."""
    return StreamingStatisticsResponse(
        events_generated=streaming_statistics["events_generated"],
        events_sent_successfully=streaming_statistics["events_sent_successfully"],
        events_failed=streaming_statistics["events_failed"],
        batches_sent=streaming_statistics["batches_sent"],
        total_streaming_time=streaming_statistics["total_streaming_time"],
        events_per_second=streaming_statistics["events_per_second"],
        bytes_sent=streaming_statistics["bytes_sent"],
        last_event_time=streaming_statistics["last_event_time"],
        event_type_counts=streaming_statistics["event_type_counts"],
        error_counts=streaming_statistics["error_counts"],
        connection_failures=streaming_statistics["connection_failures"],
        circuit_breaker_trips=streaming_statistics["circuit_breaker_trips"],
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
    recent = list(recent_events)[-limit:]

    # Format for response
    formatted_events = []
    for event_data in recent:
        formatted_events.append(
            {
                "timestamp": event_data["timestamp"],
                "event_type": event_data["event"].get("event_type"),
                "trace_id": event_data["event"].get("trace_id"),
                "payload": event_data["event"].get("payload", {}),
            }
        )

    return RecentEventsResponse(
        events=formatted_events,
        count=len(formatted_events),
        timestamp=datetime.now(UTC),
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

    session_id = get_session_id()
    start_time = get_start_time()

    # Check if streaming is active
    if session_id:
        task_status = get_task_status(session_id)
        if task_status:
            checks["streaming_task"] = {
                "status": "active",
                "task_status": task_status["status"],
                "uptime_seconds": (
                    (datetime.now(UTC) - start_time).total_seconds()
                    if start_time
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
        "events_generated": streaming_statistics["events_generated"],
        "events_per_second": streaming_statistics["events_per_second"],
        "failure_rate": (
            streaming_statistics["events_failed"]
            / max(streaming_statistics["events_generated"], 1)
        ),
    }

    return {"status": overall_status, "timestamp": datetime.now(UTC), "checks": checks}


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
