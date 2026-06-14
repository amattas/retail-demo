"""
Dead Letter Queue (DLQ) management endpoints.

This module handles DLQ operations for failed streaming events.
"""

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from ...shared.dependencies import get_event_streamer
from ...streaming.event_streaming import EventStreamer

router = APIRouter()


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
