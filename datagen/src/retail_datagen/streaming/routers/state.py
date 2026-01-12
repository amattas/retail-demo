"""
Shared streaming state and helper functions.

This module contains the global state used by streaming router endpoints
and utility functions for state management.
"""

import logging
import traceback
from collections import deque
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

# Global streaming state
streaming_session_id: str | None = None
streaming_start_time: datetime | None = None
recent_events: deque = deque(maxlen=100)  # Store last 100 events
streaming_statistics = {
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
active_disruptions: dict[str, dict[str, Any]] = {}  # disruption_id -> disruption_data


def reset_streaming_state():
    """Reset global streaming state."""
    global \
        streaming_session_id, \
        streaming_start_time, \
        recent_events, \
        streaming_statistics

    streaming_session_id = None
    streaming_start_time = None
    recent_events.clear()
    streaming_statistics.update(
        {
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
    )


def update_streaming_statistics(event_data: dict[str, Any], success: bool = True):
    """Update streaming statistics with new event data."""
    global streaming_statistics

    streaming_statistics["events_generated"] += 1
    streaming_statistics["last_event_time"] = datetime.now(UTC)

    if success:
        streaming_statistics["events_sent_successfully"] += 1
        # Estimate bytes (simplified)
        streaming_statistics["bytes_sent"] += len(str(event_data))
    else:
        streaming_statistics["events_failed"] += 1

    # Update event type counts
    event_type = event_data.get("event_type", "unknown")
    streaming_statistics["event_type_counts"][event_type] = (
        streaming_statistics["event_type_counts"].get(event_type, 0) + 1
    )

    # Calculate events per second
    if streaming_start_time:
        elapsed = (datetime.now(UTC) - streaming_start_time).total_seconds()
        if elapsed > 0:
            streaming_statistics["events_per_second"] = (
                streaming_statistics["events_generated"] / elapsed
            )
            streaming_statistics["total_streaming_time"] = elapsed


def set_session(session_id: str | None, start_time: datetime | None = None):
    """Set the current streaming session."""
    global streaming_session_id, streaming_start_time

    # Log session state changes with stack trace for debugging
    old_session = streaming_session_id
    caller_stack = ''.join(traceback.format_stack()[-3:-1])

    if session_id is None and old_session is not None:
        logger.warning(
            f"🔴 Session being CLEARED: {old_session} -> None\n"
            f"Called from:\n{caller_stack}"
        )
    elif session_id is not None and old_session is None:
        logger.info(
            f"🟢 Session being SET: None -> {session_id}\n"
            f"Called from:\n{caller_stack}"
        )
    elif session_id != old_session:
        logger.info(
            f"🟡 Session being CHANGED: {old_session} -> {session_id}\n"
            f"Called from:\n{caller_stack}"
        )

    streaming_session_id = session_id
    streaming_start_time = start_time


def get_session_id() -> str | None:
    """Get the current streaming session ID."""
    # Periodically log session state for monitoring
    # Only log every ~10 calls to avoid spam
    import random
    if random.random() < 0.1:
        logger.debug(f"📊 get_session_id() returning: {streaming_session_id}")
    return streaming_session_id


def get_start_time() -> datetime | None:
    """Get the streaming start time."""
    return streaming_start_time
