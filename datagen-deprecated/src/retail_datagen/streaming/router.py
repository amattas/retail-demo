"""
FastAPI router for real-time streaming endpoints.

This module provides REST API endpoints for managing real-time event streaming
to Azure Event Hub with comprehensive monitoring and control capabilities.

NOTE: This is a facade module that re-exports from the modularized routers package.
The actual implementation is in streaming/routers/ submodules:
- routers/control.py: Start, stop, pause, resume streaming
- routers/outbox.py: Outbox management endpoints
- routers/config.py: Configuration and connection validation
- routers/monitoring.py: Statistics, health, and event monitoring
- routers/disruption.py: Supply chain disruption simulation
- routers/dlq.py: Dead letter queue management
- routers/state.py: Shared state management
"""

# Re-export everything from the routers package for backward compatibility
from .routers import (
    # State exports
    active_disruptions,
    # Disruption helpers
    apply_disruption_effects,
    get_active_disruptions_for_target,
    get_session_id,
    get_start_time,
    recent_events,
    reset_streaming_state,
    # Main router (combines all sub-routers)
    router,
    set_session,
    streaming_statistics,
    update_streaming_statistics,
)

# Legacy aliases for backward compatibility
_streaming_session_id = None  # Use get_session_id() instead
_streaming_start_time = None  # Use get_start_time() instead
_recent_events = recent_events
_streaming_statistics = streaming_statistics
_active_disruptions = active_disruptions
_reset_streaming_state = reset_streaming_state
_update_streaming_statistics = update_streaming_statistics

__all__ = [
    "router",
    "streaming_statistics",
    "active_disruptions",
    "recent_events",
    "reset_streaming_state",
    "update_streaming_statistics",
    "get_session_id",
    "get_start_time",
    "set_session",
    "get_active_disruptions_for_target",
    "apply_disruption_effects",
    # Legacy aliases
    "_streaming_statistics",
    "_active_disruptions",
    "_recent_events",
    "_reset_streaming_state",
    "_update_streaming_statistics",
]
