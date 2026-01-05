"""
Streaming routers package.

This package contains modularized FastAPI routers for streaming functionality:
- control: Start, stop, pause, resume streaming
- outbox: Outbox management endpoints
- config: Configuration and connection validation
- monitoring: Statistics, health, and event monitoring
- disruption: Supply chain disruption simulation
- dlq: Dead letter queue management
"""

from fastapi import APIRouter

from .config import router as config_router
from .control import router as control_router
from .disruption import (
    apply_disruption_effects,
    get_active_disruptions_for_target,
    router as disruption_router,
)
from .dlq import router as dlq_router
from .monitoring import router as monitoring_router
from .outbox import router as outbox_router
from .state import (
    active_disruptions,
    get_session_id,
    get_start_time,
    recent_events,
    reset_streaming_state,
    set_session,
    streaming_statistics,
    update_streaming_statistics,
)

# Create combined router
router = APIRouter()

# Include all sub-routers
router.include_router(control_router)
router.include_router(outbox_router)
router.include_router(config_router)
router.include_router(monitoring_router)
router.include_router(disruption_router)
router.include_router(dlq_router)

__all__ = [
    # Main router
    "router",
    # Sub-routers (for direct access if needed)
    "control_router",
    "outbox_router",
    "config_router",
    "monitoring_router",
    "disruption_router",
    "dlq_router",
    # State exports (for backward compatibility)
    "streaming_statistics",
    "active_disruptions",
    "recent_events",
    "reset_streaming_state",
    "update_streaming_statistics",
    "get_session_id",
    "get_start_time",
    "set_session",
    # Disruption helpers
    "get_active_disruptions_for_target",
    "apply_disruption_effects",
]
