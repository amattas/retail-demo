"""
Event streaming package.

This package provides modular event streaming functionality with
components for configuration, batch streaming, DLQ management,
monitoring, and core streaming operations.

For backward compatibility, the main EventStreamer class and
related types are re-exported at the package level.
"""

from .config import (
    DLQEntry,
    StreamingConfig,
    StreamingStatistics,
    event_generation_pipeline,
)
from .streamer import EventStreamer

__all__ = [
    "EventStreamer",
    "StreamingConfig",
    "StreamingStatistics",
    "DLQEntry",
    "event_generation_pipeline",
]
