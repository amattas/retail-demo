"""
Event streaming module for the retail data generator.

This module provides components for streaming retail events from DuckDB
to Azure Event Hub with proper error handling and monitoring.
"""

from .azure_client import AzureEventHubClient
from .event_streaming import EventStreamer
from .schemas import EventEnvelope, EventType

__all__ = [
    "EventStreamer",
    "AzureEventHubClient",
    "EventEnvelope",
    "EventType",
]
