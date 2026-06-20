"""
Real-time event streaming module for the retail data generator.

This module provides components for generating and streaming retail events
to Azure Event Hub in real-time with proper error handling and monitoring.
"""

from .azure_client import AzureEventHubClient
from .event_factory import EventFactory
from .event_streaming import EventStreamer
from .schemas import EventEnvelope, EventType

__all__ = [
    "EventStreamer",
    "EventFactory",
    "AzureEventHubClient",
    "EventEnvelope",
    "EventType",
]
