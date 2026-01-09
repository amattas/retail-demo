"""
Event generator mixins for EventFactory.

This package provides modular event generation capabilities split by event type
for maintainability while preserving backward compatibility.
"""

from .customer_events import CustomerEventsMixin
from .inventory_events import InventoryEventsMixin
from .logistics_events import LogisticsEventsMixin
from .marketing_events import MarketingEventsMixin
from .receipt_events import ReceiptEventsMixin

__all__ = [
    "ReceiptEventsMixin",
    "CustomerEventsMixin",
    "InventoryEventsMixin",
    "LogisticsEventsMixin",
    "MarketingEventsMixin",
]
