"""
Retail behavior simulation patterns for realistic data generation.

This package provides simulators for various retail behaviors including
customer shopping patterns, inventory flows, marketing campaigns,
and cross-table business logic coordination.

The package has been modularized for maintainability while preserving
backward compatibility through re-exports.

Naming Conventions:
    This module follows a consistent naming convention to distinguish between
    internal Python code and output data schemas:

    - **Python variables/parameters**: snake_case (PEP 8)
      Examples: dc_id, store_id, product_id, truck_id, shipment_id

    - **Output dictionary keys**: PascalCase (matching Pydantic model schemas)
      Examples: DCID, StoreID, ProductID, TruckId, ShipmentId

    This dual convention is intentional. Internal Python code uses snake_case for
    readability and PEP 8 compliance, while output dictionaries use PascalCase to
    match the fact table schemas defined in shared/models.py. The conversion happens
    explicitly when constructing output dictionaries, with inline comments marking
    these conversion points for clarity.
"""

# Re-export all classes and types for backward compatibility
from .business_rules import BusinessRulesEngine
from .common import (
    CAMPAIGN_START_PROBABILITY,
    DEFAULT_MIN_DAILY_IMPRESSIONS,
    CustomerSegment,
    ShoppingBasket,
    ShoppingBehaviorType,
)
from .customer_journey import CustomerJourneySimulator
from .disruption_simulator import DisruptionMixin
from .inventory_flow import InventoryFlowSimulator
from .marketing_campaign import MarketingCampaignSimulator
from .truck_operations import TruckOperationsMixin

__all__ = [
    # Simulators
    "CustomerJourneySimulator",
    "InventoryFlowSimulator",
    "MarketingCampaignSimulator",
    "BusinessRulesEngine",
    # Mixins (for advanced use/extension)
    "TruckOperationsMixin",
    "DisruptionMixin",
    # Common types and enums
    "ShoppingBehaviorType",
    "CustomerSegment",
    "ShoppingBasket",
    # Constants
    "CAMPAIGN_START_PROBABILITY",
    "DEFAULT_MIN_DAILY_IMPRESSIONS",
]
