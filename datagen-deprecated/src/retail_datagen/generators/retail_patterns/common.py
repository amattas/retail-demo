"""
Common types, enums, and constants used across retail pattern simulators.

This module contains shared data structures and constants that are used
by multiple simulators (customer journey, inventory, marketing).
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from retail_datagen.shared.models import ProductMaster


class ShoppingBehaviorType(Enum):
    """Types of shopping behaviors."""

    QUICK_TRIP = "QUICK_TRIP"  # 1-3 items, focused shopping
    GROCERY_RUN = "GROCERY_RUN"  # 5-15 items, routine shopping
    FAMILY_SHOPPING = "FAMILY_SHOPPING"  # 10-30 items, planned shopping
    BULK_SHOPPING = "BULK_SHOPPING"  # 20-50 items, bulk purchases


class CustomerSegment(Enum):
    """Customer segments with different behaviors."""

    BUDGET_CONSCIOUS = "BUDGET_CONSCIOUS"
    CONVENIENCE_FOCUSED = "CONVENIENCE_FOCUSED"
    QUALITY_SEEKER = "QUALITY_SEEKER"
    BRAND_LOYAL = "BRAND_LOYAL"


@dataclass
class ShoppingBasket:
    """Represents a shopping basket with products and quantities."""

    items: list[tuple[ProductMaster, int]]  # (product, quantity)
    behavior_type: ShoppingBehaviorType
    customer_segment: CustomerSegment
    total_items: int
    estimated_total: Decimal


# Marketing campaign generation constants
CAMPAIGN_START_PROBABILITY = (
    0.90  # Probability of starting new campaigns when no campaign is active
)

# Marketing campaign constants
DEFAULT_MIN_DAILY_IMPRESSIONS = 100  # Minimum impressions per campaign per day
