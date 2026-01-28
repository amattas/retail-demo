"""
Event generation factory for real-time streaming.

This module provides event generation capabilities that create realistic
retail events with proper timing patterns and correlations.

The module has been modularized for maintainability while preserving
backward compatibility through mixin inheritance.
"""

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..generators.seasonal_patterns import CompositeTemporalPatterns
from ..shared.models import (
    Customer,
    DistributionCenter,
    InventoryReason,
    ProductMaster,
    Store,
)
# NOTE: event_generators removed in #214
# EventFactory will be deprecated in #215 as part of the migration to batch-only streaming
from .schemas import (
    EventEnvelope,
    EventType,
    InventoryUpdatedPayload,
    OnlineOrderPickedPayload,
    OnlineOrderShippedPayload,
)


@dataclass
class EventGenerationState:
    """Maintains state for realistic event generation."""

    active_receipts: dict[str, dict] = None  # receipt_id -> receipt_info
    customer_sessions: dict[str, dict] = None  # customer_id -> session_info
    store_inventory: dict[tuple[int, int], int] = (
        None  # (store_id, product_id) -> quantity
    )
    dc_inventory: dict[tuple[int, int], int] = None  # (dc_id, product_id) -> quantity
    active_trucks: dict[str, dict] = None  # truck_id -> truck_info
    store_hours: dict[int, dict] = None  # store_id -> hours_info
    promotion_campaigns: dict[str, dict] = None  # campaign_id -> campaign_info
    marketing_conversions: dict[str, dict] = None  # impression_id -> conversion_info
    customer_to_campaign: dict[int, str] = (
        None  # customer_id -> campaign_id (O(1) lookup index)
    )

    def __post_init__(self):
        if self.active_receipts is None:
            self.active_receipts = {}
        if self.customer_sessions is None:
            self.customer_sessions = {}
        if self.store_inventory is None:
            self.store_inventory = defaultdict(lambda: random.randint(50, 500))
        if self.dc_inventory is None:
            self.dc_inventory = defaultdict(lambda: random.randint(1000, 5000))
        if self.active_trucks is None:
            self.active_trucks = {}
        if self.store_hours is None:
            self.store_hours = {}
        if self.promotion_campaigns is None:
            self.promotion_campaigns = {}
        if self.marketing_conversions is None:
            self.marketing_conversions = {}
        if self.customer_to_campaign is None:
            self.customer_to_campaign = {}


class EventFactory:
    """
    Factory for generating realistic retail events.

    DEPRECATED: This class is being deprecated as part of #184.
    Real-time event generation is replaced by batch streaming from DuckDB.
    This stub remains temporarily to avoid breaking imports until #215.

    The event generation logic has been removed. Use batch_streaming module instead.
    """

    def __init__(
        self,
        stores: list[Store],
        customers: list[Customer],
        products: list[ProductMaster],
        distribution_centers: list[DistributionCenter],
        seed: int = 42,
    ):
        """
        Initialize event factory with master data.

        Args:
            stores: List of store master records
            customers: List of customer master records
            products: List of product master records
            distribution_centers: List of DC master records
            seed: Random seed for reproducibility
        """
        self.stores = {store.ID: store for store in stores}
        self.customers = {customer.ID: customer for customer in customers}
        self.products = {product.ID: product for product in products}
        self.dcs = {dc.ID: dc for dc in distribution_centers}

        self.rng = random.Random(seed)
        self.state = EventGenerationState()
        self.temporal_patterns = CompositeTemporalPatterns(seed)

        # Initialize store operating hours (7 AM - 10 PM)
        for store_id in self.stores.keys():
            self.state.store_hours[store_id] = {
                "open_time": 7,  # 7 AM
                "close_time": 22,  # 10 PM
                "is_open": False,
                "current_customers": 0,
            }

        # Pre-generate some promotion campaigns
        self._initialize_promotions()

    def _initialize_promotions(self):
        """Initialize promotional campaigns for marketing events."""
        campaigns = [
            {"id": "WINTER2024", "discount": 0.15, "active": True},
            {"id": "SPRING2024", "discount": 0.20, "active": False},
            {"id": "LOYALTY2024", "discount": 0.10, "active": True},
            {"id": "NEWCUSTOMER", "discount": 0.25, "active": True},
        ]

        for campaign in campaigns:
            self.state.promotion_campaigns[campaign["id"]] = campaign

    def generate_trace_id(self, timestamp: datetime) -> str:
        """
        Generate unique trace ID for event tracking.

        Args:
            timestamp: Event timestamp

        Returns:
            str: Unique trace ID
        """
        epoch = int(timestamp.timestamp())
        sequence = self.rng.randint(10000, 99999)
        return f"TR_{epoch}_{sequence:05d}"

    def should_generate_event(
        self, event_type: EventType, current_time: datetime
    ) -> bool:
        """
        Determine if an event should be generated based on realistic patterns.

        Args:
            event_type: Type of event to potentially generate
            current_time: Current timestamp

        Returns:
            bool: True if event should be generated
        """
        hour = current_time.hour
        day_of_week = current_time.weekday()  # 0=Monday, 6=Sunday

        # Business hours factor (more activity 9 AM - 8 PM).
        # Online orders allowed 24/7 with evening bias.
        if event_type == EventType.ONLINE_ORDER_CREATED:
            if 18 <= hour <= 23:
                business_factor = 1.0
            elif 0 <= hour < 7 or 12 <= hour < 18:
                business_factor = 0.7
            else:
                business_factor = 0.5
        else:
            if 9 <= hour <= 20:
                business_factor = 1.0
            elif 7 <= hour < 9 or 20 < hour <= 22:
                business_factor = 0.6
            else:
                business_factor = 0.1

        # Weekend factor (slightly less activity on weekends)
        weekend_factor = 0.8 if day_of_week >= 5 else 1.0

        # Event type specific probabilities
        base_probabilities = {
            EventType.RECEIPT_CREATED: 0.15,
            EventType.CUSTOMER_ENTERED: 0.20,
            EventType.INVENTORY_UPDATED: 0.10,
            EventType.BLE_PING_DETECTED: 0.25,
            EventType.AD_IMPRESSION: 0.30,
            EventType.TRUCK_ARRIVED: 0.05,
            EventType.STORE_OPENED: 0.01,
            EventType.STORE_CLOSED: 0.01,
            EventType.ONLINE_ORDER_CREATED: 0.12,
        }

        base_prob = base_probabilities.get(event_type, 0.05)
        final_prob = base_prob * business_factor * weekend_factor

        # Apply seasonal patterns to demand-like events (bounded)
        if event_type in {
            EventType.RECEIPT_CREATED,
            EventType.CUSTOMER_ENTERED,
            EventType.BLE_PING_DETECTED,
            EventType.AD_IMPRESSION,
            EventType.ONLINE_ORDER_CREATED,
        }:
            seasonal_mult = self.temporal_patterns.get_overall_multiplier(current_time)
            seasonal_mult = max(0.6, min(seasonal_mult, 1.8))
            final_prob *= seasonal_mult

        return self.rng.random() < final_prob

    def generate_event(
        self, event_type: EventType, timestamp: datetime
    ) -> EventEnvelope | None:
        """
        Generate a specific type of event.

        DEPRECATED: Event generation removed in #214.
        Use batch_streaming module instead for DuckDB-based event streaming.

        Args:
            event_type: Type of event to generate
            timestamp: Event timestamp

        Returns:
            None (event generation disabled)
        """
        import warnings
        warnings.warn(
            "EventFactory.generate_event is deprecated. Use batch_streaming module instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return None

    def generate_mixed_events(
        self,
        count: int,
        timestamp: datetime,
        event_weights: dict[EventType, float] | None = None,
    ) -> list[EventEnvelope]:
        """
        Generate a mix of different event types for realistic simulation.

        DEPRECATED: Event generation removed in #214.
        Use batch_streaming module instead for DuckDB-based event streaming.

        Args:
            count: Number of events to generate
            timestamp: Base timestamp for events
            event_weights: Optional weights for event type selection

        Returns:
            Empty list (event generation disabled)
        """
        import warnings
        warnings.warn(
            "EventFactory.generate_mixed_events is deprecated. Use batch_streaming module instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return []

    def _cleanup_expired_sessions(self, timestamp: datetime) -> None:
        """
        Cleanup expired customer sessions and marketing conversions.

        DEPRECATED: Preserved for compatibility only.
        """
        pass
