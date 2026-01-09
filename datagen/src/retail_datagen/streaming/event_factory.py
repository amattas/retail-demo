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
from .event_generators import (
    CustomerEventsMixin,
    InventoryEventsMixin,
    LogisticsEventsMixin,
    MarketingEventsMixin,
    ReceiptEventsMixin,
)
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


class EventFactory(
    ReceiptEventsMixin,
    CustomerEventsMixin,
    InventoryEventsMixin,
    LogisticsEventsMixin,
    MarketingEventsMixin,
):
    """
    Factory for generating realistic retail events.

    Creates events that follow realistic patterns:
    - Time-based distributions (more activity during business hours)
    - Store-based variations (different patterns by store size/location)
    - Correlated events (receipt -> inventory -> reorder chains)
    - Seasonal effects and promotional periods

    Inherits from:
        ReceiptEventsMixin: Receipt, line item, and payment events
        CustomerEventsMixin: Customer entry, zone change, BLE ping events
        InventoryEventsMixin: Inventory, stockout, and reorder events
        LogisticsEventsMixin: Truck arrival/departure, store open/close events
        MarketingEventsMixin: Ad impression, promotion, online order events
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

        Args:
            event_type: Type of event to generate
            timestamp: Event timestamp

        Returns:
            EventEnvelope or None if event cannot be generated
        """
        trace_id = self.generate_trace_id(timestamp)

        # Clean up expired customer sessions before generating events
        self._cleanup_expired_sessions(timestamp)

        try:
            payload = None
            correlation_id = None
            partition_key = None

            # Helper to safely unpack results that may be None
            def _unpack_result(result):
                if result is None:
                    return None, None, None
                return result

            if event_type == EventType.RECEIPT_CREATED:
                payload, correlation_id, partition_key = _unpack_result(
                    self._generate_receipt_created(timestamp)
                )
            elif event_type == EventType.RECEIPT_LINE_ADDED:
                payload, correlation_id, partition_key = _unpack_result(
                    self._generate_receipt_line_added(timestamp)
                )
            elif event_type == EventType.PAYMENT_PROCESSED:
                payload, correlation_id, partition_key = _unpack_result(
                    self._generate_payment_processed(timestamp)
                )
            elif event_type == EventType.INVENTORY_UPDATED:
                payload, correlation_id, partition_key = (
                    self._generate_inventory_updated(timestamp)
                )
            elif event_type == EventType.STOCKOUT_DETECTED:
                payload, correlation_id, partition_key = (
                    self._generate_stockout_detected(timestamp)
                )
            elif event_type == EventType.REORDER_TRIGGERED:
                payload, correlation_id, partition_key = (
                    self._generate_reorder_triggered(timestamp)
                )
            elif event_type == EventType.CUSTOMER_ENTERED:
                payload, correlation_id, partition_key = (
                    self._generate_customer_entered(timestamp)
                )
            elif event_type == EventType.CUSTOMER_ZONE_CHANGED:
                payload, correlation_id, partition_key = _unpack_result(
                    self._generate_customer_zone_changed(timestamp)
                )
            elif event_type == EventType.BLE_PING_DETECTED:
                payload, correlation_id, partition_key = _unpack_result(
                    self._generate_ble_ping_detected(timestamp)
                )
            elif event_type == EventType.TRUCK_ARRIVED:
                payload, correlation_id, partition_key = self._generate_truck_arrived(
                    timestamp
                )
            elif event_type == EventType.TRUCK_DEPARTED:
                payload, correlation_id, partition_key = _unpack_result(
                    self._generate_truck_departed(timestamp)
                )
            elif event_type == EventType.STORE_OPENED:
                payload, correlation_id, partition_key = self._generate_store_opened(
                    timestamp
                )
            elif event_type == EventType.STORE_CLOSED:
                payload, correlation_id, partition_key = self._generate_store_closed(
                    timestamp
                )
            elif event_type == EventType.AD_IMPRESSION:
                payload, correlation_id, partition_key = self._generate_ad_impression(
                    timestamp
                )
            elif event_type == EventType.PROMOTION_APPLIED:
                payload, correlation_id, partition_key = _unpack_result(
                    self._generate_promotion_applied(timestamp)
                )
            elif event_type == EventType.ONLINE_ORDER_CREATED:
                payload, correlation_id, partition_key = (
                    self._generate_online_order_created(timestamp)
                )

            if payload is None:
                return None

            return EventEnvelope(
                event_type=event_type,
                payload=(
                    payload.model_dump() if hasattr(payload, "model_dump") else payload
                ),
                trace_id=trace_id,
                ingest_timestamp=timestamp,
                correlation_id=correlation_id,
                partition_key=partition_key,
            )

        except Exception as e:
            # Log error but don't fail completely
            print(f"Error generating {event_type} event: {e}")
            return None

    def generate_mixed_events(
        self,
        count: int,
        timestamp: datetime,
        event_weights: dict[EventType, float] | None = None,
    ) -> list[EventEnvelope]:
        """
        Generate a mix of different event types for realistic simulation.

        Args:
            count: Number of events to generate
            timestamp: Base timestamp for events
            event_weights: Optional weights for event type selection

        Returns:
            List of generated events
        """
        if event_weights is None:
            event_weights = {
                EventType.RECEIPT_CREATED: 0.15,
                EventType.RECEIPT_LINE_ADDED: 0.15,
                EventType.CUSTOMER_ENTERED: 0.20,
                EventType.BLE_PING_DETECTED: 0.15,
                EventType.INVENTORY_UPDATED: 0.10,
                EventType.AD_IMPRESSION: 0.15,
                EventType.PAYMENT_PROCESSED: 0.05,
                EventType.TRUCK_ARRIVED: 0.02,
                EventType.STOCKOUT_DETECTED: 0.02,
                EventType.REORDER_TRIGGERED: 0.01,
                EventType.ONLINE_ORDER_CREATED: 0.08,
            }

        events = []
        event_types = list(event_weights.keys())
        weights = list(event_weights.values())

        for i in range(count):
            # Add small time variation to each event
            event_timestamp = timestamp + timedelta(
                milliseconds=self.rng.randint(0, 1000)
            )

            # Select event type based on weights
            event_type = self.rng.choices(event_types, weights=weights)[0]

            # Only generate if it makes sense for current time
            if self.should_generate_event(event_type, event_timestamp):
                event = self.generate_event(event_type, event_timestamp)
                if event:
                    events.append(event)
                    # If online order created, also emit follow-up events
                    if event.event_type == EventType.ONLINE_ORDER_CREATED:
                        follow_ups = self._generate_online_order_followups(
                            event, event_timestamp
                        )
                        events.extend(follow_ups)

        return events

    def _generate_online_order_followups(
        self, order_event: EventEnvelope, timestamp: datetime
    ) -> list[EventEnvelope]:
        """Generate follow-up events for an online order."""
        followup_events = []
        try:
            pl = order_event.payload
            node_type = pl.get("node_type")
            node_id = pl.get("node_id")
            product_id = self.rng.choice(list(self.products.keys()))
            qty_delta = -self.rng.randint(1, 3)
            inv_payload = InventoryUpdatedPayload(
                store_id=node_id if node_type == "STORE" else None,
                dc_id=node_id if node_type == "DC" else None,
                product_id=product_id,
                quantity_delta=qty_delta,
                reason=InventoryReason.SALE.value,
                source="ONLINE",
            )
            inv_envelope = EventEnvelope(
                event_type=EventType.INVENTORY_UPDATED,
                payload=inv_payload.model_dump(),
                trace_id=self.generate_trace_id(timestamp),
                ingest_timestamp=timestamp,
                partition_key=f"{node_type.lower()}_{node_id}",
                correlation_id=order_event.trace_id,
            )
            followup_events.append(inv_envelope)

            # Order picked event (slightly later)
            picked_payload = OnlineOrderPickedPayload(
                order_id=pl.get("order_id"),
                node_type=node_type,
                node_id=node_id,
                fulfillment_mode=pl.get("fulfillment_mode"),
                picked_time=timestamp + timedelta(seconds=self.rng.randint(60, 900)),
            )
            followup_events.append(
                EventEnvelope(
                    event_type=EventType.ONLINE_ORDER_PICKED,
                    payload=picked_payload.model_dump(),
                    trace_id=self.generate_trace_id(timestamp),
                    ingest_timestamp=timestamp,
                    partition_key=f"{node_type.lower()}_{node_id}",
                    correlation_id=order_event.trace_id,
                )
            )

            # Order shipped event (after pick)
            shipped_payload = OnlineOrderShippedPayload(
                order_id=pl.get("order_id"),
                node_type=node_type,
                node_id=node_id,
                fulfillment_mode=pl.get("fulfillment_mode"),
                shipped_time=timestamp + timedelta(seconds=self.rng.randint(900, 3600)),
            )
            followup_events.append(
                EventEnvelope(
                    event_type=EventType.ONLINE_ORDER_SHIPPED,
                    payload=shipped_payload.model_dump(),
                    trace_id=self.generate_trace_id(timestamp),
                    ingest_timestamp=timestamp,
                    partition_key=f"{node_type.lower()}_{node_id}",
                    correlation_id=order_event.trace_id,
                )
            )
        except Exception:
            pass

        return followup_events
