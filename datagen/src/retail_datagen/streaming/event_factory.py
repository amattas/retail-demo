"""
Event generation factory for real-time streaming.

This module provides event generation capabilities that create realistic
retail events with proper timing patterns and correlations.
"""

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..generators.seasonal_patterns import CompositeTemporalPatterns
from ..shared.models import (
    Customer,
    DeviceType,
    DistributionCenter,
    InventoryReason,
    MarketingChannel,
    ProductMaster,
    Store,
    TenderType,
)
from .schemas import (
    AdImpressionPayload,
    BLEPingDetectedPayload,
    CustomerEnteredPayload,
    CustomerZoneChangedPayload,
    EventEnvelope,
    EventType,
    InventoryUpdatedPayload,
    OnlineOrderCreatedPayload,
    OnlineOrderPickedPayload,
    OnlineOrderShippedPayload,
    PaymentProcessedPayload,
    PromotionAppliedPayload,
    ReceiptCreatedPayload,
    ReceiptLineAddedPayload,
    ReorderTriggeredPayload,
    StockoutDetectedPayload,
    StoreOperationPayload,
    TruckArrivedPayload,
    TruckDepartedPayload,
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


class EventFactory:
    """
    Factory for generating realistic retail events.

    Creates events that follow realistic patterns:
    - Time-based distributions (more activity during business hours)
    - Store-based variations (different patterns by store size/location)
    - Correlated events (receipt -> inventory -> reorder chains)
    - Seasonal effects and promotional periods
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

        # Business hours factor (more activity 9 AM - 8 PM). Online orders allowed 24/7 with evening bias.
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

            if event_type == EventType.RECEIPT_CREATED:
                payload, correlation_id, partition_key = self._generate_receipt_created(
                    timestamp
                )
            elif event_type == EventType.RECEIPT_LINE_ADDED:
                payload, correlation_id, partition_key = (
                    self._generate_receipt_line_added(timestamp)
                )
            elif event_type == EventType.PAYMENT_PROCESSED:
                payload, correlation_id, partition_key = (
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
                payload, correlation_id, partition_key = (
                    self._generate_customer_zone_changed(timestamp)
                )
            elif event_type == EventType.BLE_PING_DETECTED:
                payload, correlation_id, partition_key = (
                    self._generate_ble_ping_detected(timestamp)
                )
            elif event_type == EventType.TRUCK_ARRIVED:
                payload, correlation_id, partition_key = self._generate_truck_arrived(
                    timestamp
                )
            elif event_type == EventType.TRUCK_DEPARTED:
                payload, correlation_id, partition_key = self._generate_truck_departed(
                    timestamp
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
                payload, correlation_id, partition_key = (
                    self._generate_promotion_applied(timestamp)
                )
            elif event_type == EventType.ONLINE_ORDER_CREATED:
                payload, correlation_id, partition_key = (
                    self._generate_online_order_created(timestamp)
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
                        try:
                            pl = event.payload
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
                                trace_id=self.generate_trace_id(event_timestamp),
                                ingest_timestamp=event_timestamp,
                                partition_key=f"{node_type.lower()}_{node_id}",
                                correlation_id=event.trace_id,
                            )
                            events.append(inv_envelope)

                            # Order picked event (slightly later)
                            picked_payload = OnlineOrderPickedPayload(
                                order_id=pl.get("order_id"),
                                node_type=node_type,
                                node_id=node_id,
                                fulfillment_mode=pl.get("fulfillment_mode"),
                                picked_time=event_timestamp
                                + timedelta(seconds=self.rng.randint(60, 900)),
                            )
                            events.append(
                                EventEnvelope(
                                    event_type=EventType.ONLINE_ORDER_PICKED,
                                    payload=picked_payload.model_dump(),
                                    trace_id=self.generate_trace_id(event_timestamp),
                                    ingest_timestamp=event_timestamp,
                                    partition_key=f"{node_type.lower()}_{node_id}",
                                    correlation_id=event.trace_id,
                                )
                            )

                            # Order shipped event (after pick)
                            shipped_payload = OnlineOrderShippedPayload(
                                order_id=pl.get("order_id"),
                                node_type=node_type,
                                node_id=node_id,
                                fulfillment_mode=pl.get("fulfillment_mode"),
                                shipped_time=event_timestamp
                                + timedelta(seconds=self.rng.randint(900, 3600)),
                            )
                            events.append(
                                EventEnvelope(
                                    event_type=EventType.ONLINE_ORDER_SHIPPED,
                                    payload=shipped_payload.model_dump(),
                                    trace_id=self.generate_trace_id(event_timestamp),
                                    ingest_timestamp=event_timestamp,
                                    partition_key=f"{node_type.lower()}_{node_id}",
                                    correlation_id=event.trace_id,
                                )
                            )
                        except Exception:
                            pass

        return events

    # Event-specific generation methods

    def _generate_receipt_created(
        self, timestamp: datetime
    ) -> tuple[ReceiptCreatedPayload, str, str] | None:
        """Generate receipt created event - respects marketing-driven purchase likelihood."""
        # Get customers who are currently in stores and haven't made a purchase yet
        eligible_sessions = [
            session
            for session in self.state.customer_sessions.values()
            if (
                timestamp < session["expected_exit_time"]
                and not session["has_made_purchase"]
                and timestamp >= session["entered_at"] + timedelta(minutes=2)
            )  # At least 2 minutes in store
        ]

        if not eligible_sessions:
            return None  # No eligible customers to make purchases

        # Apply purchase likelihood filtering - prioritize marketing-driven customers
        weighted_sessions = []
        for session in eligible_sessions:
            purchase_likelihood = session.get("purchase_likelihood", 0.4)  # Default 40%
            # Apply probability check
            if self.rng.random() < purchase_likelihood:
                # Weight marketing-driven customers higher for selection
                weight = 3 if session.get("marketing_driven", False) else 1
                weighted_sessions.extend([session] * weight)

        if not weighted_sessions:
            return None  # No customers decided to purchase this time

        session = self.rng.choice(weighted_sessions)
        store_id = session["store_id"]
        customer_id = session["customer_id"]
        receipt_id = f"RCP_{int(timestamp.timestamp())}_{self.rng.randint(1000, 9999)}"

        # Generate realistic receipt amounts - marketing customers spend more
        is_marketing_driven = session.get("marketing_driven", False)
        base_item_count = max(1, int(self.rng.gauss(4.2, 2.0)))  # From config

        if is_marketing_driven:
            # Marketing-driven customers buy 25% more items and spend 30% more
            item_count = int(base_item_count * 1.25)
            subtotal = self.rng.uniform(13.0, 260.0)  # 30% higher range
        else:
            item_count = base_item_count
            subtotal = self.rng.uniform(10.0, 200.0)

        tax_rate = 0.08  # 8% tax
        tax = round(subtotal * tax_rate, 2)
        total = subtotal + tax

        tender_type = self.rng.choice(list(TenderType))

        # Mark customer as having made a purchase and move them to checkout
        session["has_made_purchase"] = True
        session["current_zone"] = "CHECKOUT"
        session["expected_exit_time"] = timestamp + timedelta(
            minutes=self.rng.randint(2, 8)
        )  # Exit soon after purchase

        # Store receipt in active receipts for line items
        self.state.active_receipts[receipt_id] = {
            "store_id": store_id,
            "customer_id": customer_id,
            "item_count": item_count,
            "timestamp": timestamp,
            "marketing_driven": is_marketing_driven,
        }

        # Look up campaign_id for marketing-driven purchases (attribution tracking)
        campaign_id = None
        if is_marketing_driven:
            # Find the marketing conversion that drove this customer
            for impression_id, conversion in self.state.marketing_conversions.items():
                if (
                    conversion["customer_id"] == customer_id
                    and conversion.get("converted", False)
                ):
                    campaign_id = conversion.get("campaign_id")
                    break

        payload = ReceiptCreatedPayload(
            store_id=store_id,
            customer_id=customer_id,
            receipt_id=receipt_id,
            subtotal=subtotal,
            tax=tax,
            total=total,
            tender_type=tender_type.value,
            item_count=item_count,
            campaign_id=campaign_id,  # Attribution tracking for marketing campaigns
        )

        return payload, receipt_id, f"store_{store_id}"

    def _generate_receipt_line_added(
        self, timestamp: datetime
    ) -> tuple[ReceiptLineAddedPayload, str, str] | None:
        """Generate receipt line added event."""
        if not self.state.active_receipts:
            return None

        receipt_id = self.rng.choice(list(self.state.active_receipts.keys()))
        receipt_info = self.state.active_receipts[receipt_id]

        # Generate line item
        product_id = self.rng.choice(list(self.products.keys()))
        product = self.products[product_id]
        quantity = self.rng.randint(1, 3)
        unit_price = float(product.SalePrice)
        extended_price = unit_price * quantity

        # Randomly apply promotion
        promo_code = None
        if self.rng.random() < 0.2:  # 20% chance of promotion
            promo_code = self.rng.choice(list(self.state.promotion_campaigns.keys()))

        payload = ReceiptLineAddedPayload(
            receipt_id=receipt_id,
            line_number=self.rng.randint(1, 10),
            product_id=product_id,
            quantity=quantity,
            unit_price=unit_price,
            extended_price=extended_price,
            promo_code=promo_code,
        )

        return payload, receipt_id, f"store_{receipt_info['store_id']}"

    def _generate_payment_processed(
        self, timestamp: datetime
    ) -> tuple[PaymentProcessedPayload, str, str] | None:
        """Generate payment processed event."""
        if not self.state.active_receipts:
            return None

        receipt_id = self.rng.choice(list(self.state.active_receipts.keys()))
        receipt_info = self.state.active_receipts[receipt_id]

        payment_method = self.rng.choice(list(TenderType)).value
        amount = self.rng.uniform(10.0, 200.0)
        transaction_id = (
            f"TXN_{int(timestamp.timestamp())}_{self.rng.randint(1000, 9999)}"
        )

        payload = PaymentProcessedPayload(
            receipt_id=receipt_id,
            payment_method=payment_method,
            amount=amount,
            transaction_id=transaction_id,
            processing_time=timestamp,
            status="APPROVED",
        )

        # Remove receipt from active receipts after payment
        if self.rng.random() < 0.8:  # 80% chance to complete receipt
            del self.state.active_receipts[receipt_id]

        return payload, receipt_id, f"store_{receipt_info['store_id']}"

    def _generate_inventory_updated(
        self, timestamp: datetime
    ) -> tuple[InventoryUpdatedPayload, str, str]:
        """Generate inventory updated event."""
        # Decide between store or DC inventory
        is_store = self.rng.random() < 0.7  # 70% store, 30% DC

        if is_store:
            location_id = self.rng.choice(list(self.stores.keys()))
            store_id = location_id
            dc_id = None
            partition_key = f"store_{location_id}"
        else:
            location_id = self.rng.choice(list(self.dcs.keys()))
            store_id = None
            dc_id = location_id
            partition_key = f"dc_{location_id}"

        product_id = self.rng.choice(list(self.products.keys()))
        reason = self.rng.choice(list(InventoryReason))

        # Generate realistic quantity delta based on reason
        if reason in [
            InventoryReason.SALE,
            InventoryReason.DAMAGED,
            InventoryReason.LOST,
        ]:
            qty_delta = -self.rng.randint(1, 10)
        else:
            qty_delta = self.rng.randint(10, 100)

        # Update internal inventory tracking
        inventory_key = (location_id, product_id)
        if is_store:
            self.state.store_inventory[inventory_key] += qty_delta
        else:
            self.state.dc_inventory[inventory_key] += qty_delta

        payload = InventoryUpdatedPayload(
            store_id=store_id,
            dc_id=dc_id,
            product_id=product_id,
            quantity_delta=qty_delta,
            reason=reason.value,
            source=f"truck_{self.rng.randint(1000, 9999)}" if qty_delta > 0 else None,
        )

        return payload, f"inventory_{location_id}_{product_id}", partition_key

    def _generate_customer_entered(
        self, timestamp: datetime
    ) -> tuple[CustomerEnteredPayload, str, str]:
        """Generate customer entered event with session tracking and marketing conversions."""
        store_id = self.rng.choice(list(self.stores.keys()))
        sensor_id = f"SENSOR_{store_id}_1"  # Use store-specific entrance sensor
        zone = "ENTRANCE"  # Always start at entrance

        # Check for marketing-driven visits first
        marketing_driven_customers = []
        for impression_id, conversion in self.state.marketing_conversions.items():
            if (
                not conversion["converted"]
                and timestamp >= conversion["scheduled_visit_time"]
                and timestamp <= conversion["scheduled_visit_time"] + timedelta(hours=2)
            ):  # 2-hour window
                customer_id = conversion["customer_id"]
                customer = self.customers.get(customer_id)
                session_key = f"{customer_id}_{store_id}"

                if customer and session_key not in self.state.customer_sessions:
                    marketing_driven_customers.append(
                        (customer, impression_id, conversion)
                    )

        # Select customers (prioritize marketing-driven visits)
        available_customers = [
            cust
            for cust in self.customers.values()
            if f"{cust.ID}_{store_id}" not in self.state.customer_sessions
        ]

        entering_customers = []

        # First, add marketing-driven customers
        if marketing_driven_customers:
            # Select 1-2 marketing-driven customers
            selected_marketing = self.rng.sample(
                marketing_driven_customers,
                min(self.rng.randint(1, 2), len(marketing_driven_customers)),
            )
            for customer, impression_id, conversion in selected_marketing:
                entering_customers.append(customer)
                # Mark conversion as completed
                conversion["converted"] = True
                conversion["actual_visit_time"] = timestamp

        # Then add random customers if needed
        remaining_slots = self.rng.randint(1, 3) - len(entering_customers)
        if remaining_slots > 0 and available_customers:
            random_customers = [
                c for c in available_customers if c not in entering_customers
            ]
            if random_customers:
                additional_customers = self.rng.sample(
                    random_customers, min(remaining_slots, len(random_customers))
                )
                entering_customers.extend(additional_customers)

        customer_count = len(entering_customers)
        if customer_count == 0:
            # Fallback: generic foot traffic event
            customer_count = self.rng.randint(1, 2)
        else:
            # Create customer sessions
            for customer in entering_customers:
                session_id = f"{customer.ID}_{store_id}"
                base_visit_duration = self.rng.randint(10, 45)

                # Marketing-driven customers tend to stay longer and are more likely to purchase
                is_marketing_driven = any(
                    customer == mc[0]
                    for mc in marketing_driven_customers
                    if mc[0] in entering_customers
                )

                if is_marketing_driven:
                    # 20% longer visit, higher purchase intent
                    visit_duration = int(base_visit_duration * 1.2)
                    purchase_likelihood = 0.8  # 80% likely to purchase
                else:
                    visit_duration = base_visit_duration
                    purchase_likelihood = 0.4  # 40% likely to purchase

                self.state.customer_sessions[session_id] = {
                    "customer_id": customer.ID,
                    "customer_ble_id": customer.BLEId,
                    "store_id": store_id,
                    "entered_at": timestamp,
                    "current_zone": "ENTRANCE",
                    "has_made_purchase": False,
                    "expected_exit_time": timestamp + timedelta(minutes=visit_duration),
                    "marketing_driven": is_marketing_driven,
                    "purchase_likelihood": purchase_likelihood,
                }

        # Update store customer count
        self.state.store_hours[store_id]["current_customers"] += customer_count

        payload = CustomerEnteredPayload(
            store_id=store_id,
            sensor_id=sensor_id,
            zone=zone,
            customer_count=customer_count,
            dwell_time=0,
        )

        return payload, f"foottraffic_{store_id}", f"store_{store_id}"

    def _generate_customer_zone_changed(
        self, timestamp: datetime
    ) -> tuple[CustomerZoneChangedPayload, str, str] | None:
        """Generate customer zone changed event."""
        store_id = self.rng.choice(list(self.stores.keys()))
        customer = self.rng.choice(list(self.customers.values()))
        customer_ble_id = customer.BLEId

        zones = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]
        from_zone = self.rng.choice(zones)
        to_zone = self.rng.choice([z for z in zones if z != from_zone])

        payload = CustomerZoneChangedPayload(
            store_id=store_id,
            customer_ble_id=customer_ble_id,
            from_zone=from_zone,
            to_zone=to_zone,
            timestamp=timestamp,
        )

        return payload, customer_ble_id, f"store_{store_id}"

    def _generate_ble_ping_detected(
        self, timestamp: datetime
    ) -> tuple[BLEPingDetectedPayload, str, str] | None:
        """Generate BLE ping detected event - only for customers currently in store."""
        # Get customers who are currently in stores
        active_sessions = [
            session
            for session in self.state.customer_sessions.values()
            if timestamp < session["expected_exit_time"]
        ]

        if not active_sessions:
            return None  # No customers in any store

        session = self.rng.choice(active_sessions)
        store_id = session["store_id"]
        customer_ble_id = session["customer_ble_id"]
        current_zone = session["current_zone"]

        # Use appropriate beacon for the zone
        beacon_id = f"BEACON_{store_id}_{current_zone}"
        rssi = self.rng.randint(-80, -30)  # Typical RSSI range

        # Occasionally move customer to a different zone (20% chance)
        zones = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]
        if self.rng.random() < 0.2:
            new_zone = self.rng.choice([z for z in zones if z != current_zone])
            session["current_zone"] = new_zone
            current_zone = new_zone
            beacon_id = f"BEACON_{store_id}_{new_zone}"

        payload = BLEPingDetectedPayload(
            store_id=store_id,
            beacon_id=beacon_id,
            customer_ble_id=customer_ble_id,
            rssi=rssi,
            zone=current_zone,
        )

        return payload, customer_ble_id, f"store_{store_id}"

    def _generate_truck_arrived(
        self, timestamp: datetime
    ) -> tuple[TruckArrivedPayload, str, str]:
        """Generate truck arrived event."""
        truck_id = f"TRUCK_{self.rng.randint(1000, 9999)}"

        # 70% to stores, 30% to DCs
        if self.rng.random() < 0.7:
            store_id = self.rng.choice(list(self.stores.keys()))
            dc_id = None
            partition_key = f"store_{store_id}"
        else:
            dc_id = self.rng.choice(list(self.dcs.keys()))
            store_id = None
            partition_key = f"dc_{dc_id}"

        shipment_id = f"SHIP_{int(timestamp.timestamp())}_{self.rng.randint(100, 999)}"
        estimated_unload_duration = self.rng.randint(30, 180)  # 30-180 minutes

        # Track active truck
        self.state.active_trucks[truck_id] = {
            "store_id": store_id,
            "dc_id": dc_id,
            "arrival_time": timestamp,
            "shipment_id": shipment_id,
        }

        payload = TruckArrivedPayload(
            truck_id=truck_id,
            store_id=store_id,
            dc_id=dc_id,
            shipment_id=shipment_id,
            arrival_time=timestamp,
            estimated_unload_duration=estimated_unload_duration,
        )

        return payload, truck_id, partition_key

    def _generate_truck_departed(
        self, timestamp: datetime
    ) -> tuple[TruckDepartedPayload, str, str] | None:
        """Generate truck departed event."""
        if not self.state.active_trucks:
            return None

        truck_id = self.rng.choice(list(self.state.active_trucks.keys()))
        truck_info = self.state.active_trucks[truck_id]

        actual_unload_duration = self.rng.randint(25, 200)  # Actual vs estimated

        if truck_info["store_id"]:
            partition_key = f"store_{truck_info['store_id']}"
        else:
            partition_key = f"dc_{truck_info['dc_id']}"

        payload = TruckDepartedPayload(
            truck_id=truck_id,
            store_id=truck_info["store_id"],
            dc_id=truck_info["dc_id"],
            shipment_id=truck_info["shipment_id"],
            departure_time=timestamp,
            actual_unload_duration=actual_unload_duration,
        )

        # Remove from active trucks
        del self.state.active_trucks[truck_id]

        return payload, truck_id, partition_key

    def _generate_store_opened(
        self, timestamp: datetime
    ) -> tuple[StoreOperationPayload, str, str]:
        """Generate store opened event."""
        store_id = self.rng.choice(list(self.stores.keys()))
        self.state.store_hours[store_id]["is_open"] = True

        payload = StoreOperationPayload(
            store_id=store_id, operation_time=timestamp, operation_type="opened"
        )

        return payload, f"store_ops_{store_id}", f"store_{store_id}"

    def _generate_store_closed(
        self, timestamp: datetime
    ) -> tuple[StoreOperationPayload, str, str]:
        """Generate store closed event."""
        store_id = self.rng.choice(list(self.stores.keys()))
        self.state.store_hours[store_id]["is_open"] = False
        self.state.store_hours[store_id]["current_customers"] = 0

        payload = StoreOperationPayload(
            store_id=store_id, operation_time=timestamp, operation_type="closed"
        )

        return payload, f"store_ops_{store_id}", f"store_{store_id}"

    def _generate_ad_impression(
        self, timestamp: datetime
    ) -> tuple[AdImpressionPayload, str, str]:
        """Generate ad impression event with conversion tracking."""
        channel = self.rng.choice(list(MarketingChannel))
        campaign_id = self.rng.choice(list(self.state.promotion_campaigns.keys()))
        creative_id = f"CRE_{self.rng.randint(1000, 9999)}"
        customer = self.rng.choice(list(self.customers.values()))
        customer_ad_id = customer.AdId
        impression_id = (
            f"IMP_{int(timestamp.timestamp())}_{self.rng.randint(1000, 9999)}"
        )
        cost = self.rng.uniform(0.10, 2.50)  # Cost per impression
        device_type = self.rng.choice(list(DeviceType))

        # Industry standard conversion rates by channel
        conversion_rates = {
            MarketingChannel.SOCIAL: 0.012,  # 1.2% - social media ads
            MarketingChannel.SEARCH: 0.035,  # 3.5% - search ads
            MarketingChannel.DISPLAY: 0.008,  # 0.8% - display ads
            MarketingChannel.EMAIL: 0.025,  # 2.5% - email campaigns
            MarketingChannel.VIDEO: 0.015,  # 1.5% - video ads
        }

        # Determine if this impression will convert to store visit
        conversion_rate = conversion_rates.get(channel, 0.015)
        will_convert = self.rng.random() < conversion_rate

        if will_convert:
            # Schedule conversion: customer will visit store within 1-48 hours
            conversion_delay_hours = self.rng.uniform(1, 48)
            conversion_time = timestamp + timedelta(hours=conversion_delay_hours)

            self.state.marketing_conversions[impression_id] = {
                "customer_id": customer.ID,
                "customer_ad_id": customer_ad_id,
                "campaign_id": campaign_id,
                "channel": channel.value,
                "scheduled_visit_time": conversion_time,
                "converted": False,
            }

        payload = AdImpressionPayload(
            channel=channel.value,
            campaign_id=campaign_id,
            creative_id=creative_id,
            customer_ad_id=customer_ad_id,
            impression_id=impression_id,
            cost=cost,
            device_type=device_type.value,
        )

        return payload, impression_id, f"marketing_{channel.value}"

    def _generate_promotion_applied(
        self, timestamp: datetime
    ) -> tuple[PromotionAppliedPayload, str, str] | None:
        """Generate promotion applied event."""
        if not self.state.active_receipts:
            return None

        receipt_id = self.rng.choice(list(self.state.active_receipts.keys()))
        receipt_info = self.state.active_receipts[receipt_id]

        promo_code = self.rng.choice(list(self.state.promotion_campaigns.keys()))
        self.state.promotion_campaigns[promo_code]

        discount_amount = self.rng.uniform(5.0, 25.0)
        discount_type = "percentage" if self.rng.random() < 0.7 else "fixed"
        product_ids = [
            self.rng.choice(list(self.products.keys()))
            for _ in range(self.rng.randint(1, 3))
        ]

        payload = PromotionAppliedPayload(
            receipt_id=receipt_id,
            promo_code=promo_code,
            discount_amount=discount_amount,
            discount_type=discount_type,
            product_ids=product_ids,
        )

        return payload, receipt_id, f"store_{receipt_info['store_id']}"

    def _generate_online_order_created(
        self, timestamp: datetime
    ) -> tuple[OnlineOrderCreatedPayload, str, str]:
        """Generate an online order created event with fulfillment details.

        Fulfillment mode distribution:
        - SHIP_FROM_DC: 60% (most common)
        - SHIP_FROM_STORE: 30% (ship-from-store programs)
        - BOPIS: 10% (buy online, pick up in store)
        """
        customer_id = self.rng.choice(list(self.customers.keys()))
        mode = self.rng.choices(
            ["SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"], weights=[0.60, 0.30, 0.10]
        )[0]

        if mode in ("SHIP_FROM_STORE", "BOPIS"):
            node_type = "STORE"
            node_id = self.rng.choice(list(self.stores.keys()))
        else:
            node_type = "DC"
            node_id = self.rng.choice(list(self.dcs.keys()))

        item_count = max(1, int(self.rng.gauss(3.5, 1.8)))
        subtotal = self.rng.uniform(15.0, 220.0)
        tax = round(subtotal * 0.08, 2)
        total = subtotal + tax
        tender_type = self.rng.choice(list(TenderType)).value

        order_id = f"ONL_{int(timestamp.timestamp())}_{self.rng.randint(1000, 9999)}"
        trace_id = self.generate_trace_id(timestamp)

        payload = OnlineOrderCreatedPayload(
            order_id=order_id,
            customer_id=customer_id,
            fulfillment_mode=mode,
            node_type=node_type,
            node_id=node_id,
            item_count=item_count,
            subtotal=subtotal,
            tax=tax,
            total=total,
            tender_type=tender_type,
        )

        partition_key = f"{node_type.lower()}_{node_id}"
        return payload, trace_id, partition_key

    def _generate_stockout_detected(
        self, timestamp: datetime
    ) -> tuple[StockoutDetectedPayload, str, str]:
        """Generate stockout detected event."""
        # Find low inventory items
        low_inventory_items = [
            (location_id, product_id, qty)
            for (location_id, product_id), qty in self.state.store_inventory.items()
            if qty <= 5
        ]

        if not low_inventory_items:
            # Generate a random stockout
            store_id = self.rng.choice(list(self.stores.keys()))
            product_id = self.rng.choice(list(self.products.keys()))
            last_known_quantity = 0
            dc_id = None
        else:
            location_id, product_id, last_known_quantity = self.rng.choice(
                low_inventory_items
            )
            store_id = location_id
            dc_id = None

        payload = StockoutDetectedPayload(
            store_id=store_id,
            dc_id=dc_id,
            product_id=product_id,
            last_known_quantity=last_known_quantity,
            detection_time=timestamp,
        )

        return payload, f"stockout_{store_id}_{product_id}", f"store_{store_id}"

    def _generate_reorder_triggered(
        self, timestamp: datetime
    ) -> tuple[ReorderTriggeredPayload, str, str]:
        """Generate reorder triggered event."""
        store_id = self.rng.choice(list(self.stores.keys()))
        product_id = self.rng.choice(list(self.products.keys()))

        current_quantity = max(
            0, self.state.store_inventory.get((store_id, product_id), 0)
        )
        reorder_point = self.rng.randint(10, 30)
        reorder_quantity = self.rng.randint(50, 200)

        priority = self.rng.choices(
            ["NORMAL", "HIGH", "URGENT"], weights=[0.7, 0.2, 0.1]
        )[0]

        payload = ReorderTriggeredPayload(
            store_id=store_id,
            dc_id=None,
            product_id=product_id,
            current_quantity=current_quantity,
            reorder_quantity=reorder_quantity,
            reorder_point=reorder_point,
            priority=priority,
        )

        return payload, f"reorder_{store_id}_{product_id}", f"store_{store_id}"

    def _cleanup_expired_sessions(self, timestamp: datetime) -> None:
        """Clean up expired customer sessions and update store occupancy."""
        expired_sessions = []

        for session_id, session in self.state.customer_sessions.items():
            if timestamp >= session["expected_exit_time"]:
                expired_sessions.append(session_id)

                # Decrease store occupancy count
                store_id = session["store_id"]
                if store_id in self.state.store_hours:
                    current_count = self.state.store_hours[store_id][
                        "current_customers"
                    ]
                    self.state.store_hours[store_id]["current_customers"] = max(
                        0, current_count - 1
                    )

        # Remove expired sessions
        for session_id in expired_sessions:
            del self.state.customer_sessions[session_id]

        # Clean up old marketing conversions (older than 72 hours)
        expired_conversions = []
        cutoff_time = timestamp - timedelta(hours=72)

        for impression_id, conversion in self.state.marketing_conversions.items():
            scheduled_time = conversion["scheduled_visit_time"]
            if scheduled_time < cutoff_time:
                expired_conversions.append(impression_id)

        for impression_id in expired_conversions:
            del self.state.marketing_conversions[impression_id]
