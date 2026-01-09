"""
Type definitions for event generator mixins.

This module provides a base class with type annotations for attributes
that are shared across event generator mixins. Mixins should inherit from
this base class to get proper type checking support.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from retail_datagen.generators.seasonal_patterns import CompositeTemporalPatterns
    from retail_datagen.shared.models import (
        Customer,
        DistributionCenter,
        ProductMaster,
        Store,
    )

    from ..schemas import EventEnvelope


class EventGeneratorBase:
    """
    Base class providing type annotations for event generator mixins.

    This class enables mypy to understand the mixin pattern used by
    EventFactory. All event generator mixins inherit from this class
    to get type checking support for cross-mixin calls.

    MAINTENANCE CONTRACT:
    ---------------------
    1. Attributes here must match those in EventFactory.__init__
    2. Method signatures must match actual implementations in the mixins
    3. When adding a new attribute to EventFactory, add it here too
    4. When adding a cross-mixin method, add a stub here

    Type checking flow:
    - Mixins inherit from EventGeneratorBase (for type hints)
    - EventFactory inherits from all mixins (for implementations)
    - mypy sees attributes/methods via EventGeneratorBase
    - Runtime uses actual implementations from mixins
    """

    # Master data (stored as dicts for O(1) lookup)
    stores: dict[int, Store]
    customers: dict[int, Customer]
    products: dict[int, ProductMaster]
    dcs: dict[int, DistributionCenter]

    # Random number generator
    rng: random.Random

    # Event generation state
    state: Any  # EventGenerationState (avoid circular import)

    # Temporal patterns for realistic timing
    temporal_patterns: CompositeTemporalPatterns

    # ------------------------------------------------------------------------
    # Method stubs for cross-mixin calls
    # These are implemented in various mixins but called from others.
    # Actual implementations are in the respective mixin files.
    # ------------------------------------------------------------------------

    def generate_trace_id(self, timestamp: Any) -> str:
        """Generate a unique trace ID."""
        raise NotImplementedError

    def _cleanup_expired_sessions(self, current_time: Any) -> None:
        """Clean up expired customer sessions."""
        ...

    def _generate_receipt_created(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a receipt created event."""
        ...

    def _generate_receipt_line_added(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a receipt line added event."""
        ...

    def _generate_payment_processed(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a payment processed event."""
        ...

    def _generate_customer_entered(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a customer entered event."""
        ...

    def _generate_customer_zone_changed(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a customer zone changed event."""
        ...

    def _generate_ble_ping_detected(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a BLE ping detected event."""
        ...

    def _generate_inventory_updated(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate an inventory updated event."""
        ...

    def _generate_stockout_detected(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a stockout detected event."""
        ...

    def _generate_reorder_triggered(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a reorder triggered event."""
        ...

    def _generate_truck_arrived(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a truck arrived event."""
        ...

    def _generate_truck_departed(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a truck departed event."""
        ...

    def _generate_store_opened(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a store opened event."""
        ...

    def _generate_store_closed(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a store closed event."""
        ...

    def _generate_ad_impression(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate an ad impression event."""
        ...

    def _generate_promotion_applied(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate a promotion applied event."""
        ...

    def _generate_online_order_created(
        self, timestamp: Any, trace_id: str
    ) -> EventEnvelope | None:
        """Generate an online order created event."""
        ...

    def generate_online_order_followups(
        self, order_event: Any, current_time: Any
    ) -> list[EventEnvelope]:
        """Generate follow-up events for an online order."""
        raise NotImplementedError
