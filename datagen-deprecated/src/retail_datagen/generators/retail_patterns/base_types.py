"""
Type definitions for retail pattern mixins.

This module provides a base class with type annotations for attributes
that are shared across retail pattern mixins. Mixins should inherit from
this base class to get proper type checking support.
"""

from __future__ import annotations

import random
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from retail_datagen.shared.models import (
        DistributionCenter,
        ProductMaster,
        Store,
        Truck,
    )


class InventoryFlowBase:
    """
    Base class providing type annotations for inventory flow mixins.

    This class enables mypy to understand the mixin pattern used by
    InventoryFlowSimulator. All inventory flow mixins inherit from this
    class to get type checking support for cross-mixin calls.

    MAINTENANCE CONTRACT:
    ---------------------
    1. Attributes here must match those in InventoryFlowSimulator.__init__
    2. Method signatures must match actual implementations in the mixins
    3. When adding a new attribute to InventoryFlowSimulator, add it here too
    4. When adding a cross-mixin method, add a stub here

    Type checking flow:
    - Mixins inherit from InventoryFlowBase (for type hints)
    - InventoryFlowSimulator inherits from all mixins (for implementations)
    - mypy sees attributes/methods via InventoryFlowBase
    - Runtime uses actual implementations from mixins
    """

    # Master data
    dcs: list[DistributionCenter]
    stores: list[Store]
    products: list[ProductMaster]

    # Random number generator
    _rng: random.Random

    # Current inventory levels
    _dc_inventory: dict[tuple[int, int], int]
    _store_inventory: dict[tuple[int, int], int]

    # Truck fleet simulation
    _truck_capacity: int
    _active_shipments: dict[str, dict]
    _in_transit_inventory: dict[tuple[int, int], int]

    # Truck management
    _trucks: list[Truck]
    _trucks_by_dc: dict[int | None, list[int]]
    _truck_rr_index: dict[int | None, int]
    _truck_availability: dict[int, datetime]
    _shipment_queue: list[dict]

    # Disruption tracking
    _active_disruptions: dict[int, dict[str, Any]]

    # ------------------------------------------------------------------------
    # Method stubs for cross-mixin calls
    # These are implemented in various mixins but called from others.
    # Actual implementations are in the respective mixin files.
    # ------------------------------------------------------------------------

    def get_dc_capacity_multiplier(self, dc_id: int, date: datetime) -> float:
        """Get capacity multiplier for a DC considering active disruptions."""
        raise NotImplementedError

    def _initialize_inventory(self) -> None:
        """Initialize baseline inventory levels."""
        ...

    def get_dc_balance(self, dc_id: int, product_id: int) -> int:
        """Get current balance for a DC-product pair."""
        raise NotImplementedError

    def get_store_balance(self, store_id: int, product_id: int) -> int:
        """Get current balance for a store-product pair."""
        raise NotImplementedError

    def adjust_dc_inventory(
        self, dc_id: int, product_id: int, quantity: int, reason: Any
    ) -> int:
        """Adjust DC inventory and return new balance."""
        raise NotImplementedError

    def adjust_store_inventory(
        self, store_id: int, product_id: int, quantity: int, reason: Any
    ) -> int:
        """Adjust store inventory and return new balance."""
        raise NotImplementedError

    def simulate_dc_receiving(self, dc_id: int, date: datetime) -> list[dict]:
        """Simulate supplier deliveries to a DC."""
        raise NotImplementedError

    def track_truck_inventory_status(self, date: datetime) -> list[dict]:
        """Track inventory status for trucks in transit."""
        raise NotImplementedError
