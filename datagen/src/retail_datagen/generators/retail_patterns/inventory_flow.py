"""
Inventory flow simulation for realistic supply chain operations.

This module models DC receiving, truck movements, store deliveries, and inventory
adjustments with realistic timing and quantities, including supply chain disruptions.
"""

import logging
import random
from datetime import datetime

from retail_datagen.shared.models import (
    DistributionCenter,
    InventoryReason,
    ProductMaster,
    Store,
)

from .disruption_simulator import DisruptionMixin
from .truck_operations import TruckOperationsMixin

logger = logging.getLogger(__name__)


class InventoryFlowSimulator(TruckOperationsMixin, DisruptionMixin):
    """
    Simulates realistic inventory flows through the supply chain.

    Models DC receiving, truck movements, store deliveries, and inventory
    adjustments with realistic timing and quantities.

    Inherits from:
        TruckOperationsMixin: Truck selection, shipments, loading/unloading
        DisruptionMixin: Supply chain disruption simulation
    """

    def __init__(
        self,
        distribution_centers: list[DistributionCenter],
        stores: list[Store],
        products: list[ProductMaster],
        seed: int = 42,
        trucks: list | None = None,
        truck_capacity: int = 15000,
    ):
        """
        Initialize inventory flow simulator.

        Args:
            distribution_centers: List of DC dimension records
            stores: List of store dimension records
            products: List of product master records
            seed: Random seed for reproducible simulations
            trucks: Optional list of truck dimension records
            truck_capacity: Max items per truck (default 15,000 for semi-trailer)
        """
        self.dcs = distribution_centers
        self.stores = stores
        self.products = products
        self._rng = random.Random(seed)

        # Current inventory levels
        self._dc_inventory: dict[tuple[int, int], int] = {}
        self._store_inventory: dict[tuple[int, int], int] = {}

        # Initialize with baseline inventory
        self._initialize_inventory()

        # Truck fleet simulation
        self._truck_capacity = truck_capacity
        self._active_shipments: dict[str, dict] = {}

        # Truck management
        self._trucks = trucks or []
        self._trucks_by_dc: dict[int | None, list[int]] = {}
        self._truck_rr_index: dict[int | None, int] = {}
        self._truck_availability: dict[int, datetime] = {}
        self._shipment_queue: list[dict] = []
        self._in_transit_inventory: dict[tuple[int, int], int] = {}

        if self._trucks:
            for t in self._trucks:
                dc_key = getattr(t, "DCID", None)
                truck_id = getattr(t, "ID")
                self._trucks_by_dc.setdefault(dc_key, []).append(truck_id)
                self._truck_availability[truck_id] = datetime.min
            for key in self._trucks_by_dc.keys():
                self._truck_rr_index[key] = 0

        # Reorder points
        self._reorder_points = self._calculate_reorder_points()

        # Supply chain disruptions tracking
        self._active_disruptions: dict[int, dict] = {}
        self._disruption_counter = 1

    def _initialize_inventory(self):
        """Initialize baseline inventory levels."""
        for dc in self.dcs:
            for product in self.products:
                initial_qty = self._rng.randint(100, 1000)
                self._dc_inventory[(dc.ID, product.ID)] = initial_qty

        for store in self.stores:
            for product in self.products:
                initial_qty = self._rng.randint(10, 100)
                self._store_inventory[(store.ID, product.ID)] = initial_qty

    def _calculate_reorder_points(self) -> dict[tuple[int, int], int]:
        """Calculate reorder points for store inventory."""
        reorder_points = {}
        for store in self.stores:
            for product in self.products:
                base_reorder = self._rng.randint(5, 20)
                reorder_points[(store.ID, product.ID)] = base_reorder
        return reorder_points

    def simulate_dc_receiving(self, dc_id: int, date: datetime) -> list[dict]:
        """
        Simulate DC receiving shipments from suppliers.

        Args:
            dc_id: Distribution center ID
            date: Date of receiving

        Returns:
            List of inventory transaction records
        """
        transactions = []
        capacity_multiplier = self.get_dc_capacity_multiplier(dc_id, date)

        base_shipments = self._rng.randint(1, 3)
        num_shipments = max(1, int(base_shipments * capacity_multiplier))

        for _ in range(num_shipments):
            base_products = self._rng.randint(10, 50)
            num_products = max(5, int(base_products * capacity_multiplier))
            products_in_shipment = self._rng.sample(
                self.products, min(num_products, len(self.products))
            )

            for product in products_in_shipment:
                base_qty = self._rng.randint(50, 500)
                receive_qty = max(10, int(base_qty * capacity_multiplier))

                key = (dc_id, product.ID)
                self._dc_inventory[key] = self._dc_inventory.get(key, 0) + receive_qty

                transactions.append(
                    {
                        "DCID": dc_id,
                        "ProductID": product.ID,
                        "QtyDelta": receive_qty,
                        "Reason": InventoryReason.INBOUND_SHIPMENT,
                        "EventTS": date,
                    }
                )

        return transactions

    def simulate_store_demand(
        self, store_id: int, date: datetime, traffic_multiplier: float = 1.0
    ) -> list[dict]:
        """
        Simulate store demand and generate inventory deductions.

        Args:
            store_id: Store ID
            date: Date of demand
            traffic_multiplier: Multiplier for demand based on traffic patterns

        Returns:
            List of store inventory transactions
        """
        transactions = []
        base_products_sold = int(50 * traffic_multiplier)
        num_products = max(5, base_products_sold)

        available_products = []
        for product in self.products:
            key = (store_id, product.ID)
            current_inventory = self._store_inventory.get(key, 0)
            if current_inventory > 0:
                available_products.append(product)

        if not available_products:
            return transactions

        products_to_sell = self._rng.sample(
            available_products, min(num_products, len(available_products))
        )

        for product in products_to_sell:
            key = (store_id, product.ID)
            current_inventory = self._store_inventory.get(key, 0)
            max_sale_qty = min(current_inventory, self._rng.randint(1, 5))

            if max_sale_qty > 0:
                sale_qty = self._rng.randint(1, max_sale_qty)
                self._store_inventory[key] = current_inventory - sale_qty

                transactions.append(
                    {
                        "StoreID": store_id,
                        "ProductID": product.ID,
                        "QtyDelta": -sale_qty,
                        "Reason": InventoryReason.SALE,
                        "Source": "CUSTOMER_PURCHASE",
                        "EventTS": date,
                    }
                )

        return transactions

    def check_reorder_needs(self, store_id: int) -> list[tuple[int, int]]:
        """
        Check which products need reordering for a store.

        Accounts for both current inventory AND in-transit inventory.

        Args:
            store_id: Store ID to check

        Returns:
            List of (product_id, reorder_quantity) tuples
        """
        reorders = []
        if not hasattr(self, "_reorder_points") or self._reorder_points is None:
            self._reorder_points = self._calculate_reorder_points()

        for product in self.products:
            key = (store_id, product.ID)
            current_inventory = self._store_inventory.get(key, 0)
            in_transit = self._in_transit_inventory.get(key, 0)
            effective_inventory = current_inventory + in_transit
            reorder_point = self._reorder_points.get(key, 10)

            if effective_inventory <= reorder_point:
                reorder_qty = self._rng.randint(50, 200)
                reorders.append((product.ID, reorder_qty))

        return reorders

    def get_dc_balance(self, dc_id: int, product_id: int) -> int:
        """Get current inventory balance for a DC-product combination."""
        return self._dc_inventory.get((dc_id, product_id), 0)

    def get_store_balance(self, store_id: int, product_id: int) -> int:
        """Get current inventory balance for a store-product combination."""
        return self._store_inventory.get((store_id, product_id), 0)

    def get_in_transit_quantity(self, store_id: int, product_id: int) -> int:
        """Get quantity currently in transit to a store for a product."""
        return self._in_transit_inventory.get((store_id, product_id), 0)

    def get_effective_store_inventory(self, store_id: int, product_id: int) -> int:
        """Get effective inventory (on-hand + in-transit) for reorder decisions."""
        on_hand = self.get_store_balance(store_id, product_id)
        in_transit = self.get_in_transit_quantity(store_id, product_id)
        return on_hand + in_transit

    def get_reorder_point(self, store_id: int, product_id: int) -> int:
        """Get reorder point for a store-product combination."""
        return self._reorder_points.get((store_id, product_id), 10)
