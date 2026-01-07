"""
Inventory flow simulation for realistic supply chain operations.

This module models DC receiving, truck movements, store deliveries, and inventory
adjustments with realistic timing and quantities, including supply chain disruptions.
"""

import json
import logging
import random
import warnings
from datetime import datetime, timedelta

from retail_datagen.shared.models import (
    DisruptionSeverity,
    DisruptionType,
    DistributionCenter,
    InventoryReason,
    ProductMaster,
    Store,
    TruckStatus,
)

logger = logging.getLogger(__name__)


class InventoryFlowSimulator:
    """
    Simulates realistic inventory flows through the supply chain.

    Models DC receiving, truck movements, store deliveries, and inventory
    adjustments with realistic timing and quantities.
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

        # Current inventory levels (simplified tracking)
        self._dc_inventory: dict[
            tuple[int, int], int
        ] = {}  # (dc_id, product_id) -> quantity
        self._store_inventory: dict[
            tuple[int, int], int
        ] = {}  # (store_id, product_id) -> quantity

        # Initialize with baseline inventory
        self._initialize_inventory()

        # Truck fleet simulation
        # A 53-foot semi-trailer holds ~20-26 pallets, each with 50-200+ items
        # Default 15,000 items represents a realistic full truckload capacity
        self._truck_capacity = truck_capacity
        self._active_shipments: dict[str, dict] = {}  # shipment_id -> shipment_info

        # Optional trucks list (dimension trucks) for realistic linking from facts → dim
        # When provided, we pick trucks assigned to the DC when generating shipments,
        # otherwise we fall back to pool trucks, and finally to synthetic IDs.
        self._trucks = trucks or []
        self._trucks_by_dc: dict[int | None, list[int]] = {}
        self._truck_rr_index: dict[int | None, int] = {}
        # Track when each truck becomes available again (after round trip)
        # truck_id -> datetime when truck returns to DC
        self._truck_availability: dict[int, datetime] = {}
        # Queue of pending shipments waiting for available trucks
        # Each entry: {"dc_id": int, "store_id": int, "reorder_list": list, "requested_time": datetime}
        self._shipment_queue: list[dict] = []
        # Track in-transit inventory per store/product to prevent over-ordering
        # (store_id, product_id) -> quantity already in transit
        self._in_transit_inventory: dict[tuple[int, int], int] = {}
        if self._trucks:
            for t in self._trucks:
                # Pydantic Truck model fields: ID, DCID
                dc_key = getattr(t, "DCID", None)
                truck_id = getattr(t, "ID")
                self._trucks_by_dc.setdefault(dc_key, []).append(truck_id)
                # All trucks start as available (epoch time = always available)
                self._truck_availability[truck_id] = datetime.min
            # Initialize round-robin indices
            for key in self._trucks_by_dc.keys():
                self._truck_rr_index[key] = 0

        # Reorder points and quantities
        self._reorder_points = self._calculate_reorder_points()

        # Supply chain disruptions tracking
        self._active_disruptions: dict[int, dict] = {}  # dc_id -> disruption_info
        self._disruption_counter = 1

    def _select_truck_for_shipment(self, dc_id: int, current_time: datetime) -> int | str | None:
        """Select an available truck for a shipment, preferring trucks assigned to the DC.

        Args:
            dc_id: Distribution center ID
            current_time: Current simulation time to check availability

        Returns:
            Integer Truck ID if available, synthetic code if no real trucks exist,
            or None if all trucks are currently in use.
        """
        # Prefer DC-assigned trucks - find first available
        dc_list = self._trucks_by_dc.get(dc_id) or []
        available_dc_trucks = [
            truck_id for truck_id in dc_list
            if self._truck_availability.get(truck_id, datetime.min) <= current_time
        ]
        if available_dc_trucks:
            # Round-robin among available trucks
            idx = self._truck_rr_index.get(dc_id, 0) % len(available_dc_trucks)
            self._truck_rr_index[dc_id] = idx + 1
            return available_dc_trucks[idx]

        # Fallback to pool trucks - find first available
        pool_list = self._trucks_by_dc.get(None) or []
        available_pool_trucks = [
            truck_id for truck_id in pool_list
            if self._truck_availability.get(truck_id, datetime.min) <= current_time
        ]
        if available_pool_trucks:
            return self._rng.choice(available_pool_trucks)

        # If no real trucks exist at all, use synthetic (for backwards compatibility)
        if not dc_list and not pool_list:
            return f"TRK{self._rng.randint(1000, 9999)}"

        # All real trucks are busy - return None to signal queuing needed
        return None

    def _mark_truck_unavailable(self, truck_id: int | str, return_time: datetime) -> None:
        """Mark a truck as unavailable until it returns to the DC.

        Args:
            truck_id: Truck ID (int for real trucks, str for synthetic)
            return_time: When the truck will be back at the DC
        """
        if isinstance(truck_id, int):
            self._truck_availability[truck_id] = return_time

    def _calculate_round_trip_time(self, travel_hours: float, unload_hours: float) -> float:
        """Calculate total round trip time including return journey.

        Args:
            travel_hours: One-way travel time to store
            unload_hours: Time to unload at store

        Returns:
            Total hours until truck returns to DC
        """
        # Round trip = travel to store + unload + travel back
        # Assume return trip is same duration as outbound
        return travel_hours + unload_hours + travel_hours

    def get_next_available_truck_time(self, dc_id: int, current_time: datetime) -> datetime | None:
        """Get the earliest time a truck will be available at this DC.

        Args:
            dc_id: Distribution center ID
            current_time: Current simulation time

        Returns:
            Datetime when next truck becomes available, or None if trucks are available now
        """
        dc_list = self._trucks_by_dc.get(dc_id) or []
        pool_list = self._trucks_by_dc.get(None) or []
        all_trucks = dc_list + pool_list

        if not all_trucks:
            return None  # No real trucks, synthetic will be used

        # Check if any truck is available now
        for truck_id in all_trucks:
            if self._truck_availability.get(truck_id, datetime.min) <= current_time:
                return None  # A truck is available now

        # Find earliest return time
        return_times = [
            self._truck_availability.get(truck_id, datetime.min)
            for truck_id in all_trucks
        ]
        return min(return_times)

    def _initialize_inventory(self):
        """Initialize baseline inventory levels."""
        for dc in self.dcs:
            for product in self.products:
                # DCs start with higher inventory levels
                initial_qty = self._rng.randint(100, 1000)
                self._dc_inventory[(dc.ID, product.ID)] = initial_qty

        for store in self.stores:
            for product in self.products:
                # Stores start with moderate inventory levels
                initial_qty = self._rng.randint(10, 100)
                self._store_inventory[(store.ID, product.ID)] = initial_qty

    def _calculate_reorder_points(self) -> dict[tuple[int, int], int]:
        """Calculate reorder points for store inventory."""
        reorder_points = {}

        for store in self.stores:
            for product in self.products:
                # Simple reorder point calculation (could be more sophisticated)
                base_reorder = self._rng.randint(5, 20)
                reorder_points[(store.ID, product.ID)] = base_reorder

        return reorder_points

    def simulate_dc_receiving(self, dc_id: int, date: datetime) -> list[dict]:
        """
        Simulate DC receiving shipments from suppliers.

        Args:
            dc_id: Distribution center ID (Python variable uses snake_case)
            date: Date of receiving

        Returns:
            List of inventory transaction records (dict keys use PascalCase per schema)
        """
        transactions = []

        # Get capacity multiplier based on active disruptions
        capacity_multiplier = self.get_dc_capacity_multiplier(dc_id, date)

        # Simulate receiving 1-3 shipments per day (reduced by disruptions)
        base_shipments = self._rng.randint(1, 3)
        num_shipments = max(1, int(base_shipments * capacity_multiplier))

        for _ in range(num_shipments):
            # Select random products for shipment
            base_products = self._rng.randint(10, 50)
            num_products = max(5, int(base_products * capacity_multiplier))
            products_in_shipment = self._rng.sample(
                self.products, min(num_products, len(self.products))
            )

            for product in products_in_shipment:
                # Receiving quantities based on product type (reduced by disruptions)
                base_qty = self._rng.randint(50, 500)
                receive_qty = max(10, int(base_qty * capacity_multiplier))

                # Update inventory
                key = (dc_id, product.ID)
                self._dc_inventory[key] = self._dc_inventory.get(key, 0) + receive_qty

                # Output dict: keys use PascalCase to match DCInventoryTransaction schema
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
            store_id: Store ID (Python variable uses snake_case)
            date: Date of demand
            traffic_multiplier: Multiplier for demand based on traffic patterns

        Returns:
            List of store inventory transactions (dict keys use PascalCase per schema)
        """
        transactions = []

        # Base demand adjusted by traffic patterns
        base_products_sold = int(50 * traffic_multiplier)
        num_products = max(5, base_products_sold)

        # Select products that might be sold
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

            # Sales quantity (usually 1-5 units)
            max_sale_qty = min(current_inventory, self._rng.randint(1, 5))
            if max_sale_qty > 0:
                sale_qty = self._rng.randint(1, max_sale_qty)

                # Update inventory
                self._store_inventory[key] = current_inventory - sale_qty

                # Output dict: keys use PascalCase to match StoreInventoryTransaction schema
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

        Accounts for both current inventory AND in-transit inventory to prevent
        over-ordering when shipments are already on the way.

        Args:
            store_id: Store ID to check

        Returns:
            List of (product_id, reorder_quantity) tuples
        """
        reorders = []
        # Defensive: initialize reorder points if missing
        if not hasattr(self, "_reorder_points") or self._reorder_points is None:
            self._reorder_points = self._calculate_reorder_points()

        for product in self.products:
            key = (store_id, product.ID)
            current_inventory = self._store_inventory.get(key, 0)
            in_transit = self._in_transit_inventory.get(key, 0)
            effective_inventory = current_inventory + in_transit
            reorder_point = self._reorder_points.get(key, 10)

            if effective_inventory <= reorder_point:
                # Calculate reorder quantity
                reorder_qty = self._rng.randint(50, 200)
                reorders.append((product.ID, reorder_qty))

        return reorders

    def generate_truck_shipment(
        self,
        dc_id: int,
        store_id: int,
        reorder_list: list[tuple[int, int]],
        departure_time: datetime,
    ) -> dict:
        """
        Generate truck shipment from DC to store.

        Enforces truck capacity constraints. If the reorder list exceeds
        truck capacity, items are truncated to fit. For orders requiring
        multiple trucks, use generate_truck_shipments() instead.

        Args:
            dc_id: Source distribution center ID
            store_id: Destination store ID
            reorder_list: List of (product_id, quantity) to ship
            departure_time: When truck departs

        Returns:
            Shipment information dictionary

        Raises:
            ValueError: If any quantity in reorder_list is negative
        """
        # Validate quantities are non-negative
        for product_id, qty in reorder_list:
            if qty < 0:
                raise ValueError(
                    f"Invalid negative quantity {qty} for product {product_id} in shipment"
                )

        # Enforce truck capacity - truncate if necessary
        capacity = self._truck_capacity
        total_requested = sum(qty for _, qty in reorder_list)

        if total_requested > capacity:
            logger.warning(
                f"Shipment to store {store_id} exceeds truck capacity "
                f"({total_requested} > {capacity}). Truncating to fit. "
                f"Consider using generate_truck_shipments() for multi-truck support."
            )
            warnings.warn(
                "Order exceeds truck capacity and will be truncated. "
                "Use generate_truck_shipments() for automatic multi-truck splitting "
                "to avoid data loss.",
                UserWarning,
                stacklevel=2,
            )
            # Truncate items to fit within capacity
            truncated_list = []
            remaining_capacity = capacity
            for product_id, qty in reorder_list:
                if remaining_capacity <= 0:
                    break
                actual_qty = min(qty, remaining_capacity)
                truncated_list.append((product_id, actual_qty))
                remaining_capacity -= actual_qty
            reorder_list = truncated_list

        # Generate unique shipment ID
        shipment_id = f"SHIP{departure_time.strftime('%Y%m%d')}{dc_id:02d}{store_id:03d}{self._rng.randint(100, 999)}"

        # Choose a real truck when available to maintain referential integrity
        # If no truck available, wait for the next one
        actual_departure_time = departure_time
        truck_id = self._select_truck_for_shipment(dc_id, departure_time)

        if truck_id is None:
            # All trucks are busy - find when next one is available
            next_available = self.get_next_available_truck_time(dc_id, departure_time)
            if next_available is not None:
                actual_departure_time = next_available
                truck_id = self._select_truck_for_shipment(dc_id, actual_departure_time)
                logger.debug(
                    f"No trucks available at {departure_time}, "
                    f"shipment to store {store_id} delayed to {actual_departure_time}"
                )

        # If still no truck (shouldn't happen), use synthetic fallback
        if truck_id is None:
            truck_id = f"TRK{self._rng.randint(1000, 9999)}"
            logger.warning(f"No trucks available for DC {dc_id}, using synthetic truck {truck_id}")

        # Check for active disruptions at DC
        capacity_multiplier = self.get_dc_capacity_multiplier(dc_id, actual_departure_time)

        # Add delays for disruptions (inverse of capacity - lower capacity = more delays)
        base_travel_hours = self._rng.randint(2, 12)  # 2-12 hours base travel time
        delay_multiplier = 2.0 - capacity_multiplier  # 1.0 to 2.0 range
        travel_hours = int(base_travel_hours * delay_multiplier)

        eta = actual_departure_time + timedelta(hours=travel_hours)
        # Unload duration scales with shipment size (30min - 2hrs)
        unload_hours = self._calculate_unload_duration(sum(qty for _, qty in reorder_list))
        etd = eta + timedelta(hours=unload_hours)

        # Calculate when truck returns to DC (round trip)
        round_trip_hours = self._calculate_round_trip_time(travel_hours, unload_hours)
        truck_return_time = actual_departure_time + timedelta(hours=round_trip_hours)

        # Mark truck as unavailable until it returns
        self._mark_truck_unavailable(truck_id, truck_return_time)

        shipment_info = {
            "shipment_id": shipment_id,
            "truck_id": truck_id,
            "dc_id": dc_id,
            "store_id": store_id,
            "departure_time": actual_departure_time,
            "eta": eta,
            "etd": etd,
            "status": TruckStatus.SCHEDULED,
            "products": reorder_list,
            "total_items": sum(qty for _, qty in reorder_list),
            "unload_duration_hours": unload_hours,
            "truck_return_time": truck_return_time,  # When truck is back at DC
        }

        # Track active shipment
        self._active_shipments[shipment_id] = shipment_info

        # Track in-transit inventory to prevent over-ordering
        for product_id, quantity in reorder_list:
            key = (store_id, product_id)
            self._in_transit_inventory[key] = self._in_transit_inventory.get(key, 0) + quantity

        return shipment_info

    def generate_truck_shipments(
        self,
        dc_id: int,
        store_id: int,
        reorder_list: list[tuple[int, int]],
        departure_time: datetime,
    ) -> list[dict]:
        """
        Generate one or more truck shipments from DC to store, respecting capacity.

        If the reorder list exceeds a single truck's capacity, the order is
        split across multiple trucks with staggered departure times.

        Args:
            dc_id: Source distribution center ID
            store_id: Destination store ID
            reorder_list: List of (product_id, quantity) to ship
            departure_time: Base departure time for first truck

        Returns:
            List of shipment information dictionaries

        Raises:
            ValueError: If any quantity in reorder_list is negative
        """
        # Guard against empty reorder list
        if not reorder_list:
            return []

        # Validate quantities are non-negative (same check as generate_truck_shipment)
        for product_id, qty in reorder_list:
            if qty < 0:
                raise ValueError(
                    f"Invalid negative quantity {qty} for product {product_id} in shipment"
                )

        capacity = self._truck_capacity
        total_items = sum(qty for _, qty in reorder_list)

        # If fits in one truck, use simple method
        if total_items <= capacity:
            return [self.generate_truck_shipment(dc_id, store_id, reorder_list, departure_time)]

        # Split order across multiple trucks
        shipments = []
        current_truck_items: list[tuple[int, int]] = []
        current_truck_total = 0
        truck_number = 0

        for product_id, qty in reorder_list:
            remaining_qty = qty

            while remaining_qty > 0:
                space_available = capacity - current_truck_total
                qty_to_add = min(remaining_qty, space_available)

                if qty_to_add > 0:
                    current_truck_items.append((product_id, qty_to_add))
                    current_truck_total += qty_to_add
                    remaining_qty -= qty_to_add

                # If truck is full, dispatch it
                if current_truck_total >= capacity:
                    # Stagger departure times by 30 minutes per truck
                    truck_departure = departure_time + timedelta(minutes=30 * truck_number)
                    shipment = self.generate_truck_shipment(
                        dc_id, store_id, current_truck_items, truck_departure
                    )
                    shipments.append(shipment)
                    truck_number += 1
                    current_truck_items = []
                    current_truck_total = 0

        # Dispatch any remaining items in the last truck
        if current_truck_items:
            truck_departure = departure_time + timedelta(minutes=30 * truck_number)
            shipment = self.generate_truck_shipment(
                dc_id, store_id, current_truck_items, truck_departure
            )
            shipments.append(shipment)

        logger.info(
            f"Large order for store {store_id} split across {len(shipments)} trucks "
            f"(total items: {total_items}, capacity per truck: {capacity})"
        )

        return shipments

    def _calculate_unload_duration(self, total_items: int) -> float:
        """
        Calculate unload duration based on shipment size.

        Args:
            total_items: Total number of items in shipment

        Returns:
            Unload duration in hours (0.5 to 2.0 hours)
        """
        # Scale from 30 min (small shipment) to 2 hours (full truck)
        # Linear scale based on percentage of truck capacity
        capacity = self._truck_capacity
        load_percentage = min(1.0, total_items / capacity)
        # 0.5 hours base + up to 1.5 hours for full load
        return 0.5 + (load_percentage * 1.5)

    # Maximum number of state transitions to attempt when recovering from stuck states
    # Set to 6 (the number of states in the lifecycle) to ensure we can reach any state
    MAX_RECOVERY_STEPS = 6

    # Valid state transitions for truck lifecycle
    # Each state maps to the set of valid next states
    VALID_STATE_TRANSITIONS: dict[TruckStatus, set[TruckStatus]] = {
        TruckStatus.SCHEDULED: {TruckStatus.LOADING, TruckStatus.COMPLETED},  # Can skip to COMPLETED on timeout
        TruckStatus.LOADING: {TruckStatus.IN_TRANSIT, TruckStatus.COMPLETED},
        TruckStatus.IN_TRANSIT: {TruckStatus.ARRIVED, TruckStatus.COMPLETED},
        TruckStatus.ARRIVED: {TruckStatus.UNLOADING, TruckStatus.COMPLETED},
        TruckStatus.UNLOADING: {TruckStatus.COMPLETED},
        TruckStatus.COMPLETED: set(),  # Terminal state - no further transitions
    }

    # Maximum time a shipment can be in any state before being considered stuck (hours)
    STATE_TIMEOUT_HOURS: dict[TruckStatus, int] = {
        TruckStatus.SCHEDULED: 24,   # Max 24 hours waiting to load
        TruckStatus.LOADING: 8,      # Max 8 hours loading
        TruckStatus.IN_TRANSIT: 48,  # Max 48 hours in transit
        TruckStatus.ARRIVED: 4,      # Max 4 hours waiting to unload
        TruckStatus.UNLOADING: 8,    # Max 8 hours unloading
    }

    def _validate_state_transition(
        self, shipment_id: str, current_state: TruckStatus, new_state: TruckStatus
    ) -> bool:
        """
        Validate that a state transition is allowed.

        Args:
            shipment_id: Shipment ID for logging
            current_state: Current truck status
            new_state: Proposed new status

        Returns:
            True if transition is valid, False otherwise
        """
        if new_state == current_state:
            return True  # No change is always valid

        valid_next_states = self.VALID_STATE_TRANSITIONS.get(current_state, set())
        if new_state in valid_next_states:
            return True

        # Log warning for invalid transition
        logger.warning(
            f"Invalid state transition for shipment {shipment_id}: "
            f"{current_state.value} -> {new_state.value}. "
            f"Valid transitions from {current_state.value}: {[s.value for s in valid_next_states]}"
        )
        return False

    def _check_state_timeout(
        self, shipment: dict, current_time: datetime
    ) -> TruckStatus | None:
        """
        Check if a shipment has exceeded state timeout and needs recovery.

        Args:
            shipment: Shipment information dictionary
            current_time: Current simulation time

        Returns:
            TruckStatus.COMPLETED if timed out and needs recovery, None otherwise
        """
        current_status = shipment.get("status", TruckStatus.SCHEDULED)

        # Track when we entered the current state
        state_entry_key = f"_state_entered_{current_status.value}"
        if state_entry_key not in shipment:
            # First time seeing this state - record entry time
            shipment[state_entry_key] = current_time
            return None

        state_entry_time = shipment[state_entry_key]
        timeout_hours = self.STATE_TIMEOUT_HOURS.get(current_status, 24)
        max_time_in_state = timedelta(hours=timeout_hours)

        if current_time - state_entry_time > max_time_in_state:
            logger.warning(
                f"Shipment {shipment['shipment_id']} stuck in {current_status.value} "
                f"for over {timeout_hours} hours. Forcing completion for recovery."
            )
            return TruckStatus.COMPLETED

        return None

    def update_shipment_status(
        self, shipment_id: str, current_time: datetime
    ) -> dict | None:
        """
        Update shipment status based on current time.

        Implements complete truck lifecycle state machine with validation:
        - SCHEDULED (T+0): Initial state when shipment is created
        - LOADING (T+2hrs): Truck is being loaded at DC
        - IN_TRANSIT (T+4hrs): Truck is traveling to destination
        - ARRIVED (ETA): Truck has arrived at destination
        - UNLOADING (ETA+1hr): Truck is being unloaded at store
        - COMPLETED (ETD): Delivery is complete

        Features:
        - Validates all state transitions
        - Logs warnings for unexpected transitions
        - Implements timeout recovery for stuck states

        Args:
            shipment_id: Shipment to update
            current_time: Current simulation time

        Returns:
            Updated shipment info or None if not found
        """
        if shipment_id not in self._active_shipments:
            return None

        shipment = self._active_shipments[shipment_id]
        departure_time = shipment["departure_time"]
        current_status = shipment.get("status", TruckStatus.SCHEDULED)

        # Check for timeout recovery first
        recovery_status = self._check_state_timeout(shipment, current_time)
        if recovery_status == TruckStatus.COMPLETED:
            if self._validate_state_transition(shipment_id, current_status, TruckStatus.COMPLETED):
                logger.info(f"Shipment {shipment_id} recovered from stuck state via timeout")
                shipment["status"] = TruckStatus.COMPLETED
                shipment["_recovered_via_timeout"] = True
                del self._active_shipments[shipment_id]
                return shipment

        # Calculate transition times
        loading_start = departure_time + timedelta(hours=2)
        transit_start = departure_time + timedelta(hours=4)
        arrived_time = shipment["eta"]  # ETA calculated in generate_truck_shipment
        unloading_start = arrived_time + timedelta(hours=1)
        completion_time = shipment["etd"]  # ETD is based on shipment size

        # Determine target state based on time
        if current_time >= completion_time:
            target_status = TruckStatus.COMPLETED
        elif current_time >= unloading_start:
            target_status = TruckStatus.UNLOADING
        elif current_time >= arrived_time:
            target_status = TruckStatus.ARRIVED
        elif current_time >= transit_start:
            target_status = TruckStatus.IN_TRANSIT
        elif current_time >= loading_start:
            target_status = TruckStatus.LOADING
        else:
            target_status = TruckStatus.SCHEDULED

        # Only update if state is changing
        if target_status != current_status:
            # Validate the transition
            if self._validate_state_transition(shipment_id, current_status, target_status):
                old_status = current_status
                shipment["status"] = target_status

                # Clear old state entry time and set new one
                for key in list(shipment.keys()):
                    if key.startswith("_state_entered_"):
                        del shipment[key]
                shipment[f"_state_entered_{target_status.value}"] = current_time

                # Log state transitions for debugging
                logger.debug(
                    f"Shipment {shipment_id} transitioned: {old_status.value} -> {target_status.value}"
                )

                # Remove from active tracking if completed
                if target_status == TruckStatus.COMPLETED:
                    del self._active_shipments[shipment_id]
            else:
                # Invalid transition - try to recover by stepping through states
                # This handles cases where time jumps and we skip states
                # Keep stepping until we reach the target or can't progress anymore
                state_order = [
                    TruckStatus.SCHEDULED, TruckStatus.LOADING, TruckStatus.IN_TRANSIT,
                    TruckStatus.ARRIVED, TruckStatus.UNLOADING, TruckStatus.COMPLETED
                ]
                stepping_status = current_status
                steps_taken = 0
                max_steps = self.MAX_RECOVERY_STEPS  # Prevent infinite loops

                while stepping_status != target_status and steps_taken < max_steps:
                    next_valid_states = self.VALID_STATE_TRANSITIONS.get(stepping_status, set())
                    if not next_valid_states:
                        break

                    # Find the next state in order that's valid from current position
                    next_state = None
                    for state in state_order:
                        if state in next_valid_states:
                            next_state = state
                            break

                    if next_state is None:
                        break

                    stepping_status = next_state
                    steps_taken += 1

                if stepping_status != current_status:
                    logger.info(
                        f"Shipment {shipment_id} recovered by stepping through {steps_taken} states: "
                        f"{current_status.value} -> {stepping_status.value} (target was {target_status.value})"
                    )
                    shipment["status"] = stepping_status

                    # Warn if we couldn't reach the target state
                    if stepping_status != target_status:
                        logger.warning(
                            f"Shipment {shipment_id} recovery stopped at {stepping_status.value}, "
                            f"could not reach target {target_status.value}. May need manual intervention."
                        )

                    # Clear old state entry times and set new one.
                    # Note: Only current state entry time is tracked; historical times are
                    # discarded to keep shipment dict lightweight. For state duration analysis,
                    # use the truck_moves event stream which records each transition.
                    for key in list(shipment.keys()):
                        if key.startswith("_state_entered_"):
                            del shipment[key]
                    shipment[f"_state_entered_{stepping_status.value}"] = current_time

                    # Remove from active tracking if completed
                    if stepping_status == TruckStatus.COMPLETED:
                        del self._active_shipments[shipment_id]

        return shipment

    def complete_delivery(self, shipment_id: str) -> list[dict]:
        """
        Complete delivery and update store inventory.

        Also clears in-transit inventory tracking since items have arrived.

        Args:
            shipment_id: Shipment to complete

        Returns:
            List of store inventory transactions
        """
        if shipment_id not in self._active_shipments:
            return []

        shipment = self._active_shipments[shipment_id]
        transactions = []

        store_id = shipment["store_id"]
        dc_id = shipment["dc_id"]

        # Process each product in shipment
        for product_id, quantity in shipment["products"]:
            # Update store inventory
            store_key = (store_id, product_id)
            self._store_inventory[store_key] = (
                self._store_inventory.get(store_key, 0) + quantity
            )

            # Clear in-transit tracking since items have arrived
            in_transit = self._in_transit_inventory.get(store_key, 0)
            self._in_transit_inventory[store_key] = max(0, in_transit - quantity)

            # Update DC inventory (outbound)
            dc_key = (dc_id, product_id)
            self._dc_inventory[dc_key] = max(
                0, self._dc_inventory.get(dc_key, 0) - quantity
            )

            # Create transactions
            transactions.append(
                {
                    "StoreID": store_id,
                    "ProductID": product_id,
                    "QtyDelta": quantity,
                    "Reason": InventoryReason.INBOUND_SHIPMENT,
                    "Source": shipment["truck_id"],
                    "EventTS": shipment["etd"],
                }
            )

        return transactions

    def generate_truck_loading_events(
        self, shipment_info: dict, load_time: datetime
    ) -> list[dict]:
        """
        Generate truck loading events for inventory tracking.

        Args:
            shipment_info: Shipment information with snake_case keys (internal format)
            load_time: When loading occurs

        Returns:
            List of truck inventory loading records (dict keys use PascalCase per TruckInventory schema)

        Note:
            Converts from internal snake_case keys (truck_id, shipment_id, dc_id) to
            schema PascalCase keys (TruckId, ShipmentId, ProductID, LocationID).
        """
        truck_inventory_events = []

        for product_id, quantity in shipment_info["products"]:
            # Output dict: keys use PascalCase to match TruckInventory schema
            truck_inventory_events.append(
                {
                    "TruckId": shipment_info["truck_id"],
                    "ShipmentId": shipment_info["shipment_id"],
                    "ProductID": product_id,
                    "Quantity": quantity,
                    "Action": "LOAD",
                    "LocationID": shipment_info["dc_id"],
                    "LocationType": "DC",
                    "EventTS": load_time,
                }
            )

        return truck_inventory_events

    def generate_dc_outbound_transactions(
        self, shipment_info: dict, load_time: datetime
    ) -> list[dict]:
        """
        Generate DC outbound inventory transactions when truck loading starts.

        These transactions link to the truck shipment via shipment_id in the Source field,
        creating an audit trail from DC → Truck → Store.

        Args:
            shipment_info: Shipment information with snake_case keys (internal format)
            load_time: When loading occurs (truck status = LOADING)

        Returns:
            List of DC inventory transaction records (dict keys use PascalCase per schema)
        """
        dc_transactions = []
        dc_id = shipment_info["dc_id"]
        shipment_id = shipment_info["shipment_id"]

        for product_id, quantity in shipment_info["products"]:
            # Update DC inventory (outbound - negative delta)
            dc_key = (dc_id, product_id)
            current_inventory = self._dc_inventory.get(dc_key, 0)
            self._dc_inventory[dc_key] = max(0, current_inventory - quantity)
            new_balance = self._dc_inventory[dc_key]

            # Output dict: keys use PascalCase to match DCInventoryTransaction schema
            dc_transactions.append(
                {
                    "DCID": dc_id,
                    "ProductID": product_id,
                    "QtyDelta": -quantity,  # Negative for outbound
                    "Reason": InventoryReason.OUTBOUND_SHIPMENT,
                    "Source": shipment_id,  # Link to truck shipment
                    "Balance": new_balance,
                    "EventTS": load_time,
                }
            )

        return dc_transactions

    def generate_store_inbound_transactions(
        self, shipment_info: dict, unload_time: datetime
    ) -> list[dict]:
        """
        Generate store inbound inventory transactions when truck unloading starts.

        These transactions link to the truck shipment via shipment_id in the Source field,
        creating an audit trail from DC → Truck → Store.

        Also clears the in-transit inventory tracking since items have now arrived.

        Args:
            shipment_info: Shipment information with snake_case keys (internal format)
            unload_time: When unloading occurs (truck status = UNLOADING)

        Returns:
            List of store inventory transaction records (dict keys use PascalCase per schema)
        """
        store_transactions = []
        store_id = shipment_info["store_id"]
        shipment_id = shipment_info["shipment_id"]

        for product_id, quantity in shipment_info["products"]:
            # Update store inventory (inbound - positive delta)
            store_key = (store_id, product_id)
            current_inventory = self._store_inventory.get(store_key, 0)
            self._store_inventory[store_key] = current_inventory + quantity

            # Clear in-transit tracking since items have arrived
            in_transit = self._in_transit_inventory.get(store_key, 0)
            self._in_transit_inventory[store_key] = max(0, in_transit - quantity)

            # Get balance after transaction
            balance = self._store_inventory[store_key]

            # Output dict: keys use PascalCase to match StoreInventoryTransaction schema
            store_transactions.append(
                {
                    "StoreID": store_id,
                    "ProductID": product_id,
                    "QtyDelta": quantity,  # Positive for inbound
                    "Reason": InventoryReason.INBOUND_SHIPMENT,
                    "Source": shipment_id,  # Link to truck shipment
                    "Balance": balance,
                    "EventTS": unload_time,
                }
            )

        return store_transactions

    def generate_truck_unloading_events(
        self, shipment_info: dict, unload_time: datetime
    ) -> list[dict]:
        """
        Generate truck unloading events for inventory tracking.

        Args:
            shipment_info: Shipment information with snake_case keys (internal format)
            unload_time: When unloading occurs

        Returns:
            List of truck inventory unloading records (dict keys use PascalCase per TruckInventory schema)

        Note:
            Converts from internal snake_case keys (truck_id, shipment_id, store_id) to
            schema PascalCase keys (TruckId, ShipmentId, ProductID, LocationID).
        """
        truck_inventory_events = []

        for product_id, quantity in shipment_info["products"]:
            # Output dict: keys use PascalCase to match TruckInventory schema
            truck_inventory_events.append(
                {
                    "TruckId": shipment_info["truck_id"],
                    "ShipmentId": shipment_info["shipment_id"],
                    "ProductID": product_id,
                    "Quantity": quantity,
                    "Action": "UNLOAD",
                    "LocationID": shipment_info["store_id"],
                    "LocationType": "STORE",
                    "EventTS": unload_time,
                }
            )

        return truck_inventory_events

    def track_truck_inventory_status(self, date: datetime) -> list[dict]:
        """
        Generate truck inventory tracking events for all active shipments.

        Args:
            date: Current simulation date

        Returns:
            List of truck inventory tracking records
        """
        all_truck_inventory_events = []

        for shipment_id, shipment_info in self._active_shipments.items():
            current_status = shipment_info.get("status", TruckStatus.SCHEDULED)

            # Generate loading events when truck status changes to LOADING
            if current_status == TruckStatus.LOADING:
                load_events = self.generate_truck_loading_events(shipment_info, date)
                all_truck_inventory_events.extend(load_events)

            # Generate unloading events when truck status changes to UNLOADING
            elif current_status == TruckStatus.UNLOADING:
                unload_events = self.generate_truck_unloading_events(
                    shipment_info, shipment_info["etd"]
                )
                all_truck_inventory_events.extend(unload_events)

        return all_truck_inventory_events

    def simulate_supply_chain_disruptions(self, date: datetime) -> list[dict]:
        """
        Generate and manage supply chain disruption events.

        Args:
            date: Current simulation date

        Returns:
            List of supply chain disruption records
        """

        disruption_events = []

        # Check for new disruption (2% chance per day per DC)
        for dc in self.dcs:
            if dc.ID not in self._active_disruptions and self._rng.random() < 0.02:
                disruption = self._create_disruption(dc.ID, date)
                self._active_disruptions[dc.ID] = disruption
                disruption_events.append(disruption)

        # Check for resolving existing disruptions
        resolved_dcs = []
        for dc_id, disruption in self._active_disruptions.items():
            # Check if disruption should end (based on duration)
            duration_hours = (date - disruption["start_time"]).total_seconds() / 3600
            expected_duration = disruption["expected_duration_hours"]

            # 70% chance to resolve after expected duration, increases over time
            resolve_probability = max(
                0.7, (duration_hours - expected_duration) / expected_duration * 0.5
            )

            if (
                duration_hours >= expected_duration
                and self._rng.random() < resolve_probability
            ):
                # Create resolution event
                resolution_event = {
                    "DCID": dc_id,
                    "DisruptionType": disruption["type"],
                    "Severity": disruption["severity"],
                    "Description": f"Resolved: {disruption['description']}",
                    "StartTime": disruption["start_time"],
                    "EndTime": date,
                    "ImpactPercentage": disruption["impact_percentage"],
                    "AffectedProducts": disruption["affected_products"],
                    "EventTS": date,
                }
                disruption_events.append(resolution_event)
                resolved_dcs.append(dc_id)

        # Remove resolved disruptions
        for dc_id in resolved_dcs:
            del self._active_disruptions[dc_id]

        return disruption_events

    def _create_disruption(self, dc_id: int, date: datetime) -> dict:
        """Create a new supply chain disruption event."""

        # Select disruption type based on weights
        disruption_types = [
            (DisruptionType.CAPACITY_CONSTRAINT, 0.3),
            (DisruptionType.EQUIPMENT_FAILURE, 0.25),
            (DisruptionType.WEATHER_DELAY, 0.2),
            (DisruptionType.LABOR_SHORTAGE, 0.15),
            (DisruptionType.SYSTEM_OUTAGE, 0.1),
        ]

        disruption_type = self._rng.choices(
            [dt[0] for dt in disruption_types],
            weights=[dt[1] for dt in disruption_types],
        )[0]

        # Select severity (minor more common)
        severity_weights = {
            DisruptionSeverity.MINOR: 0.6,
            DisruptionSeverity.MODERATE: 0.3,
            DisruptionSeverity.SEVERE: 0.1,
        }
        severity = self._rng.choices(
            list(severity_weights.keys()), weights=list(severity_weights.values())
        )[0]

        # Calculate impact percentage based on severity
        impact_ranges = {
            DisruptionSeverity.MINOR: (10, 30),
            DisruptionSeverity.MODERATE: (30, 60),
            DisruptionSeverity.SEVERE: (60, 90),
        }
        impact_percentage = self._rng.randint(*impact_ranges[severity])

        # Generate duration (hours)
        duration_ranges = {
            DisruptionSeverity.MINOR: (2, 12),
            DisruptionSeverity.MODERATE: (8, 48),
            DisruptionSeverity.SEVERE: (24, 168),  # Up to 1 week
        }
        expected_duration = self._rng.randint(*duration_ranges[severity])

        # Select affected products (random subset)
        num_affected = max(1, int(len(self.products) * (impact_percentage / 100) * 0.3))
        affected_product_ids = [
            p.ID for p in self._rng.sample(self.products, num_affected)
        ]

        # Generate description
        descriptions = {
            DisruptionType.CAPACITY_CONSTRAINT: "Reduced capacity due to high demand surge",
            DisruptionType.EQUIPMENT_FAILURE: "Equipment failure in sorting/loading systems",
            DisruptionType.WEATHER_DELAY: "Weather-related delays affecting inbound shipments",
            DisruptionType.LABOR_SHORTAGE: "Staff shortage impacting operations",
            DisruptionType.SYSTEM_OUTAGE: "IT system outage affecting inventory management",
        }

        return {
            "DCID": dc_id,
            "DisruptionType": disruption_type,
            "Severity": severity,
            "Description": descriptions[disruption_type],
            "StartTime": date,
            "EndTime": None,
            "ImpactPercentage": impact_percentage,
            "AffectedProducts": json.dumps(affected_product_ids),
            "EventTS": date,
            "type": disruption_type,
            "severity": severity,
            "description": descriptions[disruption_type],
            "start_time": date,
            "impact_percentage": impact_percentage,
            "affected_products": json.dumps(affected_product_ids),
            "expected_duration_hours": expected_duration,
        }

    def get_dc_capacity_multiplier(self, dc_id: int, date: datetime) -> float:
        """
        Get capacity multiplier for a DC considering active disruptions.

        Args:
            dc_id: Distribution center ID
            date: Current simulation date

        Returns:
            Capacity multiplier (1.0 = normal, 0.5 = 50% capacity, etc.)
        """
        disruptions = getattr(self, "_active_disruptions", {})
        if dc_id in disruptions:
            disruption = disruptions[dc_id]
            impact_percentage = disruption["impact_percentage"]
            return 1.0 - (impact_percentage / 100)
        return 1.0

    def get_dc_balance(self, dc_id: int, product_id: int) -> int:
        """Get current inventory balance for a DC-product combination.

        Args:
            dc_id: Distribution center ID
            product_id: Product ID

        Returns:
            Current inventory balance (0 if not found)
        """
        key = (dc_id, product_id)
        return self._dc_inventory.get(key, 0)

    def get_store_balance(self, store_id: int, product_id: int) -> int:
        """Get current inventory balance for a store-product combination.

        Args:
            store_id: Store ID
            product_id: Product ID

        Returns:
            Current inventory balance (0 if not found)
        """
        key = (store_id, product_id)
        return self._store_inventory.get(key, 0)

    def get_in_transit_quantity(self, store_id: int, product_id: int) -> int:
        """Get quantity currently in transit to a store for a product.

        Args:
            store_id: Store ID
            product_id: Product ID

        Returns:
            Quantity in transit (0 if none)
        """
        key = (store_id, product_id)
        return self._in_transit_inventory.get(key, 0)

    def get_effective_store_inventory(self, store_id: int, product_id: int) -> int:
        """Get effective inventory (on-hand + in-transit) for reorder decisions.

        Args:
            store_id: Store ID
            product_id: Product ID

        Returns:
            Total effective inventory including in-transit shipments
        """
        return self.get_store_balance(store_id, product_id) + self.get_in_transit_quantity(store_id, product_id)
