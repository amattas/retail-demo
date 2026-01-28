"""
Truck operations for supply chain simulation.

This module handles truck selection, shipment generation, status tracking,
and the complete truck lifecycle state machine.
"""

import logging
import warnings
from datetime import UTC, datetime, timedelta

from retail_datagen.shared.models import (
    InventoryReason,
    TruckStatus,
)

from .base_types import InventoryFlowBase

logger = logging.getLogger(__name__)

# Minimum datetime with UTC timezone for comparisons
DATETIME_MIN_UTC = datetime.min.replace(tzinfo=UTC)


class TruckOperationsMixin(InventoryFlowBase):
    """
    Mixin providing truck operation functionality for inventory simulation.

    Handles truck selection, shipment generation, loading/unloading,
    and the complete truck lifecycle state machine.

    Requires parent class to provide:
        - _rng: random.Random instance
        - _truck_capacity: int
        - _trucks: list
        - _trucks_by_dc: dict[int | None, list[int]]
        - _truck_rr_index: dict[int | None, int]
        - _truck_availability: dict[int, datetime]
        - _active_shipments: dict[str, dict]
        - _in_transit_inventory: dict[tuple[int, int], int]
        - _dc_inventory: dict[tuple[int, int], int]
        - _store_inventory: dict[tuple[int, int], int]
        - get_dc_capacity_multiplier(dc_id, date): float
    """

    # Maximum number of state transitions to attempt when recovering from stuck states
    MAX_RECOVERY_STEPS = 6

    # Valid state transitions for truck lifecycle
    VALID_STATE_TRANSITIONS: dict[TruckStatus, set[TruckStatus]] = {
        TruckStatus.SCHEDULED: {TruckStatus.LOADING, TruckStatus.COMPLETED},
        TruckStatus.LOADING: {TruckStatus.IN_TRANSIT, TruckStatus.COMPLETED},
        TruckStatus.IN_TRANSIT: {TruckStatus.ARRIVED, TruckStatus.COMPLETED},
        TruckStatus.ARRIVED: {TruckStatus.UNLOADING, TruckStatus.COMPLETED},
        TruckStatus.UNLOADING: {TruckStatus.COMPLETED},
        TruckStatus.COMPLETED: set(),
    }

    # Maximum time a shipment can be in any state before being considered stuck
    STATE_TIMEOUT_HOURS: dict[TruckStatus, int] = {
        TruckStatus.SCHEDULED: 24,
        TruckStatus.LOADING: 8,
        TruckStatus.IN_TRANSIT: 48,
        TruckStatus.ARRIVED: 4,
        TruckStatus.UNLOADING: 8,
    }

    def _select_truck_for_shipment(
        self, dc_id: int, current_time: datetime
    ) -> int | str | None:
        """Select an available truck for a shipment.

        Prefers trucks assigned to the DC.

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
            truck_id
            for truck_id in dc_list
            if self._truck_availability.get(truck_id, DATETIME_MIN_UTC) <= current_time
        ]
        if available_dc_trucks:
            idx = self._truck_rr_index.get(dc_id, 0) % len(available_dc_trucks)
            self._truck_rr_index[dc_id] = idx + 1
            return available_dc_trucks[idx]

        # Fallback to pool trucks - find first available
        pool_list = self._trucks_by_dc.get(None) or []
        available_pool_trucks = [
            truck_id
            for truck_id in pool_list
            if self._truck_availability.get(truck_id, DATETIME_MIN_UTC) <= current_time
        ]
        if available_pool_trucks:
            return self._rng.choice(available_pool_trucks)

        # If no real trucks exist at all, use synthetic
        if not dc_list and not pool_list:
            return f"TRK{self._rng.randint(1000, 9999)}"

        # All real trucks are busy
        return None

    def _mark_truck_unavailable(
        self, truck_id: int | str, return_time: datetime
    ) -> None:
        """Mark a truck as unavailable until it returns to the DC."""
        if isinstance(truck_id, int):
            self._truck_availability[truck_id] = return_time

    def _calculate_round_trip_time(
        self, travel_hours: float, unload_hours: float
    ) -> float:
        """Calculate total round trip time including return journey."""
        return travel_hours + unload_hours + travel_hours

    def get_next_available_truck_time(
        self, dc_id: int, current_time: datetime
    ) -> datetime | None:
        """Get the earliest time a truck will be available at this DC."""
        dc_list = self._trucks_by_dc.get(dc_id) or []
        pool_list = self._trucks_by_dc.get(None) or []
        all_trucks = dc_list + pool_list

        if not all_trucks:
            return None

        for truck_id in all_trucks:
            if self._truck_availability.get(truck_id, DATETIME_MIN_UTC) <= current_time:
                return None

        return_times = [
            self._truck_availability.get(truck_id, DATETIME_MIN_UTC)
            for truck_id in all_trucks
        ]
        return min(return_times)

    def _calculate_unload_duration(self, total_items: int) -> float:
        """Calculate unload duration based on shipment size (0.5 to 2.0 hours)."""
        capacity = self._truck_capacity
        load_percentage = min(1.0, total_items / capacity)
        return 0.5 + (load_percentage * 1.5)

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
        truck capacity, items are truncated to fit.

        Args:
            dc_id: Source distribution center ID
            store_id: Destination store ID
            reorder_list: List of (product_id, quantity) to ship
            departure_time: When truck departs

        Returns:
            Shipment information dictionary
        """
        # Validate quantities are non-negative
        for product_id, qty in reorder_list:
            if qty < 0:
                raise ValueError(
                    f"Invalid negative quantity {qty} for product {product_id}"
                )

        # Enforce truck capacity
        capacity = self._truck_capacity
        total_requested = sum(qty for _, qty in reorder_list)

        if total_requested > capacity:
            logger.warning(
                f"Shipment to store {store_id} exceeds truck capacity "
                f"({total_requested} > {capacity}). Truncating."
            )
            warnings.warn(
                "Order exceeds truck capacity and will be truncated. "
                "Use generate_truck_shipments() for automatic multi-truck splitting.",
                UserWarning,
                stacklevel=2,
            )
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
        date_str = departure_time.strftime("%Y%m%d")
        rand_suffix = self._rng.randint(100, 999)
        shipment_id = f"SHIP{date_str}{dc_id:02d}{store_id:03d}{rand_suffix}"

        # Choose a real truck when available
        actual_departure_time = departure_time
        truck_id = self._select_truck_for_shipment(dc_id, departure_time)

        if truck_id is None:
            next_available = self.get_next_available_truck_time(dc_id, departure_time)
            if next_available is not None:
                actual_departure_time = next_available
                truck_id = self._select_truck_for_shipment(dc_id, actual_departure_time)

        if truck_id is None:
            truck_id = f"TRK{self._rng.randint(1000, 9999)}"
            logger.warning(f"No trucks available for DC {dc_id}, using synthetic")

        # Check for active disruptions
        capacity_multiplier = self.get_dc_capacity_multiplier(
            dc_id, actual_departure_time
        )

        # Calculate travel time with disruption delays
        base_travel_hours = self._rng.randint(2, 12)
        delay_multiplier = 2.0 - capacity_multiplier
        travel_hours = int(base_travel_hours * delay_multiplier)

        eta = actual_departure_time + timedelta(hours=travel_hours)
        unload_hours = self._calculate_unload_duration(
            sum(qty for _, qty in reorder_list)
        )
        etd = eta + timedelta(hours=unload_hours)

        round_trip_hours = self._calculate_round_trip_time(travel_hours, unload_hours)
        truck_return_time = actual_departure_time + timedelta(hours=round_trip_hours)

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
            "truck_return_time": truck_return_time,
        }

        self._active_shipments[shipment_id] = shipment_info

        # Track in-transit inventory
        for product_id, quantity in reorder_list:
            key = (store_id, product_id)
            self._in_transit_inventory[key] = (
                self._in_transit_inventory.get(key, 0) + quantity
            )

        return shipment_info

    def generate_truck_shipments(
        self,
        dc_id: int,
        store_id: int,
        reorder_list: list[tuple[int, int]],
        departure_time: datetime,
    ) -> list[dict]:
        """
        Generate one or more truck shipments, respecting capacity.

        Splits large orders across multiple trucks with staggered departures.
        """
        if not reorder_list:
            return []

        for product_id, qty in reorder_list:
            if qty < 0:
                raise ValueError(
                    f"Invalid negative quantity {qty} for product {product_id}"
                )

        capacity = self._truck_capacity
        total_items = sum(qty for _, qty in reorder_list)

        if total_items <= capacity:
            shipment = self.generate_truck_shipment(
                dc_id, store_id, reorder_list, departure_time
            )
            return [shipment]

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

                if current_truck_total >= capacity:
                    offset = timedelta(minutes=30 * truck_number)
                    truck_departure = departure_time + offset
                    shipment = self.generate_truck_shipment(
                        dc_id, store_id, current_truck_items, truck_departure
                    )
                    shipments.append(shipment)
                    truck_number += 1
                    current_truck_items = []
                    current_truck_total = 0

        if current_truck_items:
            truck_departure = departure_time + timedelta(minutes=30 * truck_number)
            shipment = self.generate_truck_shipment(
                dc_id, store_id, current_truck_items, truck_departure
            )
            shipments.append(shipment)

        logger.info(
            f"Large order for store {store_id} split across {len(shipments)} trucks"
        )
        return shipments

    def _validate_state_transition(
        self, shipment_id: str, current_state: TruckStatus, new_state: TruckStatus
    ) -> bool:
        """Validate that a state transition is allowed."""
        if new_state == current_state:
            return True

        valid_next_states = self.VALID_STATE_TRANSITIONS.get(current_state, set())
        if new_state in valid_next_states:
            return True

        logger.warning(
            f"Invalid state transition for {shipment_id}: "
            f"{current_state.value} -> {new_state.value}"
        )
        return False

    def _check_state_timeout(
        self, shipment: dict, current_time: datetime
    ) -> TruckStatus | None:
        """Check if a shipment has exceeded state timeout."""
        current_status = shipment.get("status", TruckStatus.SCHEDULED)
        state_entry_key = f"_state_entered_{current_status.value}"

        if state_entry_key not in shipment:
            shipment[state_entry_key] = current_time
            return None

        state_entry_time = shipment[state_entry_key]
        timeout_hours = self.STATE_TIMEOUT_HOURS.get(current_status, 24)
        max_time_in_state = timedelta(hours=timeout_hours)

        if current_time - state_entry_time > max_time_in_state:
            logger.warning(
                f"Shipment {shipment['shipment_id']} stuck in {current_status.value}"
            )
            return TruckStatus.COMPLETED

        return None

    def update_shipment_status(
        self, shipment_id: str, current_time: datetime
    ) -> dict | None:
        """
        Update shipment status based on current time.

        Implements complete truck lifecycle state machine with validation.
        Advances through states progressively to avoid invalid transitions.
        """
        if shipment_id not in self._active_shipments:
            return None

        shipment = self._active_shipments[shipment_id]
        departure_time = shipment["departure_time"]
        current_status = shipment.get("status", TruckStatus.SCHEDULED)

        # Check for timeout recovery
        recovery_status = self._check_state_timeout(shipment, current_time)
        if recovery_status == TruckStatus.COMPLETED:
            if self._validate_state_transition(
                shipment_id, current_status, TruckStatus.COMPLETED
            ):
                shipment["status"] = TruckStatus.COMPLETED
                shipment["_recovered_via_timeout"] = True
                del self._active_shipments[shipment_id]
                return shipment

        # Calculate transition times
        loading_start = departure_time + timedelta(hours=2)
        transit_start = departure_time + timedelta(hours=4)
        arrived_time = shipment["eta"]
        unloading_start = arrived_time + timedelta(hours=1)
        completion_time = shipment["etd"]

        # Determine target state based on current time
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

        # Advance state progressively to avoid invalid jumps
        # Only advance ONE state per call to prevent skipping intermediate states
        if target_status != current_status:
            # Define the expected state progression order
            state_order = [
                TruckStatus.SCHEDULED,
                TruckStatus.LOADING,
                TruckStatus.IN_TRANSIT,
                TruckStatus.ARRIVED,
                TruckStatus.UNLOADING,
                TruckStatus.COMPLETED,
            ]

            # Find current and target positions in the sequence
            try:
                current_index = state_order.index(current_status)
                target_index = state_order.index(target_status)
            except ValueError:
                logger.error(
                    f"Invalid state for shipment {shipment_id}: {current_status}"
                )
                return shipment

            # Advance only ONE state at a time
            if current_index < target_index:
                next_index = current_index + 1
                next_status = state_order[next_index]

                # Validate transition is allowed
                is_valid = self._validate_state_transition(
                    shipment_id, current_status, next_status
                )
                if is_valid:
                    shipment["status"] = next_status
                    # Clear old state entry timestamps
                    for key in list(shipment.keys()):
                        if key.startswith("_state_entered_"):
                            del shipment[key]
                    shipment[f"_state_entered_{next_status.value}"] = current_time

                    if next_status == TruckStatus.COMPLETED:
                        del self._active_shipments[shipment_id]
                else:
                    # Should not happen with proper state machine, but handle gracefully
                    logger.error(
                        f"Blocked transition for {shipment_id}: "
                        f"{current_status.value} -> {next_status.value}"
                    )

        return shipment

    def complete_delivery(self, shipment_id: str) -> list[dict]:
        """Complete delivery and update store inventory."""
        if shipment_id not in self._active_shipments:
            return []

        shipment = self._active_shipments[shipment_id]
        transactions = []
        store_id = shipment["store_id"]
        dc_id = shipment["dc_id"]

        for product_id, quantity in shipment["products"]:
            store_key = (store_id, product_id)
            self._store_inventory[store_key] = (
                self._store_inventory.get(store_key, 0) + quantity
            )
            in_transit = self._in_transit_inventory.get(store_key, 0)
            self._in_transit_inventory[store_key] = max(0, in_transit - quantity)

            dc_key = (dc_id, product_id)
            self._dc_inventory[dc_key] = max(
                0, self._dc_inventory.get(dc_key, 0) - quantity
            )

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
        """Generate truck loading events for inventory tracking."""
        truck_inventory_events = []
        for product_id, quantity in shipment_info["products"]:
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
        """Generate DC outbound inventory transactions when truck loading starts."""
        dc_transactions = []
        dc_id = shipment_info["dc_id"]
        shipment_id = shipment_info["shipment_id"]

        for product_id, quantity in shipment_info["products"]:
            dc_key = (dc_id, product_id)
            current_inventory = self._dc_inventory.get(dc_key, 0)
            self._dc_inventory[dc_key] = max(0, current_inventory - quantity)
            new_balance = self._dc_inventory[dc_key]

            dc_transactions.append(
                {
                    "DCID": dc_id,
                    "ProductID": product_id,
                    "QtyDelta": -quantity,
                    "Reason": InventoryReason.OUTBOUND_SHIPMENT,
                    "Source": shipment_id,
                    "Balance": new_balance,
                    "EventTS": load_time,
                }
            )
        return dc_transactions

    def generate_store_inbound_transactions(
        self, shipment_info: dict, unload_time: datetime
    ) -> list[dict]:
        """Generate store inbound inventory transactions when truck unloading starts."""
        store_transactions = []
        store_id = shipment_info["store_id"]
        shipment_id = shipment_info["shipment_id"]

        for product_id, quantity in shipment_info["products"]:
            store_key = (store_id, product_id)
            current_inventory = self._store_inventory.get(store_key, 0)
            self._store_inventory[store_key] = current_inventory + quantity

            in_transit = self._in_transit_inventory.get(store_key, 0)
            self._in_transit_inventory[store_key] = max(0, in_transit - quantity)

            balance = self._store_inventory[store_key]
            store_transactions.append(
                {
                    "StoreID": store_id,
                    "ProductID": product_id,
                    "QtyDelta": quantity,
                    "Reason": InventoryReason.INBOUND_SHIPMENT,
                    "Source": shipment_id,
                    "Balance": balance,
                    "EventTS": unload_time,
                }
            )
        return store_transactions

    def generate_truck_unloading_events(
        self, shipment_info: dict, unload_time: datetime
    ) -> list[dict]:
        """Generate truck unloading events for inventory tracking."""
        truck_inventory_events = []
        for product_id, quantity in shipment_info["products"]:
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
        """Generate truck inventory tracking events for all active shipments."""
        all_truck_inventory_events = []

        for shipment_id, shipment_info in self._active_shipments.items():
            current_status = shipment_info.get("status", TruckStatus.SCHEDULED)

            if current_status == TruckStatus.LOADING:
                load_events = self.generate_truck_loading_events(shipment_info, date)
                all_truck_inventory_events.extend(load_events)
            elif current_status == TruckStatus.UNLOADING:
                unload_events = self.generate_truck_unloading_events(
                    shipment_info, shipment_info["etd"]
                )
                all_truck_inventory_events.extend(unload_events)

        return all_truck_inventory_events
