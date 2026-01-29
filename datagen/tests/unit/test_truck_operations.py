"""
Unit tests for truck operations and state transitions.

This module tests the truck state machine to ensure proper state transitions
and prevent invalid jumps (e.g., LOADING -> ARRIVED without IN_TRANSIT).
"""

import random
from datetime import UTC, datetime, timedelta

import pytest

from retail_datagen.generators.retail_patterns.truck_operations import (
    TruckOperationsMixin,
)
from retail_datagen.shared.models import TruckStatus


class MockTruckOperations(TruckOperationsMixin):
    """Mock class for testing TruckOperationsMixin."""

    def __init__(self, seed: int = 42):
        """Initialize mock with required attributes."""
        self._rng = random.Random(seed)
        self._truck_capacity = 1000
        self._trucks = [1, 2, 3]
        self._trucks_by_dc = {1: [1, 2], 2: [3], None: []}
        self._truck_rr_index = {1: 0, 2: 0}
        self._truck_availability = {}
        self._active_shipments = {}
        self._in_transit_inventory = {}
        self._dc_inventory = {}
        self._store_inventory = {}

    def get_dc_capacity_multiplier(self, dc_id: int, date: datetime) -> float:
        """Mock capacity multiplier (no disruptions)."""
        return 1.0


class TestTruckStateTransitions:
    """Test suite for truck state machine transitions."""

    @pytest.fixture
    def truck_ops(self):
        """Create a mock truck operations instance."""
        return MockTruckOperations()

    @pytest.fixture
    def base_time(self):
        """Base timestamp for testing."""
        return datetime(2024, 1, 15, 8, 0, 0, tzinfo=UTC)

    def test_state_transitions_scheduled_to_completed(self, truck_ops, base_time):
        """Test complete state progression from SCHEDULED to COMPLETED."""
        # Create a shipment
        reorder_list = [(101, 50), (102, 75)]
        shipment = truck_ops.generate_truck_shipment(
            dc_id=1, store_id=5, reorder_list=reorder_list, departure_time=base_time
        )

        shipment_id = shipment["shipment_id"]
        assert shipment["status"] == TruckStatus.SCHEDULED

        # Advance to LOADING (2 hours after departure)
        loading_time = base_time + timedelta(hours=2)
        updated = truck_ops.update_shipment_status(shipment_id, loading_time)
        assert updated["status"] == TruckStatus.LOADING

        # Advance to IN_TRANSIT (4 hours after departure)
        transit_time = base_time + timedelta(hours=4)
        updated = truck_ops.update_shipment_status(shipment_id, transit_time)
        assert updated["status"] == TruckStatus.IN_TRANSIT

        # Advance to ARRIVED (at ETA)
        arrived_time = shipment["eta"]
        updated = truck_ops.update_shipment_status(shipment_id, arrived_time)
        assert updated["status"] == TruckStatus.ARRIVED

        # Advance to UNLOADING (1 hour after arrival)
        unloading_time = arrived_time + timedelta(hours=1)
        updated = truck_ops.update_shipment_status(shipment_id, unloading_time)
        assert updated["status"] == TruckStatus.UNLOADING

        # Advance to COMPLETED (at ETD)
        completion_time = shipment["etd"]
        updated = truck_ops.update_shipment_status(shipment_id, completion_time)
        assert updated["status"] == TruckStatus.COMPLETED
        assert shipment_id not in truck_ops._active_shipments

    def test_no_invalid_jump_loading_to_arrived(self, truck_ops, base_time):
        """Test LOADING cannot jump to ARRIVED (must go through IN_TRANSIT)."""
        reorder_list = [(101, 50)]
        shipment = truck_ops.generate_truck_shipment(
            dc_id=1, store_id=5, reorder_list=reorder_list, departure_time=base_time
        )

        shipment_id = shipment["shipment_id"]

        # Advance to LOADING
        loading_time = base_time + timedelta(hours=2)
        truck_ops.update_shipment_status(shipment_id, loading_time)
        assert shipment["status"] == TruckStatus.LOADING

        # Try to jump to ARRIVED (should go through IN_TRANSIT first)
        arrived_time = shipment["eta"]
        updated = truck_ops.update_shipment_status(shipment_id, arrived_time)

        # Should advance to IN_TRANSIT, not ARRIVED
        assert updated["status"] == TruckStatus.IN_TRANSIT

    def test_no_invalid_jump_scheduled_to_arrived(self, truck_ops, base_time):
        """Test that SCHEDULED cannot jump directly to ARRIVED."""
        reorder_list = [(101, 50)]
        shipment = truck_ops.generate_truck_shipment(
            dc_id=1, store_id=5, reorder_list=reorder_list, departure_time=base_time
        )

        shipment_id = shipment["shipment_id"]
        assert shipment["status"] == TruckStatus.SCHEDULED

        # Try to jump directly to ARRIVED
        arrived_time = shipment["eta"]
        updated = truck_ops.update_shipment_status(shipment_id, arrived_time)

        # Should advance only to LOADING (one step at a time)
        assert updated["status"] == TruckStatus.LOADING

    def test_progressive_state_advancement(self, truck_ops, base_time):
        """Test that state advances progressively when time jumps ahead."""
        reorder_list = [(101, 50)]
        shipment = truck_ops.generate_truck_shipment(
            dc_id=1, store_id=5, reorder_list=reorder_list, departure_time=base_time
        )

        shipment_id = shipment["shipment_id"]
        assert shipment["status"] == TruckStatus.SCHEDULED

        # Jump time far ahead (should advance one state per update)
        far_future = shipment["etd"]

        # First update: SCHEDULED -> LOADING
        updated = truck_ops.update_shipment_status(shipment_id, far_future)
        assert updated["status"] == TruckStatus.LOADING

        # Second update: LOADING -> IN_TRANSIT
        updated = truck_ops.update_shipment_status(shipment_id, far_future)
        assert updated["status"] == TruckStatus.IN_TRANSIT

        # Third update: IN_TRANSIT -> ARRIVED
        updated = truck_ops.update_shipment_status(shipment_id, far_future)
        assert updated["status"] == TruckStatus.ARRIVED

        # Fourth update: ARRIVED -> UNLOADING
        updated = truck_ops.update_shipment_status(shipment_id, far_future)
        assert updated["status"] == TruckStatus.UNLOADING

        # Fifth update: UNLOADING -> COMPLETED
        updated = truck_ops.update_shipment_status(shipment_id, far_future)
        assert updated["status"] == TruckStatus.COMPLETED

    def test_valid_state_transition_validation(self, truck_ops):
        """Test state transition validation logic."""
        shipment_id = "TEST123"

        # Valid transitions
        assert truck_ops._validate_state_transition(
            shipment_id, TruckStatus.SCHEDULED, TruckStatus.LOADING
        )
        assert truck_ops._validate_state_transition(
            shipment_id, TruckStatus.LOADING, TruckStatus.IN_TRANSIT
        )
        assert truck_ops._validate_state_transition(
            shipment_id, TruckStatus.IN_TRANSIT, TruckStatus.ARRIVED
        )
        assert truck_ops._validate_state_transition(
            shipment_id, TruckStatus.ARRIVED, TruckStatus.UNLOADING
        )
        assert truck_ops._validate_state_transition(
            shipment_id, TruckStatus.UNLOADING, TruckStatus.COMPLETED
        )

        # Invalid transitions (skipping states)
        assert not truck_ops._validate_state_transition(
            shipment_id, TruckStatus.LOADING, TruckStatus.ARRIVED
        )
        assert not truck_ops._validate_state_transition(
            shipment_id, TruckStatus.SCHEDULED, TruckStatus.IN_TRANSIT
        )
        assert not truck_ops._validate_state_transition(
            shipment_id, TruckStatus.SCHEDULED, TruckStatus.ARRIVED
        )

    def test_state_remains_unchanged_when_time_hasnt_progressed(
        self, truck_ops, base_time
    ):
        """Test state doesn't change if time hasn't reached next transition."""
        reorder_list = [(101, 50)]
        shipment = truck_ops.generate_truck_shipment(
            dc_id=1, store_id=5, reorder_list=reorder_list, departure_time=base_time
        )

        shipment_id = shipment["shipment_id"]
        assert shipment["status"] == TruckStatus.SCHEDULED

        # Update with time before loading_start
        early_time = base_time + timedelta(hours=1)
        updated = truck_ops.update_shipment_status(shipment_id, early_time)
        assert updated["status"] == TruckStatus.SCHEDULED

    def test_multiple_shipments_independent_states(self, truck_ops, base_time):
        """Test that multiple shipments maintain independent states."""
        # Create two shipments
        shipment1 = truck_ops.generate_truck_shipment(
            dc_id=1, store_id=5, reorder_list=[(101, 50)], departure_time=base_time
        )
        shipment2 = truck_ops.generate_truck_shipment(
            dc_id=1,
            store_id=6,
            reorder_list=[(102, 75)],
            departure_time=base_time + timedelta(hours=3),
        )

        id1 = shipment1["shipment_id"]
        id2 = shipment2["shipment_id"]

        # Advance shipment1 to LOADING
        loading_time = base_time + timedelta(hours=2)
        truck_ops.update_shipment_status(id1, loading_time)
        truck_ops.update_shipment_status(id2, loading_time)

        assert truck_ops._active_shipments[id1]["status"] == TruckStatus.LOADING
        assert truck_ops._active_shipments[id2]["status"] == TruckStatus.SCHEDULED

    def test_state_entry_timestamp_tracking(self, truck_ops, base_time):
        """Test that state entry timestamps are tracked correctly."""
        reorder_list = [(101, 50)]
        shipment = truck_ops.generate_truck_shipment(
            dc_id=1, store_id=5, reorder_list=reorder_list, departure_time=base_time
        )

        shipment_id = shipment["shipment_id"]

        # Advance to LOADING
        loading_time = base_time + timedelta(hours=2)
        updated = truck_ops.update_shipment_status(shipment_id, loading_time)

        # Check that state entry timestamp is set
        assert f"_state_entered_{TruckStatus.LOADING.value}" in updated
        assert updated[f"_state_entered_{TruckStatus.LOADING.value}"] == loading_time

    def test_shipment_removed_from_active_when_completed(self, truck_ops, base_time):
        """Test that shipments are removed from active tracking when completed."""
        reorder_list = [(101, 50)]
        shipment = truck_ops.generate_truck_shipment(
            dc_id=1, store_id=5, reorder_list=reorder_list, departure_time=base_time
        )

        shipment_id = shipment["shipment_id"]
        assert shipment_id in truck_ops._active_shipments

        # Advance through all states to COMPLETED
        completion_time = shipment["etd"]

        # Need to call update multiple times to advance through all states
        for _ in range(6):  # Maximum 6 states to traverse
            truck_ops.update_shipment_status(shipment_id, completion_time)
            if shipment_id not in truck_ops._active_shipments:
                break

        # Verify shipment is no longer in active shipments
        assert shipment_id not in truck_ops._active_shipments

    def test_update_nonexistent_shipment_returns_none(self, truck_ops, base_time):
        """Test that updating a non-existent shipment returns None."""
        result = truck_ops.update_shipment_status("INVALID123", base_time)
        assert result is None

    def test_truck_capacity_enforcement(self, truck_ops, base_time):
        """Test that shipments respect truck capacity limits."""
        # Try to ship more than capacity (1000 items)
        large_order = [(101, 1500)]

        with pytest.warns(UserWarning, match="exceeds truck capacity"):
            shipment = truck_ops.generate_truck_shipment(
                dc_id=1,
                store_id=5,
                reorder_list=large_order,
                departure_time=base_time,
            )

        # Should be truncated to capacity
        assert shipment["total_items"] == 1000

    def test_generate_truck_shipments_splits_large_orders(self, truck_ops, base_time):
        """Test that large orders are automatically split across multiple trucks."""
        large_order = [(101, 1500), (102, 800)]  # Total 2300 items, capacity is 1000

        shipments = truck_ops.generate_truck_shipments(
            dc_id=1, store_id=5, reorder_list=large_order, departure_time=base_time
        )

        # Should create 3 trucks (1000 + 1000 + 300)
        assert len(shipments) == 3

        # All shipments should be in SCHEDULED state initially
        for shipment in shipments:
            assert shipment["status"] == TruckStatus.SCHEDULED

        # Total items should match original order
        total_items = sum(s["total_items"] for s in shipments)
        assert total_items == 2300


class TestTruckSelectionAndAvailability:
    """Test suite for truck selection and availability tracking."""

    @pytest.fixture
    def truck_ops(self):
        """Create a mock truck operations instance."""
        return MockTruckOperations()

    @pytest.fixture
    def base_time(self):
        """Base timestamp for testing."""
        return datetime(2024, 1, 15, 8, 0, 0, tzinfo=UTC)

    def test_select_truck_prefers_dc_assigned(self, truck_ops, base_time):
        """Test that DC-assigned trucks are preferred over pool trucks."""
        # DC 1 has trucks [1, 2], pool has []
        truck_id = truck_ops._select_truck_for_shipment(1, base_time)
        assert truck_id in [1, 2]

    def test_select_truck_uses_pool_when_dc_trucks_busy(self, truck_ops, base_time):
        """Test that pool trucks are used when DC trucks are unavailable."""
        # Mark all DC 1 trucks as unavailable
        future_time = base_time + timedelta(hours=10)
        truck_ops._mark_truck_unavailable(1, future_time)
        truck_ops._mark_truck_unavailable(2, future_time)

        # Add a pool truck
        truck_ops._trucks_by_dc[None] = [99]

        truck_id = truck_ops._select_truck_for_shipment(1, base_time)
        assert truck_id == 99

    def test_select_truck_returns_none_when_all_busy(self, truck_ops, base_time):
        """Test that None is returned when all trucks are busy."""
        # Mark all trucks as unavailable
        future_time = base_time + timedelta(hours=10)
        for truck_id in [1, 2, 3]:
            truck_ops._mark_truck_unavailable(truck_id, future_time)

        result = truck_ops._select_truck_for_shipment(1, base_time)
        assert result is None

    def test_truck_availability_tracking(self, truck_ops, base_time):
        """Test that truck availability is tracked correctly."""
        return_time = base_time + timedelta(hours=5)
        truck_ops._mark_truck_unavailable(1, return_time)

        # Truck should not be available before return time
        truck_id = truck_ops._select_truck_for_shipment(1, base_time)
        assert truck_id != 1

        # After return time, round-robin may select any available truck
        later_time = base_time + timedelta(hours=6)
        truck_id = truck_ops._select_truck_for_shipment(1, later_time)
        # Truck 1 is available again, in pool of available trucks
        assert truck_id in [1, 2]  # Both DC trucks are available

    def test_calculate_round_trip_time(self, truck_ops):
        """Test round trip time calculation."""
        travel_hours = 5.0
        unload_hours = 2.0
        round_trip = truck_ops._calculate_round_trip_time(travel_hours, unload_hours)

        # Should be: travel + unload + return travel
        assert round_trip == 5.0 + 2.0 + 5.0

    def test_calculate_unload_duration(self, truck_ops):
        """Test unload duration calculation based on shipment size."""
        # Small shipment (10% capacity)
        duration_small = truck_ops._calculate_unload_duration(100)
        assert 0.5 <= duration_small <= 2.0

        # Full capacity shipment
        duration_full = truck_ops._calculate_unload_duration(1000)
        assert duration_full == 2.0  # 0.5 + (1.0 * 1.5)

        # Half capacity
        duration_half = truck_ops._calculate_unload_duration(500)
        assert duration_half == 1.25  # 0.5 + (0.5 * 1.5)
