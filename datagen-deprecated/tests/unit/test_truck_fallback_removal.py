"""
Unit tests for Issue #185: Remove synthetic truck ID fallback.

Tests verify that:
1. ValueError is raised when no trucks are configured for a DC
2. Shipments are queued when all trucks are busy
3. Queued shipments are dispatched when trucks become available
4. No synthetic truck IDs are generated
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from retail_datagen.generators.retail_patterns import InventoryFlowSimulator


class TestNoTrucksConfigured:
    """Tests for Case 1: No trucks configured for DC."""

    @pytest.fixture
    def mock_inventory_simulator_no_trucks(self):
        """Create InventoryFlowSimulator with no trucks."""
        sim = InventoryFlowSimulator.__new__(InventoryFlowSimulator)
        sim._truck_capacity = 1000
        sim._active_shipments = {}
        sim._in_transit_inventory = {}
        sim._truck_availability = {}
        sim._shipment_queue = []
        sim._trucks_by_dc = {}  # No trucks configured
        sim._truck_rr_index = {}
        sim._rng = MagicMock()
        sim._rng.randint = lambda a, b: (a + b) // 2
        sim.distribution_centers = []
        sim.trucks = []
        sim.get_dc_capacity_multiplier = lambda dc_id, time: 1.0
        return sim

    def test_raises_error_when_no_trucks_configured(
        self, mock_inventory_simulator_no_trucks
    ):
        """Test that ValueError is raised when no trucks exist for DC."""
        sim = mock_inventory_simulator_no_trucks
        dc_id = 1
        current_time = datetime(2024, 1, 15, 8, 0)

        # Should raise ValueError with clear message
        with pytest.raises(
            ValueError,
            match=r"No trucks configured for DC 1.*truck master data",
        ):
            sim._select_truck_for_shipment(dc_id, current_time)

    def test_error_message_includes_dc_id(self, mock_inventory_simulator_no_trucks):
        """Test that error message includes the specific DC ID."""
        sim = mock_inventory_simulator_no_trucks
        dc_id = 5
        current_time = datetime(2024, 1, 15, 8, 0)

        with pytest.raises(ValueError) as exc_info:
            sim._select_truck_for_shipment(dc_id, current_time)

        assert "DC 5" in str(exc_info.value)

    def test_no_synthetic_truck_id_generated_in_generate_shipment(
        self, mock_inventory_simulator_no_trucks
    ):
        """
        Test generate_truck_shipment raises error instead of synthetic ID.
        """
        sim = mock_inventory_simulator_no_trucks
        dc_id = 1
        store_id = 101
        reorder_list = [(1, 100)]
        departure_time = datetime(2024, 1, 15, 8, 0)

        # Should raise ValueError instead of generating synthetic truck ID
        with pytest.raises(ValueError, match="No trucks configured"):
            sim.generate_truck_shipment(dc_id, store_id, reorder_list, departure_time)


class TestAllTrucksBusy:
    """Tests for Case 2: All trucks are busy."""

    @pytest.fixture
    def mock_inventory_simulator_with_busy_trucks(self):
        """Create InventoryFlowSimulator with all trucks busy."""
        sim = InventoryFlowSimulator.__new__(InventoryFlowSimulator)
        sim._truck_capacity = 1000
        sim._active_shipments = {}
        sim._in_transit_inventory = {}
        sim._shipment_queue = []
        sim._dc_inventory = {}
        sim._store_inventory = {}
        sim._rng = MagicMock()
        sim._rng.randint = lambda a, b: (a + b) // 2
        sim.get_dc_capacity_multiplier = lambda dc_id, time: 1.0

        # Configure trucks - all busy until future time
        future_time = datetime(2024, 1, 15, 16, 0)  # 4 PM
        sim._trucks_by_dc = {1: [101, 102, 103]}  # 3 trucks for DC 1
        sim._truck_rr_index = {1: 0}
        sim._truck_availability = {
            101: future_time,
            102: future_time,
            103: future_time,
        }

        return sim

    def test_shipment_queued_when_all_trucks_busy(
        self, mock_inventory_simulator_with_busy_trucks
    ):
        """Test that shipment is queued when all trucks are busy."""
        sim = mock_inventory_simulator_with_busy_trucks
        dc_id = 1
        store_id = 201
        reorder_list = [(1, 50), (2, 75)]
        departure_time = datetime(2024, 1, 15, 8, 0)  # 8 AM (trucks busy until 4 PM)

        # Should return None and add to queue
        result = sim.generate_truck_shipment(
            dc_id, store_id, reorder_list, departure_time
        )

        assert result is None, "Should return None when shipment is queued"
        assert len(sim._shipment_queue) == 1, "Should have one queued shipment"

        # Verify queued shipment details
        queued = sim._shipment_queue[0]
        assert queued["dc_id"] == dc_id
        assert queued["store_id"] == store_id
        assert queued["reorder_list"] == reorder_list
        assert queued["requested_departure"] == departure_time

    def test_no_synthetic_truck_id_when_busy(
        self, mock_inventory_simulator_with_busy_trucks
    ):
        """Test that no synthetic truck ID is generated when trucks are busy."""
        sim = mock_inventory_simulator_with_busy_trucks
        dc_id = 1
        store_id = 201
        reorder_list = [(1, 50)]
        departure_time = datetime(2024, 1, 15, 8, 0)

        result = sim.generate_truck_shipment(
            dc_id, store_id, reorder_list, departure_time
        )

        # Should be queued, not dispatched with synthetic truck
        assert result is None
        assert len(sim._active_shipments) == 0, "No shipments should be active"

    def test_multiple_shipments_queued_in_order(
        self, mock_inventory_simulator_with_busy_trucks
    ):
        """Test that multiple shipments are queued in FIFO order."""
        sim = mock_inventory_simulator_with_busy_trucks
        dc_id = 1

        # Queue three shipments at different times
        shipment1 = {
            "store_id": 201,
            "reorder_list": [(1, 10)],
            "departure_time": datetime(2024, 1, 15, 8, 0),
        }
        shipment2 = {
            "store_id": 202,
            "reorder_list": [(2, 20)],
            "departure_time": datetime(2024, 1, 15, 9, 0),
        }
        shipment3 = {
            "store_id": 203,
            "reorder_list": [(3, 30)],
            "departure_time": datetime(2024, 1, 15, 7, 0),  # Earlier time
        }

        sim.generate_truck_shipment(
            dc_id,
            shipment1["store_id"],
            shipment1["reorder_list"],
            shipment1["departure_time"],
        )
        sim.generate_truck_shipment(
            dc_id,
            shipment2["store_id"],
            shipment2["reorder_list"],
            shipment2["departure_time"],
        )
        sim.generate_truck_shipment(
            dc_id,
            shipment3["store_id"],
            shipment3["reorder_list"],
            shipment3["departure_time"],
        )

        assert len(sim._shipment_queue) == 3


class TestPendingShipmentProcessing:
    """Tests for processing queued shipments when trucks become available."""

    @pytest.fixture
    def mock_inventory_simulator_with_queue(self):
        """Create InventoryFlowSimulator with queued shipments."""
        sim = InventoryFlowSimulator.__new__(InventoryFlowSimulator)
        sim._truck_capacity = 1000
        sim._active_shipments = {}
        sim._in_transit_inventory = {}
        sim._shipment_queue = []
        sim._dc_inventory = {}
        sim._store_inventory = {}
        sim._rng = MagicMock()
        sim._rng.randint = lambda a, b: (a + b) // 2
        sim.get_dc_capacity_multiplier = lambda dc_id, time: 1.0

        # Configure trucks
        sim._trucks_by_dc = {1: [101, 102]}
        sim._truck_rr_index = {1: 0}
        sim._truck_availability = {
            101: datetime(2024, 1, 15, 10, 0),  # Available at 10 AM
            102: datetime(2024, 1, 15, 12, 0),  # Available at noon
        }

        return sim

    def test_process_shipment_queue_dispatches_when_truck_available(
        self, mock_inventory_simulator_with_queue
    ):
        """Test that queued shipments are dispatched when trucks become available."""
        sim = mock_inventory_simulator_with_queue

        # Add shipments to queue
        sim._shipment_queue = [
            {
                "dc_id": 1,
                "store_id": 201,
                "reorder_list": [(1, 50)],
                "requested_departure": datetime(2024, 1, 15, 8, 0),
            },
            {
                "dc_id": 1,
                "store_id": 202,
                "reorder_list": [(2, 75)],
                "requested_departure": datetime(2024, 1, 15, 8, 30),
            },
        ]

        # Process queue at 10:30 AM (truck 101 available since 10 AM)
        current_time = datetime(2024, 1, 15, 10, 30)
        dispatched = sim._process_shipment_queue(current_time)

        # Should dispatch first shipment
        assert len(dispatched) == 1, "Should dispatch one shipment"
        assert dispatched[0]["store_id"] == 201, "Should dispatch first queued shipment"
        assert dispatched[0]["truck_id"] == 101, "Should use available truck"

    def test_process_pending_respects_fifo_order(
        self, mock_inventory_simulator_with_queue
    ):
        """Test that pending shipments are processed in FIFO order by requested time."""
        sim = mock_inventory_simulator_with_queue

        # Add shipments to queue (not in time order)
        sim._shipment_queue = [
            {
                "dc_id": 1,
                "store_id": 202,
                "reorder_list": [(2, 75)],
                "requested_departure": datetime(2024, 1, 15, 9, 0),  # Later
            },
            {
                "dc_id": 1,
                "store_id": 201,
                "reorder_list": [(1, 50)],
                "requested_departure": datetime(2024, 1, 15, 8, 0),  # Earlier
            },
        ]

        current_time = datetime(2024, 1, 15, 10, 30)
        dispatched = sim._process_shipment_queue(current_time)

        # Should dispatch the one with earlier requested_departure first
        assert dispatched[0]["store_id"] == 201, "Should process earlier request first"

    def test_remaining_shipments_stay_in_queue(
        self, mock_inventory_simulator_with_queue
    ):
        """Test that shipments remain queued if no trucks available."""
        sim = mock_inventory_simulator_with_queue

        # Add shipments to queue
        sim._shipment_queue = [
            {
                "dc_id": 1,
                "store_id": 201,
                "reorder_list": [(1, 50)],
                "requested_departure": datetime(2024, 1, 15, 8, 0),
            },
            {
                "dc_id": 1,
                "store_id": 202,
                "reorder_list": [(2, 75)],
                "requested_departure": datetime(2024, 1, 15, 8, 30),
            },
        ]

        # Process queue at 9 AM (all trucks still busy)
        current_time = datetime(2024, 1, 15, 9, 0)
        dispatched = sim._process_shipment_queue(current_time)

        # Nothing should be dispatched, all should remain queued
        assert len(dispatched) == 0, "Should not dispatch anything"
        assert len(sim._shipment_queue) == 2, "Both shipments should remain queued"

    def test_partial_queue_processing(self, mock_inventory_simulator_with_queue):
        """Test processing when only some trucks become available."""
        sim = mock_inventory_simulator_with_queue

        # Add 3 shipments to queue
        sim._shipment_queue = [
            {
                "dc_id": 1,
                "store_id": 201,
                "reorder_list": [(1, 50)],
                "requested_departure": datetime(2024, 1, 15, 8, 0),
            },
            {
                "dc_id": 1,
                "store_id": 202,
                "reorder_list": [(2, 75)],
                "requested_departure": datetime(2024, 1, 15, 8, 10),
            },
            {
                "dc_id": 1,
                "store_id": 203,
                "reorder_list": [(3, 100)],
                "requested_departure": datetime(2024, 1, 15, 8, 20),
            },
        ]

        # Process at 10:30 (only truck 101 available, 102 busy until noon)
        current_time = datetime(2024, 1, 15, 10, 30)
        dispatched = sim._process_shipment_queue(current_time)

        # Should dispatch first shipment only (one truck available)
        assert len(dispatched) == 1, "Should dispatch one shipment"
        assert len(sim._shipment_queue) == 2, "Two shipments should remain queued"


class TestIntegration:
    """Integration tests for the complete flow."""

    @pytest.fixture
    def mock_inventory_simulator(self):
        """Create a realistic InventoryFlowSimulator."""
        sim = InventoryFlowSimulator.__new__(InventoryFlowSimulator)
        sim._truck_capacity = 1000
        sim._active_shipments = {}
        sim._in_transit_inventory = {}
        sim._shipment_queue = []
        sim._dc_inventory = {(1, 1): 500, (1, 2): 300}
        sim._store_inventory = {(201, 1): 10, (201, 2): 5}
        sim._rng = MagicMock()
        sim._rng.randint = lambda a, b: (a + b) // 2
        sim.get_dc_capacity_multiplier = lambda dc_id, time: 1.0

        # Two trucks: one available, one busy
        sim._trucks_by_dc = {1: [101, 102]}
        sim._truck_rr_index = {1: 0}
        sim._truck_availability = {
            101: datetime.min,  # Available now
            102: datetime(2024, 1, 15, 14, 0),  # Busy until 2 PM
        }

        return sim

    def test_first_shipment_dispatched_second_queued(self, mock_inventory_simulator):
        """Test that first shipment uses available truck, second is queued."""
        sim = mock_inventory_simulator
        dc_id = 1
        departure_time = datetime(2024, 1, 15, 10, 0)

        # First shipment - should use truck 101
        result1 = sim.generate_truck_shipment(dc_id, 201, [(1, 50)], departure_time)
        assert result1 is not None, "First shipment should be dispatched"
        assert result1["truck_id"] == 101, "Should use truck 101"

        # Second shipment - truck 102 busy, should queue
        result2 = sim.generate_truck_shipment(
            dc_id, 202, [(2, 75)], departure_time + timedelta(minutes=30)
        )
        assert result2 is None, "Second shipment should be queued"
        assert len(sim._shipment_queue) == 1, "Should have one queued shipment"

    def test_queued_shipment_dispatched_after_truck_returns(
        self, mock_inventory_simulator
    ):
        """Test that queued shipment is dispatched when truck returns."""
        sim = mock_inventory_simulator
        dc_id = 1

        # Queue a shipment (all trucks busy initially)
        sim._truck_availability = {
            101: datetime(2024, 1, 15, 12, 0),
            102: datetime(2024, 1, 15, 14, 0),
        }

        shipment_time = datetime(2024, 1, 15, 10, 0)
        result = sim.generate_truck_shipment(dc_id, 201, [(1, 50)], shipment_time)
        assert result is None, "Should be queued"
        assert len(sim._shipment_queue) == 1

        # Process queue after truck 101 returns
        current_time = datetime(2024, 1, 15, 12, 30)
        dispatched = sim._process_shipment_queue(current_time)

        assert len(dispatched) == 1, "Should dispatch queued shipment"
        assert dispatched[0]["truck_id"] == 101, "Should use returned truck"
        assert len(sim._shipment_queue) == 0, "Queue should be empty"

    def test_no_synthetic_truck_ids_in_active_shipments(self, mock_inventory_simulator):
        """Test that active shipments never contain synthetic truck IDs."""
        sim = mock_inventory_simulator
        dc_id = 1

        # Dispatch shipment
        result = sim.generate_truck_shipment(
            dc_id, 201, [(1, 50)], datetime(2024, 1, 15, 10, 0)
        )

        if result is not None:
            # If dispatched, truck_id must be integer
            assert isinstance(result["truck_id"], int), "Truck ID must be integer"
            assert not isinstance(result["truck_id"], str), (
                "Truck ID must not be string (synthetic)"
            )

        # Check all active shipments
        for shipment in sim._active_shipments.values():
            assert isinstance(shipment["truck_id"], int), (
                "All active shipments must have real truck IDs"
            )
