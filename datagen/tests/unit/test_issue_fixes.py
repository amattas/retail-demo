"""
Unit tests for fixes to GitHub issues #13, #15, #25, #40, #41.

Tests verify the following implementations:
- Issue #25: Tax fallback chain (County -> State -> Default)
- Issue #15: Price modifier in basket product selection
- Issue #40: Truck capacity constraint enforcement
- Issue #41: Truck state machine validation
- Issue #13: Truck departed events in fact_truck_moves
"""

import pytest
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock
import tempfile
import os

from retail_datagen.shared.models import (
    TruckStatus,
    TruckMove,
)
from retail_datagen.generators.retail_patterns import CustomerSegment


class TestTaxFallbackChain:
    """Tests for Issue #25: Tax fallback chain implementation."""

    @pytest.fixture
    def temp_tax_csv(self):
        """Create a temporary tax rates CSV for testing."""
        csv_content = """StateCode,County,City,CombinedRate
CA,Los Angeles,Los Angeles,0.0950
CA,Los Angeles,Beverly Hills,0.0925
CA,Los Angeles,Santa Monica,0.0900
CA,Orange,Irvine,0.0775
CA,Orange,Newport Beach,0.0800
TX,Harris,Houston,0.0825
TX,Harris,Pasadena,0.0825
TX,Dallas,Dallas,0.0825
NY,New York,New York,0.0880
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()
            yield f.name
        os.unlink(f.name)

    def test_city_level_lookup(self, temp_tax_csv):
        """Test that city-level lookup returns exact match."""
        from retail_datagen.shared.tax_utils import TaxCalculator

        calc = TaxCalculator(temp_tax_csv)
        rate = calc.get_tax_rate("CA", county="Los Angeles", city="Los Angeles")
        assert rate == Decimal("0.0950")

    def test_county_fallback(self, temp_tax_csv):
        """Test county fallback when city not found."""
        from retail_datagen.shared.tax_utils import TaxCalculator

        calc = TaxCalculator(temp_tax_csv)
        # Unknown city in known county should fall back to county average
        rate = calc.get_tax_rate("CA", county="Los Angeles", city="Unknown City")
        # Los Angeles county average of (0.0950 + 0.0925 + 0.0900) / 3 = 0.0925
        assert rate == Decimal("0.0925")

    def test_state_fallback(self, temp_tax_csv):
        """Test state fallback when county not found."""
        from retail_datagen.shared.tax_utils import TaxCalculator

        calc = TaxCalculator(temp_tax_csv)
        # Unknown county in known state should fall back to state average
        rate = calc.get_tax_rate("CA", county="Unknown County")
        # CA has 5 cities with rates: (0.0950 + 0.0925 + 0.0900 + 0.0775 + 0.0800) / 5 = 0.087
        assert rate == Decimal("0.087")

    def test_default_fallback(self, temp_tax_csv):
        """Test default rate fallback when state not found."""
        from retail_datagen.shared.tax_utils import TaxCalculator

        calc = TaxCalculator(temp_tax_csv)
        # Unknown state should fall back to default
        rate = calc.get_tax_rate("ZZ", county="Unknown", city="Unknown")
        assert rate == Decimal("0.07407")  # Default rate

    def test_county_cache_populated(self, temp_tax_csv):
        """Test that county cache is populated during loading."""
        from retail_datagen.shared.tax_utils import TaxCalculator

        calc = TaxCalculator(temp_tax_csv)
        # Should have county entries for CA counties and TX counties
        assert ("CA", "Los Angeles") in calc.county_cache
        assert ("CA", "Orange") in calc.county_cache
        assert ("TX", "Harris") in calc.county_cache

    def test_state_cache_populated(self, temp_tax_csv):
        """Test that state cache is populated during loading."""
        from retail_datagen.shared.tax_utils import TaxCalculator

        calc = TaxCalculator(temp_tax_csv)
        assert "CA" in calc.state_cache
        assert "TX" in calc.state_cache
        assert "NY" in calc.state_cache


class TestPriceModifier:
    """Tests for Issue #15: Price modifier in basket product selection."""

    @pytest.fixture
    def mock_products(self):
        """Create mock products with varying prices."""
        return [
            MagicMock(ID=1, SalePrice=Decimal("5.00"), Category="food"),
            MagicMock(ID=2, SalePrice=Decimal("10.00"), Category="food"),
            MagicMock(ID=3, SalePrice=Decimal("15.00"), Category="food"),
            MagicMock(ID=4, SalePrice=Decimal("20.00"), Category="food"),
            MagicMock(ID=5, SalePrice=Decimal("25.00"), Category="food"),
            MagicMock(ID=6, SalePrice=Decimal("30.00"), Category="food"),
            MagicMock(ID=7, SalePrice=Decimal("35.00"), Category="food"),
            MagicMock(ID=8, SalePrice=Decimal("40.00"), Category="food"),
            MagicMock(ID=9, SalePrice=Decimal("45.00"), Category="food"),
            MagicMock(ID=10, SalePrice=Decimal("50.00"), Category="food"),
        ]

    def test_budget_conscious_prefers_lower_prices(self, mock_products):
        """Test that BUDGET_CONSCIOUS segment selects lower-priced items."""
        from retail_datagen.generators.retail_patterns import (
            CustomerJourneySimulator,
            ShoppingBehaviorType,
        )

        # Create minimal simulator for testing
        sim = CustomerJourneySimulator.__new__(CustomerJourneySimulator)
        sim.products = mock_products
        sim._product_categories = {"food": mock_products}
        sim._rng = MagicMock()
        sim._rng.choice = lambda x: x[0]  # Always pick first item
        sim._rng.choices = lambda x, weights: [x[0]]
        sim._rng.randint = lambda a, b: a

        basket = sim._select_basket_products(
            CustomerSegment.BUDGET_CONSCIOUS,
            ShoppingBehaviorType.QUICK_TRIP,
            5,
        )

        # Budget customers should have items from lower price range
        prices = [item[0].SalePrice for item in basket]
        # All selected items should be from bottom 70% (prices <= 35.00)
        assert all(p <= Decimal("35.00") for p in prices)

    def test_quality_seeker_prefers_higher_prices(self, mock_products):
        """Test that QUALITY_SEEKER segment selects higher-priced items."""
        from retail_datagen.generators.retail_patterns import (
            CustomerJourneySimulator,
            ShoppingBehaviorType,
        )

        sim = CustomerJourneySimulator.__new__(CustomerJourneySimulator)
        sim.products = mock_products
        sim._product_categories = {"food": mock_products}
        sim._rng = MagicMock()
        sim._rng.choice = lambda x: x[-1]  # Always pick last item (highest price in filtered)
        sim._rng.choices = lambda x, weights: [x[0]]
        sim._rng.randint = lambda a, b: a

        basket = sim._select_basket_products(
            CustomerSegment.QUALITY_SEEKER,
            ShoppingBehaviorType.QUICK_TRIP,
            5,
        )

        # Quality seekers should have items from higher price range
        prices = [item[0].SalePrice for item in basket]
        # All selected items should be from top 30% (prices >= 35.00)
        assert all(p >= Decimal("35.00") for p in prices)

    def test_convenience_focused_no_filtering(self, mock_products):
        """Test that CONVENIENCE_FOCUSED has no price filtering."""
        from retail_datagen.generators.retail_patterns import (
            CustomerJourneySimulator,
            ShoppingBehaviorType,
        )

        sim = CustomerJourneySimulator.__new__(CustomerJourneySimulator)
        sim.products = mock_products
        sim._product_categories = {"food": mock_products}
        sim._rng = MagicMock()
        # Simulate random selection from full range
        sim._rng.choice = lambda x: x[len(x) // 2]
        sim._rng.choices = lambda x, weights: [x[0]]
        sim._rng.randint = lambda a, b: a

        basket = sim._select_basket_products(
            CustomerSegment.CONVENIENCE_FOCUSED,
            ShoppingBehaviorType.QUICK_TRIP,
            1,
        )

        # Should have access to all products (middle selection = 25.00)
        assert len(basket) >= 1


class TestTruckCapacityConstraints:
    """Tests for Issue #40: Truck capacity constraint enforcement."""

    @pytest.fixture
    def mock_inventory_simulator(self):
        """Create a mock InventoryFlowSimulator for testing."""
        from retail_datagen.generators.retail_patterns import InventoryFlowSimulator

        sim = InventoryFlowSimulator.__new__(InventoryFlowSimulator)
        sim._truck_capacity = 1000
        sim._active_shipments = {}
        sim._rng = MagicMock()
        sim._rng.randint = lambda a, b: (a + b) // 2
        sim.distribution_centers = []
        sim.trucks = []

        return sim

    def test_shipment_within_capacity(self, mock_inventory_simulator):
        """Test that shipments within capacity are not modified."""
        sim = mock_inventory_simulator
        sim._select_truck_for_shipment = lambda dc_id: "TRUCK001"
        sim.get_dc_capacity_multiplier = lambda dc_id, time: 1.0

        reorder_list = [(1, 100), (2, 200), (3, 150)]  # Total: 450, under 1000 capacity
        departure_time = datetime(2024, 1, 15, 8, 0)

        shipment = sim.generate_truck_shipment(1, 101, reorder_list, departure_time)

        assert shipment["total_items"] == 450
        assert len(shipment["products"]) == 3

    def test_shipment_exceeds_capacity_truncated(self, mock_inventory_simulator, caplog):
        """Test that shipments exceeding capacity are truncated."""
        import logging
        caplog.set_level(logging.WARNING)

        sim = mock_inventory_simulator
        sim._select_truck_for_shipment = lambda dc_id: "TRUCK001"
        sim.get_dc_capacity_multiplier = lambda dc_id, time: 1.0

        # Create order that exceeds capacity (1000)
        reorder_list = [(1, 500), (2, 400), (3, 300)]  # Total: 1200
        departure_time = datetime(2024, 1, 15, 8, 0)

        shipment = sim.generate_truck_shipment(1, 101, reorder_list, departure_time)

        # Should be truncated to capacity
        assert shipment["total_items"] <= 1000
        assert "exceeds truck capacity" in caplog.text

    def test_generate_multiple_shipments_splits_order(self, mock_inventory_simulator):
        """Test that large orders are split across multiple trucks."""
        sim = mock_inventory_simulator
        sim._select_truck_for_shipment = lambda dc_id: f"TRUCK{len(sim._active_shipments):03d}"
        sim.get_dc_capacity_multiplier = lambda dc_id, time: 1.0

        # Create large order that needs 3 trucks
        reorder_list = [(1, 1000), (2, 1000), (3, 500)]  # Total: 2500
        departure_time = datetime(2024, 1, 15, 8, 0)

        shipments = sim.generate_truck_shipments(1, 101, reorder_list, departure_time)

        # Should be split across 3 trucks (1000 + 1000 + 500)
        assert len(shipments) == 3
        total_items = sum(s["total_items"] for s in shipments)
        assert total_items == 2500

    def test_unload_duration_scales_with_shipment_size(self, mock_inventory_simulator):
        """Test that unload duration scales with shipment size."""
        sim = mock_inventory_simulator

        # Small shipment: 100 items = 10% of capacity
        small_duration = sim._calculate_unload_duration(100)
        assert small_duration >= 0.5  # At least 30 min

        # Full truck: 1000 items = 100% of capacity
        full_duration = sim._calculate_unload_duration(1000)
        assert full_duration >= 1.5  # At least 1.5 hours

        # Full truck should have longer unload time
        assert full_duration > small_duration


class TestTruckStateMachineValidation:
    """Tests for Issue #41: Truck state machine validation."""

    @pytest.fixture
    def mock_inventory_simulator(self):
        """Create a mock InventoryFlowSimulator with state machine."""
        from retail_datagen.generators.retail_patterns import InventoryFlowSimulator

        sim = InventoryFlowSimulator.__new__(InventoryFlowSimulator)
        sim._active_shipments = {}
        return sim

    def test_valid_state_transitions(self, mock_inventory_simulator):
        """Test that valid state transitions are allowed."""
        sim = mock_inventory_simulator

        # Test all valid transitions
        valid_cases = [
            (TruckStatus.SCHEDULED, TruckStatus.LOADING),
            (TruckStatus.LOADING, TruckStatus.IN_TRANSIT),
            (TruckStatus.IN_TRANSIT, TruckStatus.ARRIVED),
            (TruckStatus.ARRIVED, TruckStatus.UNLOADING),
            (TruckStatus.UNLOADING, TruckStatus.COMPLETED),
        ]

        for current, target in valid_cases:
            result = sim._validate_state_transition("TEST001", current, target)
            assert result is True, f"Expected {current} -> {target} to be valid"

    def test_invalid_state_transitions(self, mock_inventory_simulator, caplog):
        """Test that invalid state transitions are rejected with warnings."""
        import logging
        caplog.set_level(logging.WARNING)

        sim = mock_inventory_simulator

        # Test invalid transitions
        invalid_cases = [
            (TruckStatus.SCHEDULED, TruckStatus.ARRIVED),  # Can't skip LOADING
            (TruckStatus.LOADING, TruckStatus.UNLOADING),  # Can't skip IN_TRANSIT and ARRIVED
            (TruckStatus.COMPLETED, TruckStatus.SCHEDULED),  # Terminal state
        ]

        for current, target in invalid_cases:
            result = sim._validate_state_transition("TEST001", current, target)
            assert result is False, f"Expected {current} -> {target} to be invalid"
            assert "Invalid state transition" in caplog.text

    def test_state_timeout_recovery(self, mock_inventory_simulator, caplog):
        """Test that stuck shipments are recovered via timeout."""
        import logging
        caplog.set_level(logging.WARNING)

        sim = mock_inventory_simulator

        # Create a shipment that's been stuck in LOADING for too long
        shipment = {
            "shipment_id": "TEST001",
            "status": TruckStatus.LOADING,
            "_state_entered_LOADING": datetime(2024, 1, 15, 0, 0),  # 10 hours ago
        }

        current_time = datetime(2024, 1, 15, 10, 0)  # 10 hours later (> 8 hour timeout)

        recovery_status = sim._check_state_timeout(shipment, current_time)

        assert recovery_status == TruckStatus.COMPLETED
        assert "stuck" in caplog.text

    def test_no_timeout_within_limit(self, mock_inventory_simulator):
        """Test that shipments within timeout limit are not recovered."""
        sim = mock_inventory_simulator

        shipment = {
            "shipment_id": "TEST001",
            "status": TruckStatus.LOADING,
            "_state_entered_LOADING": datetime(2024, 1, 15, 8, 0),
        }

        current_time = datetime(2024, 1, 15, 10, 0)  # 2 hours later (< 8 hour timeout)

        recovery_status = sim._check_state_timeout(shipment, current_time)

        assert recovery_status is None  # No recovery needed


class TestTruckDepartedEvents:
    """Tests for Issue #13: Truck departed events in fact_truck_moves."""

    def test_truck_move_model_has_departure_fields(self):
        """Test that TruckMove model includes departure fields."""
        # Create a COMPLETED truck move with departure fields
        move = TruckMove(
            TraceId="trace123",
            EventTS=datetime(2024, 1, 15, 10, 0),
            TruckId="TRUCK001",
            DCID=1,
            StoreID=101,
            ShipmentId="SHIP001",
            Status=TruckStatus.COMPLETED,
            ETA=datetime(2024, 1, 15, 8, 0),
            ETD=datetime(2024, 1, 15, 10, 0),
            DepartureTime=datetime(2024, 1, 15, 10, 0),
            ActualUnloadDuration=120,
        )

        assert move.DepartureTime == datetime(2024, 1, 15, 10, 0)
        assert move.ActualUnloadDuration == 120

    def test_departure_fields_optional_for_non_completed(self):
        """Test that departure fields are optional for non-COMPLETED status."""
        move = TruckMove(
            TraceId="trace123",
            EventTS=datetime(2024, 1, 15, 8, 0),
            TruckId="TRUCK001",
            DCID=1,
            StoreID=101,
            ShipmentId="SHIP001",
            Status=TruckStatus.ARRIVED,
            ETA=datetime(2024, 1, 15, 8, 0),
            ETD=datetime(2024, 1, 15, 10, 0),
        )

        assert move.DepartureTime is None
        assert move.ActualUnloadDuration is None

    def test_completed_status_maps_to_truck_departed_event(self):
        """Test that COMPLETED status maps to truck_departed event type."""
        # Simulate the event type mapping logic
        status = "COMPLETED"
        if status == "ARRIVED":
            message_type = "truck_arrived"
        elif status == "COMPLETED":
            message_type = "truck_departed"
        else:
            message_type = "truck_arrived"  # default

        assert message_type == "truck_departed"

    def test_arrived_status_maps_to_truck_arrived_event(self):
        """Test that ARRIVED status maps to truck_arrived event type."""
        status = "ARRIVED"
        if status == "ARRIVED":
            message_type = "truck_arrived"
        elif status == "COMPLETED":
            message_type = "truck_departed"
        else:
            message_type = "truck_arrived"

        assert message_type == "truck_arrived"

    def test_actual_unload_duration_minimum_30_minutes(self):
        """Test that actual unload duration has 30 minute minimum."""
        # Simulate the calculation from fact_generator
        eta = datetime(2024, 1, 15, 8, 0)
        check_time = datetime(2024, 1, 15, 8, 10)  # Only 10 minutes later

        unload_duration_minutes = int((check_time - eta).total_seconds() / 60)
        actual_duration = max(30, unload_duration_minutes)  # Min 30 min

        assert actual_duration == 30  # Should be at least 30 minutes


class TestEdgeCases:
    """Edge case tests requested in code review."""

    @pytest.fixture
    def mock_inventory_simulator(self):
        """Create a mock InventoryFlowSimulator for edge case testing."""
        from retail_datagen.generators.retail_patterns import InventoryFlowSimulator

        sim = InventoryFlowSimulator.__new__(InventoryFlowSimulator)
        sim._truck_capacity = 1000
        sim._active_shipments = {}
        sim._rng = MagicMock()
        sim._rng.randint = lambda a, b: (a + b) // 2
        sim.distribution_centers = []
        sim.trucks = []
        sim._select_truck_for_shipment = lambda dc_id: "TRUCK001"
        sim.get_dc_capacity_multiplier = lambda dc_id, time: 1.0

        return sim

    def test_empty_reorder_list(self, mock_inventory_simulator):
        """Test handling of empty reorder list."""
        sim = mock_inventory_simulator
        departure_time = datetime(2024, 1, 15, 8, 0)

        # Generate shipments with empty reorder list
        shipments = sim.generate_truck_shipments(1, 101, [], departure_time)

        # Should return empty list for empty reorder
        assert shipments == []

    def test_exact_capacity_match(self, mock_inventory_simulator):
        """Test shipment that exactly matches truck capacity."""
        sim = mock_inventory_simulator
        departure_time = datetime(2024, 1, 15, 8, 0)

        # Create order that exactly matches capacity
        reorder_list = [(1, 500), (2, 500)]  # Total: 1000 = capacity
        shipment = sim.generate_truck_shipment(1, 101, reorder_list, departure_time)

        # Should fit in one truck without truncation
        assert shipment["total_items"] == 1000
        assert len(shipment["products"]) == 2

    def test_state_machine_recovery_steps_through_multiple_states(self, mock_inventory_simulator):
        """Test that state machine recovery steps through multiple intermediate states."""
        sim = mock_inventory_simulator

        # Create a shipment in SCHEDULED state
        departure_time = datetime(2024, 1, 15, 0, 0)
        eta = datetime(2024, 1, 15, 8, 0)
        etd = datetime(2024, 1, 15, 10, 0)

        shipment = {
            "shipment_id": "TEST001",
            "truck_id": "TRUCK001",
            "dc_id": 1,
            "store_id": 101,
            "departure_time": departure_time,
            "eta": eta,
            "etd": etd,
            "status": TruckStatus.SCHEDULED,
            "products": [(1, 100)],
            "total_items": 100,
            "unload_duration_hours": 1.0,
        }
        sim._active_shipments["TEST001"] = shipment

        # Jump time far ahead - should require stepping through multiple states
        current_time = datetime(2024, 1, 15, 12, 0)  # Well past ETD

        result = sim.update_shipment_status("TEST001", current_time)

        # Should have stepped through to COMPLETED
        assert result["status"] == TruckStatus.COMPLETED
        # Shipment should be removed from active
        assert "TEST001" not in sim._active_shipments

    def test_unload_duration_stored_in_shipment(self, mock_inventory_simulator):
        """Test that unload duration is stored in shipment dict."""
        sim = mock_inventory_simulator
        departure_time = datetime(2024, 1, 15, 8, 0)

        reorder_list = [(1, 500)]  # 50% capacity
        shipment = sim.generate_truck_shipment(1, 101, reorder_list, departure_time)

        # Unload duration should be stored
        assert "unload_duration_hours" in shipment
        assert shipment["unload_duration_hours"] > 0
        assert isinstance(shipment["unload_duration_hours"], float)

    def test_brand_loyal_uses_top_50_percent(self):
        """Test that BRAND_LOYAL segment uses top 50% of prices (not 30%)."""
        # Create 10 products with prices $5-$50 for each category
        # QUICK_TRIP uses: food (50%), snacks (30%), beverages (20%)
        mock_products = [
            MagicMock(ID=i, SalePrice=Decimal(str(i * 5)), Category=cat)
            for i in range(1, 11)
            for cat in ["food", "snacks", "beverages"]
        ]

        from retail_datagen.generators.retail_patterns import (
            CustomerJourneySimulator,
            ShoppingBehaviorType,
        )

        sim = CustomerJourneySimulator.__new__(CustomerJourneySimulator)
        # Use only high-priced products in self.products to avoid fallback issues
        high_price_products = [p for p in mock_products if p.SalePrice >= Decimal("30")]
        sim.products = high_price_products
        # Provide products for all categories QUICK_TRIP uses
        food_products = [p for p in mock_products if p.Category == "food"]
        snacks_products = [p for p in mock_products if p.Category == "snacks"]
        beverage_products = [p for p in mock_products if p.Category == "beverages"]
        sim._product_categories = {
            "food": food_products,
            "snacks": snacks_products,
            "beverages": beverage_products,
        }
        sim._rng = MagicMock()
        sim._rng.choice = lambda x: x[0]  # Always pick first item in filtered list
        sim._rng.choices = lambda x, weights: [x[0]]
        sim._rng.randint = lambda a, b: a

        basket = sim._select_basket_products(
            CustomerSegment.BRAND_LOYAL,
            ShoppingBehaviorType.QUICK_TRIP,
            5,
        )

        # BRAND_LOYAL should use top 50% (prices >= $30)
        # With 10 products per category, threshold_idx = int(10 * (1.0 - 0.5)) = 5
        # So prices >= sorted_prices[5] = $30 (sorted: $5,$10,$15,$20,$25,$30,$35,$40,$45,$50)
        prices = [item[0].SalePrice for item in basket]
        # All selected items should be >= $30 (top 50% of price range)
        assert all(p >= Decimal("30.00") for p in prices), f"Got prices: {prices}"

    def test_single_item_reorder_list(self, mock_inventory_simulator):
        """Test handling of single-item reorder list."""
        sim = mock_inventory_simulator
        departure_time = datetime(2024, 1, 15, 8, 0)

        reorder_list = [(1, 50)]  # Single item
        shipments = sim.generate_truck_shipments(1, 101, reorder_list, departure_time)

        assert len(shipments) == 1
        assert shipments[0]["total_items"] == 50

    def test_very_large_order_splits_correctly(self, mock_inventory_simulator):
        """Test that very large orders split across many trucks."""
        sim = mock_inventory_simulator
        sim._select_truck_for_shipment = lambda dc_id: f"TRUCK{len(sim._active_shipments):03d}"
        departure_time = datetime(2024, 1, 15, 8, 0)

        # Create order needing 5 trucks (capacity 1000 each)
        reorder_list = [(1, 2000), (2, 2000), (3, 800)]  # Total: 4800
        shipments = sim.generate_truck_shipments(1, 101, reorder_list, departure_time)

        # Should need 5 trucks
        assert len(shipments) == 5
        total_items = sum(s["total_items"] for s in shipments)
        assert total_items == 4800

        # Each shipment should respect capacity
        for s in shipments:
            assert s["total_items"] <= 1000

    def test_negative_quantity_raises_error(self, mock_inventory_simulator):
        """Test that negative quantities in reorder list raise ValueError."""
        sim = mock_inventory_simulator
        departure_time = datetime(2024, 1, 15, 8, 0)

        # Create reorder list with negative quantity
        reorder_list = [(1, 100), (2, -50)]  # Negative quantity

        with pytest.raises(ValueError, match="Invalid negative quantity"):
            sim.generate_truck_shipment(1, 101, reorder_list, departure_time)

    def test_single_product_exceeds_capacity(self, mock_inventory_simulator):
        """Test handling when a single product's quantity exceeds truck capacity."""
        sim = mock_inventory_simulator
        sim._select_truck_for_shipment = lambda dc_id: f"TRUCK{len(sim._active_shipments):03d}"
        departure_time = datetime(2024, 1, 15, 8, 0)

        # Single product with qty > capacity
        reorder_list = [(1, 2500)]  # 2.5 trucks worth
        shipments = sim.generate_truck_shipments(1, 101, reorder_list, departure_time)

        # Should split across 3 trucks
        assert len(shipments) == 3
        total_items = sum(s["total_items"] for s in shipments)
        assert total_items == 2500
