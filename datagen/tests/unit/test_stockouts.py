"""
Unit tests for stockouts detection and generation.
"""

from datetime import UTC, datetime

from retail_datagen.generators.fact_generators.stockouts_mixin import (
    StockoutsMixin,
)


class MockStockoutsGenerator(StockoutsMixin):
    """Mock class to test StockoutsMixin methods."""

    def __init__(self):
        self._last_stockout_detection = {}
        self._trace_counter = 0

    def _generate_trace_id(self):
        """Generate a simple trace ID for testing."""
        self._trace_counter += 1
        return f"TRACE-{self._trace_counter}"


class TestStockoutsDetection:
    """Test stockout detection logic."""

    def test_detect_stockout_when_balance_zero(self):
        """Test that stockout is detected when balance reaches zero."""
        generator = MockStockoutsGenerator()

        result = generator._detect_and_record_stockout(
            node_type="STORE",
            node_id=1,
            product_id=100,
            last_known_quantity=5,
            detection_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            current_balance=0,
        )

        assert result is not None
        assert result["StoreID"] == 1
        assert result["DCID"] is None
        assert result["ProductID"] == 100
        assert result["LastKnownQuantity"] == 5
        assert result["TraceId"] == "TRACE-1"

    def test_no_stockout_when_balance_positive(self):
        """Test that no stockout is detected when balance is still positive."""
        generator = MockStockoutsGenerator()

        result = generator._detect_and_record_stockout(
            node_type="STORE",
            node_id=1,
            product_id=100,
            last_known_quantity=5,
            detection_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            current_balance=3,
        )

        assert result is None

    def test_duplicate_stockout_prevention(self):
        """Test that duplicate stockouts within 24 hours are prevented."""
        generator = MockStockoutsGenerator()

        # First stockout
        result1 = generator._detect_and_record_stockout(
            node_type="STORE",
            node_id=1,
            product_id=100,
            last_known_quantity=5,
            detection_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            current_balance=0,
        )

        # Second stockout 12 hours later (should be prevented)
        result2 = generator._detect_and_record_stockout(
            node_type="STORE",
            node_id=1,
            product_id=100,
            last_known_quantity=3,
            detection_time=datetime(2024, 1, 1, 22, 0, 0, tzinfo=UTC),
            current_balance=0,
        )

        assert result1 is not None
        assert result2 is None  # Duplicate prevented

    def test_stockout_after_24_hours_allowed(self):
        """Test that stockout is allowed after 24 hours have passed."""
        generator = MockStockoutsGenerator()

        # First stockout
        result1 = generator._detect_and_record_stockout(
            node_type="STORE",
            node_id=1,
            product_id=100,
            last_known_quantity=5,
            detection_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            current_balance=0,
        )

        # Second stockout 25 hours later (should be allowed)
        result2 = generator._detect_and_record_stockout(
            node_type="STORE",
            node_id=1,
            product_id=100,
            last_known_quantity=3,
            detection_time=datetime(2024, 1, 2, 11, 0, 0, tzinfo=UTC),
            current_balance=0,
        )

        assert result1 is not None
        assert result2 is not None

    def test_dc_stockout_detection(self):
        """Test stockout detection for distribution centers."""
        generator = MockStockoutsGenerator()

        result = generator._detect_and_record_stockout(
            node_type="DC",
            node_id=5,
            product_id=200,
            last_known_quantity=10,
            detection_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            current_balance=0,
        )

        assert result is not None
        assert result["DCID"] == 5
        assert result["StoreID"] is None
        assert result["ProductID"] == 200

    def test_generate_stockouts_from_transactions(self):
        """Test generating stockouts from inventory transaction records."""
        generator = MockStockoutsGenerator()

        store_txns = [
            {
                "StoreID": 1,
                "ProductID": 100,
                "Balance": 0,
                "QtyDelta": -5,
                "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            },
            {
                "StoreID": 1,
                "ProductID": 101,
                "Balance": 10,  # Not a stockout
                "QtyDelta": -2,
                "EventTS": datetime(2024, 1, 1, 11, 0, 0, tzinfo=UTC),
            },
            {
                "StoreID": 2,
                "ProductID": 100,
                "Balance": 0,
                "QtyDelta": -3,
                "EventTS": datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            },
        ]

        dc_txns = [
            {
                "DCID": 5,
                "ProductID": 200,
                "Balance": 0,
                "QtyDelta": -8,
                "EventTS": datetime(2024, 1, 1, 13, 0, 0, tzinfo=UTC),
            }
        ]

        stockouts = generator._generate_stockouts_from_inventory_txns(
            store_txns, dc_txns
        )

        # Should detect 3 stockouts: 2 from stores, 1 from DC
        assert len(stockouts) == 3

        # Verify store stockouts
        store_stockouts = [s for s in stockouts if s["StoreID"] is not None]
        assert len(store_stockouts) == 2

        # Verify DC stockout
        dc_stockouts = [s for s in stockouts if s["DCID"] is not None]
        assert len(dc_stockouts) == 1
        assert dc_stockouts[0]["DCID"] == 5

    def test_negative_last_known_quantity_converted_to_positive(self):
        """Test that negative quantity deltas are converted to positive."""
        generator = MockStockoutsGenerator()

        store_txns = [
            {
                "StoreID": 1,
                "ProductID": 100,
                "Balance": 0,
                "QtyDelta": -7,  # Negative delta
                "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            }
        ]

        stockouts = generator._generate_stockouts_from_inventory_txns(store_txns, [])

        assert len(stockouts) == 1
        assert stockouts[0]["LastKnownQuantity"] == 7  # Converted to positive

    def test_zero_delta_yields_zero_last_known_quantity(self):
        """Test that zero delta results in zero last known quantity."""
        generator = MockStockoutsGenerator()

        store_txns = [
            {
                "StoreID": 1,
                "ProductID": 100,
                "Balance": 0,
                "QtyDelta": 0,  # Zero delta
                "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            }
        ]

        stockouts = generator._generate_stockouts_from_inventory_txns(store_txns, [])

        assert len(stockouts) == 1
        assert stockouts[0]["LastKnownQuantity"] == 0
