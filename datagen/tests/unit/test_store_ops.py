"""
Unit tests for store operations fact generation.
"""

from datetime import datetime

import pytest

from retail_datagen.generators.fact_generators.store_ops_mixin import StoreOpsMixin
from retail_datagen.shared.models import Store


class MockStoreOpsGenerator(StoreOpsMixin):
    """Mock generator for testing store ops mixin."""

    def __init__(self):
        self._trace_counter = 1

    def _generate_trace_id(self):
        """Generate a simple trace ID for testing."""
        trace_id = f"TR_{self._trace_counter:06d}"
        self._trace_counter += 1
        return trace_id


class TestStoreOpsMixin:
    """Test suite for StoreOpsMixin."""

    @pytest.fixture
    def generator(self):
        """Create a mock generator with StoreOpsMixin."""
        return MockStoreOpsGenerator()

    @pytest.fixture
    def sample_store(self):
        """Create a sample store for testing."""
        return Store(
            ID=1,
            StoreNumber="S001",
            Address="123 Main St",
            GeographyID=1,
            operating_hours="8am-10pm",
        )

    def test_parse_operating_hours_standard(self, generator):
        """Test parsing standard operating hours format."""
        open_hour, close_hour = generator._parse_operating_hours("8am-10pm")
        assert open_hour == 8
        assert close_hour == 22

    def test_parse_operating_hours_24_7(self, generator):
        """Test parsing 24/7 operating hours."""
        open_hour, close_hour = generator._parse_operating_hours("24/7")
        assert open_hour == 0
        assert close_hour == 24

    def test_parse_operating_hours_extended(self, generator):
        """Test parsing extended operating hours."""
        open_hour, close_hour = generator._parse_operating_hours("6am-midnight")
        assert open_hour == 6
        assert close_hour == 24

    def test_parse_operating_hours_limited(self, generator):
        """Test parsing limited operating hours."""
        open_hour, close_hour = generator._parse_operating_hours("9am-9pm")
        assert open_hour == 9
        assert close_hour == 21

    def test_parse_operating_hours_none(self, generator):
        """Test parsing None operating hours (should use default)."""
        open_hour, close_hour = generator._parse_operating_hours(None)
        assert open_hour == 8
        assert close_hour == 22

    def test_parse_operating_hours_invalid(self, generator):
        """Test parsing invalid operating hours (should use default)."""
        open_hour, close_hour = generator._parse_operating_hours("invalid format")
        assert open_hour == 8
        assert close_hour == 22

    @pytest.mark.parametrize(
        "hours_str,expected_open,expected_close",
        [
            ("10pm-2am", 22, 26),  # 10pm to 2am next day
            ("11pm-1am", 23, 25),  # 11pm to 1am next day
            ("8pm-6am", 20, 30),   # 8pm to 6am next day (overnight shift)
            ("6pm-midnight", 18, 24),  # 6pm to midnight (should not add 24)
        ],
    )
    def test_parse_operating_hours_late_night(
        self, generator, hours_str, expected_open, expected_close
    ):
        """Test parsing late-night store hours that span midnight."""
        # This tests the fix for issue #96
        open_hour, close_hour = generator._parse_operating_hours(hours_str)
        assert open_hour == expected_open
        assert close_hour == expected_close

    def test_generate_store_operations_for_day(self, generator, sample_store):
        """Test generating store operations for a single day."""
        test_date = datetime(2024, 1, 15, 0, 0, 0)
        operations = generator._generate_store_operations_for_day(
            sample_store, test_date
        )

        # Should have 2 operations: opened and closed
        assert len(operations) == 2

        # Check opened event
        opened = operations[0]
        assert opened["store_id"] == 1
        assert opened["operation_type"] == "opened"
        assert opened["operation_time"].hour == 8
        assert opened["operation_time"].minute == 0
        assert "trace_id" in opened

        # Check closed event
        closed = operations[1]
        assert closed["store_id"] == 1
        assert closed["operation_type"] == "closed"
        assert closed["operation_time"].hour == 22
        assert closed["operation_time"].minute == 0
        assert "trace_id" in closed

    def test_generate_store_operations_christmas(self, generator, sample_store):
        """Test that stores are closed on Christmas Day."""
        christmas = datetime(2024, 12, 25, 0, 0, 0)
        operations = generator._generate_store_operations_for_day(
            sample_store, christmas
        )

        # Should have no operations on Christmas
        assert len(operations) == 0

    def test_generate_store_operations_24_7_store(self, generator):
        """Test generating operations for a 24/7 store."""
        store_24_7 = Store(
            ID=2,
            StoreNumber="S002",
            Address="456 Oak Ave",
            GeographyID=2,
            operating_hours="24/7",
        )

        test_date = datetime(2024, 1, 15, 0, 0, 0)
        operations = generator._generate_store_operations_for_day(store_24_7, test_date)

        # 24/7 stores should still have open/close events
        assert len(operations) == 2

        opened = operations[0]
        assert opened["operation_time"].hour == 0

        closed = operations[1]
        # Midnight close should be 23:59:59
        assert closed["operation_time"].hour == 23
        assert closed["operation_time"].minute == 59

    def test_generate_store_operations_different_dates(self, generator, sample_store):
        """Test generating operations for different dates."""
        date1 = datetime(2024, 1, 15, 0, 0, 0)
        date2 = datetime(2024, 1, 16, 0, 0, 0)

        ops1 = generator._generate_store_operations_for_day(sample_store, date1)
        ops2 = generator._generate_store_operations_for_day(sample_store, date2)

        # Should generate operations for both days
        assert len(ops1) == 2
        assert len(ops2) == 2

        # Dates should be different
        assert ops1[0]["operation_time"].day == 15
        assert ops2[0]["operation_time"].day == 16

    def test_trace_ids_are_unique(self, generator, sample_store):
        """Test that trace IDs are unique across operations."""
        test_date = datetime(2024, 1, 15, 0, 0, 0)
        operations = generator._generate_store_operations_for_day(
            sample_store, test_date
        )

        trace_ids = [op["trace_id"] for op in operations]
        # All trace IDs should be unique
        assert len(trace_ids) == len(set(trace_ids))
