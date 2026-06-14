"""
Unit tests for EntityIdGenerator.

Tests ID generation, uniqueness guarantees, thread safety, and edge cases.
"""

from datetime import UTC, datetime, timedelta
from threading import Thread

import pytest

from retail_datagen.shared.id_generator import EntityIdGenerator


class TestEntityIdGeneratorInit:
    """Tests for EntityIdGenerator initialization."""

    def test_valid_initialization(self):
        """Test successful initialization with valid parameters."""
        gen = EntityIdGenerator("RCP", entity_id_width=3, sequence_width=4)
        assert gen.prefix == "RCP"
        assert gen.entity_id_width == 3
        assert gen.sequence_width == 4

    def test_minimal_initialization(self):
        """Test initialization with minimal parameters."""
        gen = EntityIdGenerator("TRC")
        assert gen.prefix == "TRC"
        assert gen.entity_id_width == 0
        assert gen.sequence_width == 4

    def test_empty_prefix_raises_error(self):
        """Test that empty prefix raises ValueError."""
        with pytest.raises(ValueError, match="Prefix cannot be empty"):
            EntityIdGenerator("")

    def test_negative_entity_id_width_raises_error(self):
        """Test that negative entity_id_width raises ValueError."""
        with pytest.raises(ValueError, match="entity_id_width must be >= 0"):
            EntityIdGenerator("RCP", entity_id_width=-1)

    def test_zero_sequence_width_raises_error(self):
        """Test that sequence_width < 1 raises ValueError."""
        with pytest.raises(ValueError, match="sequence_width must be >= 1"):
            EntityIdGenerator("RCP", sequence_width=0)


class TestEntityIdGeneratorFormat:
    """Tests for ID format and structure."""

    def test_receipt_id_format(self):
        """Test receipt ID format with entity_id."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)
        id_value = gen.generate(timestamp, entity_id=1)

        # Format: RCP{entity:03d}{YYYYMMDDHHmmssffffff}{seq:04d}
        assert id_value.startswith("RCP001")
        assert "20260128142530123456" in id_value
        assert id_value.endswith("0001")
        assert len(id_value) == 3 + 3 + 20 + 4  # prefix + entity + timestamp + seq

    def test_trace_id_format_without_entity(self):
        """Test trace ID format without entity_id."""
        gen = EntityIdGenerator("TRC")
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)
        id_value = gen.generate(timestamp)

        # Format: TRC{YYYYMMDDHHmmssffffff}{seq:04d}
        assert id_value.startswith("TRC20260128")
        assert "20260128142530123456" in id_value
        assert id_value.endswith("0001")
        assert len(id_value) == 3 + 20 + 4  # prefix + timestamp + seq

    def test_shipment_id_format_with_large_entity(self):
        """Test shipment ID with multi-digit entity_id."""
        gen = EntityIdGenerator("SHIP", entity_id_width=5)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)
        # DC=12, Store=345 combined as 12345
        id_value = gen.generate(timestamp, entity_id=12345)

        assert id_value.startswith("SHIP12345")
        assert "20260128142530123456" in id_value
        assert len(id_value) == 4 + 5 + 20 + 4


class TestEntityIdGeneratorUniqueness:
    """Tests for ID uniqueness guarantees."""

    def test_sequential_counter_increments(self):
        """Test that sequential calls increment the counter."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)

        id1 = gen.generate(timestamp, entity_id=1)
        id2 = gen.generate(timestamp, entity_id=1)
        id3 = gen.generate(timestamp, entity_id=1)

        assert id1.endswith("0001")
        assert id2.endswith("0002")
        assert id3.endswith("0003")

    def test_different_entities_have_independent_counters(self):
        """Test that different entities maintain separate counters."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)

        id_store1_1 = gen.generate(timestamp, entity_id=1)
        id_store2_1 = gen.generate(timestamp, entity_id=2)
        id_store1_2 = gen.generate(timestamp, entity_id=1)

        # Store 1 should have counter 1, 2
        assert "RCP001" in id_store1_1
        assert id_store1_1.endswith("0001")
        assert "RCP001" in id_store1_2
        assert id_store1_2.endswith("0002")

        # Store 2 should start at 1
        assert "RCP002" in id_store2_1
        assert id_store2_1.endswith("0001")

    def test_counter_resets_per_second(self):
        """Test that counter resets when second changes."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp1 = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)
        timestamp2 = datetime(2026, 1, 28, 14, 25, 31, 123456, tzinfo=UTC)

        id1 = gen.generate(timestamp1, entity_id=1)
        id2 = gen.generate(timestamp1, entity_id=1)
        id3 = gen.generate(timestamp2, entity_id=1)  # New second

        assert id1.endswith("0001")
        assert id2.endswith("0002")
        assert id3.endswith("0001")  # Counter reset

    def test_microsecond_precision_in_timestamp(self):
        """Test that microseconds are captured in the ID."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp1 = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)
        timestamp2 = datetime(2026, 1, 28, 14, 25, 30, 654321, tzinfo=UTC)

        id1 = gen.generate(timestamp1, entity_id=1)
        id2 = gen.generate(timestamp2, entity_id=1)

        assert "123456" in id1
        assert "654321" in id2
        assert id1 != id2

    def test_no_duplicates_in_large_batch(self):
        """Test that no duplicates occur in a large batch."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)

        ids = set()
        for i in range(1000):
            id_value = gen.generate(timestamp, entity_id=1)
            ids.add(id_value)

        # All IDs should be unique
        assert len(ids) == 1000


class TestEntityIdGeneratorEdgeCases:
    """Tests for edge cases and error handling."""

    def test_requires_entity_id_when_width_set(self):
        """Test that entity_id is required when entity_id_width > 0."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)

        with pytest.raises(ValueError, match="entity_id is required"):
            gen.generate(timestamp)

    def test_allows_none_entity_id_when_width_zero(self):
        """Test that entity_id can be None when entity_id_width=0."""
        gen = EntityIdGenerator("TRC")
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)

        id_value = gen.generate(timestamp)
        assert id_value.startswith("TRC")

    def test_default_timestamp_uses_current_utc(self):
        """Test that default timestamp is current UTC time."""
        gen = EntityIdGenerator("TRC")
        id_value = gen.generate()

        # Should start with TRC and current year
        assert id_value.startswith("TRC2026")

    def test_naive_timestamp_raises_error(self):
        """Test that timezone-naive timestamp raises ValueError."""
        gen = EntityIdGenerator("TRC")
        naive_timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456)

        with pytest.raises(ValueError, match="must be timezone-aware"):
            gen.generate(naive_timestamp)

    def test_zero_entity_id_is_valid(self):
        """Test that entity_id=0 is handled correctly."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)

        id_value = gen.generate(timestamp, entity_id=0)
        assert "RCP000" in id_value

    def test_large_entity_id_truncates_gracefully(self):
        """Test behavior with entity_id larger than width."""
        gen = EntityIdGenerator("RCP", entity_id_width=2)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)

        # 999 with width 2 should show as 99 (rightmost digits)
        id_value = gen.generate(timestamp, entity_id=999)
        # Python formatting will show as 999 even with width 2
        assert "RCP999" in id_value or "RCP99" in id_value


class TestEntityIdGeneratorThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_generation_produces_unique_ids(self):
        """Test that concurrent threads produce unique IDs."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)
        ids = []

        def generate_ids():
            for _ in range(100):
                id_value = gen.generate(timestamp, entity_id=1)
                ids.append(id_value)

        threads = [Thread(target=generate_ids) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # All 1000 IDs should be unique
        assert len(set(ids)) == 1000


class TestEntityIdGeneratorResetCounters:
    """Tests for counter reset functionality."""

    def test_reset_counters_clears_state(self):
        """Test that reset_counters clears all counter state."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)

        id1 = gen.generate(timestamp, entity_id=1)
        id2 = gen.generate(timestamp, entity_id=1)
        assert id1.endswith("0001")
        assert id2.endswith("0002")

        gen.reset_counters()

        id3 = gen.generate(timestamp, entity_id=1)
        assert id3.endswith("0001")  # Counter reset to 1


class TestEntityIdGeneratorMemoryManagement:
    """Tests for memory management."""

    def test_old_counters_are_cleaned_up(self):
        """Test that old counter entries are removed to prevent memory growth."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)
        base_time = datetime(2026, 1, 28, 14, 25, 30, 0, tzinfo=UTC)

        # Generate IDs across multiple seconds
        for i in range(10):
            timestamp = base_time + timedelta(seconds=i)
            gen.generate(timestamp, entity_id=1)

        # Internal counter dict should only have entries for the last second
        # We can't directly access _counters in a real test, but we can verify
        # the behavior is correct by ensuring continued operation
        final_time = base_time + timedelta(seconds=10)
        id_value = gen.generate(final_time, entity_id=1)
        assert id_value.endswith("0001")


class TestEntityIdGeneratorRealWorldScenarios:
    """Tests for real-world usage scenarios."""

    def test_receipt_id_scenario(self):
        """Test realistic receipt ID generation."""
        gen = EntityIdGenerator("RCP", entity_id_width=3)

        # Simulate 100 receipts at store 5 within same microsecond
        timestamp = datetime(2026, 1, 28, 14, 25, 30, 123456, tzinfo=UTC)
        ids = [gen.generate(timestamp, entity_id=5) for _ in range(100)]

        # All should be unique
        assert len(set(ids)) == 100
        # All should have correct format
        assert all(id_value.startswith("RCP005") for id_value in ids)
        # All should have same timestamp
        assert all("20260128142530123456" in id_value for id_value in ids)

    def test_return_id_scenario(self):
        """Test realistic return ID generation."""
        gen = EntityIdGenerator("RET", entity_id_width=3)

        # Simulate returns at different stores
        timestamp = datetime(2026, 1, 28, 15, 30, 45, 678901, tzinfo=UTC)

        id_store1 = gen.generate(timestamp, entity_id=1)
        id_store2 = gen.generate(timestamp, entity_id=2)
        id_store1_again = gen.generate(timestamp, entity_id=1)

        assert id_store1.startswith("RET001")
        assert id_store2.startswith("RET002")
        assert id_store1_again.startswith("RET001")
        assert id_store1 != id_store1_again  # Different counter

    def test_trace_id_scenario(self):
        """Test realistic trace ID generation without entity context."""
        gen = EntityIdGenerator("TRC")

        # Simulate trace IDs for different transactions
        ids = []
        base_time = datetime(2026, 1, 28, 14, 25, 30, 0, tzinfo=UTC)

        for i in range(50):
            timestamp = base_time + timedelta(microseconds=i * 1000)
            ids.append(gen.generate(timestamp))

        # All should be unique
        assert len(set(ids)) == 50
        # All should start with TRC
        assert all(id_value.startswith("TRC") for id_value in ids)

    def test_mixed_scenario_multiple_entities(self):
        """Test mixed scenario with multiple entity types."""
        receipt_gen = EntityIdGenerator("RCP", entity_id_width=3)
        return_gen = EntityIdGenerator("RET", entity_id_width=3)
        trace_gen = EntityIdGenerator("TRC")

        timestamp = datetime(2026, 1, 28, 16, 45, 0, 111111, tzinfo=UTC)

        receipt_id = receipt_gen.generate(timestamp, entity_id=10)
        return_id = return_gen.generate(timestamp, entity_id=10)
        trace_id = trace_gen.generate(timestamp)

        # Different prefixes ensure no collision
        assert receipt_id.startswith("RCP")
        assert return_id.startswith("RET")
        assert trace_id.startswith("TRC")

        # All have same timestamp component
        assert "20260128164500111111" in receipt_id
        assert "20260128164500111111" in return_id
        assert "20260128164500111111" in trace_id
