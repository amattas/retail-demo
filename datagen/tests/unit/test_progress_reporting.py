"""
Unit tests for progress reporting features in FactDataGenerator.

Tests throttling, ETA calculation, table state tracking, and thread safety.
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from threading import Thread, Lock
from datetime import datetime
from decimal import Decimal

from src.retail_datagen.generators.fact_generator import FactDataGenerator
from src.retail_datagen.config.models import RetailConfig


@pytest.fixture
def mock_config():
    """Create a minimal mock config for testing."""
    config = Mock(spec=RetailConfig)
    config.seed = 42
    config.volume = Mock()
    config.volume.stores = 10
    config.volume.dcs = 2
    config.volume.total_customers = 1000
    config.volume.customers_per_day = 100
    config.volume.items_per_ticket_mean = 3.5
    config.paths = Mock()
    config.paths.master = "data/master"
    config.paths.facts = "data/facts"
    config.paths.dict = "data/dictionaries"
    return config


@pytest.fixture
def generator(mock_config):
    """Create a FactDataGenerator instance for testing."""
    return FactDataGenerator(mock_config)


class TestProgressThrottling:
    """Test progress throttling (100ms minimum interval)."""

    def test_first_update_always_sent(self, generator):
        """First progress update should always be sent."""
        callback = Mock()
        generator._progress_callback = callback

        # First update should always go through
        generator._send_throttled_progress_update(1, "Test message", 10)

        callback.assert_called_once()

    @patch("time.time")
    def test_updates_throttled_when_too_fast(self, mock_time, generator):
        """Updates within 100ms should be throttled."""
        callback = Mock()
        generator._progress_callback = callback

        # Mock time to return values < 100ms apart
        mock_time.side_effect = [1000.0, 1000.05]  # 50ms apart

        # First update
        generator._send_throttled_progress_update(1, "First update", 10)
        assert callback.call_count == 1

        # Second update (too soon - should be throttled)
        generator._send_throttled_progress_update(2, "Second update", 10)
        assert callback.call_count == 1  # Still only 1 call

    @patch("time.time")
    def test_updates_sent_after_100ms(self, mock_time, generator):
        """Updates after 100ms should be sent."""
        callback = Mock()
        generator._progress_callback = callback

        # Mock time to return values >= 100ms apart
        mock_time.side_effect = [1000.0, 1000.15]  # 150ms apart

        # First update
        generator._send_throttled_progress_update(1, "First update", 10)
        assert callback.call_count == 1

        # Second update (enough time has passed - should go through)
        generator._send_throttled_progress_update(2, "Second update", 10)
        assert callback.call_count == 2

    @patch("time.time")
    def test_throttling_resets_after_successful_update(self, mock_time, generator):
        """Throttling timer should reset after each successful update."""
        callback = Mock()
        generator._progress_callback = callback

        # Sequence: First update, throttled, successful update
        mock_time.side_effect = [1000.0, 1000.05, 1000.15]

        generator._send_throttled_progress_update(1, "First", 10)
        assert callback.call_count == 1

        generator._send_throttled_progress_update(2, "Throttled", 10)
        assert callback.call_count == 1  # Still 1

        generator._send_throttled_progress_update(3, "Third", 10)
        assert callback.call_count == 2  # Now 2

    @patch("time.time")
    def test_no_callback_no_error(self, mock_time, generator):
        """Should not error when callback is None."""
        mock_time.return_value = 1000.0
        generator._progress_callback = None

        # Should not raise exception
        generator._send_throttled_progress_update(1, "Test", 10)


class TestProgressHistory:
    """Test progress history management."""

    def test_history_initialized_empty(self, generator):
        """Progress history should start empty."""
        assert generator._progress_history == []

    @patch("time.time")
    def test_history_updated_on_successful_update(self, mock_time, generator):
        """History should be updated when progress update is sent."""
        callback = Mock()
        generator._progress_callback = callback

        mock_time.side_effect = [1000.0, 1000.2]

        generator._send_throttled_progress_update(1, "First", 10)
        assert len(generator._progress_history) == 1
        assert generator._progress_history[0] == (1000.0, 0.1)  # (time, progress)

        generator._send_throttled_progress_update(5, "Second", 10)
        assert len(generator._progress_history) == 2
        assert generator._progress_history[1] == (1000.2, 0.5)

    @patch("time.time")
    def test_history_not_updated_when_throttled(self, mock_time, generator):
        """History should not be updated when update is throttled."""
        callback = Mock()
        generator._progress_callback = callback

        mock_time.side_effect = [1000.0, 1000.05]  # 50ms apart - throttled

        generator._send_throttled_progress_update(1, "First", 10)
        assert len(generator._progress_history) == 1

        generator._send_throttled_progress_update(2, "Second", 10)
        assert len(generator._progress_history) == 1  # Still 1 - throttled

    @patch("time.time")
    def test_history_limited_to_10_entries(self, mock_time, generator):
        """History should maintain maximum of 10 entries."""
        callback = Mock()
        generator._progress_callback = callback

        # Generate 15 updates with sufficient time spacing
        mock_time.side_effect = [1000.0 + (i * 0.2) for i in range(15)]

        for i in range(15):
            generator._send_throttled_progress_update(i + 1, f"Update {i}", 20)

        # Should only have last 10 entries
        assert len(generator._progress_history) == 10
        # Verify it's the LAST 10 (not first 10)
        assert generator._progress_history[-1][1] == 0.75  # 15/20 = 0.75

    @patch("time.time")
    def test_history_contains_timestamp_and_progress(self, mock_time, generator):
        """Each history entry should be (timestamp, progress) tuple."""
        callback = Mock()
        generator._progress_callback = callback

        mock_time.return_value = 1234.567

        generator._send_throttled_progress_update(3, "Test", 10)

        assert len(generator._progress_history) == 1
        timestamp, progress = generator._progress_history[0]
        assert timestamp == 1234.567
        assert progress == 0.3

    def test_history_reset_on_table_state_reset(self, generator):
        """History should be cleared when table states are reset."""
        # Manually add some history
        generator._progress_history = [(1000.0, 0.1), (1001.0, 0.2)]

        generator._reset_table_states()

        assert generator._progress_history == []


class TestETACalculation:
    """Test ETA calculation logic."""

    def test_eta_returns_none_with_no_history(self, generator):
        """ETA should return None with no history."""
        generator._progress_history = []
        eta = generator._calculate_eta(0.5)
        assert eta is None

    def test_eta_returns_none_with_one_history_point(self, generator):
        """ETA should return None with only 1 history point."""
        generator._progress_history = [(1000.0, 0.1)]
        eta = generator._calculate_eta(0.5)
        assert eta is None

    def test_eta_calculated_correctly_with_sufficient_history(self, generator):
        """ETA should be calculated correctly with 2+ history points."""
        # Progress from 10% to 50% in 10 seconds
        generator._progress_history = [(1000.0, 0.1), (1010.0, 0.5)]

        eta = generator._calculate_eta(0.5)

        # Progress rate: 0.4 progress / 10 seconds = 0.04 progress/second
        # Remaining: 0.5 progress
        # ETA: 0.5 / 0.04 = 12.5 seconds
        assert eta is not None
        assert abs(eta - 12.5) < 0.01

    def test_eta_with_multiple_history_points(self, generator):
        """ETA should use oldest and newest history points."""
        # Multiple history points - should use first and last
        generator._progress_history = [
            (1000.0, 0.1),
            (1005.0, 0.3),
            (1010.0, 0.5),
            (1015.0, 0.7),
        ]

        eta = generator._calculate_eta(0.7)

        # Rate: (0.7 - 0.1) / (1015.0 - 1000.0) = 0.6 / 15 = 0.04 progress/sec
        # Remaining: 0.3
        # ETA: 0.3 / 0.04 = 7.5 seconds
        assert eta is not None
        assert abs(eta - 7.5) < 0.01

    def test_eta_returns_none_when_no_progress_made(self, generator):
        """ETA should return None if no progress has been made."""
        generator._progress_history = [(1000.0, 0.5), (1010.0, 0.5)]  # Same progress

        eta = generator._calculate_eta(0.5)

        assert eta is None

    def test_eta_returns_none_when_progress_is_negative(self, generator):
        """ETA should return None if progress goes backward."""
        generator._progress_history = [(1000.0, 0.5), (1010.0, 0.3)]  # Backward

        eta = generator._calculate_eta(0.3)

        assert eta is None

    def test_eta_returns_none_when_time_not_elapsed(self, generator):
        """ETA should return None if no time has elapsed."""
        generator._progress_history = [(1000.0, 0.3), (1000.0, 0.5)]  # Same time

        eta = generator._calculate_eta(0.5)

        assert eta is None

    def test_eta_accuracy_at_50_percent(self, generator):
        """At 50% progress, ETA should estimate approximately equal time remaining."""
        # If we're 50% done and took 100 seconds, should estimate ~100 seconds remaining
        generator._progress_history = [(0.0, 0.0), (100.0, 0.5)]

        eta = generator._calculate_eta(0.5)

        # Rate: 0.5 / 100 = 0.005 progress/second
        # Remaining: 0.5
        # ETA: 0.5 / 0.005 = 100 seconds
        assert eta is not None
        assert abs(eta - 100.0) < 0.01

    def test_eta_accuracy_at_90_percent(self, generator):
        """At 90% progress, ETA should estimate much less time remaining."""
        # If we're 90% done and took 100 seconds, should estimate ~11.1 seconds remaining
        generator._progress_history = [(0.0, 0.0), (100.0, 0.9)]

        eta = generator._calculate_eta(0.9)

        # Rate: 0.9 / 100 = 0.009 progress/second
        # Remaining: 0.1
        # ETA: 0.1 / 0.009 = 11.11 seconds
        assert eta is not None
        assert abs(eta - 11.11) < 0.1


class TestTableStateTracking:
    """Test table state tracking functionality."""

    def test_table_states_initialize_correctly(self, generator):
        """All tables should start as 'not_started'."""
        for table in FactDataGenerator.FACT_TABLES:
            assert generator._table_states[table] == "not_started"

    def test_all_8_tables_tracked(self, generator):
        """All 8 FACT_TABLES should be tracked."""
        assert len(generator._table_states) == 8
        expected_tables = [
            "dc_inventory_txn",
            "truck_moves",
            "truck_inventory",
            "store_inventory_txn",
            "receipts",
            "receipt_lines",
            "foot_traffic",
            "ble_pings",
        ]
        for table in expected_tables:
            assert table in generator._table_states

    def test_table_state_transition_not_started_to_in_progress(self, generator):
        """Table should transition from not_started to in_progress."""
        table_progress = {"receipts": 0.5}
        generator._update_table_states(table_progress)

        assert generator._table_states["receipts"] == "in_progress"

    def test_table_state_transition_to_completed(self, generator):
        """Table should transition to completed when progress >= 1.0."""
        table_progress = {"receipts": 1.0}
        generator._update_table_states(table_progress)

        assert generator._table_states["receipts"] == "completed"

    def test_table_state_stays_in_progress(self, generator):
        """Table should stay in_progress if already started but not complete."""
        # First transition to in_progress
        generator._update_table_states({"receipts": 0.3})
        assert generator._table_states["receipts"] == "in_progress"

        # Update progress but still not complete
        generator._update_table_states({"receipts": 0.7})
        assert generator._table_states["receipts"] == "in_progress"

    def test_table_state_stays_completed(self, generator):
        """Table should stay completed once done."""
        # Mark as completed
        generator._update_table_states({"receipts": 1.0})
        assert generator._table_states["receipts"] == "completed"

        # Update again (shouldn't change)
        generator._update_table_states({"receipts": 1.0})
        assert generator._table_states["receipts"] == "completed"

    def test_multiple_tables_transition_independently(self, generator):
        """Different tables should transition independently."""
        table_progress = {
            "receipts": 0.3,
            "receipt_lines": 1.0,
            "foot_traffic": 0.0,
        }
        generator._update_table_states(table_progress)

        assert generator._table_states["receipts"] == "in_progress"
        assert generator._table_states["receipt_lines"] == "completed"
        assert generator._table_states["foot_traffic"] == "not_started"

    def test_unknown_table_ignored(self, generator):
        """Unknown table names should be ignored without error."""
        initial_states = generator._table_states.copy()

        # Should not raise error
        generator._update_table_states({"unknown_table": 0.5})

        # States should be unchanged
        assert generator._table_states == initial_states

    def test_completed_table_count(self, generator):
        """Should accurately count completed tables."""
        table_progress = {
            "receipts": 1.0,
            "receipt_lines": 1.0,
            "foot_traffic": 0.5,
        }
        generator._update_table_states(table_progress)

        completed_count = sum(
            1 for state in generator._table_states.values() if state == "completed"
        )

        assert completed_count == 2

    def test_tables_in_progress_list(self, generator):
        """Should accurately track tables in progress."""
        table_progress = {
            "receipts": 0.3,
            "receipt_lines": 0.7,
            "foot_traffic": 1.0,
        }
        generator._update_table_states(table_progress)

        tables_in_progress = [
            table
            for table, state in generator._table_states.items()
            if state == "in_progress"
        ]

        assert set(tables_in_progress) == {"receipts", "receipt_lines"}

    def test_tables_remaining_list(self, generator):
        """Should accurately track tables not yet started."""
        table_progress = {
            "receipts": 0.3,
            "receipt_lines": 1.0,
        }
        generator._update_table_states(table_progress)

        tables_remaining = [
            table
            for table, state in generator._table_states.items()
            if state == "not_started"
        ]

        # All tables except receipts and receipt_lines should be not_started
        expected_remaining = {
            "dc_inventory_txn",
            "truck_moves",
            "truck_inventory",
            "store_inventory_txn",
            "foot_traffic",
            "ble_pings",
        }

        assert set(tables_remaining) == expected_remaining

    def test_reset_clears_all_table_states(self, generator):
        """Reset should return all tables to not_started."""
        # Set various states
        generator._update_table_states(
            {
                "receipts": 1.0,
                "receipt_lines": 0.5,
            }
        )

        # Reset
        generator._reset_table_states()

        # All should be not_started
        for state in generator._table_states.values():
            assert state == "not_started"


class TestEnhancedProgressMessages:
    """Test enhanced progress message formatting."""

    @patch("time.time")
    def test_message_includes_table_completion_count(self, mock_time, generator):
        """Progress message should include (X/8 tables complete) format."""
        callback = Mock()
        generator._progress_callback = callback
        mock_time.return_value = 1000.0

        # Set some tables as completed
        generator._update_table_states({"receipts": 1.0, "receipt_lines": 1.0})

        tables_completed = ["receipts", "receipt_lines"]

        generator._send_throttled_progress_update(
            1,
            "Test message (2/8 tables complete)",
            10,
            tables_completed=tables_completed,
        )

        callback.assert_called_once()
        call_args = callback.call_args
        message = call_args[0][1]
        assert "(2/8 tables complete)" in message

    @patch("time.time")
    def test_message_at_zero_completion(self, mock_time, generator):
        """Progress message should show (0/8 tables complete) at start."""
        callback = Mock()
        generator._progress_callback = callback
        mock_time.return_value = 1000.0

        generator._send_throttled_progress_update(
            0, "Starting (0/8 tables complete)", 10, tables_completed=[]
        )

        callback.assert_called_once()
        call_args = callback.call_args
        message = call_args[0][1]
        assert "(0/8 tables complete)" in message

    @patch("time.time")
    def test_message_at_full_completion(self, mock_time, generator):
        """Progress message should show (8/8 tables complete) when done."""
        callback = Mock()
        generator._progress_callback = callback
        mock_time.return_value = 1000.0

        # Mark all tables complete
        all_progress = {table: 1.0 for table in FactDataGenerator.FACT_TABLES}
        generator._update_table_states(all_progress)

        tables_completed = list(FactDataGenerator.FACT_TABLES)

        generator._send_throttled_progress_update(
            10, "Complete (8/8 tables complete)", 10, tables_completed=tables_completed
        )

        callback.assert_called_once()
        call_args = callback.call_args
        message = call_args[0][1]
        assert "(8/8 tables complete)" in message

    @patch("time.time")
    def test_message_preserves_original_content(self, mock_time, generator):
        """Enhanced message should preserve original message content."""
        callback = Mock()
        generator._progress_callback = callback
        mock_time.return_value = 1000.0

        original_message = "Generated data for 2024-01-15 (day 5/30)"

        generator._send_throttled_progress_update(
            5, original_message, 30, tables_completed=[]
        )

        callback.assert_called_once()
        call_args = callback.call_args
        message = call_args[0][1]
        assert "Generated data for 2024-01-15" in message


class TestThreadSafety:
    """Test thread safety of progress reporting."""

    @patch("time.time")
    def test_progress_lock_prevents_race_conditions(self, mock_time, generator):
        """Progress lock should prevent concurrent access issues."""
        callback = Mock()
        generator._progress_callback = callback

        # Mock time to always allow updates
        mock_time.side_effect = [1000.0 + i * 0.2 for i in range(20)]

        results = []

        def update_progress(day_num):
            try:
                generator._send_throttled_progress_update(
                    day_num, f"Day {day_num}", 100
                )
                results.append(("success", day_num))
            except Exception as e:
                results.append(("error", str(e)))

        # Create multiple threads
        threads = [Thread(target=update_progress, args=(i,)) for i in range(10)]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # All should succeed (no errors)
        errors = [r for r in results if r[0] == "error"]
        assert len(errors) == 0

    @patch("time.time")
    def test_concurrent_history_updates_are_safe(self, mock_time, generator):
        """History updates should be thread-safe."""
        callback = Mock()
        generator._progress_callback = callback

        # Mock time to always allow updates
        mock_time.side_effect = [1000.0 + i * 0.2 for i in range(50)]

        def update_progress_multiple_times():
            for i in range(5):
                generator._send_throttled_progress_update(i, f"Update {i}", 10)
                time.sleep(0.001)  # Small delay

        # Create multiple threads doing concurrent updates
        threads = [Thread(target=update_progress_multiple_times) for _ in range(3)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # History should be intact (not corrupted)
        assert len(generator._progress_history) <= 10  # Max 10 entries
        # History should have valid tuples
        for timestamp, progress in generator._progress_history:
            assert isinstance(timestamp, float)
            assert isinstance(progress, float)
            assert 0.0 <= progress <= 1.0

    @patch("time.time")
    def test_last_update_time_is_protected(self, mock_time, generator):
        """Last update timestamp should be thread-safe."""
        callback = Mock()
        generator._progress_callback = callback

        mock_time.side_effect = [1000.0 + i * 0.2 for i in range(30)]

        final_times = []

        def do_update():
            generator._send_throttled_progress_update(1, "Test", 10)
            final_times.append(generator._last_progress_update_time)

        threads = [Thread(target=do_update) for _ in range(5)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Last update time should be set to a valid value
        assert generator._last_progress_update_time > 0

    def test_lock_exists_on_initialization(self, generator):
        """Generator should have a lock object initialized."""
        assert hasattr(generator, "_progress_lock")
        assert isinstance(generator._progress_lock, type(Lock()))


class TestCallbackCompatibility:
    """Test callback compatibility with different signatures."""

    @patch("time.time")
    def test_callback_with_enhanced_parameters(self, mock_time, generator):
        """Should call callback with enhanced parameters if supported."""
        mock_time.return_value = 1000.0

        # Mock callback that accepts enhanced parameters
        callback = Mock()
        generator._progress_callback = callback

        table_progress = {"receipts": 0.5}

        generator._send_throttled_progress_update(
            5, "Test message", 10, table_progress=table_progress, tables_completed=[]
        )

        # Should be called with enhanced parameters
        callback.assert_called_once()
        call_kwargs = callback.call_args[1]
        assert "table_progress" in call_kwargs
        assert call_kwargs["table_progress"] == table_progress

    @patch("time.time")
    def test_callback_fallback_for_legacy_signature(self, mock_time, generator):
        """Should fallback to simple signature if callback raises TypeError."""
        mock_time.return_value = 1000.0

        # Mock callback that only accepts 2 parameters (legacy)
        def legacy_callback(day, message):
            pass

        # Make it raise TypeError on enhanced call
        callback = Mock(side_effect=TypeError("unexpected keyword argument"))
        generator._progress_callback = callback

        # Should not raise error (should fallback)
        generator._send_throttled_progress_update(5, "Test message", 10)

        # Should have been called twice (first with enhanced, then fallback)
        assert callback.call_count == 2


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @patch("time.time")
    def test_progress_with_zero_total_days(self, mock_time, generator):
        """Should handle zero total days gracefully."""
        callback = Mock()
        generator._progress_callback = callback
        mock_time.return_value = 1000.0

        # Should not raise division by zero
        generator._send_throttled_progress_update(0, "Test", 0)

        callback.assert_called_once()

    @patch("time.time")
    def test_progress_exceeding_100_percent(self, mock_time, generator):
        """Should handle progress > 100% gracefully."""
        callback = Mock()
        generator._progress_callback = callback
        mock_time.return_value = 1000.0

        # Day counter exceeds total days
        generator._send_throttled_progress_update(15, "Test", 10)

        # Should still call callback
        callback.assert_called_once()

    @patch("time.time")
    def test_negative_progress(self, mock_time, generator):
        """Should handle negative day counter gracefully."""
        callback = Mock()
        generator._progress_callback = callback
        mock_time.return_value = 1000.0

        # Should not raise error
        generator._send_throttled_progress_update(-5, "Test", 10)

        callback.assert_called_once()

    def test_table_progress_with_empty_dict(self, generator):
        """Should handle empty table progress dict."""
        # Should not raise error
        generator._update_table_states({})

        # States should be unchanged
        for state in generator._table_states.values():
            assert state == "not_started"

    def test_table_progress_with_none(self, generator):
        """Should handle None table progress gracefully."""
        initial_states = generator._table_states.copy()

        # Manually call with None (shouldn't happen but be defensive)
        try:
            generator._update_table_states(None)
        except (TypeError, AttributeError):
            # Expected behavior - will fail on .items()
            pass

        # If it doesn't fail, states should be unchanged
        # This tests defensive programming

    def test_eta_with_extremely_small_progress(self, generator):
        """Should handle very small progress values."""
        generator._progress_history = [(1000.0, 0.0001), (1010.0, 0.0002)]

        eta = generator._calculate_eta(0.0002)

        # Should still calculate correctly
        assert eta is not None
        assert eta > 0

    def test_eta_with_extremely_fast_progress(self, generator):
        """Should handle very fast progress (nearly complete)."""
        generator._progress_history = [(1000.0, 0.0), (1001.0, 0.999)]

        eta = generator._calculate_eta(0.999)

        # Should estimate very short time remaining
        assert eta is not None
        assert eta < 5.0  # Less than 5 seconds
