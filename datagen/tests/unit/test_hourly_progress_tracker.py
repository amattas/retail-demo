"""
Unit tests for HourlyProgressTracker class.

Tests thread-safety, progress calculation, state management, and edge cases.
"""

from threading import Barrier, Thread

import pytest

from retail_datagen.generators.fact_generators import HourlyProgressTracker


@pytest.fixture
def tracker():
    """Create a HourlyProgressTracker instance for testing."""
    fact_tables = ["receipts", "receipt_lines", "store_inventory_txn"]
    return HourlyProgressTracker(fact_tables)


class TestInitialization:
    """Test HourlyProgressTracker initialization."""

    def test_initialization_with_tables(self):
        """Tracker should initialize with provided fact tables."""
        tables = ["receipts", "receipt_lines"]
        tracker = HourlyProgressTracker(tables)

        # Verify internal state
        assert tracker._fact_tables == tables
        assert len(tracker._progress_data) == 2
        assert "receipts" in tracker._progress_data
        assert "receipt_lines" in tracker._progress_data

    def test_initialization_empty_tables(self):
        """Tracker should handle empty table list."""
        tracker = HourlyProgressTracker([])
        assert tracker._fact_tables == []
        assert tracker._progress_data == {}

    def test_initialization_creates_structures(self, tracker):
        """Tracker should create all necessary data structures."""
        assert tracker._lock is not None
        assert tracker._total_days == 0
        for table in tracker._fact_tables:
            assert table in tracker._progress_data
            assert table in tracker._current_day
            assert table in tracker._current_hour
            assert tracker._current_day[table] == 0
            assert tracker._current_hour[table] == 0


class TestProgressUpdates:
    """Test progress update functionality."""

    def test_update_single_hour(self, tracker):
        """Should successfully update progress for a single hour."""
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)

        progress = tracker.get_current_progress()
        assert progress["overall_progress"] > 0
        assert progress["current_day"] == 1
        assert progress["current_hour"] == 0
        assert "receipts" in progress["tables_in_progress"]

    def test_update_multiple_hours_same_table(self, tracker):
        """Should track multiple hours for the same table."""
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
        tracker.update_hourly_progress("receipts", day=1, hour=1, total_days=5)
        tracker.update_hourly_progress("receipts", day=1, hour=2, total_days=5)

        progress = tracker.get_current_progress()
        assert progress["completed_hours"]["receipts"] == 3

    def test_update_multiple_days(self, tracker):
        """Should track progress across multiple days."""
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
        tracker.update_hourly_progress("receipts", day=2, hour=0, total_days=5)

        progress = tracker.get_current_progress()
        assert progress["completed_hours"]["receipts"] == 2
        assert progress["current_day"] == 2

    def test_update_different_tables(self, tracker):
        """Should track progress for different tables independently."""
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
        tracker.update_hourly_progress(
            "receipt_lines", day=1, hour=5, total_days=5
        )

        progress = tracker.get_current_progress()
        assert progress["completed_hours"]["receipts"] == 1
        assert progress["completed_hours"]["receipt_lines"] == 1

    def test_update_unknown_table(self, tracker):
        """Should handle updates for unknown tables gracefully."""
        # Should not raise exception
        tracker.update_hourly_progress(
            "unknown_table", day=1, hour=0, total_days=5
        )

        progress = tracker.get_current_progress()
        # Should not affect existing tables
        assert progress["completed_hours"]["receipts"] == 0

    def test_update_invalid_hour(self, tracker):
        """Should handle invalid hour values gracefully."""
        # Too high
        tracker.update_hourly_progress("receipts", day=1, hour=24, total_days=5)
        # Too low
        tracker.update_hourly_progress("receipts", day=1, hour=-1, total_days=5)

        progress = tracker.get_current_progress()
        # Should not record invalid hours
        assert progress["completed_hours"]["receipts"] == 0

    def test_update_duplicate_hour(self, tracker):
        """Should handle duplicate hour updates (idempotent)."""
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)

        progress = tracker.get_current_progress()
        # Should still count as 1 hour
        assert progress["completed_hours"]["receipts"] == 1


class TestProgressCalculation:
    """Test progress calculation and reporting."""

    def test_overall_progress_calculation(self, tracker):
        """Should calculate overall progress correctly."""
        total_days = 2  # 2 days * 24 hours = 48 hours per table

        # Complete 12 hours for receipts (25% of 48)
        for hour in range(12):
            tracker.update_hourly_progress(
                "receipts", day=1, hour=hour, total_days=total_days
            )

        progress = tracker.get_current_progress()
        # Overall progress uses max completed hours across tables (not average)
        # receipts: 12/48 = 0.25
        assert 0.24 <= progress["overall_progress"] <= 0.26

    def test_per_table_progress(self, tracker):
        """Should calculate per-table progress correctly."""
        total_days = 2  # 48 hours total

        # Complete 24 hours for receipts (50%)
        for hour in range(24):
            tracker.update_hourly_progress(
                "receipts", day=1, hour=hour, total_days=total_days
            )

        # Complete 12 hours for receipt_lines (25%)
        for hour in range(12):
            tracker.update_hourly_progress(
                "receipt_lines", day=1, hour=hour, total_days=total_days
            )

        progress = tracker.get_current_progress()
        assert abs(progress["per_table_progress"]["receipts"] - 0.5) < 0.01
        assert abs(progress["per_table_progress"]["receipt_lines"] - 0.25) < 0.01
        assert progress["per_table_progress"]["store_inventory_txn"] == 0.0

    def test_tables_in_progress(self, tracker):
        """Should correctly identify tables in progress."""
        total_days = 2

        # receipts: started but not complete
        tracker.update_hourly_progress(
            "receipts", day=1, hour=0, total_days=total_days
        )

        progress = tracker.get_current_progress()
        # In the new implementation, all tables are shown as "in progress"
        # when overall progress is between 0 and 1 (tables are processed together)
        # Since only receipts has 1/48 hours done, overall progress < 1.0
        assert len(progress["tables_in_progress"]) > 0
        # All configured tables should be listed when work is in progress
        assert "receipts" in progress["tables_in_progress"]
        assert "receipt_lines" in progress["tables_in_progress"]
        assert "store_inventory_txn" in progress["tables_in_progress"]

        # Now complete all hours for all tables
        for table in ["receipts", "receipt_lines", "store_inventory_txn"]:
            for day in range(1, total_days + 1):
                for hour in range(24):
                    tracker.update_hourly_progress(
                        table, day=day, hour=hour, total_days=total_days
                    )

        progress = tracker.get_current_progress()
        # Once all are complete, tables_in_progress should be empty
        assert progress["tables_in_progress"] == []

    def test_completed_hours_tracking(self, tracker):
        """Should accurately track completed hours per table."""
        total_days = 3

        tracker.update_hourly_progress(
            "receipts", day=1, hour=0, total_days=total_days
        )
        tracker.update_hourly_progress(
            "receipts", day=1, hour=5, total_days=total_days
        )
        tracker.update_hourly_progress(
            "receipts", day=2, hour=0, total_days=total_days
        )

        progress = tracker.get_current_progress()
        assert progress["completed_hours"]["receipts"] == 3

    def test_progress_never_exceeds_one(self, tracker):
        """Progress should never exceed 1.0 even with extra updates."""
        total_days = 1  # 24 hours total

        # Complete all hours plus some extras
        for hour in range(30):  # More than 24
            tracker.update_hourly_progress(
                "receipts", day=1, hour=hour % 24, total_days=total_days
            )

        progress = tracker.get_current_progress()
        assert progress["per_table_progress"]["receipts"] <= 1.0
        assert progress["overall_progress"] <= 1.0


class TestReset:
    """Test reset functionality."""

    def test_reset_clears_all_state(self, tracker):
        """Reset should clear all tracking state."""
        # Add some progress
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
        tracker.update_hourly_progress(
            "receipt_lines", day=1, hour=1, total_days=5
        )

        # Reset
        tracker.reset()

        # Verify state is cleared
        progress = tracker.get_current_progress()
        assert progress["overall_progress"] == 0.0
        assert progress["current_day"] == 0
        assert progress["current_hour"] == 0
        assert progress["tables_in_progress"] == []
        assert all(count == 0 for count in progress["completed_hours"].values())

    def test_reset_preserves_table_list(self, tracker):
        """Reset should preserve the original table list."""
        original_tables = tracker._fact_tables.copy()

        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
        tracker.reset()

        assert tracker._fact_tables == original_tables

    def test_reset_reinitializes_structures(self, tracker):
        """Reset should reinitialize all data structures."""
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
        tracker.reset()

        # Verify structures exist and are empty
        for table in tracker._fact_tables:
            assert table in tracker._progress_data
            assert tracker._progress_data[table] == {}
            assert tracker._current_day[table] == 0
            assert tracker._current_hour[table] == 0


class TestThreadSafety:
    """Test thread-safety of HourlyProgressTracker."""

    def test_concurrent_updates_different_tables(self, tracker):
        """Multiple threads updating different tables should work correctly."""
        total_days = 5
        hours_per_thread = 10

        def update_table(table_name, thread_id):
            """Update progress for a specific table."""
            for hour in range(hours_per_thread):
                tracker.update_hourly_progress(
                    table_name, day=1, hour=hour, total_days=total_days
                )

        threads = []
        table_names = ["receipts", "receipt_lines", "store_inventory_txn"]

        for i, table_name in enumerate(table_names):
            thread = Thread(target=update_table, args=(table_name, i))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify all updates were recorded
        progress = tracker.get_current_progress()
        for table_name in table_names:
            assert progress["completed_hours"][table_name] == hours_per_thread

    def test_concurrent_updates_same_table(self, tracker):
        """Multiple threads updating same table should work correctly."""
        total_days = 5
        num_threads = 3
        barrier = Barrier(num_threads)

        def update_same_table(thread_id):
            """Update progress for the same table from multiple threads."""
            barrier.wait()  # Synchronize thread start
            for day in range(1, 3):  # 2 days
                for hour in range(8):  # 8 hours per day
                    # Each thread updates different hours
                    actual_hour = (thread_id * 8) + hour
                    if actual_hour < 24:
                        tracker.update_hourly_progress(
                            "receipts",
                            day=day,
                            hour=actual_hour,
                            total_days=total_days,
                        )

        threads = []
        for i in range(num_threads):
            thread = Thread(target=update_same_table, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify updates from all threads were recorded
        progress = tracker.get_current_progress()
        # Each thread updates 8 hours * 2 days = 16 hours, but some overlap
        # Actual count depends on hour distribution
        assert progress["completed_hours"]["receipts"] > 0

    def test_concurrent_read_write(self, tracker):
        """Concurrent reads and writes should work correctly."""
        total_days = 5
        num_writers = 2
        num_readers = 3
        barrier = Barrier(num_writers + num_readers)

        results = []

        def writer(thread_id):
            """Write progress updates."""
            barrier.wait()
            for hour in range(10):
                tracker.update_hourly_progress(
                    "receipts", day=1, hour=hour, total_days=total_days
                )

        def reader(thread_id):
            """Read progress state."""
            barrier.wait()
            for _ in range(20):
                progress = tracker.get_current_progress()
                results.append(progress["overall_progress"])

        threads = []
        for i in range(num_writers):
            thread = Thread(target=writer, args=(i,))
            threads.append(thread)
            thread.start()

        for i in range(num_readers):
            thread = Thread(target=reader, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify no crashes and progress values are valid
        assert len(results) == num_readers * 20
        assert all(0 <= p <= 1.0 for p in results)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_total_days(self, tracker):
        """Should handle zero total_days gracefully."""
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=0)

        progress = tracker.get_current_progress()
        # Should not crash - when total_days is 0, implementation uses 1 as fallback
        # Any completed work results in full progress (1.0)
        assert progress["overall_progress"] == 1.0

    def test_single_table(self):
        """Should work correctly with single table."""
        tracker = HourlyProgressTracker(["receipts"])
        tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=1)

        progress = tracker.get_current_progress()
        assert progress["per_table_progress"]["receipts"] > 0

    def test_many_tables(self):
        """Should handle many tables efficiently."""
        tables = [f"table_{i}" for i in range(20)]
        tracker = HourlyProgressTracker(tables)

        # Update first and last table
        tracker.update_hourly_progress(tables[0], day=1, hour=0, total_days=5)
        tracker.update_hourly_progress(tables[-1], day=1, hour=5, total_days=5)

        progress = tracker.get_current_progress()
        assert len(progress["per_table_progress"]) == 20
        assert progress["completed_hours"][tables[0]] == 1
        assert progress["completed_hours"][tables[-1]] == 1

    def test_all_hours_all_days(self, tracker):
        """Should correctly track completion of all hours across all days."""
        total_days = 2

        # Complete all hours for all days for one table
        for day in range(1, total_days + 1):
            for hour in range(24):
                tracker.update_hourly_progress(
                    "receipts", day=day, hour=hour, total_days=total_days
                )

        progress = tracker.get_current_progress()
        assert progress["completed_hours"]["receipts"] == total_days * 24
        # This table should not be in progress (it's complete)
        # But other tables are still at 0, so receipts shows as complete
        assert "receipts" not in progress["tables_in_progress"]
