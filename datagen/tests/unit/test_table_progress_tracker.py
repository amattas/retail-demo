"""
Unit tests for TableProgressTracker class.

Tests state transitions, progress updates, query methods, reset functionality,
thread safety, and integration scenarios for table generation progress tracking.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from src.retail_datagen.generators.progress_tracker import TableProgressTracker


class TestInitialization:
    """Test TableProgressTracker initialization."""

    def test_init_with_table_names(self):
        """Test initialization with a list of table names."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        states = tracker.get_all_states()
        progress = tracker.get_all_progress()

        assert len(states) == 3
        assert len(progress) == 3
        assert all(table in states for table in tables)
        assert all(table in progress for table in tables)

    def test_init_all_tables_start_not_started(self):
        """Test that all tables begin in 'not_started' state."""
        tables = ["receipts", "receipt_lines", "marketing", "foot_traffic"]
        tracker = TableProgressTracker(tables)

        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_NOT_STARTED

    def test_init_all_progress_zero(self):
        """Test that all tables begin with 0.0 progress."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        for table in tables:
            assert tracker.get_progress(table) == 0.0

    def test_init_empty_table_list(self):
        """Test initialization with empty table list."""
        tracker = TableProgressTracker([])

        assert tracker.get_all_states() == {}
        assert tracker.get_all_progress() == {}

    def test_init_single_table(self):
        """Test initialization with a single table."""
        tracker = TableProgressTracker(["receipts"])

        assert tracker.get_state("receipts") == TableProgressTracker.STATE_NOT_STARTED
        assert tracker.get_progress("receipts") == 0.0


class TestStateTransitions:
    """Test state transition logic."""

    def test_mark_table_started(self):
        """Test that table transitions from 'not_started' to 'in_progress'."""
        tracker = TableProgressTracker(["receipts"])

        assert tracker.get_state("receipts") == TableProgressTracker.STATE_NOT_STARTED

        tracker.mark_table_started("receipts")

        assert tracker.get_state("receipts") == TableProgressTracker.STATE_IN_PROGRESS

    def test_mark_table_started_twice_stays_in_progress(self):
        """Test that marking a table started twice is idempotent."""
        tracker = TableProgressTracker(["receipts"])

        tracker.mark_table_started("receipts")
        assert tracker.get_state("receipts") == TableProgressTracker.STATE_IN_PROGRESS

        # Mark started again
        tracker.mark_table_started("receipts")
        assert tracker.get_state("receipts") == TableProgressTracker.STATE_IN_PROGRESS

    def test_mark_table_started_unknown_table(self):
        """Test that marking unknown table started raises KeyError."""
        tracker = TableProgressTracker(["receipts"])

        with pytest.raises(KeyError, match="Table 'unknown_table' is not being tracked"):
            tracker.mark_table_started("unknown_table")

    def test_mark_generation_complete(self):
        """Test that all 'in_progress' tables transition to 'completed'."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        # Mark all tables as started
        for table in tables:
            tracker.mark_table_started(table)

        # Verify all in_progress
        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_IN_PROGRESS

        # Mark generation complete
        tracker.mark_generation_complete()

        # Verify all completed
        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_COMPLETED

    def test_mark_generation_complete_only_affects_in_progress(self):
        """Test that 'not_started' tables remain 'not_started' when marking complete."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        # Only mark some tables as started
        tracker.mark_table_started("receipts")
        tracker.mark_table_started("receipt_lines")
        # Leave 'marketing' as not_started

        tracker.mark_generation_complete()

        assert tracker.get_state("receipts") == TableProgressTracker.STATE_COMPLETED
        assert tracker.get_state("receipt_lines") == TableProgressTracker.STATE_COMPLETED
        assert tracker.get_state("marketing") == TableProgressTracker.STATE_NOT_STARTED

    def test_mark_generation_complete_no_in_progress_tables(self):
        """Test marking complete when no tables are in_progress."""
        tables = ["receipts", "receipt_lines"]
        tracker = TableProgressTracker(tables)

        # Don't mark any tables as started
        tracker.mark_generation_complete()

        # All should still be not_started
        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_NOT_STARTED


class TestProgressUpdates:
    """Test progress update functionality."""

    def test_update_progress_valid_values(self):
        """Test updating progress with valid values (0.0-1.0)."""
        tracker = TableProgressTracker(["receipts"])

        test_values = [0.0, 0.25, 0.5, 0.75, 1.0]

        for value in test_values:
            tracker.update_progress("receipts", value)
            assert tracker.get_progress("receipts") == value

    def test_update_progress_does_not_change_state(self):
        """Test that progress at 1.0 doesn't auto-complete the table."""
        tracker = TableProgressTracker(["receipts"])

        # Table starts in not_started
        assert tracker.get_state("receipts") == TableProgressTracker.STATE_NOT_STARTED

        # Update progress to 1.0
        tracker.update_progress("receipts", 1.0)

        # State should still be not_started
        assert tracker.get_state("receipts") == TableProgressTracker.STATE_NOT_STARTED
        assert tracker.get_progress("receipts") == 1.0

        # Even if in_progress, setting to 1.0 shouldn't complete it
        tracker.mark_table_started("receipts")
        tracker.update_progress("receipts", 1.0)
        assert tracker.get_state("receipts") == TableProgressTracker.STATE_IN_PROGRESS

    def test_update_progress_invalid_negative(self):
        """Test that negative progress raises ValueError."""
        tracker = TableProgressTracker(["receipts"])

        with pytest.raises(ValueError, match="Progress must be between 0.0 and 1.0"):
            tracker.update_progress("receipts", -0.1)

        with pytest.raises(ValueError, match="Progress must be between 0.0 and 1.0"):
            tracker.update_progress("receipts", -1.0)

    def test_update_progress_invalid_over_one(self):
        """Test that progress > 1.0 raises ValueError."""
        tracker = TableProgressTracker(["receipts"])

        with pytest.raises(ValueError, match="Progress must be between 0.0 and 1.0"):
            tracker.update_progress("receipts", 1.1)

        with pytest.raises(ValueError, match="Progress must be between 0.0 and 1.0"):
            tracker.update_progress("receipts", 2.0)

    def test_update_progress_unknown_table(self):
        """Test that updating unknown table raises KeyError."""
        tracker = TableProgressTracker(["receipts"])

        with pytest.raises(KeyError, match="Table 'unknown_table' is not being tracked"):
            tracker.update_progress("unknown_table", 0.5)

    def test_update_progress_incremental(self):
        """Test incremental progress updates."""
        tracker = TableProgressTracker(["receipts"])

        # Simulate incremental progress
        for i in range(11):
            progress = i / 10.0
            tracker.update_progress("receipts", progress)
            assert tracker.get_progress("receipts") == progress

    def test_update_progress_can_decrease(self):
        """Test that progress can be decreased (though unusual)."""
        tracker = TableProgressTracker(["receipts"])

        tracker.update_progress("receipts", 0.8)
        assert tracker.get_progress("receipts") == 0.8

        # Allow progress to decrease
        tracker.update_progress("receipts", 0.3)
        assert tracker.get_progress("receipts") == 0.3


class TestQueryMethods:
    """Test query methods for retrieving state and progress."""

    def test_get_state_valid_table(self):
        """Test getting state for a valid table."""
        tracker = TableProgressTracker(["receipts", "marketing"])

        assert tracker.get_state("receipts") == TableProgressTracker.STATE_NOT_STARTED
        tracker.mark_table_started("receipts")
        assert tracker.get_state("receipts") == TableProgressTracker.STATE_IN_PROGRESS

    def test_get_state_unknown_table(self):
        """Test that getting state for unknown table raises KeyError."""
        tracker = TableProgressTracker(["receipts"])

        with pytest.raises(KeyError, match="Table 'unknown_table' is not being tracked"):
            tracker.get_state("unknown_table")

    def test_get_progress_valid_table(self):
        """Test getting progress for a valid table."""
        tracker = TableProgressTracker(["receipts"])

        assert tracker.get_progress("receipts") == 0.0
        tracker.update_progress("receipts", 0.42)
        assert tracker.get_progress("receipts") == 0.42

    def test_get_progress_unknown_table(self):
        """Test that getting progress for unknown table raises KeyError."""
        tracker = TableProgressTracker(["receipts"])

        with pytest.raises(KeyError, match="Table 'unknown_table' is not being tracked"):
            tracker.get_progress("unknown_table")

    def test_get_tables_by_state(self):
        """Test filtering tables by state."""
        tables = ["receipts", "receipt_lines", "marketing", "foot_traffic"]
        tracker = TableProgressTracker(tables)

        # Initially all not_started
        not_started = tracker.get_tables_by_state(TableProgressTracker.STATE_NOT_STARTED)
        assert set(not_started) == set(tables)
        assert tracker.get_tables_by_state(TableProgressTracker.STATE_IN_PROGRESS) == []

        # Mark some as started
        tracker.mark_table_started("receipts")
        tracker.mark_table_started("receipt_lines")

        in_progress = tracker.get_tables_by_state(TableProgressTracker.STATE_IN_PROGRESS)
        not_started = tracker.get_tables_by_state(TableProgressTracker.STATE_NOT_STARTED)

        assert set(in_progress) == {"receipts", "receipt_lines"}
        assert set(not_started) == {"marketing", "foot_traffic"}

        # Mark generation complete
        tracker.mark_generation_complete()

        completed = tracker.get_tables_by_state(TableProgressTracker.STATE_COMPLETED)
        assert set(completed) == {"receipts", "receipt_lines"}

    def test_get_all_states(self):
        """Test getting complete state dictionary."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        tracker.mark_table_started("receipts")
        tracker.mark_table_started("receipt_lines")

        states = tracker.get_all_states()

        assert states == {
            "receipts": TableProgressTracker.STATE_IN_PROGRESS,
            "receipt_lines": TableProgressTracker.STATE_IN_PROGRESS,
            "marketing": TableProgressTracker.STATE_NOT_STARTED,
        }

        # Verify it's a copy, not the internal dict
        states["receipts"] = "modified"
        assert tracker.get_state("receipts") == TableProgressTracker.STATE_IN_PROGRESS

    def test_get_all_progress(self):
        """Test getting complete progress dictionary."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        tracker.update_progress("receipts", 0.5)
        tracker.update_progress("receipt_lines", 0.75)

        progress = tracker.get_all_progress()

        assert progress == {
            "receipts": 0.5,
            "receipt_lines": 0.75,
            "marketing": 0.0,
        }

        # Verify it's a copy, not the internal dict
        progress["receipts"] = 0.99
        assert tracker.get_progress("receipts") == 0.5


class TestReset:
    """Test reset functionality."""

    def test_reset(self):
        """Test that reset returns all tables to 'not_started' with 0.0 progress."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        # Set various states and progress
        tracker.mark_table_started("receipts")
        tracker.update_progress("receipts", 0.8)

        tracker.mark_table_started("receipt_lines")
        tracker.update_progress("receipt_lines", 0.5)
        tracker.mark_generation_complete()

        tracker.update_progress("marketing", 0.3)

        # Verify pre-reset state
        assert tracker.get_state("receipts") == TableProgressTracker.STATE_COMPLETED
        assert tracker.get_state("receipt_lines") == TableProgressTracker.STATE_COMPLETED
        assert tracker.get_progress("receipts") == 0.8

        # Reset
        tracker.reset()

        # Verify all reset to initial state
        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_NOT_STARTED
            assert tracker.get_progress(table) == 0.0

    def test_reset_empty_tracker(self):
        """Test resetting an empty tracker."""
        tracker = TableProgressTracker([])
        tracker.reset()  # Should not raise

        assert tracker.get_all_states() == {}
        assert tracker.get_all_progress() == {}

    def test_reset_maintains_table_list(self):
        """Test that reset doesn't remove tables from tracking."""
        tables = ["receipts", "receipt_lines"]
        tracker = TableProgressTracker(tables)

        tracker.reset()

        # Tables should still be tracked
        assert set(tracker.get_all_states().keys()) == set(tables)
        assert set(tracker.get_all_progress().keys()) == set(tables)


class TestThreadSafety:
    """Test thread safety of TableProgressTracker."""

    def test_concurrent_progress_updates(self):
        """Test multiple threads updating progress concurrently."""
        tables = ["table1", "table2", "table3", "table4", "table5"]
        tracker = TableProgressTracker(tables)

        def update_progress_repeatedly(table_name, iterations=100):
            """Update progress for a table multiple times."""
            for i in range(iterations):
                progress = (i + 1) / iterations
                tracker.update_progress(table_name, progress)

        # Launch threads for each table
        threads = []
        for table in tables:
            thread = threading.Thread(target=update_progress_repeatedly, args=(table,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # All tables should have progress 1.0
        for table in tables:
            assert tracker.get_progress(table) == 1.0

    def test_concurrent_state_transitions(self):
        """Test multiple threads changing states concurrently."""
        tables = [f"table{i}" for i in range(10)]
        tracker = TableProgressTracker(tables)

        def mark_started_repeatedly(table_name, iterations=50):
            """Mark table as started multiple times."""
            for _ in range(iterations):
                tracker.mark_table_started(table_name)

        # Launch threads for each table
        threads = []
        for table in tables:
            thread = threading.Thread(target=mark_started_repeatedly, args=(table,))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # All tables should be in_progress
        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_IN_PROGRESS

    def test_concurrent_mixed_operations(self):
        """Test concurrent state changes, progress updates, and queries."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        results = {"errors": [], "query_results": []}
        lock = threading.Lock()

        def mixed_operations(table_name):
            """Perform various operations on a table."""
            try:
                # Mark started
                tracker.mark_table_started(table_name)

                # Update progress incrementally
                for i in range(10):
                    tracker.update_progress(table_name, i / 10.0)
                    time.sleep(0.001)  # Small delay to encourage interleaving

                # Query state and progress
                state = tracker.get_state(table_name)
                progress = tracker.get_progress(table_name)

                with lock:
                    results["query_results"].append((table_name, state, progress))

            except Exception as e:
                with lock:
                    results["errors"].append(str(e))

        # Launch threads
        threads = []
        for table in tables:
            thread = threading.Thread(target=mixed_operations, args=(table,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # No errors should occur
        assert len(results["errors"]) == 0

        # All tables should be in_progress with progress 0.9
        assert len(results["query_results"]) == 3
        for table_name, state, progress in results["query_results"]:
            assert state == TableProgressTracker.STATE_IN_PROGRESS
            assert progress == 0.9

    def test_concurrent_reset_and_updates(self):
        """Test thread safety when resetting while other threads update."""
        tables = ["table1", "table2", "table3"]
        tracker = TableProgressTracker(tables)

        stop_flag = threading.Event()
        errors = []
        error_lock = threading.Lock()

        def continuous_updates(table_name):
            """Continuously update progress until stop flag is set."""
            try:
                i = 0
                while not stop_flag.is_set():
                    progress = (i % 100) / 100.0
                    tracker.update_progress(table_name, progress)
                    i += 1
                    time.sleep(0.001)
            except Exception as e:
                with error_lock:
                    errors.append(str(e))

        def reset_periodically():
            """Reset tracker periodically."""
            try:
                for _ in range(5):
                    time.sleep(0.01)
                    tracker.reset()
            except Exception as e:
                with error_lock:
                    errors.append(str(e))
            finally:
                stop_flag.set()

        # Launch update threads
        update_threads = []
        for table in tables:
            thread = threading.Thread(target=continuous_updates, args=(table,))
            update_threads.append(thread)
            thread.start()

        # Launch reset thread
        reset_thread = threading.Thread(target=reset_periodically)
        reset_thread.start()

        # Wait for completion
        reset_thread.join()
        for thread in update_threads:
            thread.join()

        # No errors should occur
        assert len(errors) == 0

    def test_thread_pool_concurrent_access(self):
        """Test concurrent access using ThreadPoolExecutor."""
        tables = [f"table{i}" for i in range(20)]
        tracker = TableProgressTracker(tables)

        def process_table(table_name):
            """Process a single table: mark started, update progress."""
            tracker.mark_table_started(table_name)
            for i in range(10):
                tracker.update_progress(table_name, i / 10.0)
            return table_name, tracker.get_state(table_name), tracker.get_progress(table_name)

        # Use ThreadPoolExecutor for concurrent execution
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_table, table) for table in tables]
            results = [future.result() for future in as_completed(futures)]

        # All tables should be in_progress with progress 0.9
        assert len(results) == 20
        for table_name, state, progress in results:
            assert state == TableProgressTracker.STATE_IN_PROGRESS
            assert progress == 0.9


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_typical_generation_workflow(self):
        """Test a complete typical generation workflow."""
        # Simulate 8 fact tables
        tables = [
            "receipts",
            "receipt_lines",
            "dc_inventory_txn",
            "store_inventory_txn",
            "truck_moves",
            "foot_traffic",
            "ble_pings",
            "marketing",
        ]
        tracker = TableProgressTracker(tables)

        # Initial state: all not_started
        assert tracker.get_tables_by_state(TableProgressTracker.STATE_NOT_STARTED) == tables
        assert tracker.get_tables_by_state(TableProgressTracker.STATE_IN_PROGRESS) == []
        assert tracker.get_tables_by_state(TableProgressTracker.STATE_COMPLETED) == []

        # Process each table sequentially
        for table in tables:
            # Mark started
            tracker.mark_table_started(table)
            assert tracker.get_state(table) == TableProgressTracker.STATE_IN_PROGRESS

            # Simulate progress updates (0% → 100%)
            for progress_pct in [0, 25, 50, 75, 100]:
                tracker.update_progress(table, progress_pct / 100.0)

            # Verify final progress
            assert tracker.get_progress(table) == 1.0
            assert tracker.get_state(table) == TableProgressTracker.STATE_IN_PROGRESS  # Still in_progress

        # Mark entire generation complete
        tracker.mark_generation_complete()

        # All tables should be completed
        assert tracker.get_tables_by_state(TableProgressTracker.STATE_COMPLETED) == tables
        assert tracker.get_tables_by_state(TableProgressTracker.STATE_IN_PROGRESS) == []

    def test_partial_generation(self):
        """Test scenario where some tables are started, some not."""
        tables = ["receipts", "receipt_lines", "marketing", "foot_traffic"]
        tracker = TableProgressTracker(tables)

        # Start only first two tables
        tracker.mark_table_started("receipts")
        tracker.update_progress("receipts", 0.6)

        tracker.mark_table_started("receipt_lines")
        tracker.update_progress("receipt_lines", 0.3)

        # Query states
        assert set(tracker.get_tables_by_state(TableProgressTracker.STATE_IN_PROGRESS)) == {
            "receipts",
            "receipt_lines",
        }
        assert set(tracker.get_tables_by_state(TableProgressTracker.STATE_NOT_STARTED)) == {
            "marketing",
            "foot_traffic",
        }

        # Mark complete (only in_progress should complete)
        tracker.mark_generation_complete()

        assert set(tracker.get_tables_by_state(TableProgressTracker.STATE_COMPLETED)) == {
            "receipts",
            "receipt_lines",
        }
        assert set(tracker.get_tables_by_state(TableProgressTracker.STATE_NOT_STARTED)) == {
            "marketing",
            "foot_traffic",
        }

    def test_mixed_progress_levels(self):
        """Test tables at different progress percentages."""
        tables = ["receipts", "receipt_lines", "marketing", "foot_traffic"]
        tracker = TableProgressTracker(tables)

        # Mark all started
        for table in tables:
            tracker.mark_table_started(table)

        # Set different progress levels
        tracker.update_progress("receipts", 0.1)
        tracker.update_progress("receipt_lines", 0.5)
        tracker.update_progress("marketing", 0.9)
        tracker.update_progress("foot_traffic", 1.0)

        # Verify progress
        assert tracker.get_progress("receipts") == 0.1
        assert tracker.get_progress("receipt_lines") == 0.5
        assert tracker.get_progress("marketing") == 0.9
        assert tracker.get_progress("foot_traffic") == 1.0

        # All should still be in_progress
        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_IN_PROGRESS

    def test_reset_and_rerun_workflow(self):
        """Test resetting and rerunning generation."""
        tables = ["receipts", "receipt_lines"]
        tracker = TableProgressTracker(tables)

        # First run
        for table in tables:
            tracker.mark_table_started(table)
            tracker.update_progress(table, 1.0)
        tracker.mark_generation_complete()

        # Verify completed
        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_COMPLETED

        # Reset for second run
        tracker.reset()

        # Verify reset to initial state
        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_NOT_STARTED
            assert tracker.get_progress(table) == 0.0

        # Run again
        for table in tables:
            tracker.mark_table_started(table)
            tracker.update_progress(table, 0.5)

        # Verify second run state
        for table in tables:
            assert tracker.get_state(table) == TableProgressTracker.STATE_IN_PROGRESS
            assert tracker.get_progress(table) == 0.5

    def test_error_recovery_scenario(self):
        """Test recovery scenario where generation fails and restarts."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        # Start generation
        tracker.mark_table_started("receipts")
        tracker.update_progress("receipts", 0.7)

        tracker.mark_table_started("receipt_lines")
        tracker.update_progress("receipt_lines", 0.3)

        # Simulate error: reset and restart
        tracker.reset()

        # Restart from beginning
        tracker.mark_table_started("receipts")
        tracker.update_progress("receipts", 0.2)

        assert tracker.get_state("receipts") == TableProgressTracker.STATE_IN_PROGRESS
        assert tracker.get_progress("receipts") == 0.2
        assert tracker.get_state("receipt_lines") == TableProgressTracker.STATE_NOT_STARTED
        assert tracker.get_progress("receipt_lines") == 0.0

    def test_incremental_table_addition_workflow(self):
        """Test workflow where tables are processed incrementally one at a time."""
        tables = ["receipts", "receipt_lines", "marketing"]
        tracker = TableProgressTracker(tables)

        # Process first table completely
        tracker.mark_table_started("receipts")
        for i in range(101):
            tracker.update_progress("receipts", i / 100.0)

        assert tracker.get_progress("receipts") == 1.0
        assert tracker.get_state("receipts") == TableProgressTracker.STATE_IN_PROGRESS

        # Process second table
        tracker.mark_table_started("receipt_lines")
        tracker.update_progress("receipt_lines", 0.5)

        # Check current state
        in_progress = tracker.get_tables_by_state(TableProgressTracker.STATE_IN_PROGRESS)
        assert set(in_progress) == {"receipts", "receipt_lines"}

        not_started = tracker.get_tables_by_state(TableProgressTracker.STATE_NOT_STARTED)
        assert set(not_started) == {"marketing"}
