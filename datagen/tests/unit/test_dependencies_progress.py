"""
Unit tests for update_task_progress() state pass-through behavior.

Tests verify that update_task_progress() in dependencies.py correctly passes through
table state lists (tables_completed, tables_in_progress, tables_remaining, tables_failed)
without deriving them from progress percentages. This ensures TableProgressTracker
remains the authoritative source for table lifecycle states.

Key Test Areas:
    1. State Pass-Through: Verify state lists pass through unchanged
    2. No Override Behavior: Verify progress % doesn't affect state lists
    3. Edge Cases: Handle None values, empty lists, mixed scenarios
    4. Integration: Simulate calls from fact_generator with tracker states
"""

from datetime import UTC, datetime, timedelta

import pytest

from retail_datagen.shared.dependencies import (
    TASK_CLEANUP_MAX_AGE_HOURS,
    TaskStatus,
    _background_tasks,
    _task_status,
    cleanup_old_tasks,
    get_task_status,
    update_task_progress,
)


@pytest.fixture
def clean_task_store():
    """
    Clean task status store before and after each test.

    Ensures tests start with a fresh global state to avoid cross-test contamination.
    """
    global _task_status
    _task_status.clear()
    yield
    _task_status.clear()


@pytest.fixture
def initialized_task(clean_task_store):
    """
    Create a pre-initialized task for testing updates.

    Returns a task_id with baseline state that can be updated in tests.
    """
    task_id = "test_task"
    _task_status[task_id] = TaskStatus(
        status="running",
        progress=0.0,
        message="Initial state",
        started_at=datetime.now(),
    )
    return task_id


class TestStatePassThrough:
    """Test that update_task_progress() passes through state lists unchanged."""

    def test_tables_completed_passed_through(self, initialized_task):
        """Test that tables_completed list is passed through without modification."""
        task_id = initialized_task
        completed = ["receipts", "receipt_lines", "marketing"]

        update_task_progress(
            task_id=task_id,
            progress=0.6,
            message="Test update",
            tables_completed=completed,
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == completed
        assert (
            status.tables_completed is not completed
        )  # Should be a different list object

    def test_tables_in_progress_passed_through(self, initialized_task):
        """Test that tables_in_progress list is passed through without modification."""
        task_id = initialized_task
        in_progress = ["dc_inventory_txn", "store_inventory_txn"]

        update_task_progress(
            task_id=task_id,
            progress=0.4,
            message="Test update",
            tables_in_progress=in_progress,
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_in_progress == in_progress

    def test_tables_remaining_passed_through(self, initialized_task):
        """Test that tables_remaining list is passed through without modification."""
        task_id = initialized_task
        remaining = ["foot_traffic", "ble_pings", "truck_moves"]

        update_task_progress(
            task_id=task_id,
            progress=0.3,
            message="Test update",
            tables_remaining=remaining,
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_remaining == remaining

    def test_tables_failed_passed_through(self, initialized_task):
        """Test that tables_failed list is passed through without modification."""
        task_id = initialized_task
        failed = ["online_orders"]

        update_task_progress(
            task_id=task_id,
            progress=0.5,
            message="Test update",
            tables_failed=failed,
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_failed == failed

    def test_all_state_lists_passed_through_together(self, initialized_task):
        """Test that all state lists can be passed through simultaneously."""
        task_id = initialized_task
        completed = ["receipts"]
        in_progress = ["receipt_lines", "marketing"]
        remaining = ["foot_traffic", "ble_pings"]
        failed = []

        update_task_progress(
            task_id=task_id,
            progress=0.4,
            message="Test update",
            tables_completed=completed,
            tables_in_progress=in_progress,
            tables_remaining=remaining,
            tables_failed=failed,
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == completed
        assert status.tables_in_progress == in_progress
        assert status.tables_remaining == remaining
        assert status.tables_failed == failed

    def test_state_lists_persist_across_updates(self, initialized_task):
        """Test that state lists are replaced (not merged) on subsequent updates."""
        task_id = initialized_task

        # First update
        update_task_progress(
            task_id=task_id,
            progress=0.3,
            tables_completed=["receipts"],
            tables_in_progress=["receipt_lines"],
        )

        status = get_task_status(task_id)
        assert status.tables_completed == ["receipts"]
        assert status.tables_in_progress == ["receipt_lines"]

        # Second update with different lists
        update_task_progress(
            task_id=task_id,
            progress=0.6,
            tables_completed=["receipts", "receipt_lines"],
            tables_in_progress=["marketing"],
        )

        status = get_task_status(task_id)
        assert status.tables_completed == ["receipts", "receipt_lines"]
        assert status.tables_in_progress == ["marketing"]


class TestNoOverrideBehavior:
    """Test that progress percentages don't affect state list values."""

    def test_100_percent_progress_does_not_derive_completed_state(
        self, initialized_task
    ):
        """Test that table_progress at 100% doesn't override tables_completed."""
        task_id = initialized_task

        # Update with 100% progress on some tables, but empty completed list
        update_task_progress(
            task_id=task_id,
            progress=1.0,
            message="All done",
            table_progress={"receipts": 1.0, "receipt_lines": 1.0, "marketing": 1.0},
            tables_completed=[],  # Explicitly empty
            tables_in_progress=[
                "receipts",
                "receipt_lines",
                "marketing",
            ],  # Still in progress
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == []  # Should remain empty
        assert status.tables_in_progress == ["receipts", "receipt_lines", "marketing"]
        # Progress shows 100%, but state is still in_progress - they're independent

    def test_zero_percent_progress_does_not_affect_completed_state(
        self, initialized_task
    ):
        """Test that 0% progress doesn't prevent tables from being marked completed."""
        task_id = initialized_task

        # Mark tables as completed even though progress is 0%
        update_task_progress(
            task_id=task_id,
            progress=0.0,
            message="Completed",
            table_progress={"receipts": 0.0, "receipt_lines": 0.0},
            tables_completed=["receipts", "receipt_lines"],  # Completed despite 0%
            tables_in_progress=[],
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == ["receipts", "receipt_lines"]
        assert status.tables_in_progress == []

    def test_mixed_progress_does_not_derive_states(self, initialized_task):
        """Test that varied progress percentages don't affect state lists."""
        task_id = initialized_task

        # Various progress levels, but state lists are authoritative
        update_task_progress(
            task_id=task_id,
            progress=0.5,
            table_progress={
                "receipts": 1.0,  # 100% but not completed
                "receipt_lines": 0.5,  # 50% but completed
                "marketing": 0.0,  # 0% but in_progress
            },
            tables_completed=["receipt_lines"],  # Only this one marked completed
            tables_in_progress=["receipts", "marketing"],
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == ["receipt_lines"]
        assert status.tables_in_progress == ["receipts", "marketing"]

    def test_in_progress_state_persists_at_100_percent(self, initialized_task):
        """Test that tables can remain in_progress even when at 100%."""
        task_id = initialized_task

        # Table at 100% but explicitly in_progress (not completed yet)
        update_task_progress(
            task_id=task_id,
            progress=1.0,
            table_progress={"receipts": 1.0},
            tables_completed=[],
            tables_in_progress=["receipts"],  # Still in_progress despite 100%
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_in_progress == ["receipts"]
        assert status.tables_completed == []
        assert status.table_progress["receipts"] == 1.0

    def test_completed_state_valid_below_100_percent(self, initialized_task):
        """Test that tables can be completed even if progress < 100%."""
        task_id = initialized_task

        # Mark as completed despite partial progress
        update_task_progress(
            task_id=task_id,
            progress=0.8,
            table_progress={"receipts": 0.8},
            tables_completed=["receipts"],  # Completed at 80%
            tables_in_progress=[],
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == ["receipts"]
        assert status.tables_in_progress == []
        assert status.table_progress["receipts"] == 0.8


class TestEdgeCases:
    """Test edge cases: None values, empty lists, mixed scenarios."""

    def test_none_state_lists_do_not_update(self, initialized_task):
        """Test that None values for state lists leave them unchanged."""
        task_id = initialized_task

        # Set initial state
        update_task_progress(
            task_id=task_id,
            progress=0.3,
            tables_completed=["receipts"],
            tables_in_progress=["receipt_lines"],
        )

        # Update with None values (should not change existing state)
        update_task_progress(
            task_id=task_id,
            progress=0.5,
            message="Updated progress only",
            tables_completed=None,  # Don't update
            tables_in_progress=None,  # Don't update
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == ["receipts"]  # Unchanged
        assert status.tables_in_progress == ["receipt_lines"]  # Unchanged

    def test_empty_lists_set_to_empty(self, initialized_task):
        """Test that explicitly empty lists clear state fields."""
        task_id = initialized_task

        # Set initial state
        update_task_progress(
            task_id=task_id,
            progress=0.3,
            tables_completed=["receipts"],
            tables_in_progress=["receipt_lines"],
        )

        # Clear with empty lists
        update_task_progress(
            task_id=task_id,
            progress=0.5,
            tables_completed=[],  # Explicitly clear
            tables_in_progress=[],  # Explicitly clear
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == []
        assert status.tables_in_progress == []

    def test_mixed_none_and_values(self, initialized_task):
        """Test providing some state lists and leaving others as None."""
        task_id = initialized_task

        # Initial state
        update_task_progress(
            task_id=task_id,
            progress=0.2,
            tables_completed=["receipts"],
            tables_in_progress=["receipt_lines"],
            tables_remaining=["marketing"],
        )

        # Update only some fields
        update_task_progress(
            task_id=task_id,
            progress=0.4,
            tables_completed=["receipts", "receipt_lines"],  # Update
            tables_in_progress=None,  # Leave unchanged
            tables_remaining=["marketing", "foot_traffic"],  # Update
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == ["receipts", "receipt_lines"]  # Updated
        assert status.tables_in_progress == ["receipt_lines"]  # Unchanged
        assert status.tables_remaining == ["marketing", "foot_traffic"]  # Updated

    def test_single_table_in_each_state(self, initialized_task):
        """Test with single-element lists in each state."""
        task_id = initialized_task

        update_task_progress(
            task_id=task_id,
            progress=0.5,
            tables_completed=["receipts"],
            tables_in_progress=["receipt_lines"],
            tables_remaining=["marketing"],
            tables_failed=["online_orders"],
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_completed == ["receipts"]
        assert status.tables_in_progress == ["receipt_lines"]
        assert status.tables_remaining == ["marketing"]
        assert status.tables_failed == ["online_orders"]

    def test_all_tables_in_one_state(self, initialized_task):
        """Test with all tables in a single state list."""
        task_id = initialized_task
        all_tables = [
            "receipts",
            "receipt_lines",
            "dc_inventory_txn",
            "store_inventory_txn",
            "truck_moves",
            "foot_traffic",
            "ble_pings",
            "marketing",
        ]

        # All in_progress
        update_task_progress(
            task_id=task_id,
            progress=0.5,
            tables_completed=[],
            tables_in_progress=all_tables,
            tables_remaining=[],
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_in_progress == all_tables
        assert status.tables_completed == []
        assert status.tables_remaining == []

    def test_duplicate_table_names_across_states(self, initialized_task):
        """Test behavior when same table appears in multiple state lists (edge case)."""
        task_id = initialized_task

        # Note: This is an invalid state from TableProgressTracker perspective,
        # but update_task_progress should still pass it through
        update_task_progress(
            task_id=task_id,
            progress=0.5,
            tables_completed=["receipts"],
            tables_in_progress=["receipts"],  # Same table also in_progress
        )

        status = get_task_status(task_id)
        assert status is not None
        # Should pass through as-is, even if illogical
        assert status.tables_completed == ["receipts"]
        assert status.tables_in_progress == ["receipts"]


class TestTableProgressMerging:
    """Test that table_progress merging still works correctly (existing behavior)."""

    def test_table_progress_merges_with_max(self, initialized_task):
        """Test that table_progress values merge using max (existing behavior)."""
        task_id = initialized_task

        # First update
        update_task_progress(
            task_id=task_id,
            progress=0.3,
            table_progress={"receipts": 0.3, "receipt_lines": 0.5},
        )

        status = get_task_status(task_id)
        assert status.table_progress == {"receipts": 0.3, "receipt_lines": 0.5}

        # Second update with some higher, some lower values
        update_task_progress(
            task_id=task_id,
            progress=0.5,
            table_progress={"receipts": 0.6, "receipt_lines": 0.2, "marketing": 0.1},
        )

        status = get_task_status(task_id)
        # Should take max for each table
        assert status.table_progress == {
            "receipts": 0.6,  # Increased from 0.3
            "receipt_lines": 0.5,  # Kept at 0.5 (didn't decrease to 0.2)
            "marketing": 0.1,  # Newly added
        }

    def test_table_progress_independent_from_state_lists(self, initialized_task):
        """Test that table_progress and state lists are independent."""
        task_id = initialized_task

        update_task_progress(
            task_id=task_id,
            progress=0.5,
            table_progress={"receipts": 1.0, "receipt_lines": 0.5},
            tables_completed=["marketing"],  # Different table
            tables_in_progress=["receipts", "receipt_lines"],
        )

        status = get_task_status(task_id)
        assert status is not None
        # Progress and state are independent
        assert status.table_progress == {"receipts": 1.0, "receipt_lines": 0.5}
        assert status.tables_completed == ["marketing"]
        assert status.tables_in_progress == ["receipts", "receipt_lines"]


class TestIntegrationWithTableProgressTracker:
    """Test scenarios simulating calls from fact_generator with TableProgressTracker states."""

    def test_tracker_state_passed_through_during_generation(self, initialized_task):
        """
        Simulate fact_generator calling update_task_progress with tracker states.

        This mimics the actual usage pattern in fact_generator.py where
        TableProgressTracker provides authoritative state lists.
        """
        task_id = initialized_task

        # Simulate start of generation (all remaining)
        update_task_progress(
            task_id=task_id,
            progress=0.0,
            message="Starting generation",
            tables_completed=[],
            tables_in_progress=[],
            tables_remaining=[
                "receipts",
                "receipt_lines",
                "dc_inventory_txn",
                "store_inventory_txn",
            ],
            table_progress={},
        )

        status = get_task_status(task_id)
        assert status.tables_remaining == [
            "receipts",
            "receipt_lines",
            "dc_inventory_txn",
            "store_inventory_txn",
        ]

        # Simulate first table starts
        update_task_progress(
            task_id=task_id,
            progress=0.1,
            message="Generating receipts",
            current_table="receipts",
            tables_completed=[],
            tables_in_progress=["receipts"],
            tables_remaining=[
                "receipt_lines",
                "dc_inventory_txn",
                "store_inventory_txn",
            ],
            table_progress={"receipts": 0.3},
        )

        status = get_task_status(task_id)
        assert status.current_table == "receipts"
        assert status.tables_in_progress == ["receipts"]
        assert status.tables_remaining == [
            "receipt_lines",
            "dc_inventory_txn",
            "store_inventory_txn",
        ]

        # Simulate table at 100% but still in_progress (not yet marked complete)
        update_task_progress(
            task_id=task_id,
            progress=0.25,
            message="Finishing receipts",
            current_table="receipts",
            tables_completed=[],
            tables_in_progress=["receipts"],  # Still in_progress
            tables_remaining=[
                "receipt_lines",
                "dc_inventory_txn",
                "store_inventory_txn",
            ],
            table_progress={"receipts": 1.0},  # 100% progress
        )

        status = get_task_status(task_id)
        # Should respect tracker state, not derive from progress
        assert status.tables_in_progress == ["receipts"]
        assert status.tables_completed == []
        assert status.table_progress["receipts"] == 1.0

        # Simulate transition to completed when mark_generation_complete() called
        update_task_progress(
            task_id=task_id,
            progress=1.0,
            message="All tables complete",
            tables_completed=[
                "receipts",
                "receipt_lines",
                "dc_inventory_txn",
                "store_inventory_txn",
            ],
            tables_in_progress=[],
            tables_remaining=[],
        )

        status = get_task_status(task_id)
        assert status.tables_completed == [
            "receipts",
            "receipt_lines",
            "dc_inventory_txn",
            "store_inventory_txn",
        ]
        assert status.tables_in_progress == []
        assert status.tables_remaining == []

    def test_incremental_state_transitions(self, initialized_task):
        """Test incremental state transitions as tables move through lifecycle."""
        task_id = initialized_task

        # Initial: all remaining
        remaining = ["table1", "table2", "table3"]
        update_task_progress(
            task_id=task_id,
            progress=0.0,
            tables_completed=[],
            tables_in_progress=[],
            tables_remaining=remaining,
        )

        # Table1 starts
        update_task_progress(
            task_id=task_id,
            progress=0.1,
            tables_completed=[],
            tables_in_progress=["table1"],
            tables_remaining=["table2", "table3"],
        )

        status = get_task_status(task_id)
        assert status.tables_in_progress == ["table1"]

        # Table1 finishes, table2 starts
        update_task_progress(
            task_id=task_id,
            progress=0.4,
            tables_completed=["table1"],
            tables_in_progress=["table2"],
            tables_remaining=["table3"],
        )

        status = get_task_status(task_id)
        assert status.tables_completed == ["table1"]
        assert status.tables_in_progress == ["table2"]

        # All complete
        update_task_progress(
            task_id=task_id,
            progress=1.0,
            tables_completed=["table1", "table2", "table3"],
            tables_in_progress=[],
            tables_remaining=[],
        )

        status = get_task_status(task_id)
        assert status.tables_completed == ["table1", "table2", "table3"]
        assert status.tables_in_progress == []
        assert status.tables_remaining == []

    def test_failure_scenario(self, initialized_task):
        """Test state transitions when a table fails."""
        task_id = initialized_task

        # Start generation
        update_task_progress(
            task_id=task_id,
            progress=0.2,
            tables_completed=[],
            tables_in_progress=["receipts"],
            tables_remaining=["receipt_lines", "marketing"],
            tables_failed=[],
        )

        # Table fails
        update_task_progress(
            task_id=task_id,
            progress=0.3,
            message="Table generation failed",
            tables_completed=[],
            tables_in_progress=["receipt_lines"],
            tables_remaining=["marketing"],
            tables_failed=["receipts"],  # Moved to failed
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.tables_failed == ["receipts"]
        assert status.tables_in_progress == ["receipt_lines"]


class TestProgressClamping:
    """Test that progress clamping still works with state pass-through."""

    def test_progress_clamped_regardless_of_states(self, initialized_task):
        """Test that overall progress is clamped to [0.0, 1.0] independent of states."""
        task_id = initialized_task

        # Try to set progress > 1.0
        update_task_progress(
            task_id=task_id,
            progress=1.5,  # Invalid, should be clamped to 1.0
            tables_in_progress=["receipts"],
        )

        status = get_task_status(task_id)
        assert status.progress == 1.0  # Clamped
        assert status.tables_in_progress == ["receipts"]  # State preserved

    def test_progress_never_decreases_but_states_can_change(self, initialized_task):
        """Test that progress is monotonic but states can transition freely."""
        task_id = initialized_task

        # Set progress to 0.8
        update_task_progress(
            task_id=task_id,
            progress=0.8,
            tables_in_progress=["receipts", "receipt_lines"],
        )

        # Try to decrease progress but change states
        update_task_progress(
            task_id=task_id,
            progress=0.5,  # Should not decrease
            tables_completed=["receipts"],
            tables_in_progress=["receipt_lines"],
        )

        status = get_task_status(task_id)
        assert status.progress == 0.8  # Didn't decrease
        # But states updated correctly
        assert status.tables_completed == ["receipts"]
        assert status.tables_in_progress == ["receipt_lines"]


class TestSequenceNumbering:
    """Test that sequence numbers increment correctly."""

    def test_sequence_increments_with_state_updates(self, initialized_task):
        """Test that sequence number increments on each update."""
        task_id = initialized_task

        # Initial update
        update_task_progress(
            task_id=task_id,
            progress=0.0,
            tables_in_progress=[],
        )

        status = get_task_status(task_id)
        seq1 = status.sequence
        assert seq1 is not None

        # Second update
        update_task_progress(
            task_id=task_id,
            progress=0.5,
            tables_in_progress=["receipts"],
        )

        status = get_task_status(task_id)
        seq2 = status.sequence
        assert seq2 == seq1 + 1

        # Third update
        update_task_progress(
            task_id=task_id,
            progress=1.0,
            tables_completed=["receipts"],
            tables_in_progress=[],
        )

        status = get_task_status(task_id)
        seq3 = status.sequence
        assert seq3 == seq2 + 1


class TestBackwardsCompatibility:
    """Test that state pass-through doesn't break when fields are omitted."""

    def test_update_without_state_fields(self, initialized_task):
        """Test traditional update without any state fields still works."""
        task_id = initialized_task

        # Old-style update (no state fields)
        update_task_progress(
            task_id=task_id,
            progress=0.5,
            message="Traditional update",
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.progress == 0.5
        assert status.message == "Traditional update"
        # State fields should be None or their default values
        assert status.tables_completed is None
        assert status.tables_in_progress is None

    def test_update_with_only_progress_tracking(self, initialized_task):
        """Test update with table_progress but no state lists."""
        task_id = initialized_task

        update_task_progress(
            task_id=task_id,
            progress=0.6,
            table_progress={"receipts": 0.6, "receipt_lines": 0.4},
            current_table="receipts",
        )

        status = get_task_status(task_id)
        assert status is not None
        assert status.table_progress == {"receipts": 0.6, "receipt_lines": 0.4}
        assert status.current_table == "receipts"
        # State fields not provided, should remain None
        assert status.tables_completed is None
        assert status.tables_in_progress is None


# ================================
# TASK CLEANUP TESTS
# ================================


class TestTaskCleanup:
    """Tests for background task cleanup functionality."""

    @pytest.fixture(autouse=True)
    def clean_task_stores(self):
        """Clean task stores before and after each test."""
        _task_status.clear()
        _background_tasks.clear()
        yield
        _task_status.clear()
        _background_tasks.clear()

    def test_cleanup_removes_old_completed_tasks(self):
        """Test that cleanup removes completed tasks older than max_age."""
        # Create an old completed task (48 hours ago)
        old_time = datetime.now(UTC) - timedelta(hours=48)
        _task_status["old_task"] = TaskStatus(
            status="completed",
            started_at=old_time,
            completed_at=old_time,
            progress=1.0,
            message="Old task",
        )

        # Create a recent completed task (1 hour ago)
        recent_time = datetime.now(UTC) - timedelta(hours=1)
        _task_status["recent_task"] = TaskStatus(
            status="completed",
            started_at=recent_time,
            completed_at=recent_time,
            progress=1.0,
            message="Recent task",
        )

        # Run cleanup with 24 hour threshold
        cleaned = cleanup_old_tasks(max_age_hours=24)

        assert cleaned == 1
        assert "old_task" not in _task_status
        assert "recent_task" in _task_status

    def test_cleanup_preserves_running_tasks(self):
        """Test that cleanup does not remove running tasks (no completed_at)."""
        # Create an old running task
        old_time = datetime.now(UTC) - timedelta(hours=48)
        _task_status["running_task"] = TaskStatus(
            status="running",
            started_at=old_time,
            completed_at=None,
            progress=0.5,
            message="Still running",
        )

        cleaned = cleanup_old_tasks(max_age_hours=24)

        assert cleaned == 0
        assert "running_task" in _task_status

    def test_cleanup_removes_old_failed_tasks(self):
        """Test that cleanup removes failed tasks older than max_age."""
        old_time = datetime.now(UTC) - timedelta(hours=48)
        _task_status["failed_task"] = TaskStatus(
            status="failed",
            started_at=old_time,
            completed_at=old_time,
            progress=0.5,
            message="Failed",
            error="Some error",
        )

        cleaned = cleanup_old_tasks(max_age_hours=24)

        assert cleaned == 1
        assert "failed_task" not in _task_status

    def test_cleanup_with_no_tasks(self):
        """Test cleanup when no tasks exist."""
        cleaned = cleanup_old_tasks()
        assert cleaned == 0

    def test_cleanup_with_all_recent_tasks(self):
        """Test cleanup when all tasks are recent."""
        recent_time = datetime.now(UTC) - timedelta(hours=1)
        _task_status["task1"] = TaskStatus(
            status="completed",
            started_at=recent_time,
            completed_at=recent_time,
            progress=1.0,
            message="Task 1",
        )
        _task_status["task2"] = TaskStatus(
            status="completed",
            started_at=recent_time,
            completed_at=recent_time,
            progress=1.0,
            message="Task 2",
        )

        cleaned = cleanup_old_tasks(max_age_hours=24)

        assert cleaned == 0
        assert len(_task_status) == 2

    def test_cleanup_uses_default_max_age(self):
        """Test that cleanup uses default max_age when None provided."""
        # Create a task older than default (24 hours)
        old_time = datetime.now(UTC) - timedelta(hours=TASK_CLEANUP_MAX_AGE_HOURS + 1)
        _task_status["old_task"] = TaskStatus(
            status="completed",
            started_at=old_time,
            completed_at=old_time,
            progress=1.0,
            message="Old task",
        )

        cleaned = cleanup_old_tasks(max_age_hours=None)

        assert cleaned == 1
        assert "old_task" not in _task_status

    def test_cleanup_negative_hours_raises_error(self):
        """Test that negative max_age_hours raises ValueError."""
        with pytest.raises(ValueError, match="max_age_hours must be non-negative"):
            cleanup_old_tasks(max_age_hours=-1)

    def test_cleanup_exceeds_max_hours_raises_error(self):
        """Test that max_age_hours > 720 raises ValueError."""
        with pytest.raises(ValueError, match="max_age_hours must not exceed 720"):
            cleanup_old_tasks(max_age_hours=721)

    def test_cleanup_edge_case_zero_hours(self):
        """Test cleanup with zero hours removes all completed tasks."""
        recent_time = datetime.now(UTC) - timedelta(seconds=1)
        _task_status["task"] = TaskStatus(
            status="completed",
            started_at=recent_time,
            completed_at=recent_time,
            progress=1.0,
            message="Just completed",
        )

        cleaned = cleanup_old_tasks(max_age_hours=0)

        assert cleaned == 1
        assert "task" not in _task_status

    def test_cleanup_removes_old_cancelled_tasks(self):
        """Test that cleanup removes cancelled tasks older than max_age."""
        old_time = datetime.now(UTC) - timedelta(hours=48)
        _task_status["cancelled_task"] = TaskStatus(
            status="cancelled",
            started_at=old_time,
            completed_at=old_time,  # Cancelled tasks have completed_at set
            progress=0.3,
            message="Task was cancelled",
        )

        # Create a recent cancelled task
        recent_time = datetime.now(UTC) - timedelta(hours=1)
        _task_status["recent_cancelled"] = TaskStatus(
            status="cancelled",
            started_at=recent_time,
            completed_at=recent_time,
            progress=0.5,
            message="Recently cancelled",
        )

        cleaned = cleanup_old_tasks(max_age_hours=24)

        assert cleaned == 1
        assert "cancelled_task" not in _task_status
        assert "recent_cancelled" in _task_status
