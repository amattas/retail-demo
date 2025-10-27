#!/usr/bin/env python3
"""
Quick verification script for HourlyProgressTracker implementation.
Tests basic functionality and thread safety without requiring pytest.
"""

import sys
from threading import Thread, Lock
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.generators.fact_generator import HourlyProgressTracker


def test_basic_functionality():
    """Test basic progress tracking."""
    print("Testing basic functionality...")

    tables = ["receipts", "receipt_lines", "store_inventory_txn"]
    tracker = HourlyProgressTracker(tables)

    # Test initialization
    assert tracker._fact_tables == tables, "Tables not initialized correctly"
    assert tracker._total_days == 0, "Total days should start at 0"
    print("  ✓ Initialization successful")

    # Test single update
    tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
    progress = tracker.get_current_progress()
    assert progress["overall_progress"] > 0, "Progress should be > 0 after update"
    assert progress["current_day"] == 1, "Current day should be 1"
    assert progress["current_hour"] == 0, "Current hour should be 0"
    print("  ✓ Single update successful")

    # Test multiple updates
    tracker.update_hourly_progress("receipts", day=1, hour=1, total_days=5)
    tracker.update_hourly_progress("receipts", day=1, hour=2, total_days=5)
    progress = tracker.get_current_progress()
    assert progress["completed_hours"]["receipts"] == 3, "Should have 3 completed hours"
    print("  ✓ Multiple updates successful")

    # Test progress calculation
    # 5 days * 24 hours = 120 total hours per table
    # 3 hours completed = 3/120 = 0.025 per table
    # Average across 3 tables: (0.025 + 0 + 0) / 3 = 0.00833...
    expected_progress = (3 / (5 * 24)) / 3
    assert abs(progress["overall_progress"] - expected_progress) < 0.001, \
        f"Progress calculation incorrect: {progress['overall_progress']} vs {expected_progress}"
    print("  ✓ Progress calculation accurate")

    print("✓ Basic functionality tests passed!\n")


def test_reset():
    """Test reset functionality."""
    print("Testing reset functionality...")

    tracker = HourlyProgressTracker(["receipts", "receipt_lines"])

    # Add some progress
    tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
    tracker.update_hourly_progress("receipt_lines", day=1, hour=5, total_days=5)

    progress_before = tracker.get_current_progress()
    assert progress_before["overall_progress"] > 0, "Should have progress before reset"

    # Reset
    tracker.reset()

    # Verify reset
    progress_after = tracker.get_current_progress()
    assert progress_after["overall_progress"] == 0, "Progress should be 0 after reset"
    assert progress_after["current_day"] == 0, "Current day should be 0 after reset"
    assert progress_after["tables_in_progress"] == [], "No tables should be in progress"
    assert all(c == 0 for c in progress_after["completed_hours"].values()), \
        "All completed hours should be 0"

    print("✓ Reset functionality tests passed!\n")


def test_thread_safety():
    """Test thread-safe concurrent updates."""
    print("Testing thread safety...")

    tracker = HourlyProgressTracker(["receipts", "receipt_lines", "store_inventory_txn"])
    total_days = 5
    errors = []

    def update_table(table_name, hours):
        """Update progress for a specific table."""
        try:
            for hour in range(hours):
                tracker.update_hourly_progress(
                    table_name,
                    day=1,
                    hour=hour,
                    total_days=total_days
                )
        except Exception as e:
            errors.append(e)

    # Create threads for concurrent updates
    threads = []
    table_hours = [
        ("receipts", 10),
        ("receipt_lines", 8),
        ("store_inventory_txn", 12)
    ]

    for table_name, hours in table_hours:
        thread = Thread(target=update_table, args=(table_name, hours))
        threads.append(thread)
        thread.start()

    # Wait for all threads
    for thread in threads:
        thread.join()

    # Check for errors
    assert len(errors) == 0, f"Thread errors occurred: {errors}"

    # Verify results
    progress = tracker.get_current_progress()
    assert progress["completed_hours"]["receipts"] == 10, "receipts should have 10 hours"
    assert progress["completed_hours"]["receipt_lines"] == 8, "receipt_lines should have 8 hours"
    assert progress["completed_hours"]["store_inventory_txn"] == 12, "store_inventory_txn should have 12 hours"

    print("✓ Thread safety tests passed!\n")


def test_edge_cases():
    """Test edge cases."""
    print("Testing edge cases...")

    tracker = HourlyProgressTracker(["receipts"])

    # Test invalid hour
    tracker.update_hourly_progress("receipts", day=1, hour=24, total_days=5)
    tracker.update_hourly_progress("receipts", day=1, hour=-1, total_days=5)
    progress = tracker.get_current_progress()
    assert progress["completed_hours"]["receipts"] == 0, "Invalid hours should not be recorded"
    print("  ✓ Invalid hour handling")

    # Test unknown table
    tracker.update_hourly_progress("unknown_table", day=1, hour=0, total_days=5)
    progress = tracker.get_current_progress()
    assert "unknown_table" not in progress["completed_hours"], "Unknown table should be ignored"
    print("  ✓ Unknown table handling")

    # Test duplicate updates (idempotent)
    tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
    tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=5)
    progress = tracker.get_current_progress()
    assert progress["completed_hours"]["receipts"] == 1, "Duplicate updates should be idempotent"
    print("  ✓ Duplicate update handling")

    # Test zero total_days
    tracker.reset()
    tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=0)
    progress = tracker.get_current_progress()
    assert progress["overall_progress"] == 0, "Should handle zero total_days"
    print("  ✓ Zero total_days handling")

    print("✓ Edge case tests passed!\n")


def test_tables_in_progress():
    """Test tables_in_progress tracking."""
    print("Testing tables_in_progress tracking...")

    tracker = HourlyProgressTracker(["receipts", "receipt_lines", "store_inventory_txn"])
    total_days = 2  # 48 hours total

    # receipts: started but not complete
    tracker.update_hourly_progress("receipts", day=1, hour=0, total_days=total_days)

    # receipt_lines: complete all hours
    for day in range(1, total_days + 1):
        for hour in range(24):
            tracker.update_hourly_progress("receipt_lines", day=day, hour=hour, total_days=total_days)

    # store_inventory_txn: not started

    progress = tracker.get_current_progress()

    # Only receipts should be in progress
    assert "receipts" in progress["tables_in_progress"], "receipts should be in progress"
    assert "receipt_lines" not in progress["tables_in_progress"], "receipt_lines should be complete"
    assert "store_inventory_txn" not in progress["tables_in_progress"], "store_inventory_txn not started"

    print("✓ tables_in_progress tracking tests passed!\n")


def main():
    """Run all verification tests."""
    print("=" * 60)
    print("HourlyProgressTracker Verification Tests")
    print("=" * 60 + "\n")

    try:
        test_basic_functionality()
        test_reset()
        test_thread_safety()
        test_edge_cases()
        test_tables_in_progress()

        print("=" * 60)
        print("✓ ALL TESTS PASSED!")
        print("=" * 60)
        print("\nHourlyProgressTracker implementation is correct and thread-safe.")
        return 0

    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
