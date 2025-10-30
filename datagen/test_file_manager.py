#!/usr/bin/env python3
"""
Quick verification script for ExportFileManager implementation.

This script tests the basic functionality without requiring a full test suite run.
"""

from datetime import date
from pathlib import Path
import tempfile
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.services import ExportFileManager


def test_master_table_paths():
    """Test master table path resolution."""
    print("Testing master table paths...")

    with tempfile.TemporaryDirectory() as tmpdir:
        manager = ExportFileManager(Path(tmpdir))

        # Test CSV path
        csv_path = manager.get_master_table_path("stores", "csv")
        expected = Path(tmpdir) / "master" / "stores.csv"
        assert csv_path == expected, f"Expected {expected}, got {csv_path}"
        print(f"  ✓ CSV path: {csv_path}")

        # Test Parquet path
        parquet_path = manager.get_master_table_path("products_master", "parquet")
        expected = Path(tmpdir) / "master" / "products_master.parquet"
        assert parquet_path == expected, f"Expected {expected}, got {parquet_path}"
        print(f"  ✓ Parquet path: {parquet_path}")

    print("  ✓ Master table paths working correctly\n")


def test_fact_table_paths():
    """Test fact table path resolution with partitions."""
    print("Testing fact table paths...")

    with tempfile.TemporaryDirectory() as tmpdir:
        manager = ExportFileManager(Path(tmpdir))

        # Test CSV path with date partition
        test_date = date(2024, 1, 15)
        csv_path = manager.get_fact_table_path("receipts", test_date, "csv")
        expected = Path(tmpdir) / "facts" / "receipts" / "dt=2024-01-15" / "receipts_2024-01-15.csv"
        assert csv_path == expected, f"Expected {expected}, got {csv_path}"
        print(f"  ✓ CSV path: {csv_path}")

        # Test Parquet path
        parquet_path = manager.get_fact_table_path("online_orders", test_date, "parquet")
        expected = Path(tmpdir) / "facts" / "online_orders" / "dt=2024-01-15" / "online_orders_2024-01-15.parquet"
        assert parquet_path == expected, f"Expected {expected}, got {parquet_path}"
        print(f"  ✓ Parquet path: {parquet_path}")

    print("  ✓ Fact table paths working correctly\n")


def test_directory_creation():
    """Test directory creation."""
    print("Testing directory creation...")

    with tempfile.TemporaryDirectory() as tmpdir:
        manager = ExportFileManager(Path(tmpdir))

        # Test master directory creation
        master_path = manager.get_master_table_path("stores", "csv")
        manager.ensure_directory(master_path.parent)
        assert master_path.parent.exists(), f"Directory not created: {master_path.parent}"
        print(f"  ✓ Master directory created: {master_path.parent}")

        # Test nested fact directory creation
        fact_path = manager.get_fact_table_path("receipts", date(2024, 1, 15), "csv")
        manager.ensure_directory(fact_path.parent)
        assert fact_path.parent.exists(), f"Directory not created: {fact_path.parent}"
        print(f"  ✓ Fact directory created: {fact_path.parent}")

    print("  ✓ Directory creation working correctly\n")


def test_file_tracking_and_cleanup():
    """Test file tracking and cleanup."""
    print("Testing file tracking and cleanup...")

    with tempfile.TemporaryDirectory() as tmpdir:
        manager = ExportFileManager(Path(tmpdir))

        # Create and track files
        path1 = manager.get_master_table_path("stores", "csv")
        path2 = manager.get_master_table_path("customers", "csv")

        manager.ensure_directory(path1.parent)
        path1.touch()
        path2.touch()

        manager.track_file(path1)
        manager.track_file(path2)

        assert manager.get_tracked_file_count() == 2, "Should track 2 files"
        print(f"  ✓ Tracking {manager.get_tracked_file_count()} files")

        # Verify files exist
        assert path1.exists() and path2.exists(), "Files should exist"

        # Cleanup
        manager.cleanup()

        # Verify files removed
        assert not path1.exists() and not path2.exists(), "Files should be removed"
        assert manager.get_tracked_file_count() == 0, "Tracking should be cleared"
        print(f"  ✓ Cleanup removed files and cleared tracking")

    print("  ✓ File tracking and cleanup working correctly\n")


def test_path_validation():
    """Test path validation security."""
    print("Testing path validation...")

    with tempfile.TemporaryDirectory() as tmpdir:
        manager = ExportFileManager(Path(tmpdir))

        # Try to escape base directory
        try:
            invalid_path = Path(tmpdir).parent / "evil.csv"
            manager._validate_path(invalid_path)
            assert False, "Should have raised ValueError for path outside base dir"
        except ValueError as e:
            print(f"  ✓ Correctly rejected path outside base dir: {e}")

    print("  ✓ Path validation working correctly\n")


def main():
    """Run all tests."""
    print("=" * 60)
    print("ExportFileManager Verification Tests")
    print("=" * 60 + "\n")

    try:
        test_master_table_paths()
        test_fact_table_paths()
        test_directory_creation()
        test_file_tracking_and_cleanup()
        test_path_validation()

        print("=" * 60)
        print("✅ All tests passed!")
        print("=" * 60)
        return 0

    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
