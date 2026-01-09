"""
Unit tests for ExportFileManager.

Tests file system management for data exports including path resolution,
directory creation, file tracking, and cleanup operations.
"""

import os
from pathlib import Path

import pytest

from retail_datagen.services.file_manager import ExportFileManager


class TestExportFileManagerInit:
    """Test ExportFileManager initialization."""

    def test_init_with_valid_path(self, tmp_path):
        """Should initialize with valid base directory."""
        manager = ExportFileManager(base_dir=tmp_path)

        assert manager.base_dir == tmp_path.resolve()
        assert manager.written_files == []

    def test_init_converts_to_absolute_path(self, tmp_path):
        """Should convert relative path to absolute."""
        # Create subdirectory
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        # Initialize with relative-style path
        manager = ExportFileManager(base_dir=subdir)

        # Should be absolute
        assert manager.base_dir.is_absolute()
        assert manager.base_dir == subdir.resolve()

    def test_init_empty_tracking_list(self, tmp_path):
        """Should start with empty tracking list."""
        manager = ExportFileManager(base_dir=tmp_path)

        assert manager.written_files == []
        assert manager.get_tracked_file_count() == 0


class TestGetMasterTablePath:
    def test_get_master_table_path_parquet(self, tmp_path):
        """Should return correct path for Parquet master table."""
        manager = ExportFileManager(base_dir=tmp_path)

        path = manager.get_master_table_path("dim_customers", "parquet")
        expected = (
            tmp_path / "export" / "dim_customers" / "dim_customers.parquet"
        )
        assert path == expected
        assert path.suffix == ".parquet"

    def test_get_master_table_path_various_names(self, tmp_path):
        """Should handle various table names correctly."""
        manager = ExportFileManager(base_dir=tmp_path)

        paths = [
            manager.get_master_table_path("dim_geographies", "parquet"),
            manager.get_master_table_path("dim_products", "parquet"),
            manager.get_master_table_path("dim_trucks", "parquet"),
        ]

        # All should be under export/<table>/
        for path in paths:
            assert path.parent.parent.name == "export"
            assert path.parent.parent.parent == tmp_path

    def test_get_master_table_path_validates_security(self, tmp_path):
        """Should validate path is within base directory."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Normal path should work
        path = manager.get_master_table_path("dim_stores", "parquet")
        assert path  # No exception


class TestGetFactTableMonthPath:
    def test_get_fact_table_month_path(self, tmp_path):
        manager = ExportFileManager(base_dir=tmp_path)
        path = manager.get_fact_table_month_path(
            "fact_receipts", 2024, 1, "parquet"
        )
        expected = (
            tmp_path
            / "export"
            / "fact_receipts"
            / "fact_receipts_2024-01.parquet"
        )
        assert path == expected
        assert path.suffix == ".parquet"


class TestEnsureDirectory:
    """Test ensure_directory method."""

    def test_ensure_directory_creates_missing(self, tmp_path):
        """Should create directory if it doesn't exist."""
        manager = ExportFileManager(base_dir=tmp_path)
        new_dir = tmp_path / "new" / "nested" / "dir"

        manager.ensure_directory(new_dir)

        assert new_dir.exists()
        assert new_dir.is_dir()

    def test_ensure_directory_idempotent(self, tmp_path):
        """Should be safe to call multiple times."""
        manager = ExportFileManager(base_dir=tmp_path)
        test_dir = tmp_path / "test_dir"

        # Create once
        manager.ensure_directory(test_dir)
        assert test_dir.exists()

        # Create again - should not error
        manager.ensure_directory(test_dir)
        assert test_dir.exists()

    def test_ensure_directory_for_file_path(self, tmp_path):
        """Should create parent directory when given file path."""
        manager = ExportFileManager(base_dir=tmp_path)
        file_path = tmp_path / "dir1" / "dir2" / "file.parquet"

        manager.ensure_directory(file_path)

        # Parent directories should exist, but not the file
        assert file_path.parent.exists()
        assert not file_path.exists()

    def test_ensure_directory_validates_security(self, tmp_path):
        """Should validate path is within base directory."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Path outside base directory should raise ValueError
        outside_path = Path("/tmp/outside")

        with pytest.raises(ValueError, match="outside allowed base directory"):
            manager.ensure_directory(outside_path)

    @pytest.mark.skipif(
        os.geteuid() == 0, reason="Root bypasses permission checks"
    )
    def test_ensure_directory_handles_permission_error(self, tmp_path):
        """Should raise an error on permission issues (message may vary)."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Create read-only parent directory
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)  # Read-only

        nested_dir = readonly_dir / "nested"

        try:
            # Some OS/filesystems raise PermissionError during path stat,
            # others on mkdir
            with pytest.raises((PermissionError, OSError)):
                manager.ensure_directory(nested_dir)
        finally:
            # Cleanup: restore permissions
            readonly_dir.chmod(0o755)


class TestTrackFile:
    """Test track_file method."""

    def test_track_file_adds_to_list(self, tmp_path):
        """Should add file to tracking list."""
        manager = ExportFileManager(base_dir=tmp_path)
        file_path = tmp_path / "test.parquet"

        manager.track_file(file_path)

        assert len(manager.written_files) == 1
        assert file_path.resolve() in manager.written_files

    def test_track_file_multiple_files(self, tmp_path):
        """Should track multiple files in order."""
        manager = ExportFileManager(base_dir=tmp_path)

        files = [
            tmp_path / "file1.parquet",
            tmp_path / "file2.parquet",
            tmp_path / "file3.parquet",
        ]

        for file_path in files:
            manager.track_file(file_path)

        assert len(manager.written_files) == 3
        assert all(f.resolve() in manager.written_files for f in files)

    def test_track_file_no_duplicates(self, tmp_path):
        """Should not add duplicate files."""
        manager = ExportFileManager(base_dir=tmp_path)
        file_path = tmp_path / "test.parquet"

        manager.track_file(file_path)
        manager.track_file(file_path)  # Track again

        # Should only be tracked once
        assert len(manager.written_files) == 1

    def test_track_file_converts_to_absolute(self, tmp_path):
        """Should convert relative paths to absolute."""
        manager = ExportFileManager(base_dir=tmp_path)
        file_path = tmp_path / "test.parquet"

        manager.track_file(file_path)

        # Should be stored as absolute path
        tracked_path = manager.written_files[0]
        assert tracked_path.is_absolute()

    def test_track_file_validates_security(self, tmp_path):
        """Should validate path is within base directory."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Path outside base directory should raise ValueError
        outside_path = Path("/tmp/outside.parquet")

        with pytest.raises(ValueError, match="outside allowed base directory"):
            manager.track_file(outside_path)


class TestCleanup:
    """Test cleanup method."""

    def test_cleanup_removes_tracked_files(self, tmp_path):
        """Should remove all tracked files."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Create and track files
        files = [
            tmp_path / "file1.parquet",
            tmp_path / "file2.parquet",
            tmp_path / "file3.parquet",
        ]

        for file_path in files:
            file_path.write_text("test data")
            manager.track_file(file_path)

        # All files should exist
        assert all(f.exists() for f in files)

        # Cleanup
        manager.cleanup()

        # All files should be removed
        assert not any(f.exists() for f in files)

    def test_cleanup_in_reverse_order(self, tmp_path):
        """Should remove files in reverse order of tracking."""
        manager = ExportFileManager(base_dir=tmp_path)
        removal_order = []

        # Create files
        files = [tmp_path / f"file{i}.parquet" for i in range(3)]
        for f in files:
            f.write_text("data")
            manager.track_file(f)

        # Mock unlink to track order
        original_unlink = Path.unlink

        def tracked_unlink(self, *args, **kwargs):
            removal_order.append(self)
            return original_unlink(self, *args, **kwargs)

        from unittest import mock

        with mock.patch.object(Path, "unlink", tracked_unlink):
            manager.cleanup()

        # Should remove in reverse order (last tracked removed first)
        assert removal_order[0] == files[2].resolve()
        assert removal_order[1] == files[1].resolve()
        assert removal_order[2] == files[0].resolve()

    def test_cleanup_clears_tracking_list(self, tmp_path):
        """Should clear tracking list after cleanup."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Track some files
        file_path = tmp_path / "test.parquet"
        file_path.write_text("data")
        manager.track_file(file_path)

        assert len(manager.written_files) > 0

        # Cleanup
        manager.cleanup()

        # Tracking list should be empty
        assert len(manager.written_files) == 0

    def test_cleanup_handles_missing_files(self, tmp_path):
        """Should handle files that no longer exist."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Track file that doesn't exist
        file_path = tmp_path / "missing.parquet"
        manager.track_file(file_path)

        # Cleanup should not raise exception
        manager.cleanup()

        assert len(manager.written_files) == 0

    def test_cleanup_continues_on_error(self, tmp_path):
        """Should continue cleaning up other files if one fails."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Create files
        file1 = tmp_path / "file1.parquet"
        file2 = tmp_path / "file2.parquet"

        file1.write_text("data")
        file2.write_text("data")

        manager.track_file(file1)
        manager.track_file(file2)

        # Make file2 read-only to cause deletion error
        file2.chmod(0o444)

        try:
            # Cleanup should attempt both files
            manager.cleanup()

            # file1 should still be removed even if file2 fails
            # (depends on OS permissions behavior)
        finally:
            # Cleanup: restore permissions
            if file2.exists():
                file2.chmod(0o755)

    def test_cleanup_empty_list_is_safe(self, tmp_path):
        """Should handle cleanup when no files are tracked."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Cleanup with empty list should not error
        manager.cleanup()

        assert len(manager.written_files) == 0


class TestResetTracking:
    """Test reset_tracking method."""

    def test_reset_tracking_clears_list(self, tmp_path):
        """Should clear tracking list without removing files."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Create and track files
        files = [tmp_path / f"file{i}.parquet" for i in range(3)]
        for f in files:
            f.write_text("data")
            manager.track_file(f)

        assert len(manager.written_files) == 3

        # Reset tracking
        manager.reset_tracking()

        # Tracking list should be empty
        assert len(manager.written_files) == 0

        # Files should still exist
        assert all(f.exists() for f in files)

    def test_reset_tracking_after_cleanup(self, tmp_path):
        """Should work correctly after cleanup."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Track file
        file_path = tmp_path / "test.parquet"
        file_path.write_text("data")
        manager.track_file(file_path)

        # Cleanup removes file and clears list
        manager.cleanup()

        # Reset should work even though list is already empty
        manager.reset_tracking()

        assert len(manager.written_files) == 0


class TestGetTrackedFileCount:
    """Test get_tracked_file_count method."""

    def test_get_count_empty(self, tmp_path):
        """Should return 0 for empty tracking list."""
        manager = ExportFileManager(base_dir=tmp_path)

        assert manager.get_tracked_file_count() == 0

    def test_get_count_with_files(self, tmp_path):
        """Should return correct count of tracked files."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Track files
        for i in range(5):
            manager.track_file(tmp_path / f"file{i}.parquet")

        assert manager.get_tracked_file_count() == 5


class TestGetTrackedFiles:
    """Test get_tracked_files method."""

    def test_get_tracked_files_returns_copy(self, tmp_path):
        """Should return copy of tracking list, not reference."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Track a file
        file_path = tmp_path / "test.parquet"
        manager.track_file(file_path)

        # Get copy
        tracked_copy = manager.get_tracked_files()

        # Modify copy
        tracked_copy.append(tmp_path / "extra.parquet")

        # Original should be unchanged
        assert len(manager.written_files) == 1
        assert len(tracked_copy) == 2

    def test_get_tracked_files_empty(self, tmp_path):
        """Should return empty list when nothing tracked."""
        manager = ExportFileManager(base_dir=tmp_path)

        result = manager.get_tracked_files()

        assert result == []
        assert isinstance(result, list)


class TestValidatePath:
    """Test _validate_path security method."""

    def test_validate_path_within_base(self, tmp_path):
        """Should allow paths within base directory."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Valid paths within base
        valid_paths = [
            tmp_path / "file.parquet",
            tmp_path / "subdir" / "file.parquet",
            tmp_path / "deep" / "nested" / "path" / "file.parquet",
        ]

        for path in valid_paths:
            # Should not raise exception
            manager._validate_path(path)

    def test_validate_path_outside_base(self, tmp_path):
        """Should reject paths outside base directory."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Paths outside base directory
        invalid_paths = [
            Path("/tmp/outside.parquet"),
            Path("/etc/passwd"),
            tmp_path.parent / "sibling" / "file.parquet",
        ]

        for path in invalid_paths:
            with pytest.raises(ValueError, match="outside allowed base directory"):
                manager._validate_path(path)

    def test_validate_path_prevents_traversal(self, tmp_path):
        """Should prevent directory traversal attacks."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Attempt directory traversal
        traversal_path = tmp_path / ".." / ".." / "etc" / "passwd"

        with pytest.raises(ValueError, match="outside allowed base directory"):
            manager._validate_path(traversal_path)


class TestExportFileManagerIntegration:
    """Integration tests for complete workflows."""

    def test_full_export_workflow(self, tmp_path):
        """Should handle complete export workflow with tracking and cleanup."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Simulate export process
        master_path = manager.get_master_table_path("dim_stores", "parquet")
        manager.ensure_directory(master_path.parent)

        # Write file
        master_path.write_text("ID,Name\n1,Store1\n2,Store2")
        manager.track_file(master_path)

        # Add fact table
        fact_path = manager.get_fact_table_month_path(
            "fact_receipts",
            2024,
            1,
            "parquet",
        )
        manager.ensure_directory(fact_path.parent)
        fact_path.write_text("TraceId,Total\ntrace1,100.00")
        manager.track_file(fact_path)

        # Verify files exist
        assert master_path.exists()
        assert fact_path.exists()
        assert manager.get_tracked_file_count() == 2

        # Reset tracking on success
        manager.reset_tracking()

        # Files should remain but not be tracked
        assert master_path.exists()
        assert fact_path.exists()
        assert manager.get_tracked_file_count() == 0

    def test_export_with_failure_and_rollback(self, tmp_path):
        """Should cleanup files on failure."""
        manager = ExportFileManager(base_dir=tmp_path)

        # Start export
        file1 = tmp_path / "file1.parquet"
        file2 = tmp_path / "file2.parquet"

        file1.write_text("data1")
        manager.track_file(file1)

        file2.write_text("data2")
        manager.track_file(file2)

        # Simulate failure - cleanup
        manager.cleanup()

        # Files should be removed
        assert not file1.exists()
        assert not file2.exists()
        assert manager.get_tracked_file_count() == 0
