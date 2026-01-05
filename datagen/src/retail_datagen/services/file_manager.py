"""
File system manager for data export functionality.

This module provides utilities for managing file paths, directory creation,
and cleanup operations for exported master and fact data.
"""

import logging
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)


class ExportFileManager:
    """
    Manages file paths and directories for data export operations.

    Responsibilities:
    - Resolve output paths for master tables (data/export/<table>/<table>.parquet)
    - Resolve output paths for fact tables (Parquet monthly files):
        data/export/<table>/<table>_YYYY-MM.parquet
    - Create necessary directories including partitions
    - Track files written during export for potential rollback
    - Validate paths are within allowed directories

    Usage:
        manager = ExportFileManager(base_dir=Path("data"))

        # Get path for master table
        path = manager.get_master_table_path("stores", "parquet")
        manager.ensure_directory(path.parent)

        # Write file and track it
        write_data_to_file(path)
        manager.track_file(path)

        # On success: reset tracking
        manager.reset_tracking()

        # On failure: cleanup
        manager.cleanup()
    """

    def __init__(self, base_dir: Path):
        """
        Initialize the file manager.

        Args:
            base_dir: Base directory for all exports (e.g., Path("data"))
        """
        self.base_dir = base_dir.resolve()  # Convert to absolute path
        self.written_files: list[Path] = []

        logger.debug(f"ExportFileManager initialized with base_dir: {self.base_dir}")

    def get_master_table_path(
        self, table_name: str, format: Literal["parquet"]
    ) -> Path:
        """
        Get output path for a master table.

        Path pattern: data/export/<table>/<table>.parquet

        Args:
            table_name: Name of the master table (e.g., "stores", "products_master")
            format: Output format ("parquet")

        Returns:
            Path object for the master table file

        Example:
            >>> manager.get_master_table_path("products_master", "parquet")
            Path("data/export/products_master/products_master.parquet")
        """
        extension = f".{format}"
        path = self.base_dir / "export" / table_name / f"{table_name}{extension}"

        # Validate path is within base directory
        self._validate_path(path)

        logger.debug(f"Resolved master table path: {path}")
        return path

    def get_fact_table_month_path(
        self, table_name: str, year: int, month: int, format: Literal["parquet"]
    ) -> Path:
        """
        Get output path for a fact table monthly file (primarily for Parquet).

        Path pattern: data/export/<table>/<table>_YYYY-MM.parquet

        Args:
            table_name: Fact table name
            year: Year component
            month: Month component (1-12)
            format: Output format ("parquet")

        Returns:
            Path object for the monthly file
        """
        ym = f"{year:04d}-{month:02d}"
        extension = f".{format}"
        filename = f"{table_name}_{ym}{extension}"
        path = self.base_dir / "export" / table_name / filename
        self._validate_path(path)
        logger.debug(f"Resolved monthly fact table path: {path}")
        return path

    def ensure_directory(self, path: Path) -> None:
        """
        Create directory if it doesn't exist, including parent directories.

        This method is idempotent - safe to call multiple times for the same path.

        Args:
            path: Directory path to create (or file path - will create parent)

        Raises:
            OSError: If directory creation fails due to permissions or disk space
            ValueError: If path is not within base directory

        Example:
            >>> path = manager.get_fact_table_month_path("receipts", 2024, 1, "parquet")
            >>> manager.ensure_directory(path.parent)  # Creates data/export/receipts/
        """
        # Validate path is within base directory
        self._validate_path(path)

        # If path is a file, get parent directory
        directory = path if path.is_dir() or not path.suffix else path.parent

        try:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
        except OSError as e:
            logger.error(f"Failed to create directory {directory}: {e}")
            raise OSError(
                f"Failed to create directory {directory}. "
                f"Check permissions and disk space. Error: {e}"
            ) from e

    def track_file(self, file_path: Path) -> None:
        """
        Track a file for potential cleanup (rollback).

        Files are tracked in order of creation, which is important for
        cleanup order in case of dependencies.

        Args:
            file_path: Path to the file to track

        Example:
            >>> path = manager.get_master_table_path("stores", "parquet")
            >>> write_data(path)
            >>> manager.track_file(path)
        """
        # Convert to absolute path for consistent tracking
        absolute_path = file_path.resolve()

        # Validate path is within base directory
        self._validate_path(absolute_path)

        if absolute_path not in self.written_files:
            self.written_files.append(absolute_path)
            logger.debug(f"Tracking file: {absolute_path}")
        else:
            logger.debug(f"File already tracked: {absolute_path}")

    def cleanup(self) -> None:
        """
        Remove all tracked files (rollback operation).

        Files are removed in reverse order of tracking to handle any dependencies.
        Errors during cleanup are logged but don't stop the process - we attempt
        to clean up as many files as possible.

        Example:
            >>> manager.track_file(path1)
            >>> manager.track_file(path2)
            >>> # Something went wrong
            >>> manager.cleanup()  # Removes path2, then path1
        """
        if not self.written_files:
            logger.info("No files to clean up")
            return

        logger.info(f"Starting cleanup of {len(self.written_files)} tracked files")

        # Remove files in reverse order (most recent first)
        removed_count = 0
        error_count = 0

        for file_path in reversed(self.written_files):
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.debug(f"Removed file: {file_path}")
                    removed_count += 1
                else:
                    logger.debug(f"File does not exist (already removed?): {file_path}")
            except OSError as e:
                logger.warning(f"Failed to remove file {file_path}: {e}")
                error_count += 1

        logger.info(
            f"Cleanup complete: {removed_count} files removed, "
            f"{error_count} errors, "
            f"{len(self.written_files) - removed_count - error_count} already gone"
        )

        # Clear tracking list after cleanup attempt
        self.written_files.clear()

    def reset_tracking(self) -> None:
        """
        Clear the list of tracked files without removing them.

        Call this after a successful export to indicate files should be kept.

        Example:
            >>> manager.track_file(path1)
            >>> manager.track_file(path2)
            >>> # Export succeeded
            >>> manager.reset_tracking()  # Keep files, clear tracking
        """
        file_count = len(self.written_files)
        self.written_files.clear()
        logger.info(f"Reset tracking: cleared {file_count} tracked files")

    def _validate_path(self, path: Path) -> None:
        """
        Validate that a path is within the allowed base directory.

        This security check prevents directory traversal attacks and ensures
        all operations stay within the intended data directory.

        Args:
            path: Path to validate

        Raises:
            ValueError: If path is outside base directory

        Example:
            >>> manager._validate_path(Path("data/export/stores/stores.parquet"))  # OK
            >>> manager._validate_path(Path("/etc/passwd"))  # Raises ValueError
        """
        try:
            # Convert both to absolute paths for comparison
            absolute_path = path.resolve()
            absolute_base = self.base_dir.resolve()

            # Check if path is relative to base directory
            absolute_path.relative_to(absolute_base)
        except ValueError:
            error_msg = (
                f"Path {path} is outside allowed base directory {self.base_dir}. "
                f"This may indicate a security issue or configuration error."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

    def get_tracked_file_count(self) -> int:
        """
        Get the number of currently tracked files.

        Returns:
            Number of files being tracked for potential cleanup

        Example:
            >>> manager.track_file(path1)
            >>> manager.track_file(path2)
            >>> manager.get_tracked_file_count()
            2
        """
        return len(self.written_files)

    def get_tracked_files(self) -> list[Path]:
        """
        Get a copy of the list of tracked files.

        Returns a copy to prevent external modification of internal state.

        Returns:
            List of tracked file paths

        Example:
            >>> files = manager.get_tracked_files()
            >>> for f in files:
            ...     print(f"Tracked: {f}")
        """
        return self.written_files.copy()
