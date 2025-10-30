"""
Export service orchestrator for data export functionality.

This module provides the main ExportService class that coordinates database reading,
format writing, and file management to perform complete export operations.

The ExportService brings together:
- Database reader (db_reader) for reading master and fact tables
- Format writers (CSVWriter, ParquetWriter) for writing data
- File manager (ExportFileManager) for path resolution and cleanup

Usage:
    from pathlib import Path
    from retail_datagen.services import ExportService
    from retail_datagen.db.session import get_retail_session

    # Initialize service
    service = ExportService(base_dir=Path("data"))

    # Export master tables to CSV
    async with get_retail_session() as session:
        master_files = await service.export_master_tables(
            session,
            format="csv",
            progress_callback=lambda msg, curr, total: print(f"{msg}: {curr}/{total}")
        )

    # Export fact tables to Parquet with date filtering
    async with get_retail_session() as session:
        fact_files = await service.export_fact_tables(
            session,
            format="parquet",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
"""

import logging
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Literal

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from retail_datagen.services import db_reader
from retail_datagen.services.file_manager import ExportFileManager
from retail_datagen.services.writers import BaseWriter, CSVWriter, ParquetWriter

logger = logging.getLogger(__name__)

# Type aliases for clarity
ExportFormat = Literal["csv", "parquet"]
ProgressCallback = Callable[[str, int, int], None]


class ExportService:
    """
    Main export service orchestrator.

    Coordinates database reading, format writing, and file management to perform
    complete export operations for master and fact tables.

    Features:
    - Export all master dimension tables
    - Export all fact tables with optional date filtering
    - Support for CSV and Parquet formats
    - Progress callbacks for UI integration
    - Automatic cleanup on failure (rollback)

    Attributes:
        base_dir: Base directory for all exports (e.g., Path("data"))
        file_manager: ExportFileManager instance for path resolution and cleanup
    """

    def __init__(self, base_dir: Path):
        """
        Initialize the export service.

        Args:
            base_dir: Base directory for all exports (e.g., Path("data"))

        Example:
            >>> service = ExportService(base_dir=Path("data"))
        """
        self.base_dir = base_dir
        self.file_manager = ExportFileManager(base_dir)
        logger.info(f"ExportService initialized with base_dir: {base_dir}")

    def _get_writer(self, format: ExportFormat) -> BaseWriter:
        """
        Get appropriate writer instance for the specified format.

        Args:
            format: Output format ("csv" or "parquet")

        Returns:
            BaseWriter instance (CSVWriter or ParquetWriter)

        Example:
            >>> writer = service._get_writer("csv")
            >>> isinstance(writer, CSVWriter)
            True
        """
        if format == "csv":
            logger.debug("Creating CSVWriter instance")
            return CSVWriter(index=False)
        elif format == "parquet":
            logger.debug("Creating ParquetWriter instance")
            return ParquetWriter(engine="pyarrow", compression="snappy")
        else:
            # This should never happen due to Literal type, but included for safety
            raise ValueError(f"Unsupported format: {format}")

    async def export_master_tables(
        self,
        session: AsyncSession,
        format: ExportFormat,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Path]:
        """
        Export all master dimension tables.

        Reads all 6 master tables from the database and writes them to files
        in the data/master/ directory. Format: data/master/<table>.<ext>

        Args:
            session: AsyncSession for database operations
            format: Output format ("csv" or "parquet")
            progress_callback: Optional callback for progress updates
                Signature: callback(message: str, current: int, total: int)

        Returns:
            Dictionary mapping table names to output file paths:
            {
                "dim_geographies": Path("data/master/dim_geographies.csv"),
                "dim_stores": Path("data/master/dim_stores.csv"),
                ...
            }

        Raises:
            Exception: If export fails (after attempting cleanup)

        Example:
            >>> async with get_retail_session() as session:
            ...     files = await service.export_master_tables(
            ...         session,
            ...         format="csv",
            ...         progress_callback=lambda msg, curr, total: print(f"{msg}: {curr}/{total}")
            ...     )
            ...     print(f"Exported {len(files)} master tables")
        """
        logger.info(f"Starting master table export (format={format})")

        try:
            # Read all master tables from database
            logger.debug("Reading all master tables from database")
            all_master_data = await db_reader.read_all_master_tables(session)

            # Get writer for the specified format
            writer = self._get_writer(format)

            # Track results
            result: dict[str, Path] = {}
            total_tables = len(all_master_data)
            current_table = 0

            # Export each master table
            for table_name, df in all_master_data.items():
                current_table += 1

                # Report progress
                if progress_callback:
                    progress_callback(
                        f"Exporting {table_name}", current_table, total_tables
                    )

                logger.info(
                    f"Exporting master table {current_table}/{total_tables}: {table_name}"
                )

                # Skip empty tables
                if df.empty:
                    logger.warning(f"Skipping empty master table: {table_name}")
                    continue

                # Get output path from file manager
                output_path = self.file_manager.get_master_table_path(
                    table_name, format
                )

                # Ensure directory exists
                self.file_manager.ensure_directory(output_path.parent)

                # Write data
                logger.debug(f"Writing {len(df):,} rows to {output_path}")
                writer.write(df, output_path)

                # Track file for potential rollback
                self.file_manager.track_file(output_path)

                # Add to results
                result[table_name] = output_path

                logger.info(
                    f"Successfully exported {table_name}: {len(df):,} rows to {output_path}"
                )

            # Success - reset file tracking (don't cleanup)
            self.file_manager.reset_tracking()

            logger.info(
                f"Master table export complete: {len(result)} tables, "
                f"{sum(len(pd.read_csv(p) if format == 'csv' else pd.read_parquet(p)) for p in result.values() if p.exists())} total rows"
            )

            return result

        except Exception as e:
            logger.error(f"Master table export failed: {e}", exc_info=True)

            # Cleanup any files written before the error
            logger.info("Attempting to cleanup partial export")
            self.file_manager.cleanup()

            # Re-raise exception for caller to handle
            raise

    async def export_fact_tables(
        self,
        session: AsyncSession,
        format: ExportFormat,
        start_date: date | None = None,
        end_date: date | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, list[Path]]:
        """
        Export all fact tables with optional date filtering.

        Reads all 9 fact tables from the database and writes them to partitioned
        files in the data/facts/ directory. Format:
        data/facts/<table>/dt=YYYY-MM-DD/<table>_YYYY-MM-DD.<ext>

        Args:
            session: AsyncSession for database operations
            format: Output format ("csv" or "parquet")
            start_date: Optional start date for filtering (inclusive)
            end_date: Optional end date for filtering (inclusive)
            progress_callback: Optional callback for progress updates
                Signature: callback(message: str, current: int, total: int)

        Returns:
            Dictionary mapping table names to lists of partition file paths:
            {
                "fact_receipts": [
                    Path("data/facts/fact_receipts/dt=2024-01-01/fact_receipts_2024-01-01.csv"),
                    Path("data/facts/fact_receipts/dt=2024-01-02/fact_receipts_2024-01-02.csv"),
                    ...
                ],
                ...
            }

        Raises:
            Exception: If export fails (after attempting cleanup)

        Example:
            >>> async with get_retail_session() as session:
            ...     files = await service.export_fact_tables(
            ...         session,
            ...         format="parquet",
            ...         start_date=date(2024, 1, 1),
            ...         end_date=date(2024, 1, 31)
            ...     )
            ...     print(f"Exported {len(files)} fact tables")
            ...     for table, paths in files.items():
            ...         print(f"  {table}: {len(paths)} partitions")
        """
        logger.info(
            f"Starting fact table export "
            f"(format={format}, start_date={start_date}, end_date={end_date})"
        )

        try:
            # Read all fact tables from database with date filtering
            logger.debug("Reading all fact tables from database")
            all_fact_data = await db_reader.read_all_fact_tables(
                session, start_date, end_date
            )

            # Get writer for the specified format
            writer = self._get_writer(format)

            # Track results
            result: dict[str, list[Path]] = {}
            total_tables = len(all_fact_data)
            current_table = 0

            # Export each fact table
            for table_name, df in all_fact_data.items():
                current_table += 1

                # Report progress
                if progress_callback:
                    progress_callback(
                        f"Exporting {table_name}", current_table, total_tables
                    )

                logger.info(
                    f"Exporting fact table {current_table}/{total_tables}: {table_name}"
                )

                # Skip empty tables
                if df.empty:
                    logger.warning(f"Skipping empty fact table: {table_name}")
                    result[table_name] = []
                    continue

                # Extract partition dates from event_ts column
                # Convert datetime to date for partitioning
                if "event_ts" not in df.columns:
                    logger.error(f"Fact table {table_name} missing event_ts column")
                    raise ValueError(
                        f"Fact table {table_name} must have event_ts column"
                    )

                # Add date partition column
                df["dt"] = pd.to_datetime(df["event_ts"]).dt.date

                # Get unique dates for partition tracking
                unique_dates = sorted(df["dt"].unique())
                logger.info(
                    f"Table {table_name} contains {len(df):,} rows "
                    f"across {len(unique_dates)} date partitions"
                )

                # Write partitioned data
                partition_files: list[Path] = []

                for partition_date in unique_dates:
                    # Filter data for this partition
                    partition_df = df[df["dt"] == partition_date].copy()

                    # Remove the temporary dt column before writing
                    partition_df = partition_df.drop(columns=["dt"])

                    # Get output path from file manager
                    output_path = self.file_manager.get_fact_table_path(
                        table_name, partition_date, format
                    )

                    # Ensure directory exists
                    self.file_manager.ensure_directory(output_path.parent)

                    # Write partition data
                    logger.debug(f"Writing {len(partition_df):,} rows to {output_path}")
                    writer.write(partition_df, output_path)

                    # Track file for potential rollback
                    self.file_manager.track_file(output_path)

                    # Add to partition files
                    partition_files.append(output_path)

                # Add to results
                result[table_name] = partition_files

                logger.info(
                    f"Successfully exported {table_name}: "
                    f"{len(df):,} rows across {len(partition_files)} partitions"
                )

            # Success - reset file tracking (don't cleanup)
            self.file_manager.reset_tracking()

            total_partitions = sum(len(files) for files in result.values())
            logger.info(
                f"Fact table export complete: {len(result)} tables, "
                f"{total_partitions} total partitions"
            )

            return result

        except Exception as e:
            logger.error(f"Fact table export failed: {e}", exc_info=True)

            # Cleanup any files written before the error
            logger.info("Attempting to cleanup partial export")
            self.file_manager.cleanup()

            # Re-raise exception for caller to handle
            raise
