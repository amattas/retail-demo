"""
Export service orchestrator for data export functionality.

This module provides the main ExportService class that coordinates DuckDB reading,
format writing, and file management to perform complete export operations.

The ExportService brings together:
- DuckDB reader (duckdb_reader) for reading master and fact tables
- Parquet writer for writing data (Parquet-only)
- File manager (ExportFileManager) for path resolution and cleanup

Usage:
    from pathlib import Path
    from retail_datagen.services import ExportService
    # Initialize service
    service = ExportService(base_dir=Path("data"))

    # Export master tables to Parquet
    master_files = await service.export_master_tables(
        None,
        format="parquet",
        progress_callback=lambda msg, curr, total: print(f"{msg}: {curr}/{total}")
    )

    # Export fact tables to Parquet with date filtering
    fact_files = await service.export_fact_tables(
        None,
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

from retail_datagen.services import duckdb_reader
from retail_datagen.services.file_manager import ExportFileManager
from retail_datagen.services.writers import BaseWriter, ParquetWriter

logger = logging.getLogger(__name__)

# Type aliases for clarity
ExportFormat = Literal["parquet"]
ProgressCallback = Callable[[str, int, int], None]


class ExportService:
    """
    Main export service orchestrator.

    Coordinates database reading, format writing, and file management to perform
    complete export operations for master and fact tables.

    Features:
    - Export all master dimension tables
    - Export all fact tables with optional date filtering
    - Parquet-only format (monthly for facts)
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
            format: Output format ("parquet")

        Returns:
            BaseWriter instance (ParquetWriter)
        """
        if format == "parquet":
            logger.debug("Creating ParquetWriter instance")
            return ParquetWriter(engine="pyarrow", compression="snappy")
        else:
            raise ValueError("Only 'parquet' export is supported")

    async def export_master_tables(
        self,
        session: None,
        format: ExportFormat,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Path]:
        """
        Export all master dimension tables.

        Reads all 6 master tables from the database and writes them to files
        under data/export/<table>/<table>.parquet.

        Args:
            session: Unused; kept for backward compatibility (DuckDB-only)
            format: Output format ("parquet")
            progress_callback: Optional callback for progress updates
                Signature: callback(message: str, current: int, total: int)

        Returns:
            Dictionary mapping table names to output file paths:
            { "dim_stores": Path("data/export/dim_stores/dim_stores.parquet"), ... }

        Raises:
            Exception: If export fails (after attempting cleanup)

        Example:
            >>> files = await service.export_master_tables(None, format="parquet")
        """
        logger.info(f"Starting master table export (format={format})")

        try:
            # Read all master tables from DuckDB
            logger.debug("Reading all master tables from DuckDB")
            all_master_data = duckdb_reader.read_all_master_tables()

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

            logger.info(f"Master table export complete: {len(result)} tables")

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
        session: None,
        format: ExportFormat,
        start_date: date | None = None,
        end_date: date | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, list[Path]]:
        """
        Export all fact tables with optional date filtering.

        Reads all fact tables from the database and writes monthly Parquet files
        to data/export/<table>/<table>_YYYY-MM.parquet.

        Args:
            session: Unused; kept for backward compatibility (DuckDB-only)
            format: Output format ("parquet")
            start_date: Optional start date for filtering (inclusive)
            end_date: Optional end date for filtering (inclusive)
            progress_callback: Optional callback for progress updates
                Signature: callback(message: str, current: int, total: int)

        Returns:
            Dictionary mapping table names to lists of monthly Parquet files.

        Raises:
            Exception: If export fails (after attempting cleanup)

        Example:
            >>> files = await service.export_fact_tables(
            ...     None,
            ...     format="parquet",
            ...     start_date=date(2024, 1, 1),
            ...     end_date=date(2024, 1, 31)
            ... )
            >>> print(f"Exported {len(files)} fact tables")
        """
        logger.info(
            f"Starting fact table export "
            f"(format={format}, start_date={start_date}, end_date={end_date})"
        )

        try:
            # Read all fact tables from DuckDB with date filtering
            logger.debug("Reading all fact tables from DuckDB")
            all_fact_data = duckdb_reader.read_all_fact_tables(
                start_date, end_date
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

                # Clear old export files for this table to prevent append mode
                # from adding to stale data (only on first chunk)
                # Note: This is now handled by the router clearing before the loop

                # Identify a timestamp column for partitioning
                ts_candidates = [
                    "event_ts",
                    "picked_ts",
                    "shipped_ts",
                    "delivered_ts",
                    "completed_ts",
                    "eta",
                    "etd",
                ]
                ts_col = next((c for c in ts_candidates if c in df.columns), None)

                # Special handling for fact_online_order_lines:
                # Use COALESCE(picked_ts, shipped_ts, delivered_ts, order_event_ts)
                # to partition pending orders by their creation time
                if table_name == "fact_online_order_lines" and "order_event_ts" in df.columns:
                    # Create a partition timestamp that falls back to order creation time
                    df["_partition_ts"] = df["picked_ts"].combine_first(
                        df["shipped_ts"]
                    ).combine_first(
                        df["delivered_ts"]
                    ).combine_first(
                        df["order_event_ts"]
                    )
                    ts_col = "_partition_ts"
                    # Drop the order_event_ts column from final output (it was just for partitioning)
                    df = df.drop(columns=["order_event_ts"])

                if not ts_col:
                    raise ValueError(
                        f"Cannot determine timestamp column for table {table_name}; columns={list(df.columns)}"
                    )

                # Normalize timestamp dtype
                df[ts_col] = pd.to_datetime(df[ts_col])

                partition_files: list[Path] = []

                if format == "parquet":
                    # Monthly partition for Parquet
                    df["ym"] = df[ts_col].dt.to_period("M")
                    months = sorted(df["ym"].unique())
                    logger.info(
                        f"Table {table_name} contains {len(df):,} rows across {len(months)} month partitions"
                    )
                    for per in months:
                        month_mask = df["ym"] == per
                        part_df = df.loc[month_mask].copy()
                        # Drop temporary columns used for partitioning
                        cols_to_drop = ["ym"]
                        if "_partition_ts" in part_df.columns:
                            cols_to_drop.append("_partition_ts")
                        part_df = part_df.drop(columns=cols_to_drop)
                        year = int(str(per).split("-")[0])
                        month = int(str(per).split("-")[1])
                        output_path = self.file_manager.get_fact_table_month_path(
                            table_name, year, month, format
                        )
                        self.file_manager.ensure_directory(output_path.parent)
                        logger.debug(
                            f"Writing {len(part_df):,} rows to {output_path} (monthly)"
                        )
                        # Use append=True to support chunked exports that may span months
                        writer.write(part_df, output_path, append=True)
                        self.file_manager.track_file(output_path)
                        partition_files.append(output_path)
                else:
                    raise ValueError("Only 'parquet' export is supported")

                result[table_name] = partition_files
                logger.info(
                    f"Successfully exported {table_name}: {len(df):,} rows across {len(partition_files)} partitions"
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
