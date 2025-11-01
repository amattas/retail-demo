"""
CSV format writer implementation.

This module provides CSV writing functionality with support for partitioned
outputs, matching the format used by existing generators in the codebase.
"""

import logging
from pathlib import Path

import pandas as pd

from retail_datagen.services.writers.base_writer import BaseWriter

logger = logging.getLogger(__name__)


class CSVWriter(BaseWriter):
    """
    CSV format writer with partitioning support.

    Writes pandas DataFrames to CSV files using the same format and conventions
    as the existing fact and master data generators.
    """

    def __init__(self, index: bool = False, **default_kwargs):
        """
        Initialize CSV writer.

        Args:
            index: Whether to write row indices (default: False)
            **default_kwargs: Default arguments passed to pandas to_csv()
        """
        self.index = index
        self.default_kwargs = default_kwargs

    def write(self, df: pd.DataFrame, output_path: Path, **kwargs) -> None:
        """
        Write DataFrame to a single CSV file.

        Args:
            df: DataFrame to write
            output_path: Path where the CSV file should be written
            **kwargs: Additional arguments passed to pandas to_csv()

        Raises:
            ValueError: If DataFrame is empty
            IOError: If file cannot be written
        """
        if df.empty:
            raise ValueError("Cannot write empty DataFrame")

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Merge default kwargs with provided kwargs
        write_kwargs = {**self.default_kwargs, **kwargs}
        if "index" not in write_kwargs:
            write_kwargs["index"] = self.index

        try:
            df.to_csv(output_path, **write_kwargs)
            logger.info(f"Wrote {len(df):,} records to {output_path}")
        except Exception as e:
            logger.error(f"Failed to write CSV to {output_path}: {e}")
            raise OSError(f"Failed to write CSV file: {e}") from e

    def write_partitioned(
        self,
        df: pd.DataFrame,
        output_dir: Path,
        partition_col: str,
        table_name: str | None = None,
        **kwargs,
    ) -> list[Path]:
        """
        Write DataFrame partitioned by column value.

        Creates subdirectories for each unique partition value and writes
        separate CSV files for each partition. Format:
        <output_dir>/<partition_col>=<value>/<table_name>_<value>.csv

        Args:
            df: DataFrame to write
            output_dir: Base directory for partitioned output
            partition_col: Column name to partition by
            table_name: Optional table name for file naming (defaults to 'data')
            **kwargs: Additional arguments passed to pandas to_csv()

        Returns:
            List of paths to created CSV files

        Raises:
            ValueError: If DataFrame is empty or partition column doesn't exist
            IOError: If files cannot be written
        """
        if df.empty:
            raise ValueError("Cannot write empty DataFrame")

        if partition_col not in df.columns:
            raise ValueError(
                f"Partition column '{partition_col}' not found in DataFrame"
            )

        table_name = table_name or "data"
        created_files: list[Path] = []

        # Group by partition column
        grouped = df.groupby(partition_col)
        total_partitions = len(grouped)

        logger.info(
            f"Writing {len(df):,} records to {total_partitions} partitions "
            f"by '{partition_col}'"
        )

        # Write each partition
        for partition_value, partition_df in grouped:
            # Create partition directory: <partition_col>=<value>
            partition_dir = output_dir / f"{partition_col}={partition_value}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Create output file: <table_name>_<value>.csv
            output_file = partition_dir / f"{table_name}_{partition_value}.csv"

            # Write partition
            self.write(partition_df, output_file, **kwargs)
            created_files.append(output_file)

        logger.info(
            f"Created {len(created_files)} partitioned CSV files in {output_dir}"
        )
        return created_files
