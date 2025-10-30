"""
Parquet format writer implementation.

This module provides Parquet writing functionality with support for partitioned
outputs using pyarrow engine and optimized compression settings.
"""

import logging
from pathlib import Path

import pandas as pd

from retail_datagen.services.writers.base_writer import BaseWriter

logger = logging.getLogger(__name__)


class ParquetWriter(BaseWriter):
    """
    Parquet format writer with partitioning support.

    Writes pandas DataFrames to Parquet files using pyarrow engine with
    optimized compression settings for analytical workloads.
    """

    def __init__(
        self, engine: str = "pyarrow", compression: str = "snappy", **default_kwargs
    ):
        """
        Initialize Parquet writer.

        Args:
            engine: Parquet engine to use (default: 'pyarrow')
            compression: Compression algorithm (default: 'snappy')
            **default_kwargs: Default arguments passed to pandas to_parquet()
        """
        self.engine = engine
        self.compression = compression
        self.default_kwargs = default_kwargs

    def write(self, df: pd.DataFrame, output_path: Path, **kwargs) -> None:
        """
        Write DataFrame to a single Parquet file.

        Args:
            df: DataFrame to write
            output_path: Path where the Parquet file should be written
            **kwargs: Additional arguments passed to pandas to_parquet()

        Raises:
            ValueError: If DataFrame is empty
            IOError: If file cannot be written
            ImportError: If pyarrow is not installed
        """
        if df.empty:
            raise ValueError("Cannot write empty DataFrame")

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Merge default kwargs with provided kwargs
        write_kwargs = {**self.default_kwargs, **kwargs}
        if "engine" not in write_kwargs:
            write_kwargs["engine"] = self.engine
        if "compression" not in write_kwargs:
            write_kwargs["compression"] = self.compression

        try:
            df.to_parquet(output_path, **write_kwargs)
            logger.info(f"Wrote {len(df):,} records to {output_path}")
        except ImportError as e:
            logger.error(
                f"pyarrow is required for Parquet writing. "
                f"Install with: pip install pyarrow"
            )
            raise ImportError(
                "pyarrow is required for Parquet writing. "
                "Install with: pip install pyarrow"
            ) from e
        except Exception as e:
            logger.error(f"Failed to write Parquet to {output_path}: {e}")
            raise IOError(f"Failed to write Parquet file: {e}") from e

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
        separate Parquet files for each partition. Format:
        <output_dir>/<partition_col>=<value>/<table_name>_<value>.parquet

        Args:
            df: DataFrame to write
            output_dir: Base directory for partitioned output
            partition_col: Column name to partition by
            table_name: Optional table name for file naming (defaults to 'data')
            **kwargs: Additional arguments passed to pandas to_parquet()

        Returns:
            List of paths to created Parquet files

        Raises:
            ValueError: If DataFrame is empty or partition column doesn't exist
            IOError: If files cannot be written
            ImportError: If pyarrow is not installed
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

            # Create output file: <table_name>_<value>.parquet
            output_file = partition_dir / f"{table_name}_{partition_value}.parquet"

            # Write partition
            self.write(partition_df, output_file, **kwargs)
            created_files.append(output_file)

        logger.info(
            f"Created {len(created_files)} partitioned Parquet files in {output_dir}"
        )
        return created_files
