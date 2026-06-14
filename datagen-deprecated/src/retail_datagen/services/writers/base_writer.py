"""
Abstract base class for data format writers.

This module defines the interface that all format writers must implement,
providing a consistent API for writing DataFrames to different file formats.
"""

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class BaseWriter(ABC):
    """
    Abstract base class for data format writers.

    Defines the interface for writing pandas DataFrames to various file formats
    with support for both simple and partitioned outputs.
    """

    @abstractmethod
    def write(self, df: pd.DataFrame, output_path: Path, **kwargs) -> None:
        """
        Write a DataFrame to a single file.

        Args:
            df: DataFrame to write
            output_path: Path where the file should be written
            **kwargs: Additional format-specific options

        Raises:
            ValueError: If DataFrame is empty
            IOError: If file cannot be written
        """
        pass

    @abstractmethod
    def write_partitioned(
        self,
        df: pd.DataFrame,
        output_dir: Path,
        partition_col: str,
        table_name: str | None = None,
        **kwargs,
    ) -> list[Path]:
        """
        Write a DataFrame partitioned by a column value.

        Creates subdirectories for each unique partition value and writes
        separate files for each partition. Follows the format:
        <output_dir>/<partition_col>=<value>/<table_name>_<value>.<ext>

        Args:
            df: DataFrame to write
            output_dir: Base directory for partitioned output
            partition_col: Column name to partition by
            table_name: Optional table name for file naming (defaults to 'data')
            **kwargs: Additional format-specific options

        Returns:
            List of paths to created files

        Raises:
            ValueError: If DataFrame is empty or partition column doesn't exist
            IOError: If files cannot be written
        """
        pass
