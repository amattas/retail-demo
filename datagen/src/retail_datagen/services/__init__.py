"""
Services module for retail data generator.

This module provides reusable service components for data export,
transformation, and other cross-cutting concerns.
"""

from .export_service import ExportService
from .file_manager import ExportFileManager
from .writers import BaseWriter, CSVWriter, ParquetWriter
from .db_reader import (
    read_master_table,
    read_all_master_tables,
    read_fact_table,
    read_all_fact_tables,
    get_table_row_count,
    get_fact_table_date_range,
    get_all_fact_table_date_ranges,
    MASTER_TABLES,
    FACT_TABLES,
    DEFAULT_CHUNK_SIZE,
)

__all__ = [
    # Export service
    "ExportService",
    # File management
    "ExportFileManager",
    # Writers
    "BaseWriter",
    "CSVWriter",
    "ParquetWriter",
    # Master table reading
    "read_master_table",
    "read_all_master_tables",
    # Fact table reading
    "read_fact_table",
    "read_all_fact_tables",
    # Utility functions
    "get_table_row_count",
    "get_fact_table_date_range",
    "get_all_fact_table_date_ranges",
    # Constants
    "MASTER_TABLES",
    "FACT_TABLES",
    "DEFAULT_CHUNK_SIZE",
]
