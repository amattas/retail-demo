"""
Services module for retail data generator.

This module provides reusable service components for data export,
transformation, and other cross-cutting concerns.
"""

from .duckdb_reader import (
    FACT_TABLES,
    MASTER_TABLES,
    get_all_fact_table_date_ranges,  # type: ignore[attr-defined]
    get_fact_table_date_range,  # type: ignore[attr-defined]
    read_all_fact_tables,
    read_all_master_tables,
)
from .export_service import ExportService
from .file_manager import ExportFileManager
from .writers import BaseWriter, ParquetWriter

__all__ = [
    # Export service
    "ExportService",
    # File management
    "ExportFileManager",
    # Writers
    "BaseWriter",
    "ParquetWriter",
    # Readers (DuckDB)
    "read_all_master_tables",
    "read_all_fact_tables",
    # Date range helpers (DuckDB)
    "get_fact_table_date_range",
    "get_all_fact_table_date_ranges",
    # Constants (DuckDB)
    "MASTER_TABLES",
    "FACT_TABLES",
]
