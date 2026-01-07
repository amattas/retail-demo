"""
Data models and dataclasses for fact generation.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class FactGenerationSummary:
    """Summary of fact data generation results."""

    date_range: tuple[datetime, datetime]
    facts_generated: dict[str, int]
    total_records: int
    validation_results: dict[str, Any]
    generation_time_seconds: float
    partitions_created: int


@dataclass
class MasterTableSpec:
    """Deprecated: CSV-based master specs removed in DuckDB-only mode."""

    attr_name: str
    filename: str
    model_cls: type[Any]
    dtype: dict[str, Any] | None = None
    row_adapter: Callable[[dict[str, Any]], dict[str, Any]] | None = None


