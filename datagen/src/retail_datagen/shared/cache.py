"""
Caching mechanism for dashboard statistics.

This module provides a simple file-based cache for storing table counts
to improve dashboard performance.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class TableCount(BaseModel):
    """Model for a single table's count information."""

    table_name: str
    count: int
    table_type: str  # "master" or "fact"
    last_updated: datetime = Field(default_factory=datetime.now)


class DashboardCache(BaseModel):
    """Model for the dashboard statistics cache."""

    master_tables: dict[str, TableCount] = Field(default_factory=dict)
    fact_tables: dict[str, TableCount] = Field(default_factory=dict)
    last_updated: datetime = Field(default_factory=datetime.now)


class CacheManager:
    """Manager for dashboard statistics cache."""

    def __init__(self, cache_path: str = "data/cache/dashboard_stats.json"):
        """
        Initialize cache manager.

        Args:
            cache_path: Path to the cache file
        """
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)

    def load_cache(self) -> DashboardCache:
        """
        Load cache from disk.

        Returns:
            DashboardCache object, empty if file doesn't exist
        """
        if not self.cache_path.exists():
            return DashboardCache()

        try:
            with open(self.cache_path, encoding="utf-8") as f:
                data = json.load(f)
                return DashboardCache(**data)
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
            return DashboardCache()

    def save_cache(self, cache: DashboardCache) -> None:
        """
        Save cache to disk.

        Args:
            cache: DashboardCache object to save
        """
        try:
            cache.last_updated = datetime.now()
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(cache.model_dump(mode="json"), f, indent=2, default=str)
            logger.info(f"Cache saved successfully to {self.cache_path}")
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")

    def update_master_table(
        self, table_name: str, count: int, table_type: str = "master"
    ) -> None:
        """
        Update count for a master table.

        Args:
            table_name: Name of the table
            count: Number of records
            table_type: Type of table (default: "master")
        """
        cache = self.load_cache()
        cache.master_tables[table_name] = TableCount(
            table_name=table_name, count=count, table_type=table_type
        )
        self.save_cache(cache)

    def update_fact_table(
        self, table_name: str, count: int, table_type: str = "fact"
    ) -> None:
        """
        Update count for a fact table.

        Args:
            table_name: Name of the table
            count: Number of records
            table_type: Type of table (default: "fact")
        """
        cache = self.load_cache()
        cache.fact_tables[table_name] = TableCount(
            table_name=table_name, count=count, table_type=table_type
        )
        self.save_cache(cache)

    def get_table_count(
        self, table_name: str, table_type: str = "master"
    ) -> int | None:
        """
        Get count for a specific table.

        Args:
            table_name: Name of the table
            table_type: Type of table ("master" or "fact")

        Returns:
            Count if cached, None otherwise
        """
        cache = self.load_cache()
        if table_type == "master":
            table_count = cache.master_tables.get(table_name)
        else:
            table_count = cache.fact_tables.get(table_name)

        return table_count.count if table_count else None

    def get_all_counts(self) -> dict[str, Any]:
        """
        Get all cached counts.

        Returns:
            Dictionary with master and fact table counts
        """
        cache = self.load_cache()
        return {
            "master_tables": {
                name: count.count for name, count in cache.master_tables.items()
            },
            "fact_tables": {
                name: count.count for name, count in cache.fact_tables.items()
            },
            "last_updated": cache.last_updated.isoformat(),
        }

    def clear_cache(self) -> None:
        """Clear all cached data."""
        if self.cache_path.exists():
            self.cache_path.unlink()
            logger.info("Cache cleared")
