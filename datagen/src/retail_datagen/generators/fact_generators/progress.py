"""
Progress tracking and reporting functionality for fact data generation.
"""

from __future__ import annotations

import logging
from threading import Lock

logger = logging.getLogger(__name__)


class HourlyProgressTracker:
    """
    Thread-safe tracker for hourly progress across fact tables.

    Tracks progress on a per-table, per-day, per-hour basis to enable fine-grained
    progress reporting during historical data generation.

    Thread-safety is achieved using a threading.Lock to protect all shared state.
    """

    def __init__(self, fact_tables: list[str]):
        """
        Initialize the hourly progress tracker.

        Args:
            fact_tables: List of fact table names to track
        """
        self._fact_tables = fact_tables
        self._lock = Lock()

        # Track completed hours per table: {table: {day: {hour: True}}}
        self._progress_data: dict[str, dict[int, dict[int, bool]]] = {}

        # Track current position for each table
        self._current_day: dict[str, int] = {}
        self._current_hour: dict[str, int] = {}

        # Total days for progress calculation
        self._total_days = 0

        # Initialize tracking structures
        for table in fact_tables:
            self._progress_data[table] = {}
            self._current_day[table] = 0
            self._current_hour[table] = 0

        logger.debug(f"HourlyProgressTracker initialized for {len(fact_tables)} tables")

    def update_hourly_progress(
        self, table: str, day: int, hour: int, total_days: int
    ) -> None:
        """
        Update progress for a specific table after completing an hour.

        This method is thread-safe.

        Args:
            table: Name of the fact table
            day: Day number (1-indexed)
            hour: Hour of day (0-23)
            total_days: Total number of days being generated
        """
        with self._lock:
            # Validate inputs
            if table not in self._fact_tables:
                logger.warning(
                    f"Attempted to update progress for unknown table: {table}"
                )
                return

            if not (0 <= hour <= 23):
                logger.warning(f"Invalid hour value: {hour} (must be 0-23)")
                return

            # Update total days if changed
            self._total_days = total_days

            # Initialize day structure if needed
            if day not in self._progress_data[table]:
                self._progress_data[table][day] = {}

            # Mark hour as completed
            self._progress_data[table][day][hour] = True

            # Update current position
            self._current_day[table] = day
            self._current_hour[table] = hour

            logger.debug(
                f"Progress updated: {table} day {day} hour {hour} "
                f"({self._count_completed_hours(table)}/({total_days}*24) hours)"
            )

    def get_current_progress(self) -> dict:
        """
        Get current progress state for all tables.

        Returns:
            Dictionary containing:
            - overall_progress: float (0.0 to 1.0) - aggregate progress
              across all tables
            - tables_in_progress: list[str] - tables currently being processed
            - current_day: int - most recent day being processed
            - current_hour: int - most recent hour being processed
            - per_table_progress: dict[str, float] - progress for each table
              (0.0 to 1.0)
            - completed_hours: dict[str, int] - completed hours per table
        """
        with self._lock:
            # Calculate per-table progress
            per_table_progress = {}
            completed_hours_map = {}
            tables_in_progress = []

            total_hours_expected = self._total_days * 24 if self._total_days > 0 else 1

            for table in self._fact_tables:
                completed_hours = self._count_completed_hours(table)
                completed_hours_map[table] = completed_hours

                # Calculate progress as fraction of total hours
                progress = (
                    completed_hours / total_hours_expected
                    if total_hours_expected > 0
                    else 0.0
                )
                per_table_progress[table] = min(1.0, progress)

                # Table is in progress if it has completed some hours but not all
                if 0 < progress < 1.0:
                    tables_in_progress.append(table)

            # Calculate overall progress based on hours completed
            # (not per-table average). Since all tables are generated
            # hour-by-hour together, use max hours completed
            max_completed_hours = (
                max(completed_hours_map.values()) if completed_hours_map else 0
            )
            overall_progress = (
                max_completed_hours / total_hours_expected
                if total_hours_expected > 0
                else 0.0
            )

            # All tables are "in progress" until all hours are complete
            # (since they move together). Only show tables as in_progress
            # if we've started and haven't finished
            if 0 < overall_progress < 1.0:
                tables_in_progress = sorted(self._fact_tables)
            else:
                tables_in_progress = []

            # Find the most advanced position across all tables.
            # Return None instead of 0 to avoid validation issues
            # (current_day must be >= 1)
            max_day = max(self._current_day.values()) if self._current_day else None
            max_hour = max(self._current_hour.values()) if self._current_hour else None

            return {
                "overall_progress": min(1.0, overall_progress),
                "tables_in_progress": tables_in_progress,
                "current_day": max_day,
                "current_hour": max_hour,
                "per_table_progress": per_table_progress,
                "completed_hours": completed_hours_map,
                "total_days": self._total_days,
            }

    def reset(self) -> None:
        """
        Reset all tracking state.

        Called when starting a new generation run.
        """
        with self._lock:
            self._progress_data = {}
            self._current_day = {}
            self._current_hour = {}
            self._total_days = 0

            # Reinitialize structures for each table
            for table in self._fact_tables:
                self._progress_data[table] = {}
                self._current_day[table] = 0
                self._current_hour[table] = 0

            logger.debug("HourlyProgressTracker reset")

    def _count_completed_hours(self, table: str) -> int:
        """
        Count total completed hours for a table.

        Must be called with lock held.

        Args:
            table: Table name

        Returns:
            Total number of completed hours
        """
        if table not in self._progress_data:
            return 0

        total = 0
        for day_hours in self._progress_data[table].values():
            total += len(day_hours)

        return total
