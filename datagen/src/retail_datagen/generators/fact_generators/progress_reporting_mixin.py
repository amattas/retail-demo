"""
Progress reporting and tracking methods for FactDataGenerator.
"""

from __future__ import annotations

import inspect
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from ..progress_tracker import TableProgressTracker

logger = logging.getLogger(__name__)


class ProgressReportingMixin:
    """Progress reporting and callback management for fact generation."""

    def _reset_table_states(self) -> None:
        """Reset table states using progress tracker."""
        active_tables = self._active_fact_tables()
        self._progress_tracker = TableProgressTracker(active_tables)
        self._progress_history = []
        # Initialize batch buffers for batched inserts (by hour)
        self._batch_buffers: dict[str, list[dict]] = {t: [] for t in self.FACT_TABLES}

    def _calculate_eta(self, current_progress: float) -> float | None:
        """
        Calculate estimated seconds remaining based on progress rate.

        Args:
            current_progress: Current progress as a fraction (0.0 to 1.0)

        Returns:
            Estimated seconds remaining, or None if not enough data
        """
        if len(self._progress_history) < 2:
            return None

        # Calculate progress rate from history
        oldest = self._progress_history[0]
        newest = self._progress_history[-1]
        time_elapsed = newest[0] - oldest[0]
        progress_made = newest[1] - oldest[1]

        if progress_made <= 0 or time_elapsed <= 0:
            return None

        progress_rate = progress_made / time_elapsed  # progress per second
        remaining_progress = 1.0 - current_progress

        if progress_rate > 0:
            return remaining_progress / progress_rate

        return None

    # Per-table progress (master-style), similar to MasterDataGenerator

    def set_table_progress_callback(
        self,
        callback: Callable[[str, float, str | None, dict | None], None] | None,
    ) -> None:
        self._table_progress_callback = callback

    def set_progress_callback(
        self,
        callback: Callable[[int, str, dict], None] | None,
    ) -> None:
        """
        Set the day-based progress callback for historical generation.

        The callback will be invoked with progress updates during generation.
        Matches the pattern used by MasterDataGenerator for consistency.

        Args:
            callback: Progress callback function(day_num, message, **kwargs), or None to clear
        """
        self._progress_callback = callback

    def _emit_table_progress(
        self,
        table_name: str,
        progress: float,
        message: str | None = None,
        table_counts: dict | None = None,
    ) -> None:
        if not self._table_progress_callback:
            return
        try:
            clamped = max(0.0, min(1.0, progress))
            self._table_progress_callback(table_name, clamped, message, table_counts)
        except Exception as e:
            logger.warning(f"Progress callback failed for table {table_name}: {e}")

    # Removed legacy CSV loader: DuckDB-only path is used for master data

    async def load_master_data_from_db(self) -> None:
        """Deprecated SQLite path removed; DuckDB-only runtime."""
        raise RuntimeError("SQLite master load is not supported. Use DuckDB loader.")

    def _send_throttled_progress_update(
        self,
        day_counter: int,
        message: str,
        total_days: int,
        table_progress: dict[str, float] | None = None,
        tables_completed: list[str] | None = None,
        tables_in_progress: list[str] | None = None,
        tables_remaining: list[str] | None = None,
        tables_failed: list[str] | None = None,
        table_counts: dict[str, int] | None = None,
    ) -> None:
        """
        Send progress update to callback with throttling and ETA calculation.

        Updates are throttled to minimum 100ms intervals to ensure they're
        visible to users and don't overwhelm the API.

        Args:
            day_counter: Current day number
            message: Progress message
            total_days: Total number of days
            table_progress: Per-table progress percentages
            tables_completed: List of completed tables
            tables_in_progress: List of in-progress tables
            tables_remaining: List of not-started tables
            tables_failed: List of failed tables
        """
        if not self._progress_callback:
            logger.warning(f"Progress callback is None! Cannot send update: {message}")
            return

        thread_name = threading.current_thread().name
        logger.info(
            f"[PROGRESS][{thread_name}] Callback exists, sending update: {message[:50]}"
        )
        with self._progress_lock:
            current_time = time.time()
            progress = day_counter / total_days if total_days > 0 else 0.0

            # Throttle: Skip update if too soon (less than 50ms since last update)
            time_since_last = current_time - self._last_progress_update_time
            if time_since_last < 0.05:
                logger.debug(
                    f"[{thread_name}] Throttling progress update (too soon: {time_since_last * 1000:.1f}ms < 50ms)"
                )
                return

            # Update progress history for ETA calculation
            self._progress_history.append((current_time, progress))
            if len(self._progress_history) > 10:
                self._progress_history.pop(0)

            # Calculate ETA
            eta = self._calculate_eta(progress)

            # Calculate progress rate (for informational purposes)
            progress_rate = None
            if eta is not None and (1.0 - progress) > 0:
                progress_rate = (1.0 - progress) / eta

            # Determine current table (first in_progress table, if any)
            current_table = None
            if tables_in_progress and len(tables_in_progress) > 0:
                current_table = tables_in_progress[0]

            # Get hourly progress data from tracker
            hourly_progress_data = self.hourly_tracker.get_current_progress()

            callback_kwargs = {
                "table_progress": table_progress.copy() if table_progress else None,
                "current_table": current_table,
                "tables_completed": (tables_completed or []).copy(),
                "tables_failed": (tables_failed or []).copy(),
                "tables_in_progress": (tables_in_progress or []).copy()
                if tables_in_progress is not None
                else [],
                "tables_remaining": (tables_remaining or []).copy()
                if tables_remaining is not None
                else [],
                "estimated_seconds_remaining": eta,
                "progress_rate": progress_rate,
                "table_counts": table_counts,
                # NEW: Add hourly progress fields (note: current_day is passed as first positional arg, don't duplicate)
                "current_hour": hourly_progress_data.get("current_hour"),
                "hourly_progress": hourly_progress_data.get("per_table_progress"),
                # Use the most advanced table's completed hours; all tables move together
                "total_hours_completed": (
                    max(hourly_progress_data.get("completed_hours", {}).values())
                    if hourly_progress_data.get("completed_hours")
                    else 0
                ),
            }

            filtered_kwargs = self._filter_progress_kwargs(callback_kwargs)

            # Send the progress update
            try:
                self._progress_callback(day_counter, message, **filtered_kwargs)
                logger.debug(
                    f"Progress update sent: {progress:.2%} (day {day_counter}/{total_days}) "
                    f"ETA: {eta:.0f}s, tables_in_progress: {tables_in_progress}"
                    if eta
                    else f"at {current_time:.3f}"
                )
            except TypeError:
                # Fallback for old callbacks that only accept 2 parameters
                try:
                    self._progress_callback(day_counter, message)
                    logger.debug(
                        f"Progress update sent (legacy): {progress:.2%} at {current_time:.3f}"
                    )
                except TypeError:
                    logger.debug(
                        "Legacy progress callback invocation failed; "
                        "suppressing TypeError to preserve generation flow"
                    )

            # Update last update timestamp
            self._last_progress_update_time = current_time

    def _filter_progress_kwargs(
        self, candidate_kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        """Return only the keyword arguments supported by the progress callback."""
        callback = self._progress_callback
        if not callback:
            return {}

        # Drop fields that have no value so legacy callbacks don't see noisy kwargs
        cleaned_kwargs = {
            key: value for key, value in candidate_kwargs.items() if value is not None
        }
        if not cleaned_kwargs:
            return {}

        try:
            signature = inspect.signature(callback)
        except (TypeError, ValueError):
            # If the signature can't be inspected, assume callback can handle everything we pass now
            return cleaned_kwargs

        if any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in signature.parameters.values()
        ):
            return cleaned_kwargs

        accepted_names: set[str] = set()
        for name, param in signature.parameters.items():
            if param.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                accepted_names.add(name)

        # Remove the first positional parameters since we pass them positionally (day, message)
        positional_count = 0
        for name, param in signature.parameters.items():
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                if positional_count < 2:
                    accepted_names.discard(name)
                    positional_count += 1
                continue
            break

        if not accepted_names:
            return {}

        return {
            key: value for key, value in cleaned_kwargs.items() if key in accepted_names
        }
