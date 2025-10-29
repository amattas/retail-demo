"""
Shared progress tracking for data generation.

This module provides TableProgressTracker for managing table states and progress
during master and historical data generation. It separates progress percentages
(for progress bars) from completion states (for UI icons).

Key concept: Tables remain "in_progress" throughout generation and only transition
to "completed" when mark_generation_complete() is called, ensuring icons don't
turn green prematurely.
"""

import logging
import threading

logger = logging.getLogger(__name__)


class TableProgressTracker:
    """
    Thread-safe tracker for table generation states and progress.

    Manages the lifecycle of table generation, separating visual progress (0.0-1.0)
    from completion state (not_started/in_progress/completed). This ensures that
    UI elements like progress bars and status icons can be updated independently.

    State Transitions:
        not_started → in_progress (when mark_table_started() called)
        in_progress → completed (when mark_generation_complete() called)

    Thread Safety:
        All public methods are thread-safe using an internal lock.

    Attributes:
        _states: Dictionary mapping table names to their current state.
        _progress: Dictionary mapping table names to progress percentages (0.0-1.0).
        _lock: Threading lock for synchronizing access to internal state.
    """

    # Valid states for table generation
    STATE_NOT_STARTED = "not_started"
    STATE_IN_PROGRESS = "in_progress"
    STATE_COMPLETED = "completed"

    def __init__(self, table_names: list[str]) -> None:
        """
        Initialize tracker with list of table names to track.

        Args:
            table_names: List of table names to track. All tables start in
                        'not_started' state with 0.0 progress.
        """
        self._lock = threading.Lock()
        self._states: dict[str, str] = {}
        self._progress: dict[str, float] = {}

        # Initialize all tables to not_started with 0.0 progress
        for table_name in table_names:
            self._states[table_name] = self.STATE_NOT_STARTED
            self._progress[table_name] = 0.0

        logger.debug(f"Initialized TableProgressTracker with {len(table_names)} tables")

    def reset(self) -> None:
        """
        Reset all tables to not_started state with 0.0 progress.

        This is useful when starting a new generation run.
        """
        with self._lock:
            for table_name in self._states.keys():
                self._states[table_name] = self.STATE_NOT_STARTED
                self._progress[table_name] = 0.0

            logger.debug(f"Reset {len(self._states)} tables to not_started state")

    def mark_table_started(self, table_name: str) -> None:
        """
        Mark a table as in_progress when generation starts for it.

        This transitions the table from not_started to in_progress state.

        Args:
            table_name: Name of the table that has started generation.

        Raises:
            KeyError: If table_name is not being tracked.
        """
        with self._lock:
            if table_name not in self._states:
                raise KeyError(f"Table '{table_name}' is not being tracked")

            old_state = self._states[table_name]
            self._states[table_name] = self.STATE_IN_PROGRESS

            logger.debug(
                f"Table '{table_name}' state transition: "
                f"{old_state} → {self.STATE_IN_PROGRESS}"
            )

    def update_progress(self, table_name: str, progress: float) -> None:
        """
        Update progress percentage (0.0-1.0) for a table. Does NOT change state.

        This method only updates the progress value and does not affect the
        table's state. Tables remain in_progress until mark_generation_complete()
        is explicitly called.

        Args:
            table_name: Name of the table to update.
            progress: Progress value between 0.0 and 1.0.

        Raises:
            KeyError: If table_name is not being tracked.
            ValueError: If progress is not between 0.0 and 1.0.
        """
        if not 0.0 <= progress <= 1.0:
            raise ValueError(f"Progress must be between 0.0 and 1.0, got {progress}")

        with self._lock:
            if table_name not in self._progress:
                raise KeyError(f"Table '{table_name}' is not being tracked")

            old_progress = self._progress[table_name]
            self._progress[table_name] = progress

            # Log only significant progress changes (every 10%) to reduce noise
            if int(old_progress * 10) != int(progress * 10):
                logger.debug(
                    f"Table '{table_name}' progress: "
                    f"{old_progress:.1%} → {progress:.1%}"
                )

    def mark_generation_complete(self) -> None:
        """
        Mark all in_progress tables as completed.

        Called when entire generation finishes. This is the ONLY method that
        transitions tables to the completed state.
        It ensures that tables don't show as completed prematurely while generation
        is still ongoing.
        """
        with self._lock:
            completed_count = 0

            for table_name, state in self._states.items():
                if state == self.STATE_IN_PROGRESS:
                    self._states[table_name] = self.STATE_COMPLETED
                    completed_count += 1

            logger.debug(
                f"Marked {completed_count} tables as completed (generation finished)"
            )

    def get_state(self, table_name: str) -> str:
        """
        Get current state: 'not_started', 'in_progress', or 'completed'.

        Args:
            table_name: Name of the table to query.

        Returns:
            Current state of the table.

        Raises:
            KeyError: If table_name is not being tracked.
        """
        with self._lock:
            if table_name not in self._states:
                raise KeyError(f"Table '{table_name}' is not being tracked")

            return self._states[table_name]

    def get_progress(self, table_name: str) -> float:
        """
        Get current progress percentage (0.0-1.0).

        Args:
            table_name: Name of the table to query.

        Returns:
            Current progress value between 0.0 and 1.0.

        Raises:
            KeyError: If table_name is not being tracked.
        """
        with self._lock:
            if table_name not in self._progress:
                raise KeyError(f"Table '{table_name}' is not being tracked")

            return self._progress[table_name]

    def get_tables_by_state(self, state: str) -> list[str]:
        """
        Get list of tables in the given state.

        Args:
            state: State to filter by ('not_started', 'in_progress', or 'completed').

        Returns:
            List of table names in the specified state.
        """
        with self._lock:
            return [
                table_name
                for table_name, table_state in self._states.items()
                if table_state == state
            ]

    def get_all_states(self) -> dict[str, str]:
        """
        Get dictionary mapping table names to states.

        Returns:
            Copy of the internal states dictionary.
        """
        with self._lock:
            return self._states.copy()

    def get_all_progress(self) -> dict[str, float]:
        """
        Get dictionary mapping table names to progress percentages.

        Returns:
            Copy of the internal progress dictionary.
        """
        with self._lock:
            return self._progress.copy()
