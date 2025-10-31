"""
Generation state tracking for historical and real-time data generation.

This module manages the state of data generation, tracking the last generated
timestamp to enable incremental data generation and proper sequencing between
historical and real-time modes.
"""

import json
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field


class GenerationState(BaseModel):
    """Track the state of data generation."""

    last_generated_timestamp: datetime | None = Field(
        None, description="Last generated data timestamp"
    )
    historical_start_date: datetime | None = Field(
        None, description="Configured historical start date"
    )
    has_historical_data: bool = Field(
        False, description="Whether historical data has been generated"
    )
    last_historical_run: datetime | None = Field(
        None, description="Last historical generation run time"
    )
    last_realtime_run: datetime | None = Field(
        None, description="Last real-time generation run time"
    )

    # Note: Pydantic v2 handles datetime serialization automatically
    # No need for custom model_dump_json override


class GenerationStateManager:
    """Manages generation state persistence and logic."""

    def __init__(self, state_file_path: str = "data/generation_state.json"):
        self.state_file = Path(state_file_path)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state: GenerationState | None = None

    def load_state(self) -> GenerationState:
        """Load generation state from file."""
        if self._state is not None:
            return self._state

        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    data = json.load(f)
                    # Handle datetime parsing
                    for field in [
                        "last_generated_timestamp",
                        "historical_start_date",
                        "last_historical_run",
                        "last_realtime_run",
                    ]:
                        if data.get(field):
                            data[field] = datetime.fromisoformat(data[field])
                    self._state = GenerationState(**data)
            except Exception as e:
                print(f"Warning: Could not load generation state: {e}")
                self._state = GenerationState()
        else:
            self._state = GenerationState()

        return self._state

    def save_state(self) -> None:
        """Save generation state to file."""
        if self._state is None:
            return

        try:
            with open(self.state_file, "w") as f:
                f.write(self._state.model_dump_json(indent=2))
        except Exception as e:
            print(f"Warning: Could not save generation state: {e}")

    def get_historical_date_range(
        self, config_start_date: datetime
    ) -> tuple[datetime, datetime]:
        """
        Get the date range for historical data generation.

        Returns:
            (start_date, end_date) tuple where:
            - start_date: Either config start date (first run) or last generated timestamp (subsequent runs)
            - end_date: Current datetime
        """
        state = self.load_state()
        current_time = datetime.now()

        if state.has_historical_data and state.last_generated_timestamp:
            # Subsequent run: start from last generated timestamp
            start_date = state.last_generated_timestamp
        else:
            # First run: use configured start date
            start_date = config_start_date
            state.historical_start_date = config_start_date

        return start_date, current_time

    def update_historical_generation(self, end_timestamp: datetime) -> None:
        """Update state after historical data generation."""
        state = self.load_state()
        state.last_generated_timestamp = end_timestamp
        state.has_historical_data = True
        state.last_historical_run = datetime.now()
        self._state = state
        self.save_state()

    def can_start_realtime(self) -> bool:
        """Check if real-time generation can start (requires historical data)."""
        state = self.load_state()
        return state.has_historical_data and state.last_generated_timestamp is not None

    def get_realtime_start_timestamp(self) -> datetime | None:
        """Get the starting timestamp for real-time generation."""
        if not self.can_start_realtime():
            return None

        state = self.load_state()
        return state.last_generated_timestamp

    def update_realtime_generation(self, timestamp: datetime) -> None:
        """Update state during real-time generation."""
        state = self.load_state()
        state.last_generated_timestamp = timestamp
        state.last_realtime_run = datetime.now()
        self._state = state
        self.save_state()

    def get_status(self) -> dict:
        """Get current generation status for API/UI display."""
        state = self.load_state()
        return {
            "has_historical_data": state.has_historical_data,
            "last_generated_timestamp": (
                state.last_generated_timestamp.isoformat()
                if state.last_generated_timestamp
                else None
            ),
            "historical_start_date": (
                state.historical_start_date.isoformat()
                if state.historical_start_date
                else None
            ),
            "last_historical_run": (
                state.last_historical_run.isoformat()
                if state.last_historical_run
                else None
            ),
            "last_realtime_run": (
                state.last_realtime_run.isoformat() if state.last_realtime_run else None
            ),
            "can_start_realtime": self.can_start_realtime(),
        }

    def reset_state(self) -> None:
        """Reset generation state (useful for testing or fresh start)."""
        self._state = GenerationState()
        self.save_state()

    def clear_all_data(self, config_paths: dict) -> dict:
        """
        Clear all generated data and reset state.

        Args:
            config_paths: Dictionary with 'master' and 'facts' paths from config

        Returns:
            Dictionary with deletion results
        """
        import shutil
        from pathlib import Path

        results = {
            "state_reset": False,
            "master_data_cleared": False,
            "facts_data_cleared": False,
            "files_deleted": [],
            "errors": [],
        }

        try:
            # Reset generation state
            self.reset_state()
            results["state_reset"] = True

            # Clear master data
            master_path = Path(config_paths.get("master", ""))
            if master_path.exists():
                for file_path in master_path.glob("*.csv"):
                    try:
                        file_path.unlink()
                        results["files_deleted"].append(str(file_path))
                    except Exception as e:
                        results["errors"].append(f"Failed to delete {file_path}: {e}")
                results["master_data_cleared"] = True

            # Clear facts data (entire directory structure)
            facts_path = Path(config_paths.get("facts", ""))
            if facts_path.exists():
                try:
                    shutil.rmtree(facts_path)
                    results["files_deleted"].append(str(facts_path))
                    results["facts_data_cleared"] = True
                except Exception as e:
                    results["errors"].append(f"Failed to delete facts directory: {e}")

        except Exception as e:
            results["errors"].append(f"General error during data clearing: {e}")

        return results

    def clear_fact_data(self, config_paths: dict) -> dict:
        """
        Clear only fact data and reset generation state, preserving master data.

        Args:
            config_paths: Dictionary with 'facts' path from config

        Returns:
            Dictionary with deletion results
        """
        import shutil
        from pathlib import Path

        results = {
            "state_reset": False,
            "facts_data_cleared": False,
            "files_deleted": [],
            "errors": [],
        }

        try:
            # Reset generation state
            self.reset_state()
            results["state_reset"] = True

            # Clear facts data (entire directory structure)
            facts_path = Path(config_paths.get("facts", ""))
            if facts_path.exists():
                try:
                    shutil.rmtree(facts_path)
                    results["files_deleted"].append(str(facts_path))
                    results["facts_data_cleared"] = True
                except Exception as e:
                    results["errors"].append(f"Failed to delete facts directory: {e}")

        except Exception as e:
            results["errors"].append(f"General error during fact data clearing: {e}")

        return results
