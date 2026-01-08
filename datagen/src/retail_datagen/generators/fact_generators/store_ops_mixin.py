"""
Store operations generation for tracking open/close events.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any

from retail_datagen.shared.models import Store

logger = logging.getLogger(__name__)


class StoreOpsMixin:
    """Store operations generation for open/close events"""

    def _parse_operating_hours(self, hours_str: str | None) -> tuple[int, int]:
        """
        Parse operating hours string to get open and close times.

        Args:
            hours_str: Operating hours string (e.g., "8am-10pm", "24/7", "6am-midnight")

        Returns:
            Tuple of (open_hour, close_hour) in 24-hour format

        Note:
            Returns (0, 24) for 24/7 stores
            Returns (8, 22) as default for standard hours
        """
        if not hours_str:
            # Default to standard hours (8am-10pm)
            return (8, 22)

        hours_str = hours_str.lower().strip()

        # Handle 24/7 stores
        if "24" in hours_str or "24/7" in hours_str:
            return (0, 24)

        # Parse time ranges like "8am-10pm", "6am-midnight"
        # Pattern matches formats like: 6am-midnight, 8am-10pm, 9am-9pm
        pattern = r'(\d{1,2})([ap]m)?[-\s]*(midnight|noon|\d{1,2})([ap]m)?'
        match = re.search(pattern, hours_str)

        if not match:
            # Fallback to standard hours
            logger.debug(f"Could not parse operating hours '{hours_str}', using default 8am-10pm")
            return (8, 22)

        # Parse open time
        open_hour = int(match.group(1))
        open_modifier = match.group(2)  # 'am' or 'pm'

        # Convert to 24-hour format
        if open_modifier == 'pm' and open_hour != 12:
            open_hour += 12
        elif open_modifier == 'am' and open_hour == 12:
            open_hour = 0

        # Parse close time
        close_str = match.group(3)
        close_modifier = match.group(4)

        if close_str == 'midnight':
            close_hour = 24  # Use 24 to represent midnight of next day
        elif close_str == 'noon':
            close_hour = 12
        else:
            close_hour = int(close_str)
            if close_modifier == 'pm' and close_hour != 12:
                close_hour += 12
            elif close_modifier == 'am' and close_hour == 12:
                close_hour = 0

        # Validate and fix any edge cases
        if close_hour <= open_hour and close_hour != 24:
            # If close is before open, assume it's next day
            if close_hour == 0:
                close_hour = 24
            elif close_hour < open_hour:
                close_hour += 12

        return (open_hour, close_hour)

    def _generate_store_operations_for_day(
        self,
        store: Store,
        day_date: datetime,
    ) -> list[dict[str, Any]]:
        """
        Generate store open and close events for a single day.

        Args:
            store: Store to generate operations for
            day_date: Date to generate operations for

        Returns:
            List of store operation records (opened and closed events)
        """
        operations = []

        # Skip Christmas Day - stores closed
        if day_date.month == 12 and day_date.day == 25:
            return operations

        # Parse operating hours from store
        open_hour, close_hour = self._parse_operating_hours(store.operating_hours)

        # Generate opened event
        open_time = day_date.replace(hour=open_hour, minute=0, second=0, microsecond=0)
        operations.append({
            "TraceId": self._generate_trace_id(),
            "EventTS": open_time,
            "StoreID": store.ID,
            "OperationType": "opened",
        })

        # Generate closed event
        # Handle midnight close (close_hour == 24)
        if close_hour == 24:
            close_time = day_date.replace(hour=23, minute=59, second=59, microsecond=0)
        else:
            close_time = day_date.replace(hour=close_hour, minute=0, second=0, microsecond=0)

        operations.append({
            "TraceId": self._generate_trace_id(),
            "EventTS": close_time,
            "StoreID": store.ID,
            "OperationType": "closed",
        })

        return operations
