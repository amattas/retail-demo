"""
Dead Letter Queue management for failed events.

This module provides DLQ handling, retry logic, and statistics
for events that fail to send.
"""

import asyncio
from collections import Counter
from datetime import UTC, datetime

from ..schemas import EventEnvelope
from .config import DLQEntry


class DLQManager:
    """Manages dead letter queue for failed events."""

    def __init__(
        self,
        max_size: int,
        retry_enabled: bool,
        retry_max_attempts: int,
        log,
        session_id: str,
    ):
        """
        Initialize DLQ manager.

        Args:
            max_size: Maximum number of events in DLQ
            retry_enabled: Whether automatic retry is enabled
            retry_max_attempts: Maximum retry attempts per event
            log: Structured logger instance
            session_id: Session identifier for logging
        """
        self._dlq: list[DLQEntry] = []
        self._dlq_lock = asyncio.Lock()
        self._max_size = max_size
        self._retry_enabled = retry_enabled
        self._retry_max_attempts = retry_max_attempts
        self.log = log
        self._session_id = session_id

    async def add_failed_events(
        self,
        events: list[EventEnvelope],
        error_message: str,
        error_category: str,
        error_severity: str,
    ):
        """
        Add failed events to DLQ.

        Args:
            events: Events that failed to send
            error_message: Error description
            error_category: Error category
            error_severity: Error severity level
        """
        async with self._dlq_lock:
            for event in events:
                dlq_entry = DLQEntry(
                    event=event,
                    error_message=error_message,
                    error_category=error_category,
                    error_severity=error_severity,
                    timestamp=datetime.now(UTC).isoformat(),
                    retry_count=0,
                )

                self._dlq.append(dlq_entry)

            # Trim DLQ if too large
            if len(self._dlq) > self._max_size:
                removed = len(self._dlq) - self._max_size
                self._dlq = self._dlq[-self._max_size :]
                self.log.warning(
                    "DLQ size exceeded",
                    session_id=self._session_id,
                    removed=removed,
                    dlq_size=len(self._dlq),
                    max_size=self._max_size,
                )

    async def retry_events(self, azure_client, max_retries: int | None = None) -> dict:
        """
        Retry events from DLQ.

        Args:
            azure_client: Azure Event Hub client for sending
            max_retries: Maximum retry attempts per event (default from config)

        Returns:
            {
                "total_attempted": int,
                "succeeded": int,
                "failed": int,
                "still_in_dlq": int
            }
        """
        max_retries = max_retries or self._retry_max_attempts

        async with self._dlq_lock:
            if not self._dlq:
                return {
                    "total_attempted": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "still_in_dlq": 0,
                }

            total = len(self._dlq)
            succeeded = 0
            failed = 0

            # Copy DLQ for retry
            retry_queue = self._dlq.copy()
            self._dlq = []

            for entry in retry_queue:
                # Skip if max retries exceeded
                if entry.retry_count >= max_retries:
                    self._dlq.append(entry)
                    failed += 1
                    continue

                # Retry send
                try:
                    result = await azure_client.send_events([entry.event])

                    if result:
                        succeeded += 1
                        self.log.info(
                            "DLQ event retry succeeded",
                            session_id=self._session_id,
                            trace_id=entry.event.trace_id,
                        )
                    else:
                        # Update retry count and re-add to DLQ
                        entry.retry_count += 1
                        entry.last_retry_timestamp = datetime.now(UTC).isoformat()
                        self._dlq.append(entry)
                        failed += 1

                except Exception as e:
                    # Update retry count and re-add to DLQ
                    entry.retry_count += 1
                    entry.last_retry_timestamp = datetime.now(UTC).isoformat()
                    entry.error_message = (
                        f"{entry.error_message} | Retry failed: {str(e)}"
                    )
                    self._dlq.append(entry)
                    failed += 1

            return {
                "total_attempted": total,
                "succeeded": succeeded,
                "failed": failed,
                "still_in_dlq": len(self._dlq),
            }

    def get_summary(self) -> dict:
        """Get DLQ summary statistics."""
        if not self._dlq:
            return {
                "size": 0,
                "by_category": {},
                "by_severity": {},
                "oldest_entry": None,
                "newest_entry": None,
                "max_size": self._max_size,
            }

        categories = Counter(entry.error_category for entry in self._dlq)
        severities = Counter(entry.error_severity for entry in self._dlq)

        return {
            "size": len(self._dlq),
            "by_category": dict(categories),
            "by_severity": dict(severities),
            "oldest_entry": self._dlq[0].timestamp if self._dlq else None,
            "newest_entry": self._dlq[-1].timestamp if self._dlq else None,
            "max_size": self._max_size,
        }

    def get_size(self) -> int:
        """Get current DLQ size."""
        return len(self._dlq)

    def is_empty(self) -> bool:
        """Check if DLQ is empty."""
        return len(self._dlq) == 0

    async def retry_loop(
        self,
        is_streaming_func,
        is_shutdown_func,
        azure_client,
        retry_interval: int = 300,
    ):
        """
        Background loop to retry DLQ events periodically.

        Args:
            is_streaming_func: Function that returns True if streaming is active
            is_shutdown_func: Function that returns True if shutdown requested
            azure_client: Azure Event Hub client
            retry_interval: Retry interval in seconds (default 5 minutes)
        """
        while is_streaming_func() and not is_shutdown_func():
            await asyncio.sleep(retry_interval)

            if self._retry_enabled and not self.is_empty():
                self.log.info(
                    "Auto-retrying DLQ events",
                    session_id=self._session_id,
                    dlq_size=self.get_size(),
                )
                result = await self.retry_events(azure_client)
                self.log.info(
                    "DLQ retry complete",
                    session_id=self._session_id,
                    succeeded=result["succeeded"],
                    failed=result["failed"],
                )
