"""
Monitoring, health checks, and pause/resume control.

This module provides monitoring loops, statistics collection,
and pause/resume functionality for streaming.
"""

import asyncio
import time
from datetime import UTC, datetime
from typing import Any


class MonitoringManager:
    """Manages monitoring, health checks, and pause/resume control."""

    def __init__(
        self,
        log,
        session_id: str,
        streaming_config,
        statistics,
        metrics,
        azure_client,
    ):
        """
        Initialize monitoring manager.

        Args:
            log: Structured logger instance
            session_id: Session identifier
            streaming_config: StreamingConfig instance
            statistics: StreamingStatistics instance
            metrics: Metrics collector instance
            azure_client: AzureEventHubClient instance
        """
        self.log = log
        self._session_id = session_id
        self.streaming_config = streaming_config
        self._statistics = statistics
        self.metrics = metrics
        self._azure_client = azure_client

        # Pause/Resume state
        self._is_paused = False
        self._pause_lock = asyncio.Lock()
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # Not paused initially

        # Pause statistics
        self._pause_count = 0
        self._total_pause_duration = 0.0
        self._last_pause_time: float | None = None

        # Stats lock
        self._stats_lock = asyncio.Lock()

    async def monitoring_loop(self, dlq_size_func, is_shutdown_func):
        """
        Background monitoring loop for statistics and health checks.

        Args:
            dlq_size_func: Function that returns current DLQ size
            is_shutdown_func: Function that returns True if shutdown requested
        """
        while not is_shutdown_func():
            try:
                # Update Prometheus metrics
                self.metrics.update_uptime()
                self.metrics.update_dlq_size(dlq_size_func())

                # Update performance metrics
                async with self._stats_lock:
                    if self._statistics.events_generated > 0:
                        elapsed_time = (
                            datetime.now(UTC)
                            - (self._statistics.last_event_time or datetime.now(UTC))
                        ).total_seconds()

                        if elapsed_time > 0:
                            self._statistics.events_per_second = (
                                self._statistics.events_sent_successfully
                                / max(elapsed_time, 1)
                            )

                # Log statistics periodically
                if (
                    self._statistics.events_generated % 1000 == 0
                    and self._statistics.events_generated > 0
                ):
                    stats = await self.get_statistics()
                    self.log.info(
                        "Streaming stats",
                        session_id=self._session_id,
                        events_generated=stats["events_generated"],
                        events_sent=stats["events_sent_successfully"],
                        events_per_second=stats["events_per_second"],
                    )

                # Perform health check
                if self._azure_client:
                    health = await self._azure_client.health_check()
                    if not health.get("healthy", False):
                        self.log.warning(
                            "Event Hub health check failed",
                            session_id=self._session_id,
                            health=health,
                        )
                        async with self._stats_lock:
                            self._statistics.connection_failures += 1

                await asyncio.sleep(self.streaming_config.monitoring_interval)

            except Exception as exc:
                self.log.error(
                    "Error in monitoring loop",
                    session_id=self._session_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                await asyncio.sleep(self.streaming_config.monitoring_interval)

    async def pause(self, is_streaming_func) -> dict:
        """
        Pause streaming without stopping completely.

        Events will not be generated or sent until resumed.
        State is maintained so streaming can continue seamlessly.

        Args:
            is_streaming_func: Function that returns True if streaming is active

        Returns:
            {
                "success": bool,
                "message": str,
                "paused_at": str (ISO timestamp),
                "events_sent_before_pause": int
            }
        """
        async with self._pause_lock:
            if not is_streaming_func():
                return {
                    "success": False,
                    "message": "Cannot pause: streaming is not active",
                    "paused_at": None,
                    "events_sent_before_pause": 0,
                }

            if self._is_paused:
                return {
                    "success": False,
                    "message": "Already paused",
                    "paused_at": (
                        datetime.fromtimestamp(self._last_pause_time, UTC).isoformat()
                        if self._last_pause_time
                        else None
                    ),
                    "events_sent_before_pause": self._statistics.events_sent_successfully,
                }

            # Set pause state
            self._is_paused = True
            self._pause_event.clear()  # Block streaming loop
            self._last_pause_time = time.time()
            self._pause_count += 1

            # Record pause in metrics
            self.metrics.pause_streaming()

            self.log.info("Streaming paused", session_id=self._session_id)

            return {
                "success": True,
                "message": "Streaming paused successfully",
                "paused_at": datetime.now(UTC).isoformat(),
                "events_sent_before_pause": self._statistics.events_sent_successfully,
            }

    async def resume(self, is_streaming_func) -> dict:
        """
        Resume streaming after pause.

        Continues from where it left off with no event loss.

        Args:
            is_streaming_func: Function that returns True if streaming is active

        Returns:
            {
                "success": bool,
                "message": str,
                "resumed_at": str (ISO timestamp),
                "pause_duration_seconds": float,
                "total_pause_count": int
            }
        """
        async with self._pause_lock:
            if not is_streaming_func():
                return {
                    "success": False,
                    "message": "Cannot resume: streaming is not active",
                    "resumed_at": None,
                    "pause_duration_seconds": 0,
                }

            if not self._is_paused:
                return {
                    "success": False,
                    "message": "Not paused",
                    "resumed_at": None,
                    "pause_duration_seconds": 0,
                }

            # Calculate pause duration
            pause_duration = 0.0
            if self._last_pause_time:
                pause_duration = time.time() - self._last_pause_time
                self._total_pause_duration += pause_duration

            # Resume streaming
            self._is_paused = False
            self._pause_event.set()  # Unblock streaming loop

            # Record resume in metrics
            self.metrics.resume_streaming()

            self.log.info(
                "Streaming resumed",
                session_id=self._session_id,
                pause_duration_seconds=pause_duration,
            )

            return {
                "success": True,
                "message": "Streaming resumed successfully",
                "resumed_at": datetime.now(UTC).isoformat(),
                "pause_duration_seconds": pause_duration,
                "total_pause_count": self._pause_count,
            }

    def get_pause_statistics(self) -> dict:
        """Get pause/resume statistics."""
        return {
            "is_paused": self._is_paused,
            "pause_count": self._pause_count,
            "total_pause_duration_seconds": self._total_pause_duration,
            "currently_paused_duration": (
                time.time() - self._last_pause_time
                if self._is_paused and self._last_pause_time
                else 0.0
            ),
        }

    async def get_statistics(
        self, buffer_size: int, dlq_size: int, azure_client
    ) -> dict[str, Any]:
        """
        Get current streaming statistics.

        Args:
            buffer_size: Current event buffer size
            dlq_size: Current DLQ size
            azure_client: Azure Event Hub client

        Returns:
            dict: Current statistics and performance metrics
        """
        async with self._stats_lock:
            stats = {
                "events_generated": self._statistics.events_generated,
                "events_sent_successfully": self._statistics.events_sent_successfully,
                "events_failed": self._statistics.events_failed,
                "batches_sent": self._statistics.batches_sent,
                "events_per_second": self._statistics.events_per_second,
                "bytes_sent": self._statistics.bytes_sent,
                "last_event_time": (
                    self._statistics.last_event_time.isoformat()
                    if self._statistics.last_event_time
                    else None
                ),
                "event_type_counts": dict(self._statistics.event_type_counts),
                "error_counts": dict(self._statistics.error_counts),
                "connection_failures": self._statistics.connection_failures,
                "circuit_breaker_trips": self._statistics.circuit_breaker_trips,
                "buffer_size": buffer_size,
                "dead_letter_queue_size": dlq_size,
                "pause_statistics": self.get_pause_statistics(),
            }

            # Add Azure client statistics if available
            if azure_client:
                azure_stats = await azure_client.get_statistics()
                stats.update({"azure_client": azure_stats})

            return stats

    async def get_health_status(
        self,
        is_streaming: bool,
        azure_client,
        event_factory,
        master_data_loaded: bool,
        buffer_size: int,
    ) -> dict[str, Any]:
        """
        Get comprehensive health status of the streaming system.

        Args:
            is_streaming: Whether streaming is active
            azure_client: Azure Event Hub client
            event_factory: EventFactory instance
            master_data_loaded: Whether master data is loaded
            buffer_size: Current buffer size

        Returns:
            dict: Health status information
        """
        health = {
            "overall_healthy": True,
            "streaming_active": is_streaming,
            "components": {},
            "last_updated": datetime.now(UTC).isoformat(),
        }

        # Check Azure client health
        if azure_client:
            azure_health = await azure_client.health_check()
            health["components"]["azure_event_hub"] = azure_health
            if not azure_health.get("healthy", False):
                health["overall_healthy"] = False

        # Check event factory health
        health["components"]["event_factory"] = {
            "healthy": event_factory is not None,
            "master_data_loaded": master_data_loaded,
        }

        # Check buffer health
        buffer_healthy = buffer_size < self.streaming_config.max_buffer_size
        health["components"]["event_buffer"] = {
            "healthy": buffer_healthy,
            "size": buffer_size,
            "max_size": self.streaming_config.max_buffer_size,
        }
        if not buffer_healthy:
            health["overall_healthy"] = False

        # Check error rates
        stats = await self.get_statistics(buffer_size, 0, azure_client)
        total_events = stats["events_generated"]
        failed_events = stats["events_failed"]

        if total_events > 0:
            error_rate = failed_events / total_events
            error_healthy = error_rate < 0.05  # Less than 5% error rate
            health["components"]["error_rate"] = {
                "healthy": error_healthy,
                "rate": error_rate,
                "threshold": 0.05,
            }
            if not error_healthy:
                health["overall_healthy"] = False

        return health

    def get_pause_event(self) -> asyncio.Event:
        """Get pause event for streaming loop control."""
        return self._pause_event

    def is_paused(self) -> bool:
        """Check if streaming is paused."""
        return self._is_paused
