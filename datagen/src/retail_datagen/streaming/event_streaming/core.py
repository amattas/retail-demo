"""
Core streaming operations and event generation.

This module provides the main streaming loop, event generation,
and buffer management functionality.
"""

import asyncio
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from ..errors import ErrorSeverity, classify_error
from ..schemas import EventEnvelope, EventType
from .config import event_generation_pipeline


class StreamingCore:
    """Core streaming operations and event generation."""

    def __init__(
        self,
        log,
        session_id: str,
        streaming_config,
        event_factory,
        azure_client,
        statistics,
        metrics,
    ):
        """
        Initialize streaming core.

        Args:
            log: Structured logger instance
            session_id: Session identifier
            streaming_config: StreamingConfig instance
            event_factory: EventFactory instance
            azure_client: AzureEventHubClient instance
            statistics: StreamingStatistics instance
            metrics: Metrics collector instance
        """
        self.log = log
        self._session_id = session_id
        self.streaming_config = streaming_config
        self._event_factory = event_factory
        self._azure_client = azure_client
        self._statistics = statistics
        self.metrics = metrics

        # Event buffer
        self._event_buffer: list[EventEnvelope] = []
        self._buffer_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()

        # Event hooks
        self._event_generated_hooks: list[Callable[[EventEnvelope], None]] = []
        self._event_sent_hooks: list[Callable[[EventEnvelope], None]] = []
        self._batch_sent_hooks: list[Callable[[list[EventEnvelope]], None]] = []
        self._error_hooks: list[Callable[[Exception, str], None]] = []

        # Event type filtering
        self._allowed_event_types: set[EventType] | None = None

        # Daily target tracking for event pacing
        self._daily_targets: dict[EventType, int] = {}
        self._daily_counts: dict[EventType, int] = {}
        self._current_day: str | None = None

    def set_allowed_event_types(self, event_type_names: list[str] | None):
        """Optionally restrict emitted events to a subset of EventType names."""
        if not event_type_names:
            self._allowed_event_types = None
            return
        allowed = set()
        for name in event_type_names:
            try:
                allowed.add(EventType(name))
            except Exception:
                # Ignore invalid names; router should validate
                pass
        self._allowed_event_types = allowed if allowed else None

    def compute_daily_targets(self, stores_count: int, dcs_count: int, config_volume):
        """Compute daily targets for key events based on config and master sizes."""
        cpd = getattr(config_volume, "customers_per_day", 20000) or 20000
        ood = getattr(config_volume, "online_orders_per_day", 2500) or 2500
        targets = {
            EventType.RECEIPT_CREATED: cpd,
            EventType.CUSTOMER_ENTERED: stores_count * 100,
            EventType.BLE_PING_DETECTED: stores_count * 500,
            EventType.INVENTORY_UPDATED: stores_count * 20 + dcs_count * 50,
            EventType.TRUCK_ARRIVED: 10,
            EventType.AD_IMPRESSION: 10000,  # marketing can be high volume
            EventType.ONLINE_ORDER_CREATED: ood,
        }
        self._daily_targets = targets
        self._daily_counts = {et: 0 for et in targets.keys()}
        self._current_day = datetime.now(UTC).strftime("%Y-%m-%d")

    def _reset_daily_if_needed(self, ts: datetime):
        """Reset daily counts if day has changed."""
        day = ts.strftime("%Y-%m-%d")
        if self._current_day != day:
            stores_count = len(self._stores) if hasattr(self, "_stores") else 1
            dcs_count = (
                len(self._distribution_centers)
                if hasattr(self, "_distribution_centers")
                else 1
            )
            # Need to pass config volume - this will be set by parent
            if hasattr(self, "_config_volume"):
                self.compute_daily_targets(stores_count, dcs_count, self._config_volume)

    def _build_event_weights(self, ts: datetime) -> dict[EventType, float]:
        """Build event weights biased by remaining quota for the current day."""
        self._reset_daily_if_needed(ts)
        weights: dict[EventType, float] = {}
        # Base tiny weight for all known types to keep variety
        base = 0.01
        for et, target in self._daily_targets.items():
            remaining = max(target - self._daily_counts.get(et, 0), 0)
            # Weight proportional to remaining, with base floor
            weights[et] = base + remaining / max(target, 1)
        return weights

    async def streaming_loop(
        self,
        start_time: datetime,
        end_time: datetime | None,
        pause_event,
        is_shutdown_func,
    ):
        """
        Main streaming loop that generates and sends events.

        Args:
            start_time: Streaming start time
            end_time: Optional end time
            pause_event: asyncio.Event for pause/resume control
            is_shutdown_func: Function that returns True if shutdown requested
        """
        next_burst_time = start_time
        batch_count = 0

        while not is_shutdown_func():
            # Wait if paused
            await pause_event.wait()

            # Check if still streaming (might have stopped during pause)
            if is_shutdown_func():
                break

            current_time = datetime.now(UTC)

            # Check if we've reached the end time
            if end_time and current_time >= end_time:
                self.log.info(
                    "Streaming duration completed", session_id=self._session_id
                )
                break

            # Check if it's time for the next burst
            if current_time >= next_burst_time:
                try:
                    batch_id = self.log.generate_correlation_id()
                    self.log.debug(
                        "Generating event batch",
                        batch_id=batch_id,
                        batch_number=batch_count,
                        target_size=self.streaming_config.burst,
                        session_id=self._session_id,
                    )

                    # Generate event burst
                    events = await self._generate_event_burst(current_time)

                    if events:
                        # Enrich events with correlation and session identifiers
                        for event in events:
                            if not event.correlation_id:
                                event.correlation_id = batch_id
                            event.session_id = self._session_id

                        self.log.info(
                            "Event batch generated",
                            batch_id=batch_id,
                            event_count=len(events),
                            event_types=[str(e.event_type) for e in events[:5]],
                            session_id=self._session_id,
                        )

                        # Buffer events
                        async with self._buffer_lock:
                            self._event_buffer.extend(events)

                            # Update statistics
                            async with self._stats_lock:
                                self._statistics.events_generated += len(events)
                                for event in events:
                                    self._statistics.event_type_counts[
                                        event.event_type
                                    ] += 1

                        # Send events if buffer is large enough
                        if (
                            len(self._event_buffer)
                            >= self.streaming_config.max_batch_size
                        ):
                            await self.flush_event_buffer()

                    # Calculate next burst time
                    next_burst_time = current_time + timedelta(
                        milliseconds=self.streaming_config.emit_interval_ms
                    )
                    batch_count += 1

                except Exception as exc:
                    self.log.error(
                        "Streaming loop error",
                        error=str(exc),
                        error_type=type(exc).__name__,
                        batch_number=batch_count,
                        session_id=self._session_id,
                    )
                    async with self._stats_lock:
                        self._statistics.error_counts["streaming_loop_errors"] += 1

                    # Call error hooks
                    self._run_hooks_once(self._error_hooks, exc, "streaming_loop")

            # Sleep for a short interval to avoid busy waiting
            await asyncio.sleep(0.1)

    def _process_generated_events(self, events: list[EventEnvelope]) -> None:
        """Record metrics and run hooks for generated events."""
        if not events:
            return

        for event in events:
            self.metrics.record_event_generated(event.event_type)

        self._run_event_hooks(events, self._event_generated_hooks)

    @staticmethod
    def _run_hooks_once(hooks: list[Callable[..., None]], *args: Any) -> None:
        """Run hooks with exception handling."""
        for hook in hooks:
            try:
                hook(*args)
            except Exception:
                pass

    def _run_event_hooks(
        self,
        events: list[EventEnvelope],
        hooks: list[Callable[[EventEnvelope], None]],
    ) -> None:
        """Run event hooks for each event."""
        if not events or not hooks:
            return

        for event in events:
            self._run_hooks_once(hooks, event)

    @event_generation_pipeline
    async def _generate_event_burst(self, timestamp: datetime) -> list[EventEnvelope]:
        """
        Generate a burst of mixed events.

        Args:
            timestamp: Base timestamp for event generation

        Returns:
            List of generated events
        """
        # Build weights based on daily targets
        all_weights = self._build_event_weights(timestamp)
        if self._allowed_event_types:
            weights = {
                et: w
                for et, w in all_weights.items()
                if et in self._allowed_event_types
            }
            if not weights:
                weights = {et: 1.0 for et in self._allowed_event_types}
        else:
            weights = all_weights

        events = self._event_factory.generate_mixed_events(
            count=self.streaming_config.burst,
            timestamp=timestamp,
            event_weights=weights,
        )

        # Update daily counts for pacing (protected by stats lock)
        async with self._stats_lock:
            for ev in events:
                if ev.event_type in self._daily_targets:
                    self._daily_counts[ev.event_type] = (
                        self._daily_counts.get(ev.event_type, 0) + 1
                    )

        return events

    async def flush_event_buffer(self, dlq_manager=None):
        """
        Flush events from buffer to Azure Event Hub.

        Args:
            dlq_manager: Optional DLQManager for handling failures
        """
        if not self._event_buffer:
            return

        async with self._buffer_lock:
            events_to_send = self._event_buffer.copy()
            self._event_buffer.clear()

        batch_id = events_to_send[0].correlation_id if events_to_send else "unknown"

        self.log.debug(
            "Flushing event buffer",
            batch_id=batch_id,
            event_count=len(events_to_send),
            session_id=self._session_id,
        )

        try:
            # Track batch send timing
            batch_start_time = time.time()
            success = await self._azure_client.send_events(events_to_send)
            batch_duration = time.time() - batch_start_time

            async with self._stats_lock:
                if success:
                    self._statistics.events_sent_successfully += len(events_to_send)
                    self._statistics.batches_sent += 1
                    self._statistics.last_event_time = datetime.now(UTC)

                    # Estimate bytes sent (rough calculation)
                    estimated_bytes = sum(
                        len(str(event.payload)) + 200
                        for event in events_to_send  # +200 for envelope
                    )
                    self._statistics.bytes_sent += estimated_bytes

                    # Record metrics for successful batch
                    self.metrics.record_batch_sent(estimated_bytes, batch_duration)
                    for event in events_to_send:
                        self.metrics.record_event_sent(event.event_type)

                    self.log.info(
                        "Event batch sent",
                        batch_id=batch_id,
                        event_count=len(events_to_send),
                        duration_seconds=batch_duration,
                        session_id=self._session_id,
                    )
                else:
                    # Send failed - record metrics
                    self.metrics.record_batch_failed("send_failed")
                    for event in events_to_send:
                        self.metrics.record_event_failed(
                            event.event_type, "send_failed"
                        )

                    # Send failed - add to DLQ if provided
                    if dlq_manager:
                        await self._handle_send_failure(
                            events_to_send, Exception("Send failed"), dlq_manager
                        )

                    self.log.warning(
                        "Event batch send failed",
                        batch_id=batch_id,
                        event_count=len(events_to_send),
                        session_id=self._session_id,
                    )

            # Call batch sent hooks
            if success:
                self._run_hooks_once(self._batch_sent_hooks, events_to_send)

                # Call individual event sent hooks
                self._run_event_hooks(events_to_send, self._event_sent_hooks)

        except Exception as e:
            # Record metrics for exception
            error_type = type(e).__name__
            self.metrics.record_batch_failed(error_type)
            for event in events_to_send:
                self.metrics.record_event_failed(event.event_type, error_type)

            # Handle send failure with error classification
            if dlq_manager:
                await self._handle_send_failure(events_to_send, e, dlq_manager)
            async with self._stats_lock:
                self._statistics.error_counts["flush_errors"] += 1

            self.log.error(
                "Error flushing event buffer",
                batch_id=batch_id,
                error=str(e),
                error_type=error_type,
                session_id=self._session_id,
            )

    async def flush_remaining_events(self, dlq_manager=None):
        """
        Flush any remaining events in buffer during shutdown.

        Args:
            dlq_manager: Optional DLQManager for handling failures
        """
        if self._event_buffer:
            self.log.info(
                "Flushing remaining events",
                event_count=len(self._event_buffer),
                session_id=self._session_id,
            )
            await self.flush_event_buffer(dlq_manager)

    async def _handle_send_failure(
        self, events: list[EventEnvelope], exception: Exception, dlq_manager
    ):
        """Handle send failure with error classification."""
        # Classify error
        error = classify_error(exception)

        self.log.error(
            "Event send failed",
            session_id=self._session_id,
            error_message=error.message,
            severity=error.severity.value,
            category=error.category.value,
            retryable=error.retryable,
            event_count=len(events),
        )

        # Update statistics
        async with self._stats_lock:
            self._statistics.events_failed += len(events)

        # Add to DLQ
        await dlq_manager.add_failed_events(
            events,
            error.message,
            error.category.value,
            error.severity.value,
        )

        # Return critical status for caller to handle
        return error.severity == ErrorSeverity.CRITICAL

    def get_buffer_size(self) -> int:
        """Get current buffer size."""
        return len(self._event_buffer)

    def add_event_generated_hook(self, hook: Callable[[EventEnvelope], None]):
        """Add hook called when events are generated."""
        self._event_generated_hooks.append(hook)

    def add_event_sent_hook(self, hook: Callable[[EventEnvelope], None]):
        """Add hook called when events are successfully sent."""
        self._event_sent_hooks.append(hook)

    def add_batch_sent_hook(self, hook: Callable[[list[EventEnvelope]], None]):
        """Add hook called when event batches are successfully sent."""
        self._batch_sent_hooks.append(hook)

    def add_error_hook(self, hook: Callable[[Exception, str], None]):
        """Add hook called when errors occur."""
        self._error_hooks.append(hook)
