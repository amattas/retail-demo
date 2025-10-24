"""
Structured logging additions for EventStreamer.

This file contains the updated methods with structured logging.
To apply: copy these method implementations into event_streamer.py
"""

# === Method: start ===
async def start(self, duration: timedelta | None = None) -> bool:
    """
    Start the event streaming process.

    Args:
        duration: Optional duration to stream for. If None, streams indefinitely.

    Returns:
        bool: True if streaming started successfully, False otherwise
    """
    if self._is_streaming:
        self.log.warning("Streaming is already active", session_id=self._session_id)
        return False

    if not await self.initialize():
        self.log.error(
            "Failed to initialize streaming engine", session_id=self._session_id
        )
        return False

    self._is_streaming = True
    self._is_shutdown = False
    start_time = datetime.now(UTC)
    end_time = start_time + duration if duration else None

    self.log.info(
        "Starting streaming session",
        session_id=self._session_id,
        duration=str(duration) if duration else "indefinite",
        emit_interval_ms=self.streaming_config.emit_interval_ms,
        burst_size=self.streaming_config.burst,
        start_time=start_time.isoformat(),
    )

    try:
        # Start monitoring task
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())

        # Start main streaming task
        self._streaming_task = asyncio.create_task(
            self._streaming_loop(start_time, end_time)
        )

        # Wait for streaming to complete
        await self._streaming_task

        self.log.info(
            "Event streaming completed successfully", session_id=self._session_id
        )
        return True

    except Exception as e:
        self.log.error(
            "Error during streaming",
            session_id=self._session_id,
            error=str(e),
            error_type=type(e).__name__,
        )
        return False
    finally:
        await self._cleanup()


# === Method: _streaming_loop ===
async def _streaming_loop(self, start_time: datetime, end_time: datetime | None):
    """Main streaming loop that generates and sends events."""
    next_burst_time = start_time
    batch_count = 0

    while not self._is_shutdown:
        # Wait if paused
        await self._pause_event.wait()

        # Check if still streaming (might have stopped during pause)
        if not self._is_streaming or self._is_shutdown:
            break

        current_time = datetime.now(UTC)

        # Check if we've reached the end time
        if end_time and current_time >= end_time:
            self.log.info("Streaming duration completed", session_id=self._session_id)
            break

        # Check if it's time for the next burst
        if current_time >= next_burst_time:
            try:
                # Generate batch correlation ID
                batch_corr_id = self.log.generate_correlation_id()

                self.log.debug(
                    "Generating event batch",
                    batch_id=batch_corr_id,
                    batch_number=batch_count,
                    target_size=self.streaming_config.burst,
                    session_id=self._session_id,
                )

                # Generate event burst
                events = await self._generate_event_burst(current_time)

                if events:
                    # Add batch correlation to events
                    for event in events:
                        if not event.correlation_id:
                            event.correlation_id = batch_corr_id
                        event.session_id = self._session_id

                    self.log.info(
                        "Event batch generated",
                        batch_id=batch_corr_id,
                        event_count=len(events),
                        event_types=[str(e.event_type) for e in events[:5]],  # First 5
                        session_id=self._session_id,
                    )

                    # Buffer events
                    async with self._buffer_lock:
                        self._event_buffer.extend(events)

                        # Update statistics
                        async with self._stats_lock:
                            self._statistics.events_generated += len(events)
                            for event in events:
                                self._statistics.event_type_counts[event.event_type] += (
                                    1
                                )

                    # Send events if buffer is large enough
                    if (
                        len(self._event_buffer)
                        >= self.streaming_config.max_batch_size
                    ):
                        await self._flush_event_buffer()

                # Calculate next burst time
                next_burst_time = current_time + timedelta(
                    milliseconds=self.streaming_config.emit_interval_ms
                )
                batch_count += 1

            except Exception as e:
                self.log.error(
                    "Streaming loop error",
                    error=str(e),
                    error_type=type(e).__name__,
                    batch_number=batch_count,
                    session_id=self._session_id,
                )
                async with self._stats_lock:
                    self._statistics.error_counts["streaming_loop_errors"] += 1

                # Call error hooks
                for hook in self._error_hooks:
                    try:
                        hook(e, "streaming_loop")
                    except Exception:
                        pass

        # Sleep for a short interval to avoid busy waiting
        await asyncio.sleep(0.1)

    self._is_streaming = False


# === Method: _flush_event_buffer ===
async def _flush_event_buffer(self):
    """Flush events from buffer to Azure Event Hub."""
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
        success = await self._azure_client.send_events(events_to_send)

        async with self._stats_lock:
            if success:
                self._statistics.events_sent_successfully += len(events_to_send)
                self._statistics.batches_sent += 1
                self._statistics.last_event_time = datetime.now(UTC)

                # Estimate bytes sent (rough calculation)
                estimated_bytes = sum(
                    len(str(event.payload)) + 200 for event in events_to_send
                )
                self._statistics.bytes_sent += estimated_bytes

                self.log.info(
                    "Event buffer flushed successfully",
                    batch_id=batch_id,
                    event_count=len(events_to_send),
                    total_sent=self._statistics.events_sent_successfully,
                    session_id=self._session_id,
                )
            else:
                self._statistics.events_failed += len(events_to_send)

                self.log.error(
                    "Event buffer flush failed",
                    batch_id=batch_id,
                    event_count=len(events_to_send),
                    total_failed=self._statistics.events_failed,
                    session_id=self._session_id,
                )

                # Add failed events to dead letter queue if enabled
                if self.streaming_config.enable_dead_letter_queue:
                    self._dead_letter_queue.extend(events_to_send)

                    self.log.warning(
                        "Events added to DLQ",
                        batch_id=batch_id,
                        dlq_size=len(self._dead_letter_queue),
                        session_id=self._session_id,
                    )

                    # Prevent dead letter queue from growing too large
                    if (
                        len(self._dead_letter_queue)
                        > self.streaming_config.max_buffer_size
                    ):
                        self._dead_letter_queue = self._dead_letter_queue[
                            -self.streaming_config.max_buffer_size :
                        ]

        # Call batch sent hooks
        if success:
            for hook in self._batch_sent_hooks:
                try:
                    hook(events_to_send)
                except Exception:
                    pass

            # Call individual event sent hooks
            for event in events_to_send:
                for hook in self._event_sent_hooks:
                    try:
                        hook(event)
                    except Exception:
                        pass

    except Exception as e:
        self.log.error(
            "Error flushing event buffer",
            error=str(e),
            error_type=type(e).__name__,
            batch_id=batch_id,
            session_id=self._session_id,
        )
        async with self._stats_lock:
            self._statistics.error_counts["flush_errors"] += 1
