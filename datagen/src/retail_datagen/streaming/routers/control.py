"""
Streaming control endpoints (start, stop, pause, resume, status).

This module handles the core streaming lifecycle operations.
"""

import asyncio as _asyncio
import json as _json
import logging
import random
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from uuid import uuid4 as _uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status

from ...api.models import (
    OperationResult,
    StreamingStartRequest,
    StreamingStatusResponse,
)
from ...config.models import RetailConfig
from ...db.duckdb_engine import (
    get_duckdb_conn,
    outbox_ack_sent,
    outbox_lease_next,
    outbox_nack_retry,
)
from ...generators.generation_state import GenerationStateManager
from ...shared.dependencies import (
    cancel_task,
    create_background_task,
    get_config,
    get_event_streamer,
    get_fact_generator,
    get_task_status,
    rate_limit,
    update_task_progress,
)
from ...shared.logging_utils import get_structured_logger
from ...streaming.azure_client import AzureEventHubClient
from ...streaming.event_streaming import EventStreamer
from ...streaming.schemas import EventEnvelope, EventType
from .state import (
    get_session_id,
    get_start_time,
    reset_streaming_state,
    set_session,
    streaming_statistics,
    update_streaming_statistics,
)

logger = logging.getLogger(__name__)
log = get_structured_logger(__name__)

router = APIRouter()

AVAILABLE_EVENT_TYPES = [e.value for e in EventType]


@router.post(
    "/stream/start",
    response_model=OperationResult,
    summary="Start real-time streaming",
    description="Start streaming events to Azure Event Hub",
)
@rate_limit(max_requests=5, window_seconds=60)
async def start_streaming(
    request: StreamingStartRequest,
    fastapi_request: Request,
    event_streamer: EventStreamer = Depends(get_event_streamer),
    config: RetailConfig = Depends(get_config),
):
    """Start real-time event streaming."""
    # Generate request correlation ID
    request_id = f"REQ_{uuid4().hex[:12]}"
    log.set_correlation_id(request_id)

    log.info(
        "Streaming start request received",
        request_id=request_id,
        client_ip=(
            fastapi_request.client.host if fastapi_request.client else "unknown"
        ),
        duration_minutes=request.duration_minutes,
        event_types_count=len(request.event_types) if request.event_types else "all",
    )

    # Check if already streaming
    session_id = get_session_id()
    if session_id:
        task_status = get_task_status(session_id)
        if task_status and task_status["status"] == "running":
            log.warning(
                "Streaming already active",
                request_id=request_id,
                active_session=session_id,
            )
            log.clear_correlation_id()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Streaming is already active. Stop current stream first.",
            )

    # Validate event types if specified (must match EventType values)
    if request.event_types:
        invalid_types = set(request.event_types) - set(AVAILABLE_EVENT_TYPES)
        if invalid_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid event types: {', '.join(invalid_types)}. "
                f"Valid types: {', '.join(AVAILABLE_EVENT_TYPES)}",
            )

    # Check prerequisite: historical data must be generated first
    state_manager = GenerationStateManager()
    if not state_manager.can_start_realtime():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Historical data must be generated before starting real-time streaming"
            ),
        )

    # Check Azure connection string
    if not config.realtime.azure_connection_string:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Azure Event Hub connection string not configured",
        )

    new_session_id = f"streaming_{uuid4().hex[:8]}"
    reset_streaming_state()  # Reset stats BEFORE setting session
    set_session(new_session_id, datetime.now(UTC))

    async def streaming_task_outbox():
        """Background task: drain outbox with pacing; generate next day when empty."""
        logger.info(f"🚀 Streaming task starting for session {new_session_id}")
        try:
            update_task_progress(new_session_id, 0.0, "Initializing outbox streaming")

            client = AzureEventHubClient(
                connection_string=config.realtime.get_connection_string(),
                hub_name=config.stream.hub,
                max_batch_size=config.realtime.max_batch_size,
                batch_timeout_ms=config.realtime.batch_timeout_ms,
                retry_attempts=config.realtime.retry_attempts,
                backoff_multiplier=config.realtime.backoff_multiplier,
                circuit_breaker_enabled=config.realtime.circuit_breaker_enabled,
            )
            await client.connect()

            base_interval_ms = (
                request.emit_interval_override
                if getattr(request, "emit_interval_override", None)
                else config.realtime.emit_interval_ms
            )
            jitter_pct = 0.2
            end_at = (
                datetime.now(UTC) + timedelta(minutes=request.duration_minutes)
                if request.duration_minutes
                else None
            )

            conn = get_duckdb_conn()
            fact_gen = await get_fact_generator(config)
            state_mgr = GenerationStateManager()
            total_sent = 0
            events_checked = 0

            logger.info("Starting outbox streaming loop")

            while True:
                if end_at and datetime.now(UTC) >= end_at:
                    break

                item = outbox_lease_next(conn)
                events_checked += 1

                if item:
                    logger.info(
                        f"Got event from outbox: ID={item.get('outbox_id')}, "
                        f"Type={item.get('message_type')}, "
                        f"Time={item.get('event_ts')}"
                    )

                    try:
                        mtype = str(item.get("message_type") or "receipt_created")
                        try:
                            etype = EventType(mtype)
                        except Exception:
                            logger.warning(
                                f"Invalid event type '{mtype}', "
                                f"using RECEIPT_CREATED"
                            )
                            etype = EventType.RECEIPT_CREATED
                        payload = {}
                        try:
                            payload = _json.loads(item.get("payload") or "{}")
                        except Exception as parse_err:
                            logger.warning(f"Failed to parse payload: {parse_err}")
                            payload = {"raw": str(item.get("payload"))}
                        stamp = item.get("event_ts") or datetime.now(UTC)
                        env = EventEnvelope(
                            event_type=etype,
                            payload=payload,
                            trace_id=str(
                                item.get("trace_id") or f"TR_{_uuid4().hex[:12]}"
                            ),
                            ingest_timestamp=stamp,
                            schema_version="1.0",
                            source="retail-datagen-outbox",
                            partition_key=str(item.get("partition_key") or ""),
                        )

                        logger.info(
                            f"Sending event {item.get('outbox_id')} "
                            f"to Azure Event Hub..."
                        )
                        ok = await client.send_event(env)

                        if ok:
                            logger.info(
                                f"✅ Event {item.get('outbox_id')} "
                                f"sent successfully"
                            )
                            outbox_ack_sent(conn, int(item["outbox_id"]))
                            total_sent += 1

                            # Log progress every 100 events
                            if total_sent % 100 == 0:
                                logger.info(f"Progress: {total_sent} events sent")
                        else:
                            logger.warning(
                                f"❌ Event {item.get('outbox_id')} "
                                f"send failed, will retry"
                            )
                            outbox_nack_retry(conn, int(item["outbox_id"]))

                        update_streaming_statistics(
                            {"event_type": etype.value}, success=ok
                        )
                    except Exception as send_err:
                        logger.error(
                            f"Outbox send error for ID {item.get('outbox_id')}: "
                            f"{send_err}",
                            exc_info=True,
                        )
                        try:
                            outbox_nack_retry(conn, int(item["outbox_id"]))
                        except Exception:
                            pass

                    # Delay with jitter for simulation pacing
                    interval = base_interval_ms / 1000.0
                    jitter = interval * (jitter_pct * (2 * random.random() - 1))
                    await _asyncio.sleep(max(0.0, interval + jitter))
                    continue
                else:
                    # Log when we check and find empty outbox
                    if events_checked % 10 == 1:  # Log every 10 checks
                        logger.info(
                            f"Outbox empty after checking {events_checked} "
                            f"times, {total_sent} events sent so far"
                        )

                # Outbox is empty: generate the next day of data and continue
                try:
                    st = state_mgr.load_state()
                    if st.last_generated_timestamp:
                        next_day = st.last_generated_timestamp.date() + timedelta(
                            days=1
                        )
                    else:
                        next_day = datetime.strptime(
                            config.historical.start_date, "%Y-%m-%d"
                        ).date()
                    start_dt = datetime.combine(next_day, datetime.min.time())
                    end_dt = start_dt + timedelta(days=1) - timedelta(seconds=1)

                    logger.info(
                        f"Outbox empty - generating {start_dt.date().isoformat()}"
                    )
                    update_task_progress(
                        new_session_id,
                        0.05,
                        f"Generating {start_dt.date().isoformat()}",
                    )

                    await fact_gen.generate_historical_data(
                        start_dt, end_dt, publish_to_outbox=True
                    )
                    state_mgr.update_fact_generation(end_dt)

                    # Commit and verify events were written to outbox
                    conn.commit()
                    pending_count = conn.execute(
                        "SELECT COUNT(*) FROM streaming_outbox WHERE status='pending'"
                    ).fetchone()[0]

                    logger.info(
                        f"Generated day {start_dt.date()} - "
                        f"{pending_count:,} events in outbox"
                    )

                    if pending_count == 0:
                        logger.warning(
                            f"No events written to outbox for {start_dt.date()} - "
                            f"generation may have failed"
                        )
                        await _asyncio.sleep(5.0)  # Wait before retrying
                    else:
                        # Give a brief pause before starting to drain
                        await _asyncio.sleep(0.1)

                except Exception as gen_err:
                    logger.error(f"Generation failed: {gen_err}")
                    await _asyncio.sleep(1.0)

            await client.disconnect()
            logger.info(
                f"🏁 Streaming completed normally. "
                f"Total events sent: {total_sent}"
            )
            update_task_progress(new_session_id, 1.0, "Streaming completed")
            set_session(None, None)
            return {
                "events_sent": total_sent,
                "end_reason": "completed",
                "mode": "outbox",
            }
        except _asyncio.CancelledError:
            logger.info("🛑 Streaming cancelled by user")
            update_task_progress(new_session_id, 1.0, "Streaming cancelled")
            set_session(None, None)
            return {"events_sent": 0, "end_reason": "cancelled"}
        except Exception as e:
            logger.error(f"💥 Streaming task failed with exception: {e}", exc_info=True)
            streaming_statistics["connection_failures"] += 1
            set_session(None, None)
            raise

    create_background_task(
        new_session_id,
        streaming_task_outbox(),
        f"Stream events: {request.event_types or 'all'}",
    )

    log.info(
        "Streaming started successfully",
        request_id=request_id,
        session_id=new_session_id,
        duration_minutes=request.duration_minutes,
    )
    log.clear_correlation_id()

    return OperationResult(
        success=True,
        message="Event streaming started",
        operation_id=new_session_id,
        started_at=datetime.now(UTC),
    )


@router.post(
    "/stream/stop",
    response_model=OperationResult,
    summary="Stop event streaming",
    description="Stop the currently active event streaming",
)
async def stop_streaming():
    """Stop the currently active event streaming."""
    session_id = get_session_id()

    if not session_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No active streaming session to stop",
        )

    success = cancel_task(session_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Streaming session not found or already completed",
        )

    set_session(None, None)

    return OperationResult(
        success=True, message="Event streaming stopped", operation_id=session_id
    )


@router.get(
    "/stream/status",
    response_model=StreamingStatusResponse,
    summary="Get streaming status",
    description="Get current streaming status and basic statistics",
)
async def get_streaming_status():
    """Get the current streaming status."""
    session_id = get_session_id()
    start_time = get_start_time()

    logger.info(
        f"📡 Status check: session_id={session_id}, "
        f"start_time={start_time}, "
        f"events_sent={streaming_statistics['events_sent_successfully']}"
    )

    is_streaming = session_id is not None
    status_enum = "stopped"
    uptime_seconds = 0.0

    if is_streaming and start_time:
        uptime_seconds = (datetime.now(UTC) - start_time).total_seconds()

        # Check actual task status
        task_status = get_task_status(session_id)
        logger.info(f"📡 Task status for {session_id}: {task_status}")

        if task_status:
            status_map = {
                "running": "running",
                "completed": "stopped",
                "failed": "error",
                "cancelled": "stopped",
            }
            status_enum = status_map.get(task_status["status"], "stopped")
        else:
            status_enum = "error"
            logger.warning(f"⚠️ No task status found for session {session_id}")
    else:
        if session_id is None:
            logger.info("📡 Status: No active session (session_id is None)")
        else:
            logger.info("📡 Status: Session exists but no start_time")

    return StreamingStatusResponse(
        is_streaming=is_streaming,
        status=status_enum,
        uptime_seconds=uptime_seconds,
        events_sent=streaming_statistics["events_sent_successfully"],
        events_per_second=streaming_statistics["events_per_second"],
        last_event_time=streaming_statistics["last_event_time"],
    )


@router.post(
    "/stream/pause",
    summary="Pause streaming",
    description="Pause active streaming without stopping completely",
)
async def pause_streaming(
    event_streamer: EventStreamer = Depends(get_event_streamer),
):
    """
    Pause active streaming without stopping completely.

    Streaming state is maintained, so it can be resumed seamlessly.
    No events are generated or sent while paused.
    """
    if not event_streamer:
        raise HTTPException(
            status_code=400, detail="No active streamer. Start streaming first."
        )

    result = await event_streamer.pause()

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return result


@router.post(
    "/stream/resume",
    summary="Resume streaming",
    description="Resume a paused streaming session",
)
async def resume_streaming(
    event_streamer: EventStreamer = Depends(get_event_streamer),
):
    """
    Resume paused streaming.

    Continues from where it left off with no event loss.
    """
    if not event_streamer:
        raise HTTPException(
            status_code=400, detail="No active streamer. Start streaming first."
        )

    result = await event_streamer.resume()

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return result


@router.get(
    "/stream/pause-status",
    summary="Get pause status",
    description="Get current pause status and statistics",
)
async def get_pause_status(
    event_streamer: EventStreamer = Depends(get_event_streamer),
):
    """Get current pause status and statistics."""
    if not event_streamer:
        return {
            "is_paused": False,
            "pause_count": 0,
            "total_pause_duration_seconds": 0.0,
            "currently_paused_duration": 0.0,
        }

    return event_streamer.get_pause_statistics()
