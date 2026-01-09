"""
Streaming control endpoints (start, stop, pause, resume, status).

This module handles the core streaming lifecycle operations.
"""

import asyncio
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
                "Historical data must be generated before "
                "starting real-time streaming"
            ),
        )

    # Check Azure connection string
    if not config.realtime.azure_connection_string:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Azure Event Hub connection string not configured",
        )

    new_session_id = f"streaming_{uuid4().hex[:8]}"
    set_session(new_session_id, datetime.now(UTC))
    reset_streaming_state()

    async def streaming_task():
        """Background task for event streaming via EventStreamer."""
        try:
            update_task_progress(new_session_id, 0.0, "Initializing event streaming")

            # Prefer DuckDB batch streaming
            try:
                duck_streamer = EventStreamer(
                    config=config,
                    azure_connection_string=config.realtime.azure_connection_string,
                )
                # Start DuckDB batch streaming
                await duck_streamer.start(duration=timedelta(seconds=0))
                # start() will internally choose DuckDB batch path first
                stats = await duck_streamer.get_statistics()
                update_task_progress(new_session_id, 1.0, "Batch streaming completed")
                return {
                    "events_sent": stats.get("events_sent_successfully", 0),
                    "duration_minutes": request.duration_minutes,
                    "event_types": request.event_types or AVAILABLE_EVENT_TYPES,
                    "end_reason": "batch_completed",
                    "mode": "duckdb_batch",
                }
            except Exception as db_error:
                logger.warning(
                    f"DuckDB batch streaming failed, falling back to real-time: "
                    f"{db_error}"
                )
                # Fall through to real-time mode

            # Fall back to real-time generation mode
            if request.event_types:
                event_streamer.set_allowed_event_types(request.event_types)
            duration = (
                timedelta(minutes=request.duration_minutes)
                if request.duration_minutes
                else None
            )

            # Start streaming; runs until duration or stop
            await event_streamer.start(duration=duration)

            update_task_progress(new_session_id, 1.0, "Streaming completed")

            stats = await event_streamer.get_statistics()
            return {
                "events_sent": stats.get("events_sent_successfully", 0),
                "duration_minutes": request.duration_minutes,
                "event_types": request.event_types or AVAILABLE_EVENT_TYPES,
                "end_reason": (
                    "duration_completed" if request.duration_minutes else "manual_stop"
                ),
                "mode": "real_time",
            }

        except asyncio.CancelledError:
            update_task_progress(new_session_id, 1.0, "Streaming cancelled")
            return {"events_sent": 0, "end_reason": "cancelled"}
        except Exception as e:
            logger.error(f"Streaming task failed: {e}")
            streaming_statistics["connection_failures"] += 1
            raise
        finally:
            set_session(None, None)

    async def streaming_task_outbox():
        """Background task: drain outbox with pacing; generate next day when empty."""
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

            while True:
                if end_at and datetime.now(UTC) >= end_at:
                    break

                item = outbox_lease_next(conn)
                if item:
                    try:
                        mtype = str(item.get("message_type") or "receipt_created")
                        try:
                            etype = EventType(mtype)
                        except Exception:
                            etype = EventType.RECEIPT_CREATED
                        payload = {}
                        try:
                            payload = _json.loads(item.get("payload") or "{}")
                        except Exception:
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
                        ok = await client.send_event(env)
                        update_streaming_statistics(
                            {"event_type": etype.value}, success=ok
                        )
                        if ok:
                            outbox_ack_sent(conn, int(item["outbox_id"]))
                            total_sent += 1
                        else:
                            outbox_nack_retry(conn, int(item["outbox_id"]))
                    except Exception as send_err:
                        logger.error(f"Outbox send error: {send_err}")
                        try:
                            outbox_nack_retry(conn, int(item["outbox_id"]))
                        except Exception:
                            pass

                    # Delay with jitter for simulation pacing
                    interval = base_interval_ms / 1000.0
                    jitter = interval * (jitter_pct * (2 * random.random() - 1))
                    await _asyncio.sleep(max(0.0, interval + jitter))
                    continue

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
                    update_task_progress(
                        new_session_id,
                        0.05,
                        f"Generating {start_dt.date().isoformat()}",
                    )
                    await fact_gen.generate_historical_data(
                        start_dt, end_dt, publish_to_outbox=True
                    )
                    state_mgr.update_fact_generation(end_dt)
                except Exception as gen_err:
                    logger.error(f"Generation failed: {gen_err}")
                    await _asyncio.sleep(1.0)

            await client.disconnect()
            update_task_progress(new_session_id, 1.0, "Streaming completed")
            return {
                "events_sent": total_sent,
                "end_reason": "completed",
                "mode": "outbox",
            }
        except _asyncio.CancelledError:
            update_task_progress(new_session_id, 1.0, "Streaming cancelled")
            return {"events_sent": 0, "end_reason": "cancelled"}
        except Exception as e:
            logger.error(f"Streaming task failed: {e}")
            streaming_statistics["connection_failures"] += 1
            raise
        finally:
            set_session(None, None)

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

    is_streaming = session_id is not None
    status_enum = "stopped"
    uptime_seconds = 0.0

    if is_streaming and start_time:
        uptime_seconds = (datetime.now(UTC) - start_time).total_seconds()

        # Check actual task status
        task_status = get_task_status(session_id)
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
