"""
Outbox management endpoints.

This module handles streaming outbox operations for the event queue.
"""

import asyncio as _asyncio
import json as _json
import logging
import random
from datetime import UTC, datetime
from uuid import uuid4
from uuid import uuid4 as _uuid4

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from ...api.models import OperationResult, TablePreviewResponse
from ...config.models import RetailConfig
from ...db.duckdb_engine import (
    get_duckdb_conn,
    outbox_ack_sent,
    outbox_counts,
    outbox_lease_next,
    outbox_nack_retry,
)
from ...shared.dependencies import (
    create_background_task,
    get_config,
    update_task_progress,
)
from ...streaming.azure_client import AzureEventHubClient
from ...streaming.schemas import EventEnvelope, EventType
from .state import update_streaming_statistics

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/stream/outbox/status",
    summary="Get outbox status",
    description="Return counts by status for the streaming outbox",
)
async def get_outbox_status():
    """Get outbox counts by status."""
    conn = get_duckdb_conn()
    counts = outbox_counts(conn)
    return counts


@router.get(
    "/stream/outbox/preview",
    summary="Preview streaming outbox",
    description="Return a preview of rows from the streaming_outbox table",
)
async def preview_outbox(
    status_filter: str | None = Query(
        default=None,
        description="Optional status to filter by (pending, processing, sent)",
    ),
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
):
    """Preview rows from the streaming_outbox table for UI diagnostics."""
    try:
        conn = get_duckdb_conn()

        base_query = (
            "SELECT outbox_id, event_ts, message_type, status, attempts, "
            "last_attempt_ts, sent_ts, partition_key, trace_id "
            "FROM streaming_outbox"
        )
        params: list = []
        if status_filter:
            base_query += " WHERE status = ?"
            params.append(status_filter)
        base_query += " ORDER BY event_ts DESC, outbox_id DESC LIMIT ?"
        params.append(int(limit))

        total_count = 0
        try:
            total_count = int(
                conn.execute("SELECT COUNT(*) FROM streaming_outbox").fetchone()[0]
            )
        except Exception:
            total_count = 0

        cur = conn.execute(base_query, params)
        columns = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
        preview_rows = [
            {columns[i]: rows[j][i] for i in range(len(columns))}
            for j in range(len(rows))
        ]

        return TablePreviewResponse(
            table_name="streaming_outbox",
            columns=columns,
            row_count=total_count,
            preview_rows=preview_rows,
        )
    except Exception as e:
        logger.error(f"Failed to preview streaming_outbox: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to preview outbox: {str(e)}",
        )


@router.post(
    "/stream/outbox/drain",
    response_model=OperationResult,
    summary="Drain outbox",
    description="Drain all pending outbox events with pacing until empty",
)
async def drain_outbox(
    emit_interval_ms: int | None = Body(
        default=None, description="Override emit interval in milliseconds"
    ),
    config: RetailConfig = Depends(get_config),
):
    """Drain all pending outbox events."""
    task_id = f"drain_outbox_{uuid4().hex[:8]}"

    async def _drain_task():
        try:
            update_task_progress(task_id, 0.0, "Starting outbox drain")
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
            base_interval = (emit_interval_ms or config.realtime.emit_interval_ms) / 1000.0
            jitter_pct = 0.2
            conn = get_duckdb_conn()
            sent = 0

            while True:
                item = outbox_lease_next(conn)
                if not item:
                    break
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
                        trace_id=str(item.get("trace_id") or f"TR_{_uuid4().hex[:12]}"),
                        ingest_timestamp=stamp,
                        schema_version="1.0",
                        source="retail-datagen-outbox",
                        partition_key=str(item.get("partition_key") or ""),
                    )
                    ok = await client.send_event(env)
                    update_streaming_statistics({"event_type": etype.value}, success=ok)
                    if ok:
                        outbox_ack_sent(conn, int(item["outbox_id"]))
                        sent += 1
                    else:
                        outbox_nack_retry(conn, int(item["outbox_id"]))
                finally:
                    # pacing
                    jitter = base_interval * (jitter_pct * (2 * random.random() - 1))
                    await _asyncio.sleep(max(0.0, base_interval + jitter))

            await client.disconnect()
            update_task_progress(task_id, 1.0, f"Drained outbox ({sent} events)")
            return {"events_sent": sent}
        except Exception as e:
            logger.error(f"Outbox drain failed: {e}")
            raise

    create_background_task(task_id, _drain_task(), "Drain outbox")
    return OperationResult(
        success=True,
        message="Outbox drain started",
        operation_id=task_id,
        started_at=datetime.now(UTC),
    )


@router.delete(
    "/stream/outbox/clear",
    response_model=OperationResult,
    summary="Clear outbox",
    description="Drop and recreate the streaming_outbox table (fast reset)",
)
async def clear_outbox():
    """Clear all outbox entries."""
    try:
        conn = get_duckdb_conn()
        # Fast reset: drop and recreate table
        conn.execute("DROP TABLE IF EXISTS streaming_outbox")
        # Recreate via ensure helper
        from retail_datagen.db.duckdb_engine import _ensure_outbox_table as _ensure

        _ensure(conn)
        return OperationResult(success=True, message="Outbox cleared")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clear outbox: {str(e)}",
        )
