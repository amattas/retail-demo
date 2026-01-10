"""
FastAPI router for historical fact data generation endpoints.

This module provides REST API endpoints for generating historical fact data
with comprehensive status tracking and validation.
"""

import logging
import traceback
from datetime import UTC, datetime
from threading import Lock
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from ...api.models import (
    GenerationStatusResponse,
    HistoricalDataRequest,
    OperationResult,
)
from ...config.models import RetailConfig
from ...generators.fact_generators import FactDataGenerator
from ...generators.generation_state import GenerationStateManager
from ...shared.dependencies import (
    create_background_task,
    get_config,
    get_fact_generator,
    get_task_status,
    rate_limit,
    update_task_progress,
    validate_date_range,
    validate_table_name,
)
from .common import FACT_TABLES

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/facts/date-range",
    summary="Get overall fact date range",
    description="Return the earliest and latest event_ts across all fact tables",
)
async def get_overall_fact_date_range():
    """Compute overall min and max event_ts across all fact tables."""
    try:
        from retail_datagen.services import get_all_fact_table_date_ranges

        ranges = get_all_fact_table_date_ranges()
        mins = [r[0] for r in ranges.values() if r and r[0] is not None]
        maxs = [r[1] for r in ranges.values() if r and r[1] is not None]
        min_ts = min(mins) if mins else None
        max_ts = max(maxs) if maxs else None
        return {
            "min_event_ts": min_ts.isoformat() if min_ts else None,
            "max_event_ts": max_ts.isoformat() if max_ts else None,
            "per_table": {
                k: {
                    "min_event_ts": (v[0].isoformat() if v and v[0] else None),
                    "max_event_ts": (v[1].isoformat() if v and v[1] else None),
                }
                for k, v in ranges.items()
            },
        }
    except Exception as e:
        logger.error(f"Failed to compute fact date range: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to compute fact date range: {str(e)}",
        )


@router.post(
    "/generate/fact",
    response_model=OperationResult,
    summary="Generate historical fact data",
    description="Generate fact tables using intelligent date range logic",
)
@rate_limit(
    max_requests=5, window_seconds=300
)  # 5 requests per 5 minutes (heavy operation)
async def generate_historical_data(
    request: HistoricalDataRequest | None = Body(default=None),
    fact_generator: FactDataGenerator = Depends(get_fact_generator),
    config: RetailConfig = Depends(get_config),
):
    """
    Generate historical fact data using intelligent date range logic.

    Processes data sequentially one day at a time with deterministic ordering.
    Provides rich hourly progress updates (24 updates per day per table).

    Note: Parallel processing is not supported in the current backend.
    """

    # Debug logging - log the received request
    logger.info(f"Historical data request received: {request}")

    # Handle case where no request body is provided
    if request is None:
        request = HistoricalDataRequest()  # Use defaults

    # Initialize generation state manager
    state_manager = GenerationStateManager()

    # Determine start and end dates
    if request.start_date and request.end_date:
        # Manual override provided - validate date range
        validate_date_range(request.start_date, request.end_date)
        start_date = request.start_date
        end_date = request.end_date
        logger.info(f"Using manual date range: {start_date} to {end_date}")
    else:
        # Use intelligent date range logic
        config_start_date = datetime.strptime(config.historical.start_date, "%Y-%m-%d")
        start_date, end_date = state_manager.get_fact_date_range(config_start_date)
        logger.info(f"Using intelligent date range: {start_date} to {end_date}")

    tables_to_generate = FACT_TABLES

    # Validate table names
    for table in tables_to_generate:
        validate_table_name(table, "fact")

    task_id = f"fact_generation_{uuid4().hex[:8]}"

    async def generation_task():
        """Background task for historical data generation."""
        try:
            update_task_progress(task_id, 0.0, "Starting fact data generation")

            # Pre-check: validate DuckDB has required rows for historical generation
            try:
                from retail_datagen.db.duck_master_reader import (
                    read_customers,
                    read_distribution_centers,
                    read_geographies,
                    read_products,
                    read_stores,
                )

                geo_cnt = len(read_geographies())
                store_cnt = len(read_stores())
                dc_cnt = len(read_distribution_centers())
                cust_cnt = len(read_customers())
                prod_cnt = len(read_products())

                update_task_progress(
                    task_id,
                    0.01,
                    f"Dimensions ready: {geo_cnt} geos, {store_cnt} stores, "
                    f"{dc_cnt} DCs, {cust_cnt} customers, {prod_cnt} products",
                )

                if store_cnt == 0 or cust_cnt == 0 or prod_cnt == 0:
                    raise RuntimeError(
                        "DuckDB missing required data. "
                        "Ensure master generation completed successfully."
                    )
            except Exception as pre_exc:
                update_task_progress(
                    task_id,
                    0.0,
                    f"Pre-check failed: {pre_exc}",
                )
                raise

            # Calculate total days for progress tracking
            total_days = (end_date - start_date).days + 1

            # Create a progress callback that updates the task progress
            def progress_callback(
                current_day: int,
                message: str,
                table_progress: dict[str, float] | None = None,
                current_table: str | None = None,
                tables_completed: list[str] | None = None,
                tables_failed: list[str] | None = None,
                tables_in_progress: list[str] | None = None,
                tables_remaining: list[str] | None = None,
                estimated_seconds_remaining: float | None = None,
                progress_rate: float | None = None,
                table_counts: dict[str, int] | None = None,
                # NEW: Hourly progress fields
                current_hour: int | None = None,
                hourly_progress: dict[str, float] | None = None,
                total_hours_completed: int | None = None,
            ):
                progress = current_day / total_days if total_days > 0 else 0.0
                if total_days > 0 and current_day == 0:
                    progress = max(progress, 0.01)

                # Filter table_progress to only known UI fact tables
                filtered_table_progress = None
                if table_progress is not None:
                    filtered_table_progress = {
                        name: value
                        for name, value in table_progress.items()
                        if name in FACT_TABLES
                    }

                # Filter state lists to only known UI fact tables
                # Pass through states from generator without manual calculation
                tables_in_progress_filtered = [
                    t for t in (tables_in_progress or []) if t in FACT_TABLES
                ]
                tables_remaining_filtered = [
                    t for t in (tables_remaining or []) if t in FACT_TABLES
                ]
                tables_completed_filtered = [
                    t for t in (tables_completed or []) if t in FACT_TABLES
                ]
                tables_failed_filtered = [
                    t for t in (tables_failed or []) if t in FACT_TABLES
                ]

                update_task_progress(
                    task_id,
                    progress,
                    message,
                    table_progress=filtered_table_progress,
                    current_table=current_table,
                    tables_completed=tables_completed_filtered,
                    tables_failed=tables_failed_filtered,
                    tables_in_progress=tables_in_progress_filtered,
                    tables_remaining=tables_remaining_filtered,
                    estimated_seconds_remaining=estimated_seconds_remaining,
                    progress_rate=progress_rate,
                    table_counts=table_counts,
                    # NEW: Pass hourly progress fields
                    current_day=current_day,
                    current_hour=current_hour,
                    hourly_progress=hourly_progress,
                    total_hours_completed=total_hours_completed,
                )

            # Also wire a master-style per-table progress callback for
            # consistent UI updates
            per_table_progress: dict[str, float] = {
                table: 0.0 for table in tables_to_generate
            }
            progress_lock = Lock()

            def per_table_callback(
                table_name: str,
                progress_value: float,
                detail_message: str | None,
                table_counts: dict[str, int] | None = None,
                tables_completed: list[str] | None = None,
                tables_in_progress: list[str] | None = None,
                tables_remaining: list[str] | None = None,
            ) -> None:
                """
                Per-table progress callback for historical generation.

                Now accepts table state lists directly from TableProgressTracker.
                The router no longer recalculates states from progress percentages.
                """
                if table_name not in FACT_TABLES:
                    return

                # Protect dict operations with lock for thread safety
                with progress_lock:
                    if table_name not in per_table_progress:
                        return

                    per_table_progress[table_name] = max(0.0, min(1.0, progress_value))

                    overall_progress = (
                        sum(per_table_progress.values()) / len(per_table_progress)
                        if per_table_progress
                        else progress_value
                    )

                    # Pass through state lists from generator without modification
                    # Generator's TableProgressTracker provides correct states
                    # Call update while holding lock (brief operation, acceptable)
                    update_task_progress(
                        task_id,
                        overall_progress,
                        detail_message or f"Generating {table_name.replace('_', ' ')}",
                        table_progress=dict(
                            per_table_progress
                        ),  # Pass copy while locked
                        tables_completed=tables_completed or [],
                        tables_in_progress=tables_in_progress or [],
                        tables_remaining=tables_remaining or [],
                        table_counts=table_counts,
                    )

            # Register both callbacks on the generator (per-table and day-based)
            try:
                fact_generator.set_table_progress_callback(per_table_callback)
            except Exception:
                # Backwards compatibility: if method missing, skip
                pass

            # Keep day-based progress updates as well for overall progress and ETA
            try:
                fact_generator.set_progress_callback(progress_callback)
            except Exception:
                # Backwards compatibility: fallback to direct assignment
                fact_generator._progress_callback = progress_callback

            # Emit an initialization update so the UI shows immediate activity
            try:
                progress_callback(
                    0,
                    "Preparing historical data generation",
                    table_progress={table: 0.0 for table in tables_to_generate},
                    tables_completed=[],
                    tables_in_progress=None,
                    tables_remaining=None,
                )
            except Exception as exc:
                logger.debug(f"Unable to send initial progress update: {exc}")

            # Ensure generator uses all fact tables (switches removed)
            try:
                fact_generator.set_included_tables(None)
            except Exception:
                pass

            logger.info(
                f"Starting historical generation from {start_date.date()} "
                f"to {end_date.date()}"
            )

            # Generate historical data using the fact generator
            # Historical generation runs sequentially to manage memory
            summary = await fact_generator.generate_historical_data(
                start_date, end_date, publish_to_outbox=False
            )

            # Update generation state with the end timestamp
            state_manager.update_fact_generation(end_date)

            # Final table counts from summary for accuracy
            final_counts = summary.facts_generated
            update_task_progress(
                task_id,
                1.0,
                "Historical data generation completed",
                table_counts=final_counts,
                tables_completed=tables_to_generate,
                tables_in_progress=[],
                tables_remaining=[],
            )

            return {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "days_generated": (end_date - start_date).days + 1,
                "tables_generated": tables_to_generate,
                "total_records": summary.total_records,
                "partitions_created": summary.partitions_created,
                "generation_time_seconds": summary.generation_time_seconds,
            }

        except Exception as e:
            logger.error(f"Historical data generation failed: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Surface error to task status for UI
            update_task_progress(
                task_id,
                0.0,
                f"Historical generation failed: {e}",
                tables_failed=tables_to_generate,
            )
            raise

    create_background_task(
        task_id,
        generation_task(),
        f"Generate historical data: {start_date.date()} to {end_date.date()}",
    )

    return OperationResult(
        success=True,
        message="Historical data generation started",
        operation_id=task_id,
        started_at=datetime.now(UTC),
    )


@router.get(
    "/generate/fact/status",
    response_model=GenerationStatusResponse,
    summary="Get historical data generation status",
    description="Get the status of historical data generation operations",
)
async def get_historical_generation_status(
    operation_id: str = Query(..., description="Operation ID from generation request"),
):
    """Get the status of a historical data generation operation."""

    task_status = get_task_status(operation_id)

    if not task_status:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Operation {operation_id} not found",
        )

    # Map internal status to API status
    api_status = {
        "running": "running",
        "completed": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
    }.get(task_status["status"], "pending")

    return GenerationStatusResponse(
        status=api_status,
        progress=task_status.get("progress", 0.0),
        message=task_status.get("message", ""),
        estimated_completion=task_status.get("estimated_completion"),
        error_message=task_status.get("error"),
        table_progress=task_status.get("table_progress"),
        current_table=task_status.get("current_table"),
        tables_failed=task_status.get("tables_failed") or [],
        tables_completed=task_status.get("tables_completed") or [],
        tables_remaining=task_status.get("tables_remaining") or [],
        tables_in_progress=task_status.get("tables_in_progress"),
        estimated_seconds_remaining=task_status.get("estimated_seconds_remaining"),
        progress_rate=task_status.get("progress_rate"),
        last_update_timestamp=task_status.get("last_update_timestamp"),
        sequence=task_status.get("sequence"),
        # Hourly progress fields (optional)
        current_day=task_status.get("current_day"),
        current_hour=task_status.get("current_hour"),
        hourly_progress=task_status.get("hourly_progress"),
        total_hours_completed=task_status.get("total_hours_completed"),
        # Optional table counts if provided by background task
        table_counts=task_status.get("table_counts"),
    )


@router.post(
    "/generate/fact/{table_name}",
    response_model=OperationResult,
    summary="Generate specific historical table",
    description="Generate a specific fact table using intelligent date range logic",
)
@rate_limit(max_requests=10, window_seconds=300)
async def generate_specific_historical_table(
    table_name: str,
    start_date: datetime = Query(
        None, description="Start date for generation (optional)"
    ),
    end_date: datetime = Query(None, description="End date for generation (optional)"),
    fact_generator: FactDataGenerator = Depends(get_fact_generator),
    config: RetailConfig = Depends(get_config),
):
    """Generate a specific fact table using intelligent date range logic."""

    # Validate table name
    validate_table_name(table_name, "fact")

    # Initialize generation state manager
    state_manager = GenerationStateManager()

    # Determine start and end dates
    if start_date and end_date:
        # Manual override provided - validate date range
        validate_date_range(start_date, end_date)
        logger.info(f"Using manual date range: {start_date} to {end_date}")
    else:
        # Use intelligent date range logic
        config_start_date = datetime.strptime(config.historical.start_date, "%Y-%m-%d")
        start_date, end_date = state_manager.get_fact_date_range(config_start_date)
        logger.info(f"Using intelligent date range: {start_date} to {end_date}")

    task_id = f"historical_{table_name}_{uuid4().hex[:8]}"

    async def generation_task():
        """Background task for specific historical table generation."""
        try:
            update_task_progress(task_id, 0.0, f"Starting generation of {table_name}")

            # Generate historical data using the fact generator (async)
            summary = await fact_generator.generate_historical_data(
                start_date, end_date, publish_to_outbox=False
            )

            # Update generation state with the end timestamp
            state_manager.update_fact_generation(end_date)

            update_task_progress(task_id, 1.0, f"Generated {table_name} for date range")

            return {
                "table_name": table_name,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "days_generated": (end_date - start_date).days + 1,
                "total_records": summary.total_records,
                "partitions_created": summary.partitions_created,
                "generation_time_seconds": summary.generation_time_seconds,
            }

        except Exception as e:
            logger.error(f"Generation of {table_name} failed: {e}")
            raise

    create_background_task(
        task_id,
        generation_task(),
        f"Generate {table_name}: {start_date.date()} to {end_date.date()}",
    )

    return OperationResult(
        success=True,
        message=f"Generation of {table_name} started",
        operation_id=task_id,
        started_at=datetime.now(UTC),
    )
