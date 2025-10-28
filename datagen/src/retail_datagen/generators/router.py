"""
FastAPI router for data generation endpoints.

This module provides REST API endpoints for generating master data (dimensions)
and historical fact data with comprehensive status tracking and validation.
"""

import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path
from threading import Lock
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from ..api.models import (
    GenerationStatusResponse,
    HistoricalDataRequest,
    MasterDataRequest,
    OperationResult,
    TableListResponse,
    TablePreviewResponse,
)
from ..config.models import RetailConfig
from ..generators.fact_generator import FactDataGenerator
from ..generators.generation_state import GenerationStateManager
from ..generators.master_generator import MasterDataGenerator
from ..shared.cache import CacheManager
from ..shared.dependencies import (
    cancel_task,
    create_background_task,
    get_config,
    get_fact_generator,
    get_master_generator,
    get_task_status,
    rate_limit,
    update_task_progress,
    validate_date_range,
    validate_table_name,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Available table definitions (updated to match validation)
MASTER_TABLES = [
    "geographies_master",
    "stores",
    "distribution_centers",
    "trucks",
    "customers",
    "products_master",
]

# Mapping of table names to SQLAlchemy models
from ..db.models.master import Geography, Store, DistributionCenter, Truck, Customer, Product
from ..db.models.facts import (
    DCInventoryTransaction,
    TruckMove,
    StoreInventoryTransaction,
    Receipt,
    ReceiptLine,
    FootTraffic,
    BLEPing,
    MarketingImpression,
    OnlineOrder,
)

# Master table models
MASTER_TABLE_MODELS = {
    "geographies_master": Geography,
    "stores": Store,
    "distribution_centers": DistributionCenter,
    "trucks": Truck,
    "customers": Customer,
    "products_master": Product,
}

# Fact table models
FACT_TABLE_MODELS = {
    "dc_inventory_txn": DCInventoryTransaction,
    "truck_moves": TruckMove,
    "store_inventory_txn": StoreInventoryTransaction,
    "receipts": Receipt,
    "receipt_lines": ReceiptLine,
    "foot_traffic": FootTraffic,
    "ble_pings": BLEPing,
    "marketing": MarketingImpression,
    "online_orders": OnlineOrder,
}

# Unified mapping of all tables
ALL_TABLE_MODELS = {**MASTER_TABLE_MODELS, **FACT_TABLE_MODELS}

FACT_TABLES = [
    "dc_inventory_txn",
    "truck_moves",
    "store_inventory_txn",
    "receipts",
    "receipt_lines",
    "foot_traffic",
    "ble_pings",
    "marketing",
    "online_orders",
]


# ================================
# MASTER DATA GENERATION ENDPOINTS
# ================================


@router.post(
    "/generate/master",
    response_model=OperationResult,
    summary="Generate all master data",
    description="Generate all dimension tables (master data) from dictionary sources",
)
@rate_limit(max_requests=10, window_seconds=300)  # 10 requests per 5 minutes
async def generate_all_master_data(
    request: MasterDataRequest,
    master_generator: MasterDataGenerator = Depends(get_master_generator),
    config: RetailConfig = Depends(get_config),
):
    """Generate all master data tables."""

    tables_to_generate = request.tables or MASTER_TABLES

    # Validate table names
    for table in tables_to_generate:
        validate_table_name(table, "master")

    task_id = f"master_generation_{uuid4().hex[:8]}"

    async def generation_task():
        """Background task for master data generation."""
        try:
            start_time = time.perf_counter()

            def compute_timing_metrics(progress_value: float) -> tuple[float | None, float | None]:
                """Return ETA and progress rate for the given completion fraction."""
                elapsed = time.perf_counter() - start_time
                clamped_progress = max(0.0, min(1.0, progress_value))
                if elapsed <= 0 or clamped_progress <= 0:
                    return None, None

                rate = clamped_progress / elapsed
                if rate <= 0:
                    return None, None

                if clamped_progress >= 1.0:
                    return 0.0, rate

                remaining_fraction = 1.0 - clamped_progress
                eta_seconds = remaining_fraction / rate
                return eta_seconds, rate

            total_tables = len(tables_to_generate)
            if total_tables == 0:
                update_task_progress(
                    task_id,
                    1.0,
                    "No master tables requested",
                    tables_completed=[],
                    tables_in_progress=[],
                    tables_remaining=[],
                    estimated_seconds_remaining=0.0,
                    progress_rate=None,
                )
                return {
                    "tables_generated": [],
                    "total_tables": 0,
                    "skipped_existing": False,
                }

            table_progress = {table: 0.0 for table in tables_to_generate}
            requested_tables = set(tables_to_generate)

            # Short-circuit when everything already exists and regeneration not forced
            if not request.force_regenerate:
                missing_tables: list[str] = []
                for table in tables_to_generate:
                    output_path = Path(config.paths.master) / f"{table}.csv"
                    if not output_path.exists():
                        missing_tables.append(table)

                if not missing_tables:
                    update_task_progress(
                        task_id,
                        1.0,
                        "All requested master tables already generated",
                        tables_completed=list(tables_to_generate),
                        tables_in_progress=[],
                        tables_remaining=[],
                        estimated_seconds_remaining=0.0,
                        progress_rate=None,
                    )
                    return {
                        "tables_generated": list(tables_to_generate),
                        "total_tables": total_tables,
                        "skipped_existing": True,
                    }

            def master_progress_callback(
                table_name: str,
                progress_value: float,
                detail_message: str | None,
                table_counts: dict[str, int] | None = None,
            ) -> None:
                if table_name not in table_progress:
                    return

                table_progress[table_name] = max(
                    0.0, min(1.0, progress_value)
                )

                tables_completed = [
                    table for table, value in table_progress.items() if value >= 1.0
                ]
                tables_in_progress = [
                    table for table, value in table_progress.items() if 0.0 < value < 1.0
                ]
                tables_remaining = [
                    table for table, value in table_progress.items() if value == 0.0
                ]

                overall_progress = (
                    sum(table_progress.values()) / total_tables if total_tables else progress_value
                )
                eta_estimate, rate_estimate = compute_timing_metrics(overall_progress)
                callback_message = detail_message or f"Generating {table_name.replace('_', ' ')}"

                update_task_progress(
                    task_id,
                    overall_progress,
                    callback_message,
                    tables_completed=tables_completed,
                    tables_in_progress=tables_in_progress,
                    tables_remaining=tables_remaining,
                    table_progress=table_progress.copy(),
                    estimated_seconds_remaining=eta_estimate,
                    progress_rate=rate_estimate,
                    table_counts=table_counts,
                )

            master_generator.set_progress_callback(master_progress_callback)

            update_task_progress(
                task_id,
                0.0,
                "Starting master data generation",
                tables_completed=[],
                tables_in_progress=list(tables_to_generate),
                tables_remaining=list(tables_to_generate),
                estimated_seconds_remaining=None,
                progress_rate=None,
                table_progress=table_progress.copy(),
            )

            # Run full generation once (progress callback handles per-table reporting)
            # Note: parallel=False required for SQLite (AsyncSession can't be shared across threads)
            from retail_datagen.db.session import get_master_session
            from sqlalchemy import text

            async with get_master_session() as session:
                # Clear existing data to avoid UNIQUE constraint violations
                logger.info("Clearing existing master data...")
                for table in [
                    "dim_customers",
                    "dim_products",
                    "dim_trucks",
                    "dim_stores",
                    "dim_distribution_centers",
                    "dim_geographies",
                ]:
                    await session.execute(text(f"DELETE FROM {table}"))
                await session.flush()  # Flush deletes immediately
                logger.info("Existing master data cleared")

                await master_generator.generate_all_master_data_async(
                    session=session,
                    parallel=False
                )
                # Context manager will commit everything on exit

            for table in table_progress:
                table_progress[table] = max(table_progress[table], 1.0)

            completed_tables = list(table_progress.keys())

            final_eta, final_rate = compute_timing_metrics(1.0)
            update_task_progress(
                task_id,
                1.0,
                "Master data generation completed",
                tables_completed=completed_tables,
                tables_in_progress=[],
                tables_remaining=[],
                estimated_seconds_remaining=final_eta,
                progress_rate=final_rate,
                table_progress=table_progress.copy(),
            )

            return {
                "tables_generated": completed_tables,
                "total_tables": total_tables,
                "skipped_existing": False,
            }

        except Exception as e:
            logger.error(f"Master data generation failed: {e}")
            raise
        finally:
            master_generator.set_progress_callback(None)

    create_background_task(
        task_id,
        generation_task(),
        f"Generate master data: {', '.join(tables_to_generate)}",
    )

    return OperationResult(
        success=True,
        message="Master data generation started",
        operation_id=task_id,
        started_at=datetime.now(),
    )


@router.get(
    "/generate/master/status",
    response_model=GenerationStatusResponse,
    summary="Get master data generation status",
    description="Get the status of master data generation operations",
)
async def get_master_generation_status(
    operation_id: str = Query(..., description="Operation ID from generation request"),
):
    """Get the status of a master data generation operation."""

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

    response = GenerationStatusResponse(
        status=api_status,
        progress=task_status.get("progress", 0.0),
        message=task_status.get("message", ""),
        estimated_completion=task_status.get("estimated_completion"),
        error_message=task_status.get("error"),
        table_progress=task_status.get("table_progress"),
        tables_completed=task_status.get("tables_completed") or [],
        tables_remaining=task_status.get("tables_remaining") or [],
        tables_in_progress=task_status.get("tables_in_progress"),
        estimated_seconds_remaining=task_status.get("estimated_seconds_remaining"),
        progress_rate=task_status.get("progress_rate"),
        last_update_timestamp=task_status.get("last_update_timestamp"),
        table_counts=task_status.get("table_counts"),
    )

    # Add completed tables from result if available and not already set
    if not response.tables_completed and "result" in task_status and task_status["result"]:
        result = task_status["result"]
        response.tables_completed = result.get("tables_generated", [])

    return response


@router.post(
    "/generate/master/{table_name}",
    response_model=OperationResult,
    summary="Generate specific master table",
    description="Generate a specific dimension table",
)
@rate_limit(max_requests=20, window_seconds=300)
async def generate_specific_master_table(
    table_name: str,
    force_regenerate: bool = Query(
        False, description="Force regeneration of existing data"
    ),
    master_generator: MasterDataGenerator = Depends(get_master_generator),
    config: RetailConfig = Depends(get_config),
):
    """Generate a specific master data table."""

    # Validate table name
    validate_table_name(table_name, "master")

    # Check if table already exists
    output_path = Path(config.paths.master) / f"{table_name}.csv"
    if output_path.exists() and not force_regenerate:
        return OperationResult(
            success=True,
            message=f"Table {table_name} already exists. Use force_regenerate=true to overwrite.",
            started_at=datetime.now(),
        )

    task_id = f"master_{table_name}_{uuid4().hex[:8]}"

    async def generation_task():
        """Background task for specific table generation."""
        try:
            update_task_progress(task_id, 0.0, f"Starting generation of {table_name}")

            # Generate all master tables (SQLite requires sequential mode)
            from retail_datagen.db.session import get_master_session
            from sqlalchemy import text

            async with get_master_session() as session:
                # Clear existing data to avoid UNIQUE constraint violations
                logger.info("Clearing existing master data...")
                for table in [
                    "dim_customers",
                    "dim_products",
                    "dim_trucks",
                    "dim_stores",
                    "dim_distribution_centers",
                    "dim_geographies",
                ]:
                    await session.execute(text(f"DELETE FROM {table}"))
                await session.flush()  # Flush deletes immediately
                logger.info("Existing master data cleared")

                result = await master_generator.generate_all_master_data_async(
                    session=session,
                    parallel=False
                )
                # Context manager will commit everything on exit

            update_task_progress(task_id, 1.0, f"Generated {table_name}")

            return {
                "table_name": table_name,
                "records_generated": getattr(result, "records", 0),
                "output_path": str(output_path),
            }

        except Exception as e:
            logger.error(f"Generation of {table_name} failed: {e}")
            raise

    create_background_task(task_id, generation_task(), f"Generate {table_name}")

    return OperationResult(
        success=True,
        message=f"Generation of {table_name} started",
        operation_id=task_id,
        started_at=datetime.now(),
    )


# ================================
# HISTORICAL DATA GENERATION ENDPOINTS
# ================================


@router.post(
    "/generate/historical",
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

    Supports both sequential and parallel processing modes:
    - Sequential: Processes one day at a time with deterministic ordering (default)
    - Parallel: Processes multiple days concurrently for faster generation

    Both modes provide rich hourly progress updates (24 updates per day per table).

    The parallel mode can significantly reduce generation time for multi-day ranges
    while maintaining data quality and progress visibility.
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
        start_date, end_date = state_manager.get_historical_date_range(
            config_start_date
        )
        logger.info(f"Using intelligent date range: {start_date} to {end_date}")

    tables_to_generate = FACT_TABLES

    # Validate table names
    for table in tables_to_generate:
        validate_table_name(table, "fact")

    task_id = f"historical_generation_{uuid4().hex[:8]}"

    async def generation_task():
        """Background task for historical data generation."""
        try:
            update_task_progress(task_id, 0.0, "Starting historical data generation")

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
                        name: value for name, value in table_progress.items() if name in FACT_TABLES
                    }

                # Derive sensible defaults based on filtered_table_progress if not provided
                if tables_in_progress is not None:
                    tables_in_progress_override = [
                        t for t in tables_in_progress if t in FACT_TABLES
                    ]
                else:
                    # If we don't yet have progress data, show all requested as in-progress (master-style UX)
                    if not filtered_table_progress:
                        tables_in_progress_override = list(tables_to_generate)
                    else:
                        tables_in_progress_override = [
                            table for table, value in filtered_table_progress.items() if 0.0 < value < 1.0
                        ]
                if tables_remaining is not None:
                    tables_remaining_override = [t for t in tables_remaining if t in FACT_TABLES]
                else:
                    if not filtered_table_progress:
                        tables_remaining_override = list(tables_to_generate)
                    else:
                        tables_remaining_override = [
                            table for table, value in filtered_table_progress.items() if value == 0.0
                        ]

                update_task_progress(
                    task_id,
                    progress,
                    message,
                    table_progress=filtered_table_progress,
                    current_table=current_table,
                    tables_completed=[t for t in (tables_completed or []) if t in FACT_TABLES] if tables_completed is not None else [],
                    tables_failed=[t for t in (tables_failed or []) if t in FACT_TABLES] if tables_failed is not None else [],
                    tables_in_progress=tables_in_progress_override,
                    tables_remaining=tables_remaining_override,
                    estimated_seconds_remaining=estimated_seconds_remaining,
                    progress_rate=progress_rate,
                    table_counts=table_counts,
                    # NEW: Pass hourly progress fields
                    current_day=current_day,
                    current_hour=current_hour,
                    hourly_progress=hourly_progress,
                    total_hours_completed=total_hours_completed,
                )

            # Also wire a master-style per-table progress callback for consistent UI updates
            per_table_progress: dict[str, float] = {table: 0.0 for table in tables_to_generate}
            # Thread lock to protect shared state in parallel mode
            progress_lock = Lock()

            def per_table_callback(
                table_name: str,
                progress_value: float,
                detail_message: str | None,
                table_counts: dict[str, int] | None = None,
            ) -> None:
                if table_name not in FACT_TABLES:
                    return

                # Protect all dict operations with lock for thread safety in parallel mode
                with progress_lock:
                    if table_name not in per_table_progress:
                        return

                    per_table_progress[table_name] = max(0.0, min(1.0, progress_value))

                    tables_completed = [
                        t for t, v in per_table_progress.items() if v >= 1.0
                    ]
                    tables_in_progress = [
                        t for t, v in per_table_progress.items() if 0.0 < v < 1.0
                    ]
                    tables_remaining = [
                        t for t, v in per_table_progress.items() if v == 0.0
                    ]

                    overall_progress = (
                        sum(per_table_progress.values()) / len(per_table_progress)
                        if per_table_progress else progress_value
                    )

                    # Call update while holding lock (brief operation, acceptable)
                    update_task_progress(
                        task_id,
                        overall_progress,
                        detail_message or f"Generating {table_name.replace('_',' ')}",
                        table_progress=dict(per_table_progress),  # Pass copy while locked
                        tables_completed=list(tables_completed),  # Pass copy while locked
                        tables_in_progress=list(tables_in_progress),
                        tables_remaining=list(tables_remaining),
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

            # Use user's parallel preference (both modes now support rich hourly progress)
            use_parallel = request.parallel if request.parallel is not None else False
            mode_str = "parallel" if use_parallel else "sequential"
            logger.info(f"Starting historical generation from {start_date.date()} to {end_date.date()} in {mode_str} mode")

            # Generate historical data using the fact generator
            # Note: generate_historical_data is now async (Phase 3B SQLite migration)
            # TODO: Add database session support when SQLite mode is enabled in config
            summary = await fact_generator.generate_historical_data(
                start_date,
                end_date,
                use_parallel
            )

            # Update generation state with the end timestamp
            state_manager.update_historical_generation(end_date)

            update_task_progress(task_id, 1.0, "Historical data generation completed")

            return {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "days_generated": (end_date - start_date).days + 1,
                "tables_generated": tables_to_generate,
                "parallel_processing": request.parallel,
                "total_records": summary.total_records,
                "partitions_created": summary.partitions_created,
                "generation_time_seconds": summary.generation_time_seconds,
            }

        except Exception as e:
            logger.error(f"Historical data generation failed: {e}")
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
        started_at=datetime.now(),
    )


@router.get(
    "/generate/historical/status",
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
    )


@router.post(
    "/generate/historical/{table_name}",
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
        start_date, end_date = state_manager.get_historical_date_range(
            config_start_date
        )
        logger.info(f"Using intelligent date range: {start_date} to {end_date}")

    task_id = f"historical_{table_name}_{uuid4().hex[:8]}"

    async def generation_task():
        """Background task for specific historical table generation."""
        try:
            update_task_progress(task_id, 0.0, f"Starting generation of {table_name}")

            # Generate historical data using the fact generator
            # Note: generate_historical_data is now async (Phase 3B SQLite migration)
            # TODO: Add database session support when SQLite mode is enabled in config
            summary = await fact_generator.generate_historical_data(
                start_date, end_date, False  # use_parallel=False for single table
            )

            # Update generation state with the end timestamp
            state_manager.update_historical_generation(end_date)

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
        started_at=datetime.now(),
    )


# ================================
# GENERATION STATE ENDPOINTS
# ================================


@router.get(
    "/generation/status",
    summary="Get generation state status",
    description="Get the current generation state and timestamps",
)
async def get_generation_state_status():
    """Get the current generation state status."""

    state_manager = GenerationStateManager()
    return state_manager.get_status()


@router.delete(
    "/generation/clear",
    response_model=OperationResult,
    summary="Clear all generated data",
    description="Clear all master data, fact data, and reset generation state",
)
@rate_limit(max_requests=2, window_seconds=300)  # Very restrictive for safety
async def clear_all_data(config: RetailConfig = Depends(get_config)):
    """Clear all generated data and reset state."""

    try:
        state_manager = GenerationStateManager()

        # Prepare config paths for clearing
        config_paths = {"master": config.paths.master, "facts": config.paths.facts}

        # Clear the cache
        cache_manager = CacheManager()
        cache_manager.clear_cache()

        # Clear all data in SQLite databases (master and facts)
        from sqlalchemy import text
        from ..db.session import get_master_session, get_facts_session

        # Truncate master tables (delete all rows)
        master_tables = [
            "dim_customers",
            "dim_products",
            "dim_trucks",
            "dim_stores",
            "dim_distribution_centers",
            "dim_geographies",
        ]

        async with get_master_session() as session:
            for table in master_tables:
                await session.execute(text(f"DELETE FROM {table}"))
            await session.flush()

        # Truncate facts tables and watermarks
        fact_tables = [
            "fact_receipt_lines",
            "fact_receipts",
            "fact_store_inventory_txn",
            "fact_dc_inventory_txn",
            "fact_truck_moves",
            "fact_foot_traffic",
            "fact_ble_pings",
            "fact_marketing",
            "fact_online_orders",
            "fact_data_watermarks",
        ]

        async with get_facts_session() as session:
            for table in fact_tables:
                await session.execute(text(f"DELETE FROM {table}"))
            await session.flush()

        # Also clear file-based artifacts if any (backward compatibility)
        results = state_manager.clear_all_data(config_paths)

        if results["errors"]:
            logger.warning(f"Data clearing completed with errors: {results['errors']}")
            return OperationResult(
                success=True,
                message=f"Data cleared with some errors. Files deleted: {len(results['files_deleted'])}, Errors: {len(results['errors'])}",
                started_at=datetime.now(),
            )
        else:
            logger.info("All data cleared successfully from SQLite and file cache")
            return OperationResult(
                success=True,
                message="All data cleared successfully (SQLite tables truncated; caches and legacy files removed)",
                started_at=datetime.now(),
            )

    except Exception as e:
        logger.error(f"Failed to clear data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clear data: {str(e)}",
        )


# ================================
# TABLE LISTING AND PREVIEW ENDPOINTS
# ================================


@router.get(
    "/master/tables",
    response_model=TableListResponse,
    summary="List master data tables",
    description="Get a list of available master data tables",
)
async def list_master_tables():
    """List all available master data tables."""
    return TableListResponse(tables=MASTER_TABLES, count=len(MASTER_TABLES))


@router.get(
    "/facts/tables",
    response_model=TableListResponse,
    summary="List fact tables",
    description="Get a list of generated fact tables",
)
async def list_fact_tables(config: RetailConfig = Depends(get_config)):
    """List all generated fact tables (SQLite-backed)."""
    try:
        from sqlalchemy import select, func
        from ..db.session import get_facts_session

        tables_with_data: list[str] = []
        async with get_facts_session() as session:
            for table_name, model in FACT_TABLE_MODELS.items():
                result = await session.execute(select(func.count()).select_from(model))
                if (result.scalar() or 0) > 0:
                    tables_with_data.append(table_name)

        return TableListResponse(tables=tables_with_data, count=len(tables_with_data))
    except Exception as e:
        logger.warning(f"Falling back to empty fact table list due to error: {e}")
        return TableListResponse(tables=[], count=0)


# ================================
# UNIFIED DATA ENDPOINTS
# ================================


@router.get(
    "/data/{table_name}/summary",
    summary="Get table summary",
    description="Get record counts and metadata for any table (master or fact)",
)
async def get_table_summary(table_name: str):
    """Get summary information for any table from SQLite database."""

    # Check if table exists in our models
    model = ALL_TABLE_MODELS.get(table_name)
    if not model:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} not found.",
        )

    # Determine which session to use
    is_master = table_name in MASTER_TABLE_MODELS

    try:
        from sqlalchemy import select, func, inspect
        from ..db.session import get_master_session, get_facts_session

        session_manager = get_master_session if is_master else get_facts_session

        async with session_manager() as session:
            # Get row count
            result = await session.execute(select(func.count()).select_from(model))
            total_records = result.scalar() or 0

            # Map attribute keys to DB column names for stable headers
            mapper = inspect(model)
            column_props = list(mapper.column_attrs)
            columns = [prop.columns[0].name for prop in column_props]

            return {
                "table_name": table_name,
                "total_records": total_records,
                "columns": columns,
                "table_type": "master" if is_master else "fact",
            }

    except Exception as e:
        logger.error(f"Failed to read table {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table {table_name}: {str(e)}",
        )


@router.get(
    "/data/{table_name}",
    response_model=TablePreviewResponse,
    summary="Preview table data",
    description="Get a preview of any table (master or fact)",
)
async def preview_table(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
):
    """Preview any table from SQLite database."""

    # Check if table exists in our models
    model = ALL_TABLE_MODELS.get(table_name)
    if not model:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} not found.",
        )

    # Determine which session to use
    is_master = table_name in MASTER_TABLE_MODELS

    try:
        from sqlalchemy import select, func, inspect
        from ..db.session import get_master_session, get_facts_session

        session_manager = get_master_session if is_master else get_facts_session

        async with session_manager() as session:
            # Get total row count
            count_result = await session.execute(select(func.count()).select_from(model))
            total_rows = count_result.scalar() or 0

            # Get preview rows
            result = await session.execute(select(model).limit(limit))
            rows = result.scalars().all()

            # Map attribute keys to DB column names for headers and row dicts
            mapper = inspect(model)
            column_props = list(mapper.column_attrs)
            columns = [prop.columns[0].name for prop in column_props]

            # Convert SQLAlchemy objects to dicts with DB column names
            preview_rows = []
            for row in rows:
                row_dict: dict[str, object | None] = {}
                for prop in column_props:
                    attr = prop.key
                    db_col = prop.columns[0].name
                    value = getattr(row, attr, None)
                    row_dict[db_col] = (
                        value if isinstance(value, (str, int, float, bool)) or value is None else str(value)
                    )
                preview_rows.append(row_dict)

            return TablePreviewResponse(
                table_name=table_name,
                columns=columns,
                row_count=total_rows,
                preview_rows=preview_rows,
            )

    except Exception as e:
        logger.error(f"Failed to read table {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table {table_name}: {str(e)}",
        )



# ================================
# OPERATION CONTROL ENDPOINTS
# ================================


@router.delete(
    "/generate/cancel/{operation_id}",
    response_model=OperationResult,
    summary="Cancel generation operation",
    description="Cancel a running data generation operation",
)
async def cancel_generation_operation(operation_id: str):
    """Cancel a running data generation operation."""

    success = cancel_task(operation_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Operation {operation_id} not found or already completed",
        )

    return OperationResult(
        success=True,
        message=f"Operation {operation_id} cancelled successfully",
        operation_id=operation_id,
    )


# ================================
# UI COMPATIBILITY ALIASES
# ================================


@router.get(
    "/master/{table_name}",
    response_model=TablePreviewResponse,
    summary="Preview master table (UI alias)",
    description="Alias used by UI to preview master tables",
)
async def preview_master_table_alias(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000),
):
    if table_name not in MASTER_TABLE_MODELS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Master table {table_name} not found.",
        )

    try:
        from sqlalchemy import select, func, inspect
        from ..db.session import get_master_session

        model = MASTER_TABLE_MODELS[table_name]
        async with get_master_session() as session:
            count_result = await session.execute(select(func.count()).select_from(model))
            total_rows = count_result.scalar() or 0

            result = await session.execute(select(model).limit(limit))
            rows = result.scalars().all()

            mapper = inspect(model)
            column_props = list(mapper.column_attrs)
            columns = [prop.columns[0].name for prop in column_props]

            preview_rows: list[dict[str, object]] = []
            for row in rows:
                row_dict: dict[str, object | None] = {}
                for prop in column_props:
                    attr = prop.key
                    db_col = prop.columns[0].name
                    value = getattr(row, attr, None)
                    row_dict[db_col] = (
                        value if isinstance(value, (str, int, float, bool)) or value is None else str(value)
                    )
                preview_rows.append(row_dict)

            return TablePreviewResponse(
                table_name=table_name,
                columns=columns,
                row_count=total_rows,
                preview_rows=preview_rows,
            )
    except Exception as e:
        logger.error(f"Master preview failed for {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to preview {table_name}: {str(e)}",
        )


@router.get(
    "/facts/{table_name}/recent",
    response_model=TablePreviewResponse,
    summary="Preview recent fact data (UI alias)",
    description="Alias used by UI to preview recent rows from fact tables",
)
async def preview_recent_fact_alias(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000),
):
    if table_name not in FACT_TABLE_MODELS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Fact table {table_name} not found.",
        )

    try:
        from sqlalchemy import select, func, inspect, desc
        from ..db.session import get_facts_session

        model = FACT_TABLE_MODELS[table_name]
        async with get_facts_session() as session:
            # Total count
            count_result = await session.execute(select(func.count()).select_from(model))
            total_rows = count_result.scalar() or 0

            # Most recent event_ts
            recent_result = await session.execute(select(func.max(model.event_ts)))
            most_recent = recent_result.scalar_one_or_none()

            # Recent rows by event_ts desc
            result = await session.execute(select(model).order_by(desc(model.event_ts)).limit(limit))
            rows = result.scalars().all()

            mapper = inspect(model)
            column_props = list(mapper.column_attrs)
            columns = [prop.columns[0].name for prop in column_props]

            preview_rows: list[dict[str, object]] = []
            for row in rows:
                row_dict: dict[str, object | None] = {}
                for prop in column_props:
                    attr = prop.key
                    db_col = prop.columns[0].name
                    value = getattr(row, attr, None)
                    row_dict[db_col] = (
                        value if isinstance(value, (str, int, float, bool)) or value is None else str(value)
                    )
                preview_rows.append(row_dict)

            # Include most_recent_date for UI hint if available
            response = TablePreviewResponse(
                table_name=table_name,
                columns=columns,
                row_count=total_rows,
                preview_rows=preview_rows,
                most_recent_date=str(most_recent) if most_recent is not None else None,
            )
            return response
    except Exception as e:
        logger.error(f"Fact preview failed for {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to preview {table_name}: {str(e)}",
        )


# ================================
# DASHBOARD CACHE ENDPOINT
# ================================


@router.get(
    "/dashboard/counts",
    summary="Get cached dashboard counts",
    description="Get cached table counts for fast dashboard loading",
)
async def get_dashboard_counts():
    """Get cached table counts for dashboard."""
    cache_manager = CacheManager()
    return cache_manager.get_all_counts()
