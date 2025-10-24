"""
FastAPI router for data generation endpoints.

This module provides REST API endpoints for generating master data (dimensions)
and historical fact data with comprehensive status tracking and validation.
"""

import asyncio
import csv
import logging
import time
from datetime import datetime
from pathlib import Path
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

FACT_TABLES = [
    "dc_inventory_txn",
    "truck_moves",
    "store_inventory_txn",
    "receipts",
    "receipt_lines",
    "foot_traffic",
    "ble_pings",
    "marketing",
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
            tables_remaining_list = list(tables_to_generate)

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
                tables_remaining=tables_remaining_list.copy(),
                estimated_seconds_remaining=None,
                progress_rate=None,
                table_progress=table_progress.copy(),
            )

            completed_tables: list[str] = []

            for index, table_name in enumerate(tables_to_generate):
                remaining_tables = list(tables_to_generate[index + 1 :])
                progress_before = index / total_tables if total_tables else 0.0
                eta_before, rate_before = compute_timing_metrics(progress_before)

                if table_name in table_progress:
                    table_progress[table_name] = max(table_progress[table_name], 0.05)

                update_task_progress(
                    task_id,
                    progress_before,
                    f"Generating {table_name}",
                    tables_completed=completed_tables.copy(),
                    tables_in_progress=[table_name],
                    tables_remaining=remaining_tables,
                    current_table=table_name,
                    estimated_seconds_remaining=eta_before,
                    progress_rate=rate_before,
                    table_progress=table_progress.copy(),
                )

                output_path = Path(config.paths.master) / f"{table_name}.csv"
                if output_path.exists() and not request.force_regenerate:
                    logger.info(f"Skipping {table_name} - already exists")
                    completed_tables.append(table_name)
                    if table_name in table_progress:
                        table_progress[table_name] = 1.0
                    overall_progress = (
                        sum(table_progress.values()) / total_tables if total_tables else 1.0
                    )
                    eta_after, rate_after = compute_timing_metrics(overall_progress)
                    update_task_progress(
                        task_id,
                        overall_progress,
                        f"Skipping {table_name} (already exists)",
                        tables_completed=completed_tables.copy(),
                        tables_in_progress=[],
                        tables_remaining=remaining_tables,
                        estimated_seconds_remaining=eta_after,
                        progress_rate=rate_after,
                        table_progress=table_progress.copy(),
                    )
                    continue

                if table_name == "geographies_master":
                    await asyncio.to_thread(master_generator.generate_all_master_data, None, True)
                elif table_name == "stores":
                    await asyncio.to_thread(master_generator.generate_all_master_data, None, True)
                elif table_name == "distribution_centers":
                    await asyncio.to_thread(master_generator.generate_all_master_data, None, True)
                elif table_name == "trucks":
                    await asyncio.to_thread(master_generator.generate_all_master_data, None, True)
                elif table_name == "customers":
                    await asyncio.to_thread(master_generator.generate_all_master_data, None, True)
                elif table_name == "products_master":
                    await asyncio.to_thread(master_generator.generate_all_master_data, None, True)

                completed_tables.append(table_name)
                if table_name in table_progress:
                    table_progress[table_name] = 1.0
                overall_progress = (
                    sum(table_progress.values()) / total_tables if total_tables else 1.0
                )
                eta_after, rate_after = compute_timing_metrics(overall_progress)

                update_task_progress(
                    task_id,
                    overall_progress,
                    f"Completed {table_name} ({len(completed_tables)}/{total_tables})",
                    tables_completed=completed_tables.copy(),
                    tables_in_progress=[],
                    tables_remaining=remaining_tables,
                    estimated_seconds_remaining=eta_after,
                    progress_rate=rate_after,
                    table_progress=table_progress.copy(),
                )
                logger.info(f"Generated {table_name}")

            final_eta, final_rate = compute_timing_metrics(1.0)
            update_task_progress(
                task_id,
                1.0,
                "Master data generation completed",
                tables_completed=completed_tables.copy(),
                tables_in_progress=[],
                tables_remaining=[],
                estimated_seconds_remaining=final_eta,
                progress_rate=final_rate,
                table_progress=table_progress.copy(),
            )

            return {
                "tables_generated": completed_tables,
                "total_tables": total_tables,
                "skipped_existing": not request.force_regenerate,
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
        tables_completed=task_status.get("tables_completed", []),
        tables_remaining=task_status.get("tables_remaining", []),
        tables_in_progress=task_status.get("tables_in_progress"),
        estimated_seconds_remaining=task_status.get("estimated_seconds_remaining"),
        progress_rate=task_status.get("progress_rate"),
        last_update_timestamp=task_status.get("last_update_timestamp"),
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

            # Generate the specific table (parallel=True by default)
            result = await asyncio.to_thread(master_generator.generate_all_master_data, None, True)

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
    """Generate historical fact data using intelligent date range logic."""

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

    tables_to_generate = request.tables or FACT_TABLES

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
            ):
                progress = current_day / total_days if total_days > 0 else 0.0
                if total_days > 0 and current_day == 0:
                    progress = max(progress, 0.01)

                tables_in_progress_override = (
                    tables_in_progress
                    if tables_in_progress is not None
                    else list(tables_to_generate)
                )
                tables_remaining_override = (
                    tables_remaining
                    if tables_remaining is not None
                    else [table for table, value in (table_progress or {}).items() if value == 0.0]
                )

                update_task_progress(
                    task_id,
                    progress,
                    message,
                    table_progress=table_progress,
                    current_table=current_table,
                    tables_completed=tables_completed,
                    tables_failed=tables_failed,
                    tables_in_progress=tables_in_progress_override,
                    tables_remaining=tables_remaining_override,
                    estimated_seconds_remaining=estimated_seconds_remaining,
                    progress_rate=progress_rate,
                    table_counts=table_counts,
                )

            # Set the progress callback on the generator
            fact_generator._progress_callback = progress_callback

            # Emit an initialization update so the UI shows immediate activity
            try:
                progress_callback(
                    0,
                    "Loading master data for historical generation",
                    table_progress={table: 0.0 for table in tables_to_generate},
                    tables_completed=[],
                    tables_in_progress=None,
                    tables_remaining=None,
                )
            except Exception as exc:
                logger.debug(f"Unable to send initial progress update: {exc}")

            # Generate historical data using the fact generator
            summary = await asyncio.to_thread(
                fact_generator.generate_historical_data,
                start_date,
                end_date,
                request.parallel  # ✅ WIRE UP THE CHECKBOX!
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
            summary = await asyncio.to_thread(
                fact_generator.generate_historical_data, start_date, end_date
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

        # Clear all data
        results = state_manager.clear_all_data(config_paths)

        if results["errors"]:
            logger.warning(f"Data clearing completed with errors: {results['errors']}")
            return OperationResult(
                success=True,
                message=f"Data cleared with some errors. Files deleted: {len(results['files_deleted'])}, Errors: {len(results['errors'])}",
                started_at=datetime.now(),
            )
        else:
            logger.info(
                f"All data cleared successfully. Files deleted: {len(results['files_deleted'])}"
            )
            return OperationResult(
                success=True,
                message=f"All data cleared successfully. Files deleted: {len(results['files_deleted'])}",
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
    """List all generated fact tables."""
    facts_path = Path(config.paths.facts)

    if not facts_path.exists():
        return TableListResponse(tables=[], count=0)

    # Check which fact tables actually have data
    generated_tables = []
    for table_name in FACT_TABLES:
        table_path = facts_path / table_name
        if table_path.exists() and table_path.is_dir():
            # Check if it has at least one partition
            has_data = any(
                p.is_dir() and p.name.startswith("dt=")
                for p in table_path.iterdir()
            )
            if has_data:
                generated_tables.append(table_name)

    return TableListResponse(tables=generated_tables, count=len(generated_tables))


@router.get(
    "/master/{table_name}/summary",
    summary="Get master table summary",
    description="Get record counts and metadata for a master table",
)
async def get_master_table_summary(
    table_name: str, config: RetailConfig = Depends(get_config)
):
    """Get summary information for a master table."""

    validate_table_name(table_name, "master")

    file_path = Path(config.paths.master) / f"{table_name}.csv"

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} not found. Generate it first.",
        )

    try:
        total_records = 0
        columns = []

        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            columns = reader.fieldnames or []

            for row in reader:
                total_records += 1

        return {
            "table_name": table_name,
            "total_records": total_records,
            "columns": columns,
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table {table_name}: {str(e)}",
        )


@router.get(
    "/master/{table_name}",
    response_model=TablePreviewResponse,
    summary="Preview master data table",
    description="Get a preview of a master data table (first 100 rows)",
)
async def preview_master_table(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
    config: RetailConfig = Depends(get_config),
):
    """Preview a master data table."""

    validate_table_name(table_name, "master")

    file_path = Path(config.paths.master) / f"{table_name}.csv"

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} not found. Generate it first.",
        )

    try:
        preview_rows = []
        columns = []
        total_rows = 0

        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            columns = reader.fieldnames or []

            for i, row in enumerate(reader):
                if i < limit:
                    preview_rows.append(row)
                total_rows = i + 1

        return TablePreviewResponse(
            table_name=table_name,
            columns=columns,
            row_count=total_rows,
            preview_rows=preview_rows,
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table {table_name}: {str(e)}",
        )


@router.get(
    "/facts/{table_name}",
    summary="Get fact table summary",
    description="Get record counts and metadata for a fact table across all partitions",
)
async def get_fact_table_summary(
    table_name: str, config: RetailConfig = Depends(get_config)
):
    """Get summary information for a fact table across all partitions."""

    validate_table_name(table_name, "fact")

    table_path = Path(config.paths.facts) / table_name

    if not table_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} not found. Generate it first.",
        )

    try:
        total_records = 0
        partitions = []
        columns = []

        # Find all date partitions
        for partition_dir in table_path.iterdir():
            if partition_dir.is_dir() and partition_dir.name.startswith("dt="):
                date_part = partition_dir.name[3:]  # Remove "dt=" prefix

                # Look for CSV files in the partition directory
                # Files are named like {table_name}_{YYYYMMDD}.csv
                date_suffix = date_part.replace(
                    "-", ""
                )  # Convert 2025-08-14 to 20250814
                csv_file = partition_dir / f"{table_name}_{date_suffix}.csv"

                if csv_file.exists():
                    partition_records = 0
                    with open(csv_file, newline="", encoding="utf-8") as csvfile:
                        reader = csv.DictReader(csvfile)
                        if not columns and reader.fieldnames:
                            columns = reader.fieldnames

                        for row in reader:
                            partition_records += 1

                    total_records += partition_records
                    partitions.append({"date": date_part, "records": partition_records})

        # Sort partitions by date
        partitions.sort(key=lambda x: x["date"])

        return {
            "table_name": table_name,
            "total_records": total_records,
            "partitions": partitions,
            "columns": columns,
            "partition_count": len(partitions),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table {table_name}: {str(e)}",
        )


@router.get(
    "/facts/{table_name}/recent",
    summary="Get recent fact table data summary",
    description="Get a summary of recent fact table data (most recent partition)",
)
async def get_recent_fact_data(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
    config: RetailConfig = Depends(get_config),
):
    """Get recent data from the most recent partition of a fact table."""

    validate_table_name(table_name, "fact")

    table_path = Path(config.paths.facts) / table_name

    if not table_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} not found. Generate it first.",
        )

    try:
        # Find the most recent partition
        partitions = []
        for partition_dir in table_path.iterdir():
            if partition_dir.is_dir() and partition_dir.name.startswith("dt="):
                date_part = partition_dir.name[3:]  # Remove "dt=" prefix
                partitions.append(date_part)

        if not partitions:
            return {"count": 0, "preview_rows": [], "total_estimated": 0}

        # Sort to get most recent
        partitions.sort(reverse=True)
        most_recent_date = partitions[0]

        # Read the most recent partition file
        date_suffix = most_recent_date.replace("-", "")
        file_path = (
            table_path / f"dt={most_recent_date}" / f"{table_name}_{date_suffix}.csv"
        )

        if not file_path.exists():
            return {"count": 0, "preview_rows": [], "total_estimated": 0}

        preview_rows = []
        total_rows = 0

        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            for i, row in enumerate(reader):
                if i < limit:
                    preview_rows.append(row)
                total_rows = i + 1

        # Estimate total across all partitions
        total_estimated = total_rows * len(partitions)

        return {
            "count": len(preview_rows),
            "preview_rows": preview_rows,
            "total_estimated": total_estimated,
            "partition_count": len(partitions),
            "most_recent_date": most_recent_date,
        }

    except Exception as e:
        logger.error(f"Failed to read recent data for {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table {table_name}: {str(e)}",
        )


@router.get(
    "/facts/{table_name}/{date}",
    response_model=TablePreviewResponse,
    summary="Preview fact table for date",
    description="Get a preview of a fact table for a specific date",
)
async def preview_fact_table(
    table_name: str,
    date: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
    config: RetailConfig = Depends(get_config),
):
    """Preview a fact table for a specific date."""

    validate_table_name(table_name, "fact")

    # Validate date format
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Date must be in YYYY-MM-DD format",
        )

    date_suffix = date.replace("-", "")  # Convert 2025-08-14 to 20250814
    file_path = (
        Path(config.paths.facts)
        / table_name
        / f"dt={date}"
        / f"{table_name}_{date_suffix}.csv"
    )

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} for date {date} not found. Generate it first.",
        )

    try:
        preview_rows = []
        columns = []
        total_rows = 0

        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            columns = reader.fieldnames or []

            for i, row in enumerate(reader):
                if i < limit:
                    preview_rows.append(row)
                total_rows = i + 1

        return TablePreviewResponse(
            table_name=table_name,
            columns=columns,
            row_count=total_rows,
            preview_rows=preview_rows,
            date_partition=date,
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table {table_name} for date {date}: {str(e)}",
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
