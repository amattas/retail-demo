"""
FastAPI router for data generation endpoints.

This module provides REST API endpoints for generating master data (dimensions)
and historical fact data with comprehensive status tracking and validation.
"""

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
    reset_generators,
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

# DuckDB logical→physical mappings
DUCK_MASTER_MAP = {
    "geographies_master": "dim_geographies",
    "stores": "dim_stores",
    "distribution_centers": "dim_distribution_centers",
    "trucks": "dim_trucks",
    "customers": "dim_customers",
    "products_master": "dim_products",
}

DUCK_FACT_MAP = {
    "dc_inventory_txn": "fact_dc_inventory_txn",
    "truck_moves": "fact_truck_moves",
    "store_inventory_txn": "fact_store_inventory_txn",
    "receipts": "fact_receipts",
    "receipt_lines": "fact_receipt_lines",
    "foot_traffic": "fact_foot_traffic",
    "ble_pings": "fact_ble_pings",
    "marketing": "fact_marketing",
    "online_orders": "fact_online_order_headers",
    "online_order_lines": "fact_online_order_lines",
}

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
    "online_order_lines",
]

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

# ================================
# MASTER DATA GENERATION ENDPOINTS
# ================================


@router.post(
    "/generate/dimensions",
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

            def compute_timing_metrics(
                progress_value: float,
            ) -> tuple[float | None, float | None]:
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

            # Short-circuit when everything already exists in DuckDB and regeneration not forced
            if not request.force_regenerate:
                try:
                    from retail_datagen.db.duckdb_engine import get_duckdb_conn

                    conn = get_duckdb_conn()
                    missing_tables: list[str] = []
                    for table in tables_to_generate:
                        physical = DUCK_MASTER_MAP.get(table, table)
                        try:
                            count = conn.execute(f"SELECT COUNT(*) FROM {physical}").fetchone()[0]
                            if count is None or int(count) == 0:
                                missing_tables.append(table)
                        except Exception:
                            missing_tables.append(table)

                    if not missing_tables:
                        update_task_progress(
                            task_id,
                            1.0,
                            "All requested master tables already generated (DuckDB)",
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
                except Exception:
                    # If DuckDB check fails, proceed with generation
                    pass

            def master_progress_callback(
                table_name: str,
                progress_value: float,
                detail_message: str | None,
                table_counts: dict[str, int] | None = None,
                tables_completed: list[str] | None = None,
                tables_in_progress: list[str] | None = None,
                tables_remaining: list[str] | None = None,
            ) -> None:
                """
                Master generation progress callback.

                Now accepts table state lists directly from TableProgressTracker.
                The router no longer recalculates states from progress percentages.
                """
                if table_name not in table_progress:
                    return

                table_progress[table_name] = max(0.0, min(1.0, progress_value))

                overall_progress = (
                    sum(table_progress.values()) / total_tables
                    if total_tables
                    else progress_value
                )
                eta_estimate, rate_estimate = compute_timing_metrics(overall_progress)
                callback_message = (
                    detail_message or f"Generating {table_name.replace('_', ' ')}"
                )

                # Pass through state lists from generator without modification
                # Generator's TableProgressTracker provides correct states
                # Build kwargs so we only update state lists when provided
                kwargs: dict = {
                    "table_progress": table_progress.copy(),
                    "estimated_seconds_remaining": eta_estimate,
                    "progress_rate": rate_estimate,
                    "table_counts": table_counts,
                }
                if tables_completed is not None:
                    kwargs["tables_completed"] = tables_completed
                if tables_in_progress is not None:
                    kwargs["tables_in_progress"] = tables_in_progress
                if tables_remaining is not None:
                    kwargs["tables_remaining"] = tables_remaining

                update_task_progress(
                    task_id,
                    overall_progress,
                    callback_message,
                    **kwargs,
                )

            master_generator.set_progress_callback(master_progress_callback)

            update_task_progress(
                task_id,
                0.0,
                "Starting master data generation",
                tables_completed=[],
                tables_in_progress=[],
                tables_remaining=list(tables_to_generate),
                estimated_seconds_remaining=None,
                progress_rate=None,
                table_progress=table_progress.copy(),
            )

            # Run full generation once (progress callback handles per-table reporting)
            # DuckDB-only path: clear existing tables and regenerate; no SQLAlchemy session required
            try:
                from retail_datagen.db.duckdb_engine import get_duckdb_conn

                conn = get_duckdb_conn()
                for table in [
                    "dim_customers",
                    "dim_products",
                    "dim_trucks",
                    "dim_stores",
                    "dim_distribution_centers",
                    "dim_geographies",
                ]:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info("Existing DuckDB master tables dropped")
            except Exception as drop_exc:
                logger.warning(f"Failed to drop DuckDB tables: {drop_exc}")
            await master_generator.generate_all_master_data_async(session=None)

            for table in table_progress:
                table_progress[table] = max(table_progress[table], 1.0)

            completed_tables = list(table_progress.keys())

            # Prefer DB-confirmed counts to avoid cache/DB drift
            final_counts = {}
            try:
                # Query DuckDB for counts; fall back to in-memory if not available
                from retail_datagen.db.duckdb_engine import get_duckdb_conn

                conn = get_duckdb_conn()
                q = lambda t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                final_counts = {
                    "geographies_master": q("dim_geographies"),
                    "stores": q("dim_stores"),
                    "distribution_centers": q("dim_distribution_centers"),
                    "trucks": q("dim_trucks"),
                    "customers": q("dim_customers"),
                    "products_master": q("dim_products"),
                }

                # Update dashboard cache with DB counts
                try:
                    from ..shared.cache import CacheManager

                    cache = CacheManager()
                    for k, v in final_counts.items():
                        cache.update_master_table(k, int(v), "Master Data")
                except Exception as cache_exc:
                    logger.warning(
                        f"Failed to update dashboard cache with DB counts: {cache_exc}"
                    )
            except Exception as count_exc:
                logger.warning(
                    f"Failed to query DB counts; falling back to in-memory counts: {count_exc}"
                )
                final_counts = {
                    "geographies_master": len(master_generator.geography_master),
                    "stores": len(master_generator.stores),
                    "distribution_centers": len(master_generator.distribution_centers),
                    "trucks": len(master_generator.trucks),
                    "customers": len(master_generator.customers),
                    "products_master": len(master_generator.products_master),
                }

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
                table_counts=final_counts,
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
    "/generate/dimensions/status",
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
        sequence=task_status.get("sequence"),
    )

    # Add completed tables from result if available and not already set
    if (
        not response.tables_completed
        and "result" in task_status
        and task_status["result"]
    ):
        result = task_status["result"]
        response.tables_completed = result.get("tables_generated", [])

    return response


@router.post(
    "/generate/dimensions/{table_name}",
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

    # DuckDB-first: allow regeneration; skip CSV presence check

    task_id = f"master_{table_name}_{uuid4().hex[:8]}"

    async def generation_task():
        """Background task for specific table generation."""
        try:
            update_task_progress(task_id, 0.0, f"Starting generation of {table_name}")

            # DuckDB path: drop tables and regenerate
            try:
                from retail_datagen.db.duckdb_engine import get_duckdb_conn

                conn = get_duckdb_conn()
                for tbl in [
                    "dim_customers",
                    "dim_products",
                    "dim_trucks",
                    "dim_stores",
                    "dim_distribution_centers",
                    "dim_geographies",
                ]:
                    conn.execute(f"DROP TABLE IF EXISTS {tbl}")
                logger.info("Existing DuckDB master tables dropped")
            except Exception as drop_exc:
                logger.warning(f"Failed to drop DuckDB tables: {drop_exc}")
            result = await master_generator.generate_all_master_data_async(
                session=None
            )

            # Final accurate counts
            final_counts = {
                "geographies_master": len(master_generator.geography_master),
                "stores": len(master_generator.stores),
                "distribution_centers": len(master_generator.distribution_centers),
                "trucks": len(master_generator.trucks),
                "customers": len(master_generator.customers),
                "products_master": len(master_generator.products_master),
            }

            update_task_progress(
                task_id,
                1.0,
                f"Generated {table_name}",
                table_counts=final_counts,
            )

            return {
                "table_name": table_name,
                "records_generated": getattr(result, "records", 0),
                "output_path": str(Path(config.paths.master)),
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
        start_date, end_date = state_manager.get_fact_date_range(
            config_start_date
        )
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
                    read_geographies,
                    read_stores,
                    read_distribution_centers,
                    read_customers,
                    read_products,
                )
                geo_cnt = len(read_geographies())
                store_cnt = len(read_stores())
                dc_cnt = len(read_distribution_centers())
                cust_cnt = len(read_customers())
                prod_cnt = len(read_products())

                update_task_progress(
                    task_id,
                    0.01,
                    f"Dimensions ready: {geo_cnt} geos, {store_cnt} stores, {dc_cnt} DCs, {cust_cnt} customers, {prod_cnt} products",
                )

                if store_cnt == 0 or cust_cnt == 0 or prod_cnt == 0:
                    raise RuntimeError(
                        "DuckDB missing required data. Ensure master generation completed successfully."
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

            # Also wire a master-style per-table progress callback for consistent UI updates
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
                f"Starting historical generation from {start_date.date()} to {end_date.date()}"
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
        started_at=datetime.now(),
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
        start_date, end_date = state_manager.get_fact_date_range(
            config_start_date
        )
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

        # Clear the dashboard cache first
        cache_manager = CacheManager()
        cache_manager.clear_cache()

        # Reset DuckDB by closing any connection and deleting the DB file
        deleted_files: list[str] = []
        try:
            from ..db.duckdb_engine import reset_duckdb, get_duckdb_path

            path = get_duckdb_path()
            reset_duckdb()
            deleted_files.append(str(path))
        except Exception as e:
            logger.warning(f"Failed to reset DuckDB: {e}")

        # Reset cached generators so new DuckDB connections are used
        try:
            reset_generators()
        except Exception as e:
            logger.warning(f"Failed to reset generators after clear_all_data: {e}")

        # Reset generation state and clean any legacy file artifacts
        config_paths = {"master": config.paths.master, "facts": config.paths.facts}
        file_results = state_manager.clear_all_data(config_paths)

        errors = file_results.get("errors", [])
        if errors:
            logger.warning(f"Data clearing completed with some errors: {errors}")
        logger.info(
            f"All data cleared by resetting DuckDB ({len(deleted_files)} files) and resetting state/cache"
        )

        return OperationResult(
            success=True,
            message=(
                "All data cleared by resetting DuckDB; caches and legacy files removed. "
                f"Deleted {len(deleted_files)} files."
            ),
            started_at=datetime.now(),
        )

    except Exception as e:
        logger.error(f"Failed to clear data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clear data: {str(e)}",
        )


## Removed: clear-facts endpoint (deprecated)


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
    """List all generated fact tables (DuckDB-backed)."""
    try:
        from retail_datagen.db.duckdb_engine import get_duckdb_conn
        conn = get_duckdb_conn()
        tables_with_data: list[str] = []
        mapping = {
            "dc_inventory_txn": "fact_dc_inventory_txn",
            "truck_moves": "fact_truck_moves",
            "store_inventory_txn": "fact_store_inventory_txn",
            "receipts": "fact_receipts",
            "receipt_lines": "fact_receipt_lines",
            "foot_traffic": "fact_foot_traffic",
            "ble_pings": "fact_ble_pings",
            "marketing": "fact_marketing",
            "online_orders": "fact_online_order_headers",
            "online_order_lines": "fact_online_order_lines",
        }
        for logical, duck in mapping.items():
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {duck}").fetchone()[0]
                if int(cnt) > 0:
                    tables_with_data.append(logical)
            except Exception:
                pass
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
    """Get summary information for any table from DuckDB."""

    # Check if table exists in allowed lists
    if table_name not in MASTER_TABLES + FACT_TABLES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} not found.",
        )

    # Determine table type for response
    is_master = table_name in MASTER_TABLES

    try:
        from retail_datagen.db.duckdb_engine import get_duckdb_conn
        conn = get_duckdb_conn()
        mapping = {
            "geographies_master": "dim_geographies",
            "stores": "dim_stores",
            "distribution_centers": "dim_distribution_centers",
            "trucks": "dim_trucks",
            "customers": "dim_customers",
            "products_master": "dim_products",
            "dc_inventory_txn": "fact_dc_inventory_txn",
            "truck_moves": "fact_truck_moves",
            "store_inventory_txn": "fact_store_inventory_txn",
            "receipts": "fact_receipts",
            "receipt_lines": "fact_receipt_lines",
            "foot_traffic": "fact_foot_traffic",
            "ble_pings": "fact_ble_pings",
            "marketing": "fact_marketing",
            "online_orders": "fact_online_order_headers",
            "online_order_lines": "fact_online_order_lines",
        }
        duck = mapping.get(table_name, table_name)
        total = conn.execute(f"SELECT COUNT(*) FROM {duck}").fetchone()[0]
        cols = [d[0] for d in (conn.execute(f"SELECT * FROM {duck} LIMIT 0").description or [])]
        return {
            "table_name": table_name,
            "total_records": int(total),
            "columns": cols,
            "table_type": "master" if is_master else "fact",
        }
    except Exception as e:
        # If table doesn't exist yet, return empty summary for better UX
        if "does not exist" in str(e).lower():
            return {
                "table_name": table_name,
                "total_records": 0,
                "columns": [],
                "table_type": "master" if is_master else "fact",
            }
        logger.error(f"Failed to read table {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table {table_name}: {str(e)}",
        )


# New unified alias: /api/{table_name}/summary (works for master or fact)
@router.get(
    "/{table_name}/summary",
    summary="Get table summary (unified)",
    description="Get record counts and metadata for any table (master or fact) via unified path",
)
async def get_table_summary_unified(table_name: str):
    # Reuse the existing implementation
    return await get_table_summary(table_name)


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
    """Preview any table from DuckDB."""

    # Validate table name against known logical tables
    if table_name not in MASTER_TABLES + FACT_TABLES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {table_name} not found.",
        )

    try:
        from retail_datagen.db.duckdb_engine import get_duckdb_conn
        conn = get_duckdb_conn()
        mapping = {
            "geographies_master": "dim_geographies",
            "stores": "dim_stores",
            "distribution_centers": "dim_distribution_centers",
            "trucks": "dim_trucks",
            "customers": "dim_customers",
            "products_master": "dim_products",
            "dc_inventory_txn": "fact_dc_inventory_txn",
            "truck_moves": "fact_truck_moves",
            "store_inventory_txn": "fact_store_inventory_txn",
            "receipts": "fact_receipts",
            "receipt_lines": "fact_receipt_lines",
            "foot_traffic": "fact_foot_traffic",
            "ble_pings": "fact_ble_pings",
            "marketing": "fact_marketing",
            "online_orders": "fact_online_order_headers",
            "online_order_lines": "fact_online_order_lines",
        }
        duck = mapping.get(table_name, table_name)
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {duck}").fetchone()[0]
        cur = conn.execute(f"SELECT * FROM {duck} LIMIT {int(limit)}")
        columns = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
        preview_rows = [
            {columns[i]: rows[j][i] for i in range(len(columns))}
            for j in range(len(rows))
        ]
        return TablePreviewResponse(
            table_name=table_name,
            columns=columns,
            row_count=int(total_rows),
            preview_rows=preview_rows,
        )

    except Exception as e:
        # If table doesn't exist yet, return empty preview for better UX
        if "does not exist" in str(e).lower():
            return TablePreviewResponse(
                table_name=table_name,
                columns=[],
                row_count=0,
                preview_rows=[],
            )
        logger.error(f"Failed to read table {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read table {table_name}: {str(e)}",
        )


# New unified alias: /api/{table_name} (works for master or fact)
@router.get(
    "/{table_name}",
    response_model=TablePreviewResponse,
    summary="Preview table (unified)",
    description="Preview any table (master or fact) via unified path",
)
async def preview_table_unified(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
):
    # Delegate to existing preview handler
    return await preview_table(table_name=table_name, limit=limit)


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
    if table_name not in MASTER_TABLES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Master table {table_name} not found.",
        )

    try:
        from retail_datagen.db.duckdb_engine import get_duckdb_conn
        conn = get_duckdb_conn()
        mapping = {
            "geographies_master": "dim_geographies",
            "stores": "dim_stores",
            "distribution_centers": "dim_distribution_centers",
            "trucks": "dim_trucks",
            "customers": "dim_customers",
            "products_master": "dim_products",
        }
        duck = mapping.get(table_name, table_name)
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {duck}").fetchone()[0]
        cur = conn.execute(f"SELECT * FROM {duck} LIMIT {int(limit)}")
        columns = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
        preview_rows: list[dict[str, object]] = []
        for row in rows:
            preview_rows.append({columns[i]: row[i] for i in range(len(columns))})
        return TablePreviewResponse(
            table_name=table_name,
            columns=columns,
            row_count=int(total_rows),
            preview_rows=preview_rows,
        )
    except Exception as e:
        logger.error(f"Master preview failed for {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to preview {table_name}: {str(e)}",
        )


@router.get(
    "/facts/{table_name}",
    response_model=TablePreviewResponse,
    summary="Preview fact table (UI alias)",
    description="Alias used by UI to preview fact tables; returns empty preview when no data",
)
async def preview_fact_table_alias(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000),
):
    if table_name not in FACT_TABLES:
        # Gracefully return 404 if truly unknown table
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Fact table {table_name} not found.",
        )

    try:
        from retail_datagen.db.duckdb_engine import get_duckdb_conn
        conn = get_duckdb_conn()
        mapping = {
            "dc_inventory_txn": "fact_dc_inventory_txn",
            "truck_moves": "fact_truck_moves",
            "store_inventory_txn": "fact_store_inventory_txn",
            "receipts": "fact_receipts",
            "receipt_lines": "fact_receipt_lines",
            "foot_traffic": "fact_foot_traffic",
            "ble_pings": "fact_ble_pings",
            "marketing": "fact_marketing",
            "online_orders": "fact_online_order_headers",
            "online_order_lines": "fact_online_order_lines",
        }
        duck = mapping.get(table_name, table_name)
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {duck}").fetchone()[0]
        if int(total_rows) == 0:
            cols = [d[0] for d in (conn.execute(f"SELECT * FROM {duck} LIMIT 0").description or [])]
            return TablePreviewResponse(
                table_name=table_name,
                columns=cols,
                row_count=0,
                preview_rows=[],
            )
        cur = conn.execute(f"SELECT * FROM {duck} ORDER BY event_ts DESC LIMIT {int(limit)}")
        columns = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
        preview_rows = [{columns[i]: rows[j][i] for i in range(len(columns))} for j in range(len(rows))]
        return TablePreviewResponse(
            table_name=table_name,
            columns=columns,
            row_count=int(total_rows),
            preview_rows=preview_rows,
        )
    except Exception as e:
        logger.error(f"Fact preview (alias) failed for {table_name}: {e}")
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
    if table_name not in FACT_TABLES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Fact table {table_name} not found.",
        )

    try:
        from retail_datagen.db.duckdb_engine import get_duckdb_conn
        conn = get_duckdb_conn()
        duck = DUCK_FACT_MAP.get(table_name, table_name)
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {duck}").fetchone()[0]
        recent_row = conn.execute(f"SELECT MAX(event_ts) FROM {duck}").fetchone()[0]
        cur = conn.execute(f"SELECT * FROM {duck} ORDER BY event_ts DESC LIMIT {int(limit)}")
        cols = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
        preview_rows = [{cols[i]: rows[j][i] for i in range(len(cols))} for j in range(len(rows))]
        return TablePreviewResponse(
            table_name=table_name,
            columns=cols,
            row_count=int(total_rows),
            preview_rows=preview_rows,
            most_recent_date=str(recent_row) if recent_row is not None else None,
        )
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
    summary="Get live dashboard counts",
    description="Get live table counts from DuckDB (no cache)",
)
async def get_dashboard_counts():
    """Get live table counts for dashboard (queries unified retail database directly)."""
    from datetime import datetime

    master_counts: dict[str, int] = {}
    fact_counts: dict[str, int] = {}
    try:
        from retail_datagen.db.duckdb_engine import get_duckdb_conn
        conn = get_duckdb_conn()
        m_map = {
            "geographies_master": "dim_geographies",
            "stores": "dim_stores",
            "distribution_centers": "dim_distribution_centers",
            "trucks": "dim_trucks",
            "customers": "dim_customers",
            "products_master": "dim_products",
        }
        for k, v in m_map.items():
            try:
                master_counts[k] = int(conn.execute(f"SELECT COUNT(*) FROM {v}").fetchone()[0])
            except Exception:
                master_counts[k] = 0
        f_map = {
            "dc_inventory_txn": "fact_dc_inventory_txn",
            "truck_moves": "fact_truck_moves",
            "store_inventory_txn": "fact_store_inventory_txn",
            "receipts": "fact_receipts",
            "receipt_lines": "fact_receipt_lines",
            "foot_traffic": "fact_foot_traffic",
            "ble_pings": "fact_ble_pings",
            "marketing": "fact_marketing",
            "online_orders": "fact_online_order_headers",
            "online_order_lines": "fact_online_order_lines",
        }
        for k, v in f_map.items():
            try:
                fact_counts[k] = int(conn.execute(f"SELECT COUNT(*) FROM {v}").fetchone()[0])
            except Exception:
                fact_counts[k] = 0
    except Exception as e:
        logger.warning(f"Failed to read table counts from DuckDB: {e}")

    return {
        "master_tables": master_counts,
        "fact_tables": fact_counts,
        "last_updated": datetime.utcnow().isoformat() + "Z",
    }


# (moved) facts/date-range route is defined above FACT_TABLES to avoid
# conflicts with the dynamic '/facts/{table_name}' route
