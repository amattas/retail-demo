"""
FastAPI router for master data generation endpoints.

This module provides REST API endpoints for generating master data (dimensions)
with comprehensive status tracking and validation.
"""

import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status

from ...api.models import (
    GenerationStatusResponse,
    MasterDataRequest,
    OperationResult,
)
from ...config.models import RetailConfig
from ...generators.master_generators import MasterDataGenerator
from ...shared.dependencies import (
    create_background_task,
    get_config,
    get_master_generator,
    get_task_status,
    rate_limit,
    update_task_progress,
    validate_table_name,
)
from .common import DUCK_MASTER_MAP, MASTER_TABLES

logger = logging.getLogger(__name__)

router = APIRouter()


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

            # Short-circuit when everything already exists in DuckDB
            # and regeneration not forced
            if not request.force_regenerate:
                try:
                    from retail_datagen.db.duckdb_engine import get_duckdb_conn

                    conn = get_duckdb_conn()
                    missing_tables: list[str] = []
                    for table in tables_to_generate:
                        physical = DUCK_MASTER_MAP.get(table, table)
                        try:
                            count = conn.execute(
                                f"SELECT COUNT(*) FROM {physical}"
                            ).fetchone()[0]
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
            # DuckDB-only path: clear existing tables and regenerate;
            # no SQLAlchemy session required
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

                def q(t):
                    return conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]

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
                    from ...shared.cache import CacheManager

                    cache = CacheManager()
                    for k, v in final_counts.items():
                        cache.update_master_table(k, int(v), "Master Data")
                except Exception as cache_exc:
                    logger.warning(
                        f"Failed to update dashboard cache with DB counts: {cache_exc}"
                    )
            except Exception as count_exc:
                logger.warning(
                    f"Failed to query DB counts; "
                    f"falling back to in-memory counts: {count_exc}"
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
        started_at=datetime.now(UTC),
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
            result = await master_generator.generate_all_master_data_async(session=None)

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
        started_at=datetime.now(UTC),
    )
