"""
FastAPI router for generation state and operation control endpoints.

This module provides REST API endpoints for managing generation state,
clearing data, and controlling operations.
"""

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, status

from ...api.models import OperationResult
from ...config.models import RetailConfig
from ...generators.generation_state import GenerationStateManager
from ...shared.cache import CacheManager
from ...shared.dependencies import (
    cancel_task,
    get_config,
    rate_limit,
    reset_generators,
)

logger = logging.getLogger(__name__)

router = APIRouter()


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
            from ...db.duckdb_engine import get_duckdb_path, reset_duckdb

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
            f"All data cleared by resetting DuckDB ({len(deleted_files)} files) "
            "and resetting state/cache"
        )

        return OperationResult(
            success=True,
            message=(
                "All data cleared by resetting DuckDB; "
                "caches and legacy files removed. "
                f"Deleted {len(deleted_files)} files."
            ),
            started_at=datetime.now(UTC),
        )

    except Exception as e:
        logger.error(f"Failed to clear data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clear data: {str(e)}",
        )


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
