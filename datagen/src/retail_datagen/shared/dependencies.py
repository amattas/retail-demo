"""
FastAPI dependencies and middleware for the retail data generator.

This module provides shared dependencies, authentication, rate limiting,
and other middleware components for the FastAPI application.
"""

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from ..config.models import RetailConfig
from ..generators.fact_generator import FactDataGenerator
from ..generators.master_generator import MasterDataGenerator
from ..streaming.event_streamer import EventStreamer

logger = logging.getLogger(__name__)


# ================================
# TASK STATUS MODEL
# ================================


class TaskStatus(BaseModel):
    """Status of a background task with optional per-table progress tracking."""

    status: str  # "pending", "running", "completed", "failed", "cancelled"
    progress: float = Field(default=0.0, ge=0.0, le=1.0)  # 0.0 to 1.0
    message: str = ""
    started_at: datetime | None = None
    completed_at: datetime | None = None
    last_updated: datetime | None = None
    description: str = ""
    error: str | None = None
    result: Any = None

    # Per-table progress tracking (optional for backwards compatibility)
    table_progress: dict[str, float] | None = Field(
        default=None,
        description="Progress per table (e.g., {'receipts': 0.8, 'dc_inventory_txn': 1.0})",
    )
    current_table: str | None = Field(
        default=None, description="Currently processing table name"
    )
    tables_completed: list[str] | None = Field(
        default=None, description="List of completed tables"
    )
    tables_failed: list[str] | None = Field(
        default=None, description="List of failed tables"
    )
    table_counts: dict[str, int] | None = Field(
        default=None, description="Current record counts per table"
    )

    # Enhanced progress tracking fields (for UI ETA and detailed progress)
    tables_in_progress: list[str] | None = Field(
        default=None, description="List of tables currently being generated"
    )
    tables_remaining: list[str] | None = Field(
        default=None, description="List of tables not yet started"
    )
    estimated_seconds_remaining: float | None = Field(
        default=None, description="Estimated seconds until completion"
    )
    progress_rate: float | None = Field(
        default=None, description="Progress rate (percent per second)"
    )
    last_update_timestamp: datetime | None = Field(
        default=None, description="Timestamp of last progress update"
    )
    # Monotonic sequence number for UI to de-dup/out-of-order
    sequence: int | None = Field(
        default=None, description="Monotonic update sequence per task"
    )

    # NEW: Hourly progress tracking fields
    current_day: int | None = Field(
        default=None, description="Current day being processed (1-indexed)", ge=1
    )
    current_hour: int | None = Field(
        default=None, description="Current hour being processed (0-23)", ge=0, le=23
    )
    hourly_progress: dict[str, float] | None = Field(
        default=None, description="Per-table hourly progress (0.0-1.0)"
    )
    total_hours_completed: int | None = Field(
        default=None, description="Total hours completed across all days", ge=0
    )

    def __getitem__(self, key: str) -> Any:
        """Support dictionary-style access for backwards compatibility."""
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        """Support dict.get() for backwards compatibility."""
        return getattr(self, key, default)

    def __contains__(self, key: str) -> bool:
        """Support 'in' operator for backwards compatibility."""
        return hasattr(self, key)


# Global instances (will be initialized on startup)
_config: RetailConfig | None = None
_master_generator: MasterDataGenerator | None = None
_fact_generator: FactDataGenerator | None = None
_event_streamer: EventStreamer | None = None

# Background task tracking
_background_tasks: dict[str, asyncio.Task] = {}
_task_status: dict[str, TaskStatus] = {}

# Rate limiting storage
_rate_limit_storage: dict[str, list] = defaultdict(list)

# Security
security = HTTPBearer(auto_error=False)


# ================================
# CONFIGURATION DEPENDENCIES
# ================================


async def get_config() -> RetailConfig:
    """Get the current retail configuration."""
    global _config
    if _config is None:
        # Try to load from default location
        config_path = Path("config.json")
        if config_path.exists():
            _config = RetailConfig.from_file(config_path)
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Configuration not initialized. Please provide a valid config.json file.",
            )
    return _config


async def update_config(new_config: RetailConfig) -> None:
    """Update the global configuration."""
    global _config, _master_generator, _fact_generator, _event_streamer
    _config = new_config

    # Reinitialize generators with new config
    _master_generator = MasterDataGenerator(new_config)
    # Defer FactDataGenerator creation until requested so we can supply a fresh DB session
    _fact_generator = None
    _event_streamer = EventStreamer(new_config)


# ================================
# GENERATOR DEPENDENCIES
# ================================


async def get_master_generator(
    config: RetailConfig = Depends(get_config),
) -> MasterDataGenerator:
    """Get the master data generator instance."""
    global _master_generator
    if _master_generator is None:
        _master_generator = MasterDataGenerator(config)
    return _master_generator


async def get_fact_generator(
    config: RetailConfig = Depends(get_config),
) -> FactDataGenerator:
    """Get a fact data generator; session is managed inside the generator."""
    return FactDataGenerator(config, session=None)


async def get_event_streamer(
    config: RetailConfig = Depends(get_config),
) -> EventStreamer:
    """Get the event streamer instance."""
    global _event_streamer
    if _event_streamer is None:
        _event_streamer = EventStreamer(config)
    return _event_streamer


# ================================
# BACKGROUND TASK MANAGEMENT
# ================================


def create_background_task(task_id: str, coro, description: str = "") -> str:
    """Create and track a background task."""
    global _background_tasks, _task_status

    if task_id in _background_tasks and not _background_tasks[task_id].done():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Task {task_id} is already running",
        )

    task = asyncio.create_task(coro)
    _background_tasks[task_id] = task
    _task_status[task_id] = TaskStatus(
        status="running",
        started_at=datetime.now(),
        description=description,
        progress=0.0,
        message="Task started",
        error=None,
    )

    def task_done_callback(future):
        try:
            result = future.result()
            _task_status[task_id] = TaskStatus(
                **_task_status[task_id].model_dump(exclude={"status", "completed_at", "progress", "message", "result"}),
                status="completed",
                completed_at=datetime.now(),
                progress=1.0,
                message="Task completed successfully",
                result=result,
            )
        except Exception as e:
            logger.error(f"Background task {task_id} failed: {e}")
            _task_status[task_id] = TaskStatus(
                **_task_status[task_id].model_dump(exclude={"status", "completed_at", "message", "error"}),
                status="failed",
                completed_at=datetime.now(),
                message=f"Task failed: {str(e)}",
                error=str(e),
            )

    task.add_done_callback(task_done_callback)
    return task_id


def get_task_status(task_id: str) -> TaskStatus | None:
    """Get the status of a background task."""
    return _task_status.get(task_id)


def cancel_task(task_id: str) -> bool:
    """Cancel a background task."""
    global _background_tasks, _task_status

    if task_id not in _background_tasks:
        return False

    task = _background_tasks[task_id]
    if not task.done():
        task.cancel()
        _task_status[task_id] = TaskStatus(
            **_task_status[task_id].model_dump(exclude={"status", "completed_at", "message"}),
            status="cancelled",
            completed_at=datetime.now(),
            message="Task was cancelled",
        )
        return True

    return False


def update_task_progress(
    task_id: str,
    progress: float,
    message: str = "",
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
) -> None:
    """Update progress for a background task with optional per-table tracking.

    Args:
        task_id: Unique identifier for the task
        progress: Overall progress (0.0 to 1.0)
        message: Status message
        table_progress: Optional dictionary mapping table names to their progress (0.0 to 1.0)
        current_table: Optional name of the currently processing table
        tables_completed: Optional list of completed table names
        tables_failed: Optional list of failed table names
        tables_in_progress: Optional list of tables currently being generated
        tables_remaining: Optional list of tables not yet started
        estimated_seconds_remaining: Optional estimated seconds until completion
        progress_rate: Optional progress rate (percent per second)
        current_hour: Optional current hour being processed (0-23)
        hourly_progress: Optional per-table hourly progress
        total_hours_completed: Optional total hours completed across all days
    """
    if task_id in _task_status:
        # Get existing status fields we want to preserve
        existing = _task_status[task_id].model_dump()

        # Build updated fields
        updated_fields = {
            "progress": max(0.0, min(1.0, progress)),
            "message": message,
            "last_updated": datetime.now(),
            "last_update_timestamp": datetime.now(),
        }

        # Update table-level progress if provided (merge with max to avoid regressions)
        if table_progress is not None:
            existing_progress = existing.get("table_progress") or {}
            merged_progress: dict[str, float] = {}
            # Union of keys
            keys = set(existing_progress.keys()) | set(table_progress.keys())
            for k in keys:
                old = existing_progress.get(k, 0.0) or 0.0
                new = table_progress.get(k, 0.0) or 0.0
                merged_progress[k] = max(old, new)
            updated_fields["table_progress"] = merged_progress
        if current_table is not None:
            updated_fields["current_table"] = current_table
        # Derive tables_completed/in_progress/remaining from merged table_progress when available
        if "table_progress" in updated_fields:
            prog = updated_fields["table_progress"]
            updated_fields["tables_completed"] = sorted([k for k,v in prog.items() if v >= 1.0])
            updated_fields["tables_in_progress"] = sorted([k for k,v in prog.items() if 0.0 < v < 1.0])
            updated_fields["tables_remaining"] = sorted([k for k,v in prog.items() if v == 0.0])
        else:
            if tables_completed is not None:
                updated_fields["tables_completed"] = tables_completed
        if tables_failed is not None:
            updated_fields["tables_failed"] = tables_failed
        if "tables_in_progress" not in updated_fields and tables_in_progress is not None:
            updated_fields["tables_in_progress"] = tables_in_progress
        if "tables_remaining" not in updated_fields and tables_remaining is not None:
            updated_fields["tables_remaining"] = tables_remaining
        if estimated_seconds_remaining is not None:
            updated_fields["estimated_seconds_remaining"] = estimated_seconds_remaining
        if progress_rate is not None:
            updated_fields["progress_rate"] = progress_rate
        if table_counts is not None:
            # Merge with existing counts so we don't lose prior table updates; clamp with max to avoid decreases
            existing_counts = existing.get("table_counts") or {}
            merged: dict[str, int] = {}
            keys = set(existing_counts.keys()) | set(table_counts.keys())
            for k in keys:
                old = int(existing_counts.get(k, 0) or 0)
                new = int(table_counts.get(k, 0) or 0)
                merged[k] = max(old, new)
            updated_fields["table_counts"] = merged
        # NEW: Update hourly progress fields if provided
        if current_hour is not None:
            updated_fields["current_hour"] = current_hour
        if hourly_progress is not None:
            updated_fields["hourly_progress"] = hourly_progress
        if total_hours_completed is not None:
            updated_fields["total_hours_completed"] = total_hours_completed

        # Increment sequence for out-of-order handling
        prev_seq = int(existing.get("sequence") or 0)
        updated_fields["sequence"] = prev_seq + 1

        # Create new TaskStatus with merged fields
        _task_status[task_id] = TaskStatus(**{**existing, **updated_fields})


# ================================
# RATE LIMITING
# ================================


def rate_limit(max_requests: int = 100, window_seconds: int = 60):
    """Rate limiting decorator."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract request from args/kwargs
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break

            if request is None:
                # If no request found, proceed without rate limiting
                return await func(*args, **kwargs)

            client_ip = request.client.host
            current_time = time.time()

            # Clean old requests
            cutoff_time = current_time - window_seconds
            _rate_limit_storage[client_ip] = [
                req_time
                for req_time in _rate_limit_storage[client_ip]
                if req_time > cutoff_time
            ]

            # Check rate limit
            if len(_rate_limit_storage[client_ip]) >= max_requests:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit exceeded: {max_requests} requests per {window_seconds} seconds",
                )

            # Record this request
            _rate_limit_storage[client_ip].append(current_time)

            return await func(*args, **kwargs)

        return wrapper

    return decorator


# ================================
# AUTHENTICATION
# ================================


async def get_api_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
):
    """Extract and validate API key from Authorization header."""
    # In development mode, skip authentication
    config = await get_config()
    api_key = getattr(config, "api_key", None)

    if not api_key:
        # No API key configured, allow access
        return None

    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if credentials.credentials != api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return credentials.credentials


# ================================
# VALIDATION HELPERS
# ================================


def validate_table_name(table_name: str, table_type: str = "master") -> str:
    """Validate that a table name is valid for the given type."""
    if table_type == "master":
        valid_tables = [
            "geographies_master",
            "stores",
            "distribution_centers",
            "trucks",
            "customers",
            "products_master",
        ]
    elif table_type == "fact":
        valid_tables = [
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
    else:
        raise ValueError(f"Invalid table type: {table_type}")

    if table_name not in valid_tables:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid {table_type} table name: {table_name}. "
            f"Valid options: {', '.join(valid_tables)}",
        )

    return table_name


def validate_date_range(start_date: datetime, end_date: datetime) -> None:
    """Validate that date range is reasonable."""
    # Allow single-day ranges (start == end). Only error if start is after end.
    if start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Start date must be before end date",
        )

    # Check for reasonable date range (max 2 years)
    max_duration = timedelta(days=730)
    if (end_date - start_date) > max_duration:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Date range cannot exceed 2 years",
        )

    # Check dates are not too far in the future
    max_future = datetime.now() + timedelta(days=365)
    if end_date > max_future:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="End date cannot be more than 1 year in the future",
        )


# ================================
# HEALTH CHECK HELPERS
# ================================


async def check_file_system_health() -> dict[str, Any]:
    """Check file system health and permissions."""
    try:
        config = await get_config()

        checks = {}
        for path_name, path_value in [
            ("dict_path", config.paths.dictionaries),
            ("master_path", config.paths.master),
            ("facts_path", config.paths.facts),
        ]:
            path = Path(path_value)
            checks[path_name] = {
                "exists": path.exists(),
                "is_directory": path.is_dir() if path.exists() else False,
                "writable": (
                    path.is_dir() and path.stat().st_mode & 0o200
                    if path.exists()
                    else False
                ),
                "path": str(path),
            }

        return {"status": "healthy", "details": checks}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


async def check_azure_connection() -> dict[str, Any]:
    """Check Azure Event Hub connection health."""
    try:
        config = await get_config()
        connection_string = config.realtime.azure_connection_string

        if not connection_string:
            return {
                "status": "not_configured",
                "message": "Azure connection string not configured",
            }

        # Basic connection string validation with redaction
        required_parts = ["Endpoint", "SharedAccessKeyName", "SharedAccessKey"]
        missing_parts = [
            part for part in required_parts if part not in connection_string
        ]

        if missing_parts:
            return {
                "status": "invalid_config",
                "error": f"Missing connection string parts: {', '.join(missing_parts)}",
            }

        redacted = connection_string
        for key in ["SharedAccessKey", "SharedAccessKeyName"]:
            if key in redacted:
                redacted = redacted.replace(key + "=", key + "=***REDACTED***")

        result: dict[str, Any] = {
            "status": "configured",
            "message": "Connection string appears valid",
            "redacted": redacted[:60] + "..." if len(redacted) > 60 else redacted,
        }

        # Optional live probe (disabled by default)
        import os

        if os.getenv("AZURE_LIVE_PROBE") in {"1", "true", "True"}:
            try:
                from ..streaming.azure_client import AzureEventHubClient

                client = AzureEventHubClient(
                    connection_string=connection_string,
                    hub_name=config.stream.hub,
                    max_batch_size=1,
                    retry_attempts=1,
                )
                async with client.managed_connection():
                    health = await client.health_check()
                result["live_probe"] = health
                if not health.get("healthy", False):
                    result["status"] = "degraded"
            except Exception as e:
                # Do not fail health check entirely; report degraded probe
                result["live_probe_error"] = str(e)
                result["status"] = "degraded"

        return result

    except Exception as e:
        return {"status": "error", "error": str(e)}
