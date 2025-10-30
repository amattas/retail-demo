"""
Pydantic models for FastAPI requests and responses.

This module contains all request and response models for the retail data generator API,
including generation requests, streaming configurations, and status responses.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class GenerationStatus(str, Enum):
    """Status of data generation operations."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StreamingStatus(str, Enum):
    """Status of streaming operations."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    ERROR = "error"


# ================================
# GENERATION REQUEST MODELS
# ================================


class MasterDataRequest(BaseModel):
    """Request model for master data generation."""

    tables: list[str] | None = Field(
        None,
        description="Specific tables to generate. If None, all tables are generated.",
        examples=[["geographies_master", "stores", "customers"]],
    )
    force_regenerate: bool = Field(
        False, description="Force regeneration of existing data"
    )


class HistoricalDataRequest(BaseModel):
    """Request model for historical fact data generation."""

    start_date: datetime | None = Field(
        None,
        description=(
            "Start date for historical data generation "
            "(optional - uses config or last generated if not provided)"
        ),
    )
    end_date: datetime | None = Field(
        None,
        description=(
            "End date for historical data generation "
            "(optional - uses current time if not provided)"
        ),
    )
    tables: list[str] | None = Field(
        None,
        description=(
            "Specific fact tables to generate. "
            "If None, all tables are generated."
        ),
        examples=[["receipts", "receipt_lines", "store_inventory_txn"]],
    )


class ConfigUpdateRequest(BaseModel):
    """Request model for configuration updates."""

    seed: int | None = Field(None, ge=0, le=2**32 - 1)
    volume: dict[str, Any] | None = Field(None)
    realtime: dict[str, Any] | None = Field(None)
    paths: dict[str, str] | None = Field(None)
    stream: dict[str, str] | None = Field(None)


# ================================
# STREAMING REQUEST MODELS
# ================================


class StreamingStartRequest(BaseModel):
    """Request model for starting real-time streaming."""

    duration_minutes: int | None = Field(
        None,
        gt=0,
        description="Stream duration in minutes. If None, streams indefinitely.",
    )
    event_types: list[str] | None = Field(
        None,
        description="Event types to stream. If None, all event types are streamed.",
        examples=[["receipt", "foot_traffic", "ble_ping"]],
    )
    burst_override: int | None = Field(
        None, gt=0, description="Override burst size for this streaming session"
    )
    emit_interval_override: int | None = Field(
        None,
        gt=0,
        description="Override emit interval in milliseconds for this session",
    )


class StreamingConfigUpdate(BaseModel):
    """Request model for updating streaming configuration."""

    emit_interval_ms: int | None = Field(
        None, gt=0, description="Interval between event emissions in milliseconds"
    )
    burst: int | None = Field(
        None, gt=0, description="Number of events to emit in each burst"
    )
    azure_connection_string: str | None = Field(
        None, description="Azure Event Hub connection string"
    )
    max_batch_size: int | None = Field(
        None, gt=0, description="Maximum events per batch sent to Event Hub"
    )
    batch_timeout_ms: int | None = Field(
        None,
        gt=0,
        description="Maximum time to wait for batch completion in milliseconds",
    )


# ================================
# RESPONSE MODELS
# ================================


class GenerationStatusResponse(BaseModel):
    """Response model for generation status."""

    status: GenerationStatus = Field(..., description="Current generation status")
    progress: float = Field(
        ..., ge=0.0, le=1.0, description="Progress as a value between 0.0 and 1.0"
    )
    message: str = Field(..., description="Human-readable status message")
    estimated_completion: datetime | None = Field(
        None, description="Estimated completion time"
    )
    tables_completed: list[str] = Field(
        default_factory=list, description="List of tables that have been completed"
    )
    tables_remaining: list[str] = Field(
        default_factory=list, description="List of tables still being processed"
    )
    error_message: str | None = Field(
        None, description="Error message if status is FAILED"
    )
    table_progress: dict[str, float] | None = Field(
        None, description="Progress per table (0.0 to 1.0)"
    )
    current_table: str | None = Field(
        None, description="Currently processing table name"
    )
    tables_failed: list[str] | None = Field(
        None, description="List of tables that failed during generation"
    )
    table_counts: dict[str, int] | None = Field(
        None, description="Current record counts per table"
    )

    # Enhanced progress tracking fields (all optional for backward compatibility)
    tables_in_progress: list[str] | None = Field(
        None,
        description="List of fact tables currently being generated"
    )
    estimated_seconds_remaining: float | None = Field(
        None,
        ge=0.0,
        description="Estimated seconds until completion (approximate)"
    )
    progress_rate: float | None = Field(
        None,
        ge=0.0,
        description="Progress per second (rolling average)"
    )
    last_update_timestamp: datetime | None = Field(
        None,
        description="ISO-8601 timestamp of last progress update"
    )
    sequence: int | None = Field(
        None,
        description="Monotonic update sequence (drop older updates on UI if needed)"
    )

    # Hourly progress tracking fields (Phase 1B enhancements)
    current_day: int | None = Field(
        None,
        ge=1,
        description="Current day being processed (1-indexed, e.g., 1 = first day)"
    )
    current_hour: int | None = Field(
        None,
        ge=0,
        le=23,
        description="Current hour being processed (0-23, within the current day)"
    )
    hourly_progress: dict[str, float] | None = Field(
        None,
        description="Per-table hourly progress (0.0 to 1.0) for current hour"
    )
    total_hours_completed: int | None = Field(
        None,
        ge=0,
        description="Total hours processed across all days so far"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "running",
                "progress": 0.45,
                "message": "Processing receipts (3/8 tables complete)",
                "estimated_completion": "2025-10-21T14:30:00Z",
                "tables_completed": ["dc_inventory_txn", "truck_moves", "store_inventory_txn"],
                "tables_remaining": ["receipt_lines", "foot_traffic", "ble_pings", "marketing"],
                "current_table": "receipts",
                "tables_failed": [],
                "tables_in_progress": ["receipts"],
                "error_message": None,
                "table_progress": {
                    "dc_inventory_txn": 1.0,
                    "truck_moves": 1.0,
                    "store_inventory_txn": 1.0,
                    "receipts": 0.65,
                    "receipt_lines": 0.0,
                    "foot_traffic": 0.0,
                    "ble_pings": 0.0,
                    "marketing": 0.0
                },
                "estimated_seconds_remaining": 45.2,
                "progress_rate": 0.01,
                "last_update_timestamp": "2025-10-21T14:28:15.123Z",
                "current_day": 5,
                "current_hour": 14,
                "hourly_progress": {
                    "receipts": 0.65,
                    "receipt_lines": 0.43,
                    "store_inventory_txn": 0.78
                },
                "total_hours_completed": 98
            }
        }
    )


class StreamingStatusResponse(BaseModel):
    """Response model for streaming status."""

    is_streaming: bool = Field(..., description="Whether streaming is currently active")
    status: StreamingStatus = Field(..., description="Current streaming status")
    uptime_seconds: float = Field(
        ..., ge=0.0, description="Time streaming has been active in seconds"
    )
    events_sent: int = Field(..., ge=0, description="Total number of events sent")
    events_per_second: float = Field(
        ..., ge=0.0, description="Current events per second rate"
    )
    last_event_time: datetime | None = Field(
        None, description="Timestamp of the last event sent"
    )


class StreamingStatisticsResponse(BaseModel):
    """Response model for detailed streaming statistics."""

    events_generated: int = Field(ge=0)
    events_sent_successfully: int = Field(ge=0)
    events_failed: int = Field(ge=0)
    batches_sent: int = Field(ge=0)
    total_streaming_time: float = Field(ge=0.0)
    events_per_second: float = Field(ge=0.0)
    bytes_sent: int = Field(ge=0)
    last_event_time: datetime | None = None
    event_type_counts: dict[str, int] = Field(default_factory=dict)
    error_counts: dict[str, int] = Field(default_factory=dict)
    connection_failures: int = Field(ge=0)
    circuit_breaker_trips: int = Field(ge=0)


class TableListResponse(BaseModel):
    """Response model for listing available tables."""

    tables: list[str] = Field(..., description="List of available table names")
    count: int = Field(..., ge=0, description="Number of tables available")


class TablePreviewResponse(BaseModel):
    """Response model for table data preview."""

    table_name: str = Field(..., description="Name of the table")
    columns: list[str] = Field(..., description="Column names")
    row_count: int = Field(..., ge=0, description="Total number of rows in table")
    preview_rows: list[dict[str, Any]] = Field(
        ..., description="First N rows of the table (max 100)"
    )
    date_partition: str | None = Field(
        None, description="Date partition for fact tables (YYYY-MM-DD format)"
    )
    most_recent_date: str | None = Field(
        None, description="Most recent event date/time for fact tables"
    )


class HealthCheckResponse(BaseModel):
    """Response model for health checks."""

    status: str = Field(..., description="Overall health status")
    timestamp: datetime = Field(..., description="Health check timestamp")
    version: str = Field(..., description="Application version")
    checks: dict[str, dict[str, Any]] = Field(
        ..., description="Individual component health checks"
    )


class ConnectionTestResponse(BaseModel):
    """Response model for connection tests."""

    success: bool = Field(..., description="Whether the connection test succeeded")
    message: str = Field(..., description="Detailed message about the test result")
    response_time_ms: float | None = Field(
        None, ge=0.0, description="Response time in milliseconds"
    )
    details: dict[str, Any] = Field(
        default_factory=dict, description="Additional test details"
    )


class ErrorResponse(BaseModel):
    """Standard error response model."""

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})

    error: str = Field(..., description="Error type or code")
    message: str = Field(..., description="Human-readable error message")
    details: dict[str, Any] | None = Field(None, description="Additional error details")
    timestamp: datetime = Field(..., description="Error timestamp")


class ValidationErrorResponse(BaseModel):
    """Response model for validation errors."""

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="General error message")
    field_errors: list[dict[str, Any]] = Field(
        ..., description="Detailed field validation errors"
    )
    timestamp: datetime = Field(..., description="Error timestamp")


# ================================
# OPERATION STATUS MODELS
# ================================


class OperationResult(BaseModel):
    """Generic operation result model."""

    success: bool = Field(..., description="Whether the operation succeeded")
    message: str = Field(..., description="Operation result message")
    operation_id: str | None = Field(
        None, description="Unique identifier for tracking the operation"
    )
    started_at: datetime | None = Field(None, description="When the operation started")


class BatchOperationStatus(BaseModel):
    """Status model for batch operations."""

    total_items: int = Field(..., ge=0, description="Total number of items to process")
    completed_items: int = Field(..., ge=0, description="Number of completed items")
    failed_items: int = Field(..., ge=0, description="Number of failed items")
    success_rate: float = Field(
        ..., ge=0.0, le=1.0, description="Success rate as percentage (0.0 to 1.0)"
    )
    duration_seconds: float = Field(..., ge=0.0, description="Total operation duration")


# ================================
# RECENT EVENTS MODEL
# ================================


class RecentEventsResponse(BaseModel):
    """Response model for recent streaming events."""

    events: list[dict[str, Any]] = Field(
        ..., description="List of recent event envelopes"
    )
    count: int = Field(..., ge=0, description="Number of events returned")
    timestamp: datetime = Field(..., description="Response timestamp")


# ================================
# SUPPLY CHAIN DISRUPTION MODELS
# ================================


class DisruptionType(str, Enum):
    """Types of supply chain disruptions."""

    INVENTORY_SHORTAGE = "inventory_shortage"
    DC_OUTAGE = "dc_outage"
    TRUCK_BREAKDOWN = "truck_breakdown"
    WEATHER_DELAY = "weather_delay"
    SUPPLIER_DELAY = "supplier_delay"


class DisruptionRequest(BaseModel):
    """Request model for creating supply chain disruptions."""

    disruption_type: DisruptionType = Field(..., description="Type of disruption")
    target_id: int = Field(..., gt=0, description="Target ID (DC, Store, or Truck)")
    duration_minutes: int = Field(
        ..., gt=0, le=1440, description="Duration in minutes (max 24 hours)"
    )
    severity: float = Field(
        0.5, ge=0.1, le=1.0, description="Severity from 0.1 (minor) to 1.0 (complete)"
    )
    product_ids: list[int] | None = Field(
        None, description="Specific products affected (for inventory shortages)"
    )


class DisruptionResponse(BaseModel):
    """Response model for disruption operations."""

    success: bool = Field(..., description="Whether disruption was created/updated")
    disruption_id: str = Field(..., description="Unique disruption identifier")
    message: str = Field(..., description="Operation result message")
    active_until: datetime = Field(..., description="When the disruption will end")


class ActiveDisruptionsResponse(BaseModel):
    """Response model for listing active disruptions."""

    disruptions: list[dict[str, Any]] = Field(
        ..., description="List of active disruptions"
    )
    count: int = Field(..., ge=0, description="Number of active disruptions")
    timestamp: datetime = Field(..., description="Response timestamp")
