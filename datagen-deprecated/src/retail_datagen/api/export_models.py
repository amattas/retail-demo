"""
Pydantic models for data export API endpoints.

This module contains all request and response models for the data export functionality,
including export requests, status tracking, and operation results.
"""

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ================================
# TYPE DEFINITIONS
# ================================

ExportFormat = Literal["parquet"]
ExportStatus = Literal["pending", "running", "completed", "failed"]
TableCategory = Literal["master", "facts"]


# ================================
# REQUEST MODELS
# ================================


class ExportRequest(BaseModel):
    """Base request model for data export operations."""

    format: ExportFormat = Field(
        ..., description="Output format for exported data (parquet only)"
    )
    tables: list[str] | Literal["all"] = Field(
        default="all",
        description=(
            "Specific tables to export or 'all' for all tables in the category. "
            "Table names must match existing master or fact tables."
        ),
    )
    skip_upload: bool = Field(
        default=False,
        description=(
            "If true, skip uploading to Azure Storage even if "
            "credentials are configured. Useful for local-only exports."
        ),
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "format": "parquet",
                "tables": ["stores", "customers", "products_master"],
                "skip_upload": False,
            }
        }
    )


class FactExportRequest(ExportRequest):
    """Request model for exporting fact tables with optional date filtering."""

    start_date: date | None = Field(
        None,
        description=(
            "Start date for filtering fact data (inclusive). "
            "If not provided, exports all available data."
        ),
    )
    end_date: date | None = Field(
        None,
        description=(
            "End date for filtering fact data (inclusive). "
            "If not provided, exports up to the most recent date."
        ),
    )

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v: date | None, info) -> date | None:
        """Validate that end_date is not before start_date."""
        if v is not None and info.data.get("start_date") is not None:
            start = info.data["start_date"]
            if v < start:
                raise ValueError(
                    f"end_date ({v}) must be on or after start_date ({start})"
                )
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "format": "parquet",
                "tables": ["fact_receipts", "fact_receipt_lines"],
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            }
        }
    )


# ================================
# RESPONSE MODELS
# ================================


class ExportOperationResult(BaseModel):
    """Result of initiating an export operation."""

    success: bool = Field(
        ..., description="Whether the export operation started successfully"
    )
    message: str = Field(..., description="Human-readable message about the operation")
    task_id: str = Field(
        ..., description="Unique task identifier for tracking export progress"
    )
    started_at: datetime = Field(
        ..., description="Timestamp when the export operation started"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "success": True,
                "message": "Export operation started successfully",
                "task_id": "export_master_abc123def456",
                "started_at": "2024-01-15T10:30:00Z",
            }
        }
    )


class ExportStatusResponse(BaseModel):
    """Status and progress information for an export operation."""

    task_id: str = Field(..., description="Unique task identifier")
    status: ExportStatus = Field(..., description="Current export status")
    progress: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Overall progress as a value between 0.0 and 1.0",
    )
    message: str = Field(..., description="Current status message")

    # Progress details
    tables_completed: list[str] = Field(
        default_factory=list, description="List of tables that have been exported"
    )
    tables_remaining: list[str] = Field(
        default_factory=list, description="List of tables still pending export"
    )
    current_table: str | None = Field(
        None, description="Table currently being exported (if any)"
    )

    # Results (available on completion)
    files_written: list[str] | None = Field(
        None, description="List of file paths written (relative to export directory)"
    )
    total_files: int | None = Field(
        None, ge=0, description="Total number of files written"
    )
    total_rows: int | None = Field(
        None, ge=0, description="Total number of rows exported across all tables"
    )
    output_directory: str | None = Field(
        None, description="Base directory where files were written"
    )

    # Error information
    error_message: str | None = Field(
        None, description="Detailed error message if status is 'failed'"
    )

    # Timestamps
    started_at: datetime | None = Field(
        None, description="When the export operation started"
    )
    completed_at: datetime | None = Field(
        None, description="When the export operation completed (success or failure)"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "task_id": "export_master_abc123def456",
                "status": "running",
                "progress": 0.5,
                "message": "Exporting stores (3/6 tables complete)",
                "tables_completed": ["geographies_master", "distribution_centers"],
                "tables_remaining": ["trucks", "customers", "products_master"],
                "current_table": "stores",
                "files_written": None,
                "total_files": None,
                "total_rows": None,
                "output_directory": None,
                "error_message": None,
                "started_at": "2024-01-15T10:30:00Z",
                "completed_at": None,
            }
        }
    )


class ExportTableInfo(BaseModel):
    """Information about a single table export."""

    table_name: str = Field(..., description="Name of the table")
    row_count: int = Field(..., ge=0, description="Number of rows exported")
    file_count: int = Field(
        ..., ge=0, description="Number of files written for this table"
    )
    file_paths: list[str] = Field(
        default_factory=list, description="List of file paths for this table"
    )
    export_format: ExportFormat = Field(..., description="Format used for export")
    date_range: tuple[date, date] | None = Field(
        None, description="Date range for fact tables (start, end)"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "table_name": "receipts",
                "row_count": 15420,
                "file_count": 3,
                "file_paths": [
                    "exports/receipts/dt=2024-01-01/data.parquet",
                    "exports/receipts/dt=2024-01-02/data.parquet",
                    "exports/receipts/dt=2024-01-03/data.parquet",
                ],
                "export_format": "parquet",
                "date_range": ("2024-01-01", "2024-01-03"),
            }
        }
    )


class ExportSummaryResponse(BaseModel):
    """Detailed summary of a completed export operation."""

    task_id: str = Field(..., description="Unique task identifier")
    status: ExportStatus = Field(..., description="Final export status")
    total_tables: int = Field(..., ge=0, description="Total number of tables exported")
    total_files: int = Field(..., ge=0, description="Total number of files written")
    total_rows: int = Field(..., ge=0, description="Total number of rows exported")
    output_directory: str = Field(
        ..., description="Base directory where files were written"
    )
    export_format: ExportFormat = Field(..., description="Format used for export")

    # Per-table breakdown
    tables: list[ExportTableInfo] = Field(
        default_factory=list, description="Detailed information for each exported table"
    )

    # Timestamps
    started_at: datetime = Field(..., description="When the export started")
    completed_at: datetime = Field(..., description="When the export completed")
    duration_seconds: float = Field(
        ..., ge=0.0, description="Total export duration in seconds"
    )

    # Error info (if applicable)
    error_message: str | None = Field(
        None, description="Error message if export failed"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "task_id": "export_facts_xyz789abc123",
                "status": "completed",
                "total_tables": 3,
                "total_files": 9,
                "total_rows": 45680,
                "output_directory": "/path/to/exports",
                "export_format": "parquet",
                "tables": [
                    {
                        "table_name": "receipts",
                        "row_count": 15420,
                        "file_count": 3,
                        "file_paths": ["exports/receipts/dt=2024-01-01/data.parquet"],
                        "export_format": "parquet",
                        "date_range": ("2024-01-01", "2024-01-03"),
                    }
                ],
                "started_at": "2024-01-15T10:30:00Z",
                "completed_at": "2024-01-15T10:32:15Z",
                "duration_seconds": 135.5,
                "error_message": None,
            }
        }
    )


# ================================
# VALIDATION HELPERS
# ================================

# Valid master table names
VALID_MASTER_TABLES = {
    "geographies_master",
    "stores",
    "distribution_centers",
    "trucks",
    "customers",
    "products_master",
}

# Valid fact table names (must match db_reader.FACT_TABLES keys with "fact_" prefix)
VALID_FACT_TABLES = {
    "fact_dc_inventory_txn",
    "fact_truck_moves",
    "fact_store_inventory_txn",
    "fact_receipts",
    "fact_receipt_lines",
    "fact_foot_traffic",
    "fact_ble_pings",
    "fact_marketing",
    "fact_online_orders",
}


def validate_table_names(
    tables: list[str] | Literal["all"], category: TableCategory
) -> list[str]:
    """
    Validate table names against the specified category.

    Args:
        tables: List of table names or "all"
        category: Table category ("master" or "facts")

    Returns:
        Validated list of table names

    Raises:
        ValueError: If any table name is invalid for the category
    """
    if tables == "all":
        return list(VALID_MASTER_TABLES if category == "master" else VALID_FACT_TABLES)

    valid_tables = VALID_MASTER_TABLES if category == "master" else VALID_FACT_TABLES
    invalid_tables = set(tables) - valid_tables

    if invalid_tables:
        raise ValueError(
            f"Invalid {category} table names: {sorted(invalid_tables)}. "
            f"Valid tables: {sorted(valid_tables)}"
        )

    return tables
