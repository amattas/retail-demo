# Export Models Implementation Summary

## Overview
Implemented Pydantic v2 models for the data export API endpoints in `src/retail_datagen/api/export_models.py`.

## File Location
- **Implementation**: `/Users/amattas/GitHub/retail-demo/datagen/src/retail_datagen/api/export_models.py`
- **Package exports**: Updated `/Users/amattas/GitHub/retail-demo/datagen/src/retail_datagen/api/__init__.py`

## Models Implemented

### Type Definitions

```python
ExportFormat = Literal["csv", "parquet"]
ExportStatus = Literal["pending", "running", "completed", "failed"]
TableCategory = Literal["master", "facts"]
```

### Request Models

#### 1. `ExportRequest`
Base export request with format selection and optional table filtering.

**Fields:**
- `format: ExportFormat` - Output format (csv or parquet)
- `tables: list[str] | Literal["all"]` - Tables to export (default: "all")

**Example:**
```json
{
  "format": "parquet",
  "tables": ["stores", "customers", "products_master"]
}
```

#### 2. `FactExportRequest`
Extends `ExportRequest` with date range filtering for fact tables.

**Additional Fields:**
- `start_date: date | None` - Start date for filtering (inclusive)
- `end_date: date | None` - End date for filtering (inclusive)

**Validation:**
- Ensures `end_date >= start_date` using Pydantic v2 `@field_validator`

**Example:**
```json
{
  "format": "parquet",
  "tables": ["receipts", "receipt_lines"],
  "start_date": "2024-01-01",
  "end_date": "2024-01-31"
}
```

### Response Models

#### 3. `ExportOperationResult`
Result of initiating an export operation.

**Fields:**
- `success: bool` - Whether operation started successfully
- `message: str` - Human-readable message
- `task_id: str` - Unique task identifier for tracking
- `started_at: datetime` - Operation start timestamp

**Example:**
```json
{
  "success": true,
  "message": "Export operation started successfully",
  "task_id": "export_master_abc123def456",
  "started_at": "2024-01-15T10:30:00Z"
}
```

#### 4. `ExportStatusResponse`
Status and progress tracking for export operations.

**Fields:**
- `task_id: str` - Task identifier
- `status: ExportStatus` - Current status (pending/running/completed/failed)
- `progress: float` - Progress 0.0-1.0 (validated with `ge=0.0, le=1.0`)
- `message: str` - Current status message
- `tables_completed: list[str]` - Completed tables
- `tables_remaining: list[str]` - Remaining tables
- `current_table: str | None` - Currently exporting table
- `files_written: list[str] | None` - File paths written (on completion)
- `total_files: int | None` - Total files written
- `total_rows: int | None` - Total rows exported
- `output_directory: str | None` - Base export directory
- `error_message: str | None` - Error details (if failed)
- `started_at: datetime | None` - Start timestamp
- `completed_at: datetime | None` - Completion timestamp

**Example:**
```json
{
  "task_id": "export_master_abc123def456",
  "status": "running",
  "progress": 0.5,
  "message": "Exporting stores (3/6 tables complete)",
  "tables_completed": ["geographies_master", "distribution_centers"],
  "tables_remaining": ["trucks", "customers", "products_master"],
  "current_table": "stores",
  "files_written": null,
  "total_files": null,
  "total_rows": null,
  "output_directory": null,
  "error_message": null,
  "started_at": "2024-01-15T10:30:00Z",
  "completed_at": null
}
```

#### 5. `ExportTableInfo`
Detailed information about a single table export.

**Fields:**
- `table_name: str` - Table name
- `row_count: int` - Rows exported (validated `ge=0`)
- `file_count: int` - Files written (validated `ge=0`)
- `file_paths: list[str]` - List of file paths
- `export_format: ExportFormat` - Format used
- `date_range: tuple[date, date] | None` - Date range for fact tables

**Example:**
```json
{
  "table_name": "receipts",
  "row_count": 15420,
  "file_count": 3,
  "file_paths": [
    "exports/receipts/dt=2024-01-01/data.parquet",
    "exports/receipts/dt=2024-01-02/data.parquet",
    "exports/receipts/dt=2024-01-03/data.parquet"
  ],
  "export_format": "parquet",
  "date_range": ["2024-01-01", "2024-01-03"]
}
```

#### 6. `ExportSummaryResponse`
Comprehensive summary of completed export operation.

**Fields:**
- `task_id: str` - Task identifier
- `status: ExportStatus` - Final status
- `total_tables: int` - Total tables exported
- `total_files: int` - Total files written
- `total_rows: int` - Total rows exported
- `output_directory: str` - Base export directory
- `export_format: ExportFormat` - Format used
- `tables: list[ExportTableInfo]` - Per-table details
- `started_at: datetime` - Start timestamp
- `completed_at: datetime` - Completion timestamp
- `duration_seconds: float` - Total duration
- `error_message: str | None` - Error details (if applicable)

### Validation Helpers

#### Constants
```python
VALID_MASTER_TABLES = {
    "geographies_master",
    "stores",
    "distribution_centers",
    "trucks",
    "customers",
    "products_master",
}

VALID_FACT_TABLES = {
    "dc_inventory_txn",
    "truck_moves",
    "store_inventory_txn",
    "receipts",
    "receipt_lines",
    "foot_traffic",
    "ble_pings",
    "marketing",
    "online_orders",
}
```

#### `validate_table_names()`
Validates table names against category (master/facts).

**Signature:**
```python
def validate_table_names(
    tables: list[str] | Literal["all"],
    category: TableCategory
) -> list[str]
```

**Behavior:**
- Returns all tables if `tables == "all"`
- Validates each table name against the category
- Raises `ValueError` with helpful message if invalid tables found

**Example:**
```python
# Valid usage
validate_table_names(["stores", "customers"], "master")
# Returns: ["stores", "customers"]

validate_table_names("all", "facts")
# Returns: ["dc_inventory_txn", "truck_moves", ...]

# Invalid usage
validate_table_names(["invalid_table"], "master")
# Raises: ValueError: Invalid master table names: ['invalid_table'].
#         Valid tables: ['customers', 'distribution_centers', ...]
```

## Design Patterns & Best Practices

### Pydantic v2 Features Used
1. **ConfigDict**: Used instead of deprecated `Config` class
2. **Field validators**: Modern `@field_validator` decorator
3. **Literal types**: Type-safe enums for format/status
4. **Constraints**: Built-in validators (`ge`, `le`) for numeric fields
5. **Examples**: `json_schema_extra` in ConfigDict for OpenAPI docs

### Consistency with Existing Codebase
- Matches patterns from `api/models.py`
- Uses same naming conventions (e.g., `*Request`, `*Response`)
- Follows same documentation style
- Compatible with existing `GenerationStatusResponse` patterns

### Validation Strategy
- **Field-level**: Pydantic constraints (ge, le, minLength)
- **Model-level**: Custom validators for cross-field validation
- **Function-level**: `validate_table_names()` for external validation
- **Type safety**: Literal types prevent invalid values at type-check time

### Error Handling
- Clear error messages with context (shows invalid tables + valid options)
- Structured validation (raises `ValueError` with helpful details)
- All nullable fields properly typed with `| None`
- Default factories for list fields to prevent mutable default issues

## Integration Points

### Package Exports
All models and helpers exported from `src/retail_datagen/api/__init__.py`:

```python
from .export_models import (
    ExportFormat,
    ExportOperationResult,
    ExportRequest,
    ExportStatus,
    ExportStatusResponse,
    ExportSummaryResponse,
    ExportTableInfo,
    FactExportRequest,
    TableCategory,
    validate_table_names,
    VALID_FACT_TABLES,
    VALID_MASTER_TABLES,
)
```

### Usage in API Endpoints
These models are designed to be used in FastAPI endpoints:

```python
from retail_datagen.api import (
    ExportRequest,
    FactExportRequest,
    ExportOperationResult,
    ExportStatusResponse,
)

@router.post("/export/master", response_model=ExportOperationResult)
async def export_master_data(request: ExportRequest):
    # Validate tables
    tables = validate_table_names(request.tables, "master")
    # ...

@router.post("/export/facts", response_model=ExportOperationResult)
async def export_fact_data(request: FactExportRequest):
    # Validate tables and date range
    tables = validate_table_names(request.tables, "facts")
    # request.start_date and request.end_date already validated
    # ...

@router.get("/export/status/{task_id}", response_model=ExportStatusResponse)
async def get_export_status(task_id: str):
    # ...
```

## Testing

A validation script was created at `/Users/amattas/GitHub/retail-demo/datagen/test_export_models.py` that tests:

1. Import validation
2. Constant definitions (6 master tables, 9 fact tables)
3. Model instantiation
4. Date range validation
5. Table name validation
6. All response models
7. Error cases

## Next Steps

The models are ready for use in:

1. **Export service implementation** - Business logic for CSV/Parquet export
2. **Export router** - FastAPI endpoints using these models
3. **Export worker** - Background task processing
4. **Unit tests** - Comprehensive test coverage for export functionality

## Files Modified

1. **Created**: `src/retail_datagen/api/export_models.py` (305 lines)
2. **Updated**: `src/retail_datagen/api/__init__.py` (added exports)
3. **Created**: `test_export_models.py` (validation script)

## Summary

Successfully implemented complete Pydantic models for the export API with:
- Type-safe request/response models
- Comprehensive validation (field, model, and function level)
- Full backward compatibility with existing patterns
- Production-ready with examples and documentation
- Ready for FastAPI integration
