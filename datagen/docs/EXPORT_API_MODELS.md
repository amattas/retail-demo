# Export API Models Documentation

## Model Hierarchy

```
Export Models
│
├── Request Models
│   ├── ExportRequest (base)
│   │   ├── format: ExportFormat
│   │   └── tables: list[str] | "all"
│   │
│   └── FactExportRequest (extends ExportRequest)
│       ├── start_date: date | None
│       └── end_date: date | None
│
├── Response Models
│   ├── ExportOperationResult (operation initiation)
│   │   ├── success: bool
│   │   ├── message: str
│   │   ├── task_id: str
│   │   └── started_at: datetime
│   │
│   ├── ExportStatusResponse (progress tracking)
│   │   ├── task_id: str
│   │   ├── status: ExportStatus
│   │   ├── progress: float (0.0-1.0)
│   │   ├── message: str
│   │   ├── tables_completed: list[str]
│   │   ├── tables_remaining: list[str]
│   │   ├── current_table: str | None
│   │   ├── files_written: list[str] | None
│   │   ├── total_files: int | None
│   │   ├── total_rows: int | None
│   │   ├── output_directory: str | None
│   │   ├── error_message: str | None
│   │   ├── started_at: datetime | None
│   │   └── completed_at: datetime | None
│   │
│   ├── ExportTableInfo (per-table details)
│   │   ├── table_name: str
│   │   ├── row_count: int
│   │   ├── file_count: int
│   │   ├── file_paths: list[str]
│   │   ├── export_format: ExportFormat
│   │   └── date_range: tuple[date, date] | None
│   │
│   └── ExportSummaryResponse (completion summary)
│       ├── task_id: str
│       ├── status: ExportStatus
│       ├── total_tables: int
│       ├── total_files: int
│       ├── total_rows: int
│       ├── output_directory: str
│       ├── export_format: ExportFormat
│       ├── tables: list[ExportTableInfo]
│       ├── started_at: datetime
│       ├── completed_at: datetime
│       ├── duration_seconds: float
│       └── error_message: str | None
│
└── Type Definitions
    ├── ExportFormat: "csv" | "parquet"
    ├── ExportStatus: "pending" | "running" | "completed" | "failed"
    └── TableCategory: "master" | "facts"
```

## API Flow

### Master Data Export Flow

```
1. Client POST /api/export/master
   Request: ExportRequest
   {
     "format": "parquet",
     "tables": ["stores", "customers"]
   }

2. Server validates request
   - validate_table_names(tables, "master")
   - Checks format is valid

3. Server starts export task
   Response: ExportOperationResult
   {
     "success": true,
     "message": "Export started",
     "task_id": "export_master_abc123",
     "started_at": "2024-01-15T10:30:00Z"
   }

4. Client polls GET /api/export/status/{task_id}
   Response: ExportStatusResponse
   {
     "task_id": "export_master_abc123",
     "status": "running",
     "progress": 0.5,
     "message": "Exporting stores (1/2 tables)",
     "tables_completed": ["customers"],
     "tables_remaining": ["stores"],
     "current_table": "stores",
     ...
   }

5. Export completes
   Response: ExportStatusResponse
   {
     "status": "completed",
     "progress": 1.0,
     "message": "Export completed",
     "tables_completed": ["customers", "stores"],
     "tables_remaining": [],
     "files_written": [
       "exports/customers.parquet",
       "exports/stores.parquet"
     ],
     "total_files": 2,
     "total_rows": 100214,
     "completed_at": "2024-01-15T10:31:30Z"
   }

6. Client GET /api/export/summary/{task_id}
   Response: ExportSummaryResponse
   {
     "task_id": "export_master_abc123",
     "status": "completed",
     "total_tables": 2,
     "total_files": 2,
     "total_rows": 100214,
     "tables": [
       {
         "table_name": "customers",
         "row_count": 100000,
         "file_count": 1,
         "file_paths": ["exports/customers.parquet"],
         "export_format": "parquet"
       },
       {
         "table_name": "stores",
         "row_count": 214,
         "file_count": 1,
         "file_paths": ["exports/stores.parquet"],
         "export_format": "parquet"
       }
     ],
     "duration_seconds": 90.5,
     ...
   }
```

### Fact Data Export Flow (with date filtering)

```
1. Client POST /api/export/facts
   Request: FactExportRequest
   {
     "format": "csv",
     "tables": ["receipts", "receipt_lines"],
     "start_date": "2024-01-01",
     "end_date": "2024-01-31"
   }

2. Server validates request
   - validate_table_names(tables, "facts")
   - Validates date range (end_date >= start_date)

3. Server starts export task
   Response: ExportOperationResult
   {
     "success": true,
     "task_id": "export_facts_xyz789",
     "started_at": "2024-01-15T10:35:00Z"
   }

4. Progress tracking (same as master export)

5. Completion includes date range info
   Response: ExportSummaryResponse
   {
     "tables": [
       {
         "table_name": "receipts",
         "row_count": 31000,
         "file_count": 31,  // One per day
         "file_paths": [
           "exports/receipts/dt=2024-01-01/data.csv",
           "exports/receipts/dt=2024-01-02/data.csv",
           ...
         ],
         "date_range": ["2024-01-01", "2024-01-31"]
       },
       ...
     ]
   }
```

## Validation Rules

### ExportRequest
- `format`: Must be "csv" or "parquet"
- `tables`: Either "all" or list of valid table names for category

### FactExportRequest
- Inherits all ExportRequest validations
- `end_date` must be >= `start_date` (if both provided)
- Date filtering is optional (None = all available data)

### ExportStatusResponse
- `progress`: Must be between 0.0 and 1.0 (inclusive)
- `total_files`, `total_rows`: Must be >= 0 if provided
- `status` transitions: pending → running → (completed | failed)

### ExportTableInfo
- `row_count`, `file_count`: Must be >= 0
- `date_range`: Only applicable for fact tables

## Table Categories

### Master Tables (6 total)
```python
VALID_MASTER_TABLES = {
    "geographies_master",      # Geographic hierarchies
    "stores",                  # Retail locations
    "distribution_centers",    # DC locations
    "trucks",                  # Delivery fleet
    "customers",               # Customer base
    "products_master",         # Product catalog
}
```

### Fact Tables (9 total)
```python
VALID_FACT_TABLES = {
    "dc_inventory_txn",        # DC inventory transactions
    "truck_moves",             # Truck movement logs
    "store_inventory_txn",     # Store inventory transactions
    "receipts",                # Sales receipts
    "receipt_lines",           # Receipt line items
    "foot_traffic",            # Store foot traffic
    "ble_pings",               # BLE beacon pings
    "marketing",               # Marketing impressions
    "online_orders",           # Online order events
}
```

## Error Handling

### Invalid Table Names
```python
# Request
{
  "format": "csv",
  "tables": ["invalid_table", "stores"]
}

# Response (400 Bad Request)
{
  "error": "ValidationError",
  "message": "Invalid master table names: ['invalid_table']. "
             "Valid tables: ['customers', 'distribution_centers', ...]"
}
```

### Invalid Date Range
```python
# Request
{
  "format": "parquet",
  "tables": "all",
  "start_date": "2024-01-31",
  "end_date": "2024-01-01"
}

# Response (422 Unprocessable Entity)
{
  "error": "ValidationError",
  "message": "end_date (2024-01-01) must be on or after start_date (2024-01-31)"
}
```

### Export Task Failure
```python
# Status response
{
  "task_id": "export_master_abc123",
  "status": "failed",
  "progress": 0.33,
  "message": "Export failed: Permission denied",
  "error_message": "Unable to write to exports/stores.parquet: [Errno 13] Permission denied",
  "tables_completed": ["customers"],
  "tables_remaining": ["stores"],
  "started_at": "2024-01-15T10:30:00Z",
  "completed_at": "2024-01-15T10:30:45Z"
}
```

## Usage Examples

### Export All Master Tables as Parquet
```python
from retail_datagen.api import ExportRequest

request = ExportRequest(
    format="parquet",
    tables="all"
)
```

### Export Specific Fact Tables with Date Range
```python
from datetime import date
from retail_datagen.api import FactExportRequest

request = FactExportRequest(
    format="csv",
    tables=["receipts", "receipt_lines", "foot_traffic"],
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 31)
)
```

### Validate Table Names Before Request
```python
from retail_datagen.api import validate_table_names

# Validate before creating request
try:
    tables = validate_table_names(["stores", "customers"], "master")
    request = ExportRequest(format="parquet", tables=tables)
except ValueError as e:
    print(f"Invalid tables: {e}")
```

### Check Export Status
```python
from retail_datagen.api import ExportStatusResponse

# Parse status response
status = ExportStatusResponse(
    task_id="export_master_abc123",
    status="running",
    progress=0.67,
    message="Exporting customers (4/6 tables)",
    tables_completed=["geographies_master", "stores", "distribution_centers"],
    tables_remaining=["trucks", "products_master"],
    current_table="customers"
)

# Calculate completion
if status.status == "completed":
    print(f"Export completed: {status.total_files} files written")
elif status.status == "running":
    percent = status.progress * 100
    print(f"Export {percent:.1f}% complete - {status.message}")
```

## Model Field Reference

### Required Fields (must be provided)
- `ExportRequest.format`
- `ExportOperationResult.success`
- `ExportOperationResult.message`
- `ExportOperationResult.task_id`
- `ExportOperationResult.started_at`
- `ExportStatusResponse.task_id`
- `ExportStatusResponse.status`
- `ExportStatusResponse.progress`
- `ExportStatusResponse.message`

### Optional Fields (with defaults)
- `ExportRequest.tables` (default: "all")
- `FactExportRequest.start_date` (default: None)
- `FactExportRequest.end_date` (default: None)
- `ExportStatusResponse.tables_completed` (default: [])
- `ExportStatusResponse.tables_remaining` (default: [])

### Nullable Fields (can be None)
- All fields marked with `| None` type annotation
- Set to None when data not yet available or not applicable
- Example: `files_written` is None until export completes

## Integration with FastAPI

The models are designed for seamless FastAPI integration:

```python
from fastapi import APIRouter, HTTPException
from retail_datagen.api import (
    ExportRequest,
    FactExportRequest,
    ExportOperationResult,
    ExportStatusResponse,
    validate_table_names,
)

router = APIRouter(prefix="/api/export", tags=["export"])

@router.post("/master", response_model=ExportOperationResult)
async def export_master_data(request: ExportRequest):
    """Export master dimension tables."""
    # FastAPI automatically validates request using Pydantic

    # Additional validation
    tables = validate_table_names(request.tables, "master")

    # Start export task
    task_id = await export_service.start_master_export(
        tables=tables,
        format=request.format
    )

    return ExportOperationResult(
        success=True,
        message=f"Export started for {len(tables)} table(s)",
        task_id=task_id,
        started_at=datetime.now()
    )

@router.post("/facts", response_model=ExportOperationResult)
async def export_fact_data(request: FactExportRequest):
    """Export fact tables with optional date filtering."""
    tables = validate_table_names(request.tables, "facts")

    task_id = await export_service.start_fact_export(
        tables=tables,
        format=request.format,
        start_date=request.start_date,
        end_date=request.end_date
    )

    return ExportOperationResult(
        success=True,
        message=f"Fact export started",
        task_id=task_id,
        started_at=datetime.now()
    )

@router.get("/status/{task_id}", response_model=ExportStatusResponse)
async def get_export_status(task_id: str):
    """Get status of an export task."""
    status = await export_service.get_task_status(task_id)

    if not status:
        raise HTTPException(status_code=404, detail="Task not found")

    return status  # FastAPI auto-validates against ExportStatusResponse
```

## Best Practices

1. **Use Literal types** for format/status to get type checking
2. **Validate table names** using `validate_table_names()` helper
3. **Check progress** using `progress` field (0.0 to 1.0)
4. **Poll status endpoint** until status is "completed" or "failed"
5. **Handle errors** by checking `error_message` field
6. **Use date filtering** for large fact table exports
7. **Provide examples** in API documentation using `json_schema_extra`

## OpenAPI Documentation

All models include `json_schema_extra` examples that automatically appear in FastAPI's `/docs` (Swagger UI) and `/redoc` (ReDoc) interfaces:

- Request examples show typical usage patterns
- Response examples show both in-progress and completed states
- Error examples demonstrate validation failures

This ensures developers using the API have clear, working examples for all operations.
