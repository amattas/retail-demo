"""
Main FastAPI application entry point for the retail data generator.

This module creates and configures the FastAPI application with all routes,
middleware, exception handlers, and startup/shutdown events.

Specification reference: see AGENTS.md.
"""

import logging
import os
import traceback
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .api.export_router import router as export_router
from .api.models import ErrorResponse, HealthCheckResponse, ValidationErrorResponse
from .config.models import RetailConfig
from .generators.routers import router as generators_router
from .shared.dependencies import (
    check_azure_connection,
    check_file_system_health,
    get_config,
    update_config,
)
from .streaming.router import router as streaming_router

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# RequestBodyFixMiddleware removed — router handles empty body defaults.

# Application metadata
APP_NAME = "Retail Data Generator API"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = """
**Retail Data Generator API** provides comprehensive endpoints for generating synthetic retail data.

## Features

### 🏪 Master Data Generation
Generate dimension tables including stores, customers, products, and distribution centers.

### 📊 Historical Data Generation
Create fact tables with realistic transaction data, inventory movements, and analytics events.

### 🔄 Real-Time Streaming
Stream live events to Azure Event Hub for real-time analytics and monitoring.

### ⚙️ Configuration Management
Update generation parameters, streaming settings, and system configuration.

## Data Safety
All generated data is **synthetic and fictitious**. No real personal information is used or generated.

## Modes
1. **Master Data**: Generate dimension tables from dictionary data
2. **Historical Data**: Generate fact tables for specified date ranges
3. **Real-Time**: Stream incremental events to Azure Event Hub

## Authentication
Some endpoints may require API key authentication. Include the API key in the Authorization header:
```
Authorization: Bearer your-api-key-here
```
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown events."""
    # Configure structured logging first
    from .shared.logging_config import configure_structured_logging

    configure_structured_logging(level="INFO")

    # Startup
    logger.info(f"Starting {APP_NAME} v{APP_VERSION}")

    # Initialize DuckDB
    try:
        logger.info("Initializing DuckDB database...")
        from .db.duckdb_engine import get_duckdb_conn, get_duckdb_path

        conn = get_duckdb_conn()
        conn.execute("SELECT 1")
        logger.info("✅ DuckDB initialized successfully")
        logger.info(f"  - DuckDB Path: {get_duckdb_path()}")
    except Exception as e:
        logger.error(f"❌ Failed to initialize DuckDB: {e}", exc_info=True)
        logger.warning("Application will continue but database features may not work")

    try:
        # Initialize configuration
        config_path = Path("config.json")
        if config_path.exists():
            config = RetailConfig.from_file(config_path)
            await update_config(config)
            logger.info("Configuration loaded successfully")

            # Validate streaming configuration on startup
            from .shared.credential_utils import validate_eventhub_connection_string

            conn_str = config.realtime.get_connection_string()
            if conn_str and not conn_str.startswith(("mock://", "test://")):
                is_valid, error = validate_eventhub_connection_string(conn_str)
                if is_valid:
                    logger.info("✓ Event Hub connection string validated")
                else:
                    logger.warning(
                        f"⚠ Event Hub connection string validation failed: {error}"
                    )
        else:
            logger.warning("No config.json found. Using default configuration.")

    except Exception as e:
        logger.error(f"Failed to initialize configuration: {e}")
        # Continue startup - configuration can be provided via API

    logger.info("Application startup completed")

    yield

    # Shutdown
    logger.info("Starting application shutdown")

    # Cancel any running background tasks
    from .shared.dependencies import _background_tasks

    for task_id, task in _background_tasks.items():
        if not task.done():
            logger.info(f"Cancelling background task: {task_id}")
            task.cancel()

    # Close DuckDB connection (do not delete DB file)
    try:
        from .db.duckdb_engine import close_duckdb

        close_duckdb()
        logger.info("DuckDB connection closed")
    except Exception as e:
        logger.error(f"Error closing DuckDB connection: {e}")

    logger.info("Application shutdown completed")


# Create FastAPI application
app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description=APP_DESCRIPTION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    contact={
        "name": "Retail Data Generator",
        "url": "https://github.com/your-org/retail-datagen",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    servers=[
        {"url": "http://localhost:8000", "description": "Development server"},
        {"url": "https://api.retaildatagen.com", "description": "Production server"},
    ],
)

# Configure CORS
# Allow CORS origins to be configured via env var ALLOWED_ORIGINS (comma-separated)
allowed_origins_env = os.getenv("ALLOWED_ORIGINS")
allowed_origins = (
    [o.strip() for o in allowed_origins_env.split(",") if o.strip()]
    if allowed_origins_env
    else [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://localhost:5173",
        "https://retaildatagen.com",
    ]
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Add request body fix middleware for historical data endpoint
# app.add_middleware(RequestBodyFixMiddleware)  # Temporarily disabled for debugging

# Mount static files and templates using repository-relative paths
_repo_root = Path(__file__).resolve().parents[2]
_static_dir = _repo_root / "static"
_templates_dir = _repo_root / "templates"

if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")
else:
    logger.warning(f"Static directory not found at {_static_dir}; skipping mount")

if _templates_dir.exists():
    templates = Jinja2Templates(directory=str(_templates_dir))
else:
    logger.warning(
        f"Templates directory not found at {_templates_dir}; some routes may be disabled"
    )
    # Fallback empty templates to avoid import-time errors in tests
    templates = Jinja2Templates(directory=str(_repo_root))


# ================================
# EXCEPTION HANDLERS
# ================================


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with consistent error format."""

    # Debug logging for 400 errors - capture request details for troubleshooting
    if exc.status_code == 400:
        try:
            body = await request.body()
            logger.error(f"400 Bad Request for {request.method} {request.url}")
            logger.error(f"Request headers: {dict(request.headers)}")
            logger.error(f"Request body: {body}")
            logger.error(f"Exception detail: {exc.detail}")
        except Exception as e:
            logger.error(f"Failed to log 400 error request details: {e}")

    error_response = ErrorResponse(
        error=f"HTTP_{exc.status_code}", message=exc.detail, timestamp=datetime.now(UTC)
    )
    content = error_response.model_dump()
    # Ensure datetime is serialized as string
    content["timestamp"] = (
        content["timestamp"].isoformat()
        if isinstance(content["timestamp"], datetime)
        else content["timestamp"]
    )

    return JSONResponse(status_code=exc.status_code, content=content)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors with detailed field information."""

    # Debug logging - capture request details for troubleshooting
    try:
        body = await request.body()
        logger.error(f"Validation error for {request.method} {request.url}")
        logger.error(f"Request headers: {dict(request.headers)}")
        logger.error(f"Request body: {body}")
        logger.error(f"Validation errors: {exc.errors()}")
    except Exception as e:
        logger.error(f"Failed to log request details: {e}")

    field_errors = []
    for error in exc.errors():
        field_errors.append(
            {
                "field": " -> ".join(str(loc) for loc in error["loc"]),
                "message": error["msg"],
                "type": error["type"],
                "input": error.get("input"),
            }
        )

    error_response = ValidationErrorResponse(
        error="VALIDATION_ERROR",
        message="Request validation failed",
        field_errors=field_errors,
        timestamp=datetime.now(UTC),
    )
    content = error_response.model_dump()
    # Ensure datetime is serialized as string
    content["timestamp"] = (
        content["timestamp"].isoformat()
        if isinstance(content["timestamp"], datetime)
        else content["timestamp"]
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content=content
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.error(f"Unhandled exception: {exc}")
    logger.error(traceback.format_exc())

    error_response = ErrorResponse(
        error="INTERNAL_SERVER_ERROR",
        message="An unexpected error occurred",
        details={"exception_type": type(exc).__name__},
        timestamp=datetime.now(UTC),
    )
    content = error_response.model_dump()
    # Ensure datetime is serialized as string
    content["timestamp"] = (
        content["timestamp"].isoformat()
        if isinstance(content["timestamp"], datetime)
        else content["timestamp"]
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=content
    )


# ================================
# MIDDLEWARE
# ================================


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """Log all requests and responses."""
    start_time = datetime.now(UTC)

    # Log request
    logger.info(f"{request.method} {request.url} - Client: {request.client.host}")

    # Process request
    response = await call_next(request)

    # Log response
    duration = (datetime.now(UTC) - start_time).total_seconds()
    logger.info(
        f"{request.method} {request.url} - "
        f"Status: {response.status_code} - "
        f"Duration: {duration:.3f}s"
    )

    return response


# ================================
# CORE ROUTES
# ================================


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def web_interface(request: Request):
    """Serve the web interface."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get(
    "/api",
    summary="Root endpoint",
    description="Welcome message and basic API information",
)
async def root():
    """Root endpoint with welcome message."""
    return {
        "message": f"Welcome to {APP_NAME}",
        "version": APP_VERSION,
        "docs_url": "/docs",
        "health_url": "/health",
        "timestamp": datetime.now(UTC),
    }


@app.get(
    "/health",
    response_model=HealthCheckResponse,
    summary="Health check",
    description="Comprehensive health check of all system components",
)
async def health_check():
    """Comprehensive health check endpoint."""
    checks = {}
    overall_status = "healthy"

    # Check configuration
    try:
        await get_config()
        checks["configuration"] = {
            "status": "healthy",
            "message": "Configuration loaded successfully",
        }
    except Exception as e:
        checks["configuration"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "unhealthy"

    # Check file system
    fs_check = await check_file_system_health()
    checks["file_system"] = fs_check
    if fs_check["status"] != "healthy":
        overall_status = "degraded"

    # Check Azure connection
    azure_check = await check_azure_connection()
    checks["azure_connection"] = azure_check
    if azure_check["status"] in ["error", "invalid_config"]:
        overall_status = "degraded"

    # Database health check - DuckDB singleton is used directly
    # No separate database manager; check via duckdb_engine module
    try:
        from retail_datagen.db.duckdb_engine import get_duckdb_conn

        conn = get_duckdb_conn()
        # Simple connectivity check
        conn.execute("SELECT 1").fetchone()
        checks["databases"] = {"status": "healthy"}
    except Exception as e:
        checks["databases"] = {"status": "unknown", "error": str(e)}
        logger.warning(f"Database health check failed: {e}")

    return HealthCheckResponse(
        status=overall_status,
        timestamp=datetime.now(UTC),
        version=APP_VERSION,
        checks=checks,
    )


@app.get(
    "/version",
    summary="Application version",
    description="Get current application version and build information",
)
async def get_version():
    """Get application version information."""
    return {"name": APP_NAME, "version": APP_VERSION, "timestamp": datetime.now(UTC)}


@app.get(
    "/metrics",
    summary="Prometheus metrics",
    description="Prometheus metrics endpoint for monitoring streaming health and performance",
    tags=["Monitoring"],
)
async def prometheus_metrics():
    """
    Prometheus metrics endpoint.

    Returns metrics in Prometheus exposition format for scraping.
    Includes streaming health, performance, errors, and circuit breaker state.
    """
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ================================
# CONFIGURATION ROUTES
# ================================


@app.get(
    "/api/config",
    summary="Get configuration",
    description="Get the current retail data generator configuration",
)
async def get_current_config(config: RetailConfig = Depends(get_config)):
    """Get the current configuration."""
    return config.model_dump()


@app.put(
    "/api/config",
    summary="Update configuration",
    description="Update the retail data generator configuration",
)
async def update_current_config(new_config: RetailConfig):
    """Update the current configuration."""
    try:
        await update_config(new_config)

        # Save to file
        config_path = Path("config.json")
        new_config.to_file(config_path)

        return {
            "message": "Configuration updated successfully",
            "timestamp": datetime.now(UTC),
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update configuration: {str(e)}",
        )


@app.post(
    "/api/config/reset",
    summary="Reset configuration",
    description="Reset configuration to default values",
)
async def reset_config():
    """Reset configuration to default values."""
    try:
        # Load default configuration
        default_config = RetailConfig(
            seed=42,
            volume={
                "stores": 250,
                "dcs": 12,
                "customers_per_day": 20000,
                "items_per_ticket_mean": 4.2,
                "online_orders_per_day": 2500,
                "marketing_impressions_per_day": 10000,
            },
            realtime={
                "emit_interval_ms": 500,
                "burst": 100,
                "azure_connection_string": "",
                "max_batch_size": 256,
                "batch_timeout_ms": 1000,
                "retry_attempts": 3,
                "backoff_multiplier": 2.0,
                "circuit_breaker_enabled": True,
                "monitoring_interval": 30,
                "max_buffer_size": 10000,
                "enable_dead_letter_queue": True,
            },
            paths={
                "dict": "data/dictionaries",
                "master": "data/master",
                "facts": "data/facts",
            },
            stream={"hub": "retail-events"},
        )

        await update_config(default_config)

        # Save to file
        config_path = Path("config.json")
        default_config.to_file(config_path)

        return {
            "message": "Configuration reset to defaults",
            "timestamp": datetime.now(UTC),
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reset configuration: {str(e)}",
        )


@app.post(
    "/api/config/validate",
    summary="Validate configuration",
    description="Validate a configuration without applying it",
)
async def validate_config(config_data: RetailConfig):
    """Validate a configuration without applying it."""
    try:
        # The Pydantic model validation happens automatically
        return {
            "valid": True,
            "message": "Configuration is valid",
            "timestamp": datetime.now(UTC),
        }
    except Exception as e:
        return {
            "valid": False,
            "message": f"Configuration validation failed: {str(e)}",
            "timestamp": datetime.now(UTC),
        }


# ================================
# DATABASE STATUS ROUTES
# ================================


@app.get(
    "/api/database/status",
    summary="Get database status",
    description="Get comprehensive database status and statistics",
    tags=["Database"],
)
async def database_status():
    """
    Get database status and statistics.

    Returns information about the DuckDB database including:
    - Health status
    - File existence
    - Connection state
    """
    from retail_datagen.db.duckdb_engine import get_duckdb_conn, get_duckdb_path

    try:
        db_path = get_duckdb_path()
        conn = get_duckdb_conn()
        # Simple connectivity check
        conn.execute("SELECT 1").fetchone()
        return {
            "status": "healthy",
            "path": str(db_path),
            "exists": db_path.exists(),
            "size_bytes": db_path.stat().st_size if db_path.exists() else 0,
            "timestamp": datetime.now(UTC),
        }
    except Exception as e:
        logger.error(f"Error getting database status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get database status: {str(e)}",
        )


# ================================
# TASK STATUS ROUTES
# ================================


@app.get(
    "/api/tasks/{task_id}/status",
    summary="Get task status",
    description="Get status of any async task by its ID",
)
async def get_task_status_endpoint(task_id: str):
    """Get status of any async task by its ID"""
    from .shared.dependencies import get_task_status

    task_status = get_task_status(task_id)
    if not task_status:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found",
        )

    return {
        "task_id": task_id,
        "status": task_status.status,
        "progress": task_status.progress,
        "message": task_status.message,
        "table_progress": task_status.table_progress,
        "current_table": task_status.current_table,
        "tables_completed": task_status.tables_completed,
        "tables_failed": task_status.tables_failed,
        "tables_in_progress": task_status.tables_in_progress,
        "tables_remaining": task_status.tables_remaining,
        "estimated_seconds_remaining": task_status.estimated_seconds_remaining,
        "progress_rate": task_status.progress_rate,
        "last_update_timestamp": task_status.last_update_timestamp,
        # Include sequence to support UI de-dup/out-of-order handling
        "sequence": task_status.sequence,
        # Include hourly progress fields for richer status in generic endpoint
        "current_day": getattr(task_status, "current_day", None),
        "current_hour": getattr(task_status, "current_hour", None),
        "hourly_progress": getattr(task_status, "hourly_progress", None),
        "total_hours_completed": getattr(task_status, "total_hours_completed", None),
        "table_counts": task_status.table_counts,
        "error_message": task_status.error,
        "estimated_completion": None,  # Not currently tracked in TaskStatus model
    }


@app.get(
    "/api/tasks/active",
    summary="Get active tasks",
    description="Get all currently running tasks",
)
async def get_active_tasks():
    """Get all currently running tasks"""
    from .shared.dependencies import _task_status

    active_tasks = []
    for task_id, task_status in _task_status.items():
        if task_status.status in ["running", "pending"]:
            active_tasks.append(
                {
                    "task_id": task_id,
                    "status": task_status.status,
                    "progress": task_status.progress,
                    "message": task_status.message,
                    "current_table": task_status.current_table,
                }
            )

    return {"active_tasks": active_tasks, "count": len(active_tasks)}


# ================================
# INCLUDE ROUTERS
# ================================

app.include_router(generators_router, prefix="/api", tags=["Data Generation"])

app.include_router(streaming_router, prefix="/api", tags=["Real-Time Streaming"])

app.include_router(export_router, tags=["Data Export"])


# ================================
# DEVELOPMENT SERVER
# ================================


def run_dev_server():
    """Run the development server."""
    # Import here to avoid hard dependency during module import in test envs
    import uvicorn

    uvicorn.run(
        "retail_datagen.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )


if __name__ == "__main__":
    run_dev_server()
