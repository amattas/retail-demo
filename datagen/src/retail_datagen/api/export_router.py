"""
FastAPI router for data export endpoints.

This module provides REST API endpoints for exporting master and fact tables
from the DuckDB database to CSV or Parquet files with comprehensive progress tracking.
"""

import logging
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, status

from ..config.models import RetailConfig
from ..services.export_service import ExportService
from ..shared.dependencies import (
    create_background_task,
    get_config,
    get_task_status,
    update_task_progress,
)
from .export_models import (
    ExportOperationResult,
    ExportRequest,
    ExportStatusResponse,
    FactExportRequest,
    validate_table_names,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/export", tags=["Export"])


# ================================
# EXPORT ENDPOINTS
# ================================


@router.post(
    "/master",
    response_model=ExportOperationResult,
    summary="Export master data to files",
    description=(
        "Export all master dimension tables from the database to files. "
        "Supports CSV and Parquet formats. Returns a task ID for tracking progress."
    ),
)
async def export_master_data(
    request: ExportRequest,
    config: RetailConfig = Depends(get_config),
):
    """
    Export master dimension tables to files.

    Args:
        request: Export configuration (format, tables)
        config: Application configuration

    Returns:
        ExportOperationResult with task_id for tracking progress

    Example:
        POST /api/export/master
        {
            "format": "parquet",
            "tables": ["stores", "customers", "products_master"]
        }
    """
    logger.info(
        f"Master export request received: format={request.format}, tables={request.tables}"
    )
    if request.format.lower() != "parquet":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only 'parquet' export is supported")

    # Validate table names
    try:
        tables_to_export = validate_table_names(request.tables, "master")
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    # Create unique task ID
    task_id = f"export_master_{uuid4().hex[:8]}"
    started_at = datetime.now()

    # Define background export task
    async def export_task():
        """Background task for master data export."""
        try:
            logger.info(f"Starting master export task {task_id}")

            # Initialize export service
            base_dir = Path(config.paths.master).parent  # Get base "data" directory
            service = ExportService(base_dir=base_dir)

            # Track progress
            completed_tables = []
            len(tables_to_export)
            total_files = 0
            total_rows = 0

            # Progress callback for export service
            def progress_callback(message: str, current: int, total: int):
                """Update task progress during export."""
                progress = current / total if total > 0 else 0.0

                # Determine current and remaining tables
                current_table = (
                    tables_to_export[current - 1]
                    if current > 0 and current <= total
                    else None
                )
                tables_remaining = tables_to_export[current:] if current < total else []

                update_task_progress(
                    task_id,
                    progress,
                    message,
                    current_table=current_table,
                    tables_completed=completed_tables,
                    tables_remaining=tables_remaining,
                )

            # Export master tables from DuckDB
            logger.debug(f"Exporting {len(tables_to_export)} master tables")
            result = await service.export_master_tables(
                None,
                format=request.format,
                progress_callback=progress_callback,
            )

            # Calculate results
            total_files = len(result)
            # Convert paths to strings (resolve both to absolute for relative_to to work)
            files_written = [
                str(path.resolve().relative_to(base_dir.resolve()))
                for path in result.values()
            ]

            # Row counting skipped for Parquet to avoid heavy reads
            total_rows = None

            # Update final progress
            update_task_progress(
                task_id,
                1.0,
                f"Master export completed: {total_files} files written",
                tables_completed=tables_to_export,
                tables_remaining=[],
            )

            logger.info(f"Master export task {task_id} completed successfully")

            # Optional: upload to Azure Storage if configured
            uploaded_summary = None
            try:
                if getattr(config, "storage", None) and config.storage.account_uri and config.storage.account_key:
                    from ..services.azure_uploader import upload_paths_to_blob
                    # Build absolute paths for upload
                    abs_paths = [base_dir / f for f in files_written]
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    uploaded_summary = upload_paths_to_blob(
                        config.storage.account_uri,
                        config.storage.account_key,
                        abs_paths,
                        default_container="retail",
                        blob_prefix=f"datagen/export/master/{ts}",
                    )
                    logger.info(
                        f"Uploaded master export: {uploaded_summary.get('uploaded')} files to "
                        f"{uploaded_summary.get('container')}/{uploaded_summary.get('prefix')}"
                    )
            except ImportError as e:
                logger.warning(str(e))
            except Exception as e:
                logger.error(f"Upload failed: {e}")

            return {
                "files_written": files_written,
                "total_files": total_files,
                "total_rows": None,
                "output_directory": str(base_dir / "export"),
                "uploaded": uploaded_summary,
            }

        except Exception as e:
            logger.error(f"Master export task {task_id} failed: {e}", exc_info=True)
            update_task_progress(
                task_id,
                0.0,
                f"Export failed: {str(e)}",
            )
            raise

    # Create background task
    create_background_task(
        task_id,
        export_task(),
        f"Export master data: {request.format} format",
    )

    return ExportOperationResult(
        success=True,
        message=f"Master data export started ({request.format} format)",
        task_id=task_id,
        started_at=started_at,
    )


@router.post(
    "/facts",
    response_model=ExportOperationResult,
    summary="Export fact data to files",
    description=(
        "Export fact tables from the database to partitioned files. "
        "Supports optional date filtering. Returns a task ID for tracking progress."
    ),
)
async def export_fact_data(
    request: FactExportRequest,
    config: RetailConfig = Depends(get_config),
):
    """
    Export fact tables to partitioned files.

    Args:
        request: Export configuration (format, tables, date range)
        config: Application configuration

    Returns:
        ExportOperationResult with task_id for tracking progress

    Example:
        POST /api/export/facts
        {
            "format": "parquet",
            "tables": ["receipts", "receipt_lines"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-31"
        }
    """
    logger.info(
        f"Fact export request received: format={request.format}, "
        f"tables={request.tables}, date_range={request.start_date} to {request.end_date}"
    )
    if request.format.lower() != "parquet":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only 'parquet' export is supported")

    # Validate table names
    try:
        tables_to_export = validate_table_names(request.tables, "facts")
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    # Create unique task ID
    task_id = f"export_facts_{uuid4().hex[:8]}"
    started_at = datetime.now()

    # Define background export task
    async def export_task():
        """Background task for fact data export with chunking for large date ranges."""
        try:
            logger.info(f"Starting fact export task {task_id}")
            from ..services import duckdb_reader as db_reader

            # Initialize export service
            base_dir = Path(config.paths.facts).parent  # Get base "data" directory
            service = ExportService(base_dir=base_dir)

            # Track progress
            total_files = 0
            total_rows = 0
            all_files = []

            # Determine date range (query database if not provided)
            start_date = request.start_date
            end_date = request.end_date

            if start_date is None or end_date is None:
                logger.info("No date range provided, querying DuckDB for full range")
                update_task_progress(task_id, 0.0, "Determining date range...")

                # Get date ranges for all tables from DuckDB
                date_ranges = db_reader.get_all_fact_table_date_ranges()

                # Find overall min/max across all tables
                all_starts = [
                    start for start, end in date_ranges.values() if start is not None
                ]
                all_ends = [end for start, end in date_ranges.values() if end is not None]

                if not all_starts or not all_ends:
                    raise ValueError("No data found in fact tables")

                start_date = min(all_starts).date()
                end_date = max(all_ends).date()

                logger.info(f"Determined date range: {start_date} to {end_date}")

            # Calculate chunks (7 days each to keep memory manageable)
            from datetime import timedelta

            chunk_size_days = 7
            chunks = []
            current_start = start_date

            while current_start <= end_date:
                chunk_end = min(
                    current_start + timedelta(days=chunk_size_days - 1), end_date
                )
                chunks.append((current_start, chunk_end))
                current_start = chunk_end + timedelta(days=1)

            total_chunks = len(chunks)
            logger.info(
                f"Split export into {total_chunks} chunks of {chunk_size_days} days each"
            )

            # Export each chunk
            for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
                chunk_progress = (chunk_idx - 1) / total_chunks
                days_in_chunk = (chunk_end - chunk_start).days + 1

                update_task_progress(
                    task_id,
                    chunk_progress,
                    f"Exporting chunk {chunk_idx}/{total_chunks}: {chunk_start} to {chunk_end} ({days_in_chunk} days)",
                )

                logger.info(
                    f"Exporting chunk {chunk_idx}/{total_chunks}: {chunk_start} to {chunk_end}"
                )

                # Export this chunk from DuckDB
                result = await service.export_fact_tables(
                    None,
                    format=request.format,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    progress_callback=None,
                )

                # Accumulate results
                for table_name, partition_files in result.items():
                    total_files += len(partition_files)
                    all_files.extend(
                        [
                            str(path.resolve().relative_to(base_dir.resolve()))
                            for path in partition_files
                        ]
                    )

                logger.info(
                    f"Chunk {chunk_idx}/{total_chunks} complete: {len(result)} tables, {sum(len(files) for files in result.values())} files"
                )

            # Row counting disabled for Parquet exports
            total_rows = None

            # Update final progress
            update_task_progress(
                task_id,
                1.0,
                f"Export completed: {total_files} files written across {total_chunks} chunks ({start_date} to {end_date})",
                tables_completed=tables_to_export,
                tables_remaining=[],
            )

            logger.info(f"Fact export task {task_id} completed successfully")

            # Optional: upload to Azure Storage if configured
            uploaded_summary = None
            try:
                if getattr(config, "storage", None) and config.storage.account_uri and config.storage.account_key:
                    from ..services.azure_uploader import upload_paths_to_blob
                    abs_paths = [base_dir / f for f in all_files]
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    uploaded_summary = upload_paths_to_blob(
                        config.storage.account_uri,
                        config.storage.account_key,
                        abs_paths,
                        default_container="retail",
                        blob_prefix=f"datagen/export/facts/{ts}",
                    )
                    logger.info(
                        f"Uploaded fact export: {uploaded_summary.get('uploaded')} files to "
                        f"{uploaded_summary.get('container')}/{uploaded_summary.get('prefix')}"
                    )
            except ImportError as e:
                logger.warning(str(e))
            except Exception as e:
                logger.error(f"Upload failed: {e}")

            return {
                "files_written": all_files,
                "total_files": total_files,
                "total_rows": total_rows,
                "output_directory": str(base_dir / "export"),
                "date_range": f"{start_date} to {end_date}",
                "chunks_exported": total_chunks,
                "uploaded": uploaded_summary,
            }

        except Exception as e:
            logger.error(f"Fact export task {task_id} failed: {e}", exc_info=True)
            update_task_progress(
                task_id,
                0.0,
                f"Export failed: {str(e)}",
            )
            raise

    # Create background task
    create_background_task(
        task_id,
        export_task(),
        f"Export fact data: {request.format} format",
    )

    return ExportOperationResult(
        success=True,
        message=f"Fact data export started ({request.format} format)",
        task_id=task_id,
        started_at=started_at,
    )


@router.get(
    "/status/{task_id}",
    response_model=ExportStatusResponse,
    summary="Get export operation status",
    description=(
        "Get the current status and progress of an export operation. "
        "Returns detailed progress information including tables completed, remaining, and current."
    ),
)
async def get_export_status(task_id: str):
    """
    Get the status of an export operation.

    Args:
        task_id: Unique task identifier from export request

    Returns:
        ExportStatusResponse with current status and progress

    Raises:
        HTTPException: If task_id is not found

    Example:
        GET /api/export/status/export_master_abc123
    """
    # Get task status from dependency system
    task_status = get_task_status(task_id)

    if not task_status:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Export task {task_id} not found",
        )

    # Map internal task status to export status
    status_mapping = {
        "pending": "pending",
        "running": "running",
        "completed": "completed",
        "failed": "failed",
        "cancelled": "failed",  # Map cancelled to failed
    }

    export_status = status_mapping.get(task_status.status, "pending")

    # Extract result data if available
    result = task_status.result if hasattr(task_status, "result") else None
    files_written = None
    total_files = None
    total_rows = None
    output_directory = None

    if result and isinstance(result, dict):
        files_written = result.get("files_written")
        total_files = result.get("total_files")
        total_rows = result.get("total_rows")
        output_directory = result.get("output_directory")

    # Build response
    response = ExportStatusResponse(
        task_id=task_id,
        status=export_status,
        progress=task_status.progress,
        message=task_status.message,
        tables_completed=task_status.tables_completed or [],
        tables_remaining=task_status.tables_remaining or [],
        current_table=task_status.current_table,
        files_written=files_written,
        total_files=total_files,
        total_rows=total_rows,
        output_directory=output_directory,
        error_message=task_status.error,
        started_at=task_status.started_at,
        completed_at=task_status.completed_at,
    )

    return response


# ================================
# HELPER ENDPOINT
# ================================


@router.get(
    "/formats",
    summary="Get supported export formats",
    description="Get list of supported export formats and their descriptions",
)
async def get_export_formats():
    """
    Get supported export formats.

    Returns:
        List of supported formats with descriptions

    Example:
        GET /api/export/formats
    """
    return {
        "formats": [
            {
                "name": "parquet",
                "description": "Apache Parquet (columnar, compressed, efficient)",
                "extension": ".parquet",
                "compression": "snappy",
            }
        ]
    }
