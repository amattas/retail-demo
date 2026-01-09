"""
FastAPI router for data access, table listing, and preview endpoints.

This module provides REST API endpoints for listing tables, previewing data,
and dashboard counts.
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status

from ...api.models import TableListResponse, TablePreviewResponse
from ...config.models import RetailConfig
from ...shared.dependencies import get_config
from .common import (
    ALL_TABLE_MAP,
    DUCK_FACT_MAP,
    DUCK_MASTER_MAP,
    FACT_TABLES,
    MASTER_TABLES,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ================================
# TABLE LISTING ENDPOINTS
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
        for logical, duck in DUCK_FACT_MAP.items():
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {duck}").fetchone()[0]
                if int(cnt) > 0:
                    tables_with_data.append(logical)
            except Exception:
                pass
        return TableListResponse(
            tables=tables_with_data, count=len(tables_with_data)
        )
    except Exception as e:
        logger.warning(f"Falling back to empty fact table list due to error: {e}")
        return TableListResponse(tables=[], count=0)


# ================================
# TABLE SUMMARY ENDPOINTS
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
        duck = ALL_TABLE_MAP.get(table_name, table_name)
        total = conn.execute(f"SELECT COUNT(*) FROM {duck}").fetchone()[0]
        cols = [
            d[0]
            for d in (conn.execute(f"SELECT * FROM {duck} LIMIT 0").description or [])
        ]
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
    description=(
        "Get record counts and metadata for any table (master or fact) via unified path"
    ),
)
async def get_table_summary_unified(table_name: str):
    # Reuse the existing implementation
    return await get_table_summary(table_name)


# ================================
# TABLE PREVIEW ENDPOINTS
# ================================


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
        duck = ALL_TABLE_MAP.get(table_name, table_name)
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
        duck = DUCK_MASTER_MAP.get(table_name, table_name)
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
    description=(
        "Alias used by UI to preview fact tables; returns empty preview when no data"
    ),
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
        duck = DUCK_FACT_MAP.get(table_name, table_name)
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {duck}").fetchone()[0]
        if int(total_rows) == 0:
            cols = [
                d[0]
                for d in (
                    conn.execute(f"SELECT * FROM {duck} LIMIT 0").description or []
                )
            ]
            return TablePreviewResponse(
                table_name=table_name,
                columns=cols,
                row_count=0,
                preview_rows=[],
            )
        cur = conn.execute(
            f"SELECT * FROM {duck} ORDER BY event_ts DESC LIMIT {int(limit)}"
        )
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
        cur = conn.execute(
            f"SELECT * FROM {duck} ORDER BY event_ts DESC LIMIT {int(limit)}"
        )
        cols = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
        preview_rows = [
            {cols[i]: rows[j][i] for i in range(len(cols))} for j in range(len(rows))
        ]
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
    """Get live table counts for dashboard (queries unified retail DB directly)."""

    master_counts: dict[str, int] = {}
    fact_counts: dict[str, int] = {}
    try:
        from retail_datagen.db.duckdb_engine import get_duckdb_conn

        conn = get_duckdb_conn()
        for k, v in DUCK_MASTER_MAP.items():
            try:
                master_counts[k] = int(
                    conn.execute(f"SELECT COUNT(*) FROM {v}").fetchone()[0]
                )
            except Exception:
                master_counts[k] = 0
        for k, v in DUCK_FACT_MAP.items():
            try:
                fact_counts[k] = int(
                    conn.execute(f"SELECT COUNT(*) FROM {v}").fetchone()[0]
                )
            except Exception:
                fact_counts[k] = 0
    except Exception as e:
        logger.warning(f"Failed to read table counts from DuckDB: {e}")

    return {
        "master_tables": master_counts,
        "fact_tables": fact_counts,
        "last_updated": datetime.utcnow().isoformat() + "Z",
    }
