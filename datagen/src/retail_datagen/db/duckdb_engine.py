"""
DuckDB connection manager and helpers.

Provides a singleton DuckDB connection for the generator and simple
table creation/insert utilities optimized for batch loads.
"""

from __future__ import annotations

import threading
from pathlib import Path
import os
from typing import Iterable

import duckdb
import pandas as pd
import numpy as np

_lock = threading.Lock()
_conn: duckdb.DuckDBPyConnection | None = None


def get_duckdb_path() -> Path:
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "retail.duckdb"


def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is not None:
        return _conn
    with _lock:
        if _conn is None:
            _conn = duckdb.connect(str(get_duckdb_path()))
            # Pragmas suitable for fast local writes (best-effort; tolerate older DuckDB versions)
            try:
                threads = os.cpu_count() or 2
                _conn.execute(f"PRAGMA threads={threads}")
            except Exception:
                # Older DuckDB may not support PRAGMA threads
                pass
            try:
                _conn.execute("PRAGMA temp_directory=':memory:'")
            except Exception:
                pass
    return _conn


def reset_duckdb() -> None:
    """Close the global DuckDB connection (if any) and delete the DB file.

    Subsequent calls to get_duckdb_conn() will recreate a fresh database.
    """
    global _conn
    try:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
    finally:
        path = get_duckdb_path()
        try:
            if path.exists():
                path.unlink()
        except Exception:
            # Ignore delete failures; caller can handle
            pass


def close_duckdb() -> None:
    """Close the global DuckDB connection without deleting the DB file."""
    global _conn
    try:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
    except Exception:
        pass


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    try:
        conn.execute(f"SELECT * FROM {table} LIMIT 0")
        return True
    except Exception:
        return False


def _current_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    try:
        cur = conn.execute(f"PRAGMA table_info('{table}')")
        rows = cur.fetchall()
        return {str(r[1]).lower() for r in rows}  # name at index 1
    except Exception:
        return set()


def _duck_type_from_series(s: pd.Series) -> str:
    dt = s.dtype
    if pd.api.types.is_integer_dtype(dt):
        # Use BIGINT to be safe across large ranges
        return "BIGINT"
    if pd.api.types.is_float_dtype(dt):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(dt):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dt):
        return "TIMESTAMP"
    # Fallback to VARCHAR for objects/strings and unknowns
    return "VARCHAR"


def _ensure_columns(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    """Ensure all DataFrame columns exist in the DuckDB table.

    Adds missing columns with inferred types (conservative defaults) so that
    evolving schemas (e.g., later batches adding 'Source' or 'Balance') do not fail.
    """
    existing = _current_columns(conn, table)
    for col in df.columns:
        if str(col).lower() not in existing:
            duck_type = _duck_type_from_series(df[col])
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {duck_type}")
            except Exception:
                # Best-effort; if it races or fails, proceed and let INSERT surface issues
                pass
    # No need to drop extra table columns; INSERT will only specify df columns


def insert_dataframe(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    # Register DataFrame and create table with actual data schema when first seen
    conn.register("_tmp_df", df)
    try:
        if not _table_exists(conn, table):
            # Create with data to ensure correct column types
            conn.execute(f"CREATE TABLE {table} AS SELECT * FROM _tmp_df")
        else:
            # Align columns by name to avoid positional mismatches
            # Ensure any new columns are added before INSERT
            _ensure_columns(conn, table, df)
            cols = [c for c in df.columns]
            col_list = ", ".join(cols)
            conn.execute(
                f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM _tmp_df"
            )
    finally:
        conn.unregister("_tmp_df")
    return len(df)


def insert_records(conn: duckdb.DuckDBPyConnection, table: str, records: Iterable[dict]) -> int:
    df = pd.DataFrame.from_records(list(records))
    return insert_dataframe(conn, table, df)
