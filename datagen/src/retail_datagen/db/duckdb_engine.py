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
            # Ensure streaming outbox exists for outbox-driven streaming
            try:
                _ensure_outbox_table(_conn)
            except Exception:
                # Non-fatal if creation fails here; callers may retry
                pass
    return _conn


def reset_duckdb() -> None:
    """Close the global DuckDB connection (if any) and delete the DB file.

    Subsequent calls to get_duckdb_conn() will recreate a fresh database.
    """
    global _conn
    with _lock:
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
    with _lock:
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


# ================================
# Outbox helpers (DuckDB)
# ================================

def _ensure_outbox_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create a simple outbox table if it does not exist.

    Uses VARCHAR for JSON payload to keep compatibility with pandas-based inserts.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS streaming_outbox (
            outbox_id BIGINT PRIMARY KEY,
            event_ts TIMESTAMP,
            message_type VARCHAR,
            payload VARCHAR,          -- JSON text
            status VARCHAR DEFAULT 'pending',
            attempts INT DEFAULT 0,
            last_attempt_ts TIMESTAMP,
            sent_ts TIMESTAMP,
            partition_key VARCHAR,
            trace_id VARCHAR,
            correlation_id VARCHAR,
            schema_version VARCHAR DEFAULT '1.0',
            source VARCHAR DEFAULT 'retail-datagen'
        );
        """
    )
    # Backward compatibility: add any missing columns if the table pre-existed
    try:
        existing = _current_columns(conn, "streaming_outbox")

        def _ensure(col: str, type_sql: str, default_sql: str | None = None) -> None:
            if col not in existing:
                try:
                    if default_sql is not None:
                        conn.execute(
                            f"ALTER TABLE streaming_outbox ADD COLUMN {col} {type_sql} DEFAULT {default_sql}"
                        )
                    else:
                        conn.execute(
                            f"ALTER TABLE streaming_outbox ADD COLUMN {col} {type_sql}"
                        )
                    existing.add(col)
                except Exception:
                    pass

        _ensure("status", "VARCHAR", "'pending'")
        _ensure("attempts", "INT", "0")
        _ensure("last_attempt_ts", "TIMESTAMP", None)
        _ensure("sent_ts", "TIMESTAMP", None)
        _ensure("partition_key", "VARCHAR", None)
        _ensure("payload", "VARCHAR", None)
        _ensure("trace_id", "VARCHAR", None)
        _ensure("correlation_id", "VARCHAR", None)
        _ensure("schema_version", "VARCHAR", "'1.0'")
        _ensure("source", "VARCHAR", "'retail-datagen'")
        _ensure("outbox_id", "BIGINT", None)
        _ensure("event_ts", "TIMESTAMP", None)
        _ensure("message_type", "VARCHAR", None)
    except Exception:
        pass
    # Lightweight indexes for draining
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_outbox_status_ts ON streaming_outbox(status, event_ts)"
        )
    except Exception:
        pass


def outbox_counts(conn: duckdb.DuckDBPyConnection) -> dict:
    """Return counts by status for streaming_outbox."""
    _ensure_outbox_table(conn)
    rows = conn.execute(
        "SELECT status, COUNT(*) FROM streaming_outbox GROUP BY status"
    ).fetchall()
    result = {str(r[0]): int(r[1]) for r in rows}
    result.setdefault("pending", 0)
    result.setdefault("processing", 0)
    result.setdefault("sent", 0)
    # Oldest pending for UI
    row = conn.execute(
        "SELECT MIN(event_ts) FROM streaming_outbox WHERE status='pending'"
    ).fetchone()
    result["oldest_pending_ts"] = row[0] if row else None
    row2 = conn.execute(
        "SELECT MAX(sent_ts) FROM streaming_outbox WHERE status='sent'"
    ).fetchone()
    result["latest_sent_ts"] = row2[0] if row2 else None
    return result


def outbox_has_pending(conn: duckdb.DuckDBPyConnection) -> bool:
    _ensure_outbox_table(conn)
    row = conn.execute(
        "SELECT 1 FROM streaming_outbox WHERE status='pending' LIMIT 1"
    ).fetchone()
    return bool(row)


def outbox_lease_next(conn: duckdb.DuckDBPyConnection) -> dict | None:
    """Lease the next pending outbox row (oldest-first).

    Returns the row as a dict with keys: outbox_id, event_ts, message_type, payload, partition_key, trace_id.
    """
    _ensure_outbox_table(conn)
    # Select candidate id
    row = conn.execute(
        "SELECT outbox_id FROM streaming_outbox WHERE status='pending' ORDER BY event_ts, outbox_id LIMIT 1"
    ).fetchone()
    if not row:
        return None
    oid = int(row[0])
    # Try to mark as processing with guard
    updated = conn.execute(
        "UPDATE streaming_outbox SET status='processing', last_attempt_ts=now(), attempts=attempts+1 WHERE outbox_id=? AND status='pending'",
        [oid],
    ).rowcount
    if updated != 1:
        return None
    # Fetch full row
    cur = conn.execute(
        "SELECT outbox_id, event_ts, message_type, payload, partition_key, trace_id FROM streaming_outbox WHERE outbox_id=?",
        [oid],
    )
    r = cur.fetchone()
    if not r:
        return None
    cols = [d[0] for d in (cur.description or [])]
    return {cols[i]: r[i] for i in range(len(cols))}


def outbox_ack_sent(conn: duckdb.DuckDBPyConnection, outbox_id: int) -> None:
    conn.execute(
        "UPDATE streaming_outbox SET status='sent', sent_ts=now() WHERE outbox_id=?",
        [outbox_id],
    )


def outbox_nack_retry(conn: duckdb.DuckDBPyConnection, outbox_id: int) -> None:
    """Return a processing row to pending for retry."""
    conn.execute(
        "UPDATE streaming_outbox SET status='pending' WHERE outbox_id=? AND status='processing'",
        [outbox_id],
    )


def outbox_insert_records(conn: duckdb.DuckDBPyConnection, records: Iterable[dict]) -> int:
    """Insert outbox rows, assigning monotonically increasing outbox_id values.

    Ensures compatibility with DuckDB versions lacking IDENTITY columns by
    generating outbox_id in Python.
    """
    _ensure_outbox_table(conn)
    try:
        row = conn.execute("SELECT COALESCE(MAX(outbox_id), 0) FROM streaming_outbox").fetchone()
        max_id = int(row[0] or 0)
    except Exception:
        max_id = 0

    prepared: list[dict] = []
    next_id = max_id
    for rec in records:
        if rec is None:
            continue
        rec_copy = dict(rec)
        if rec_copy.get("outbox_id") is None:
            next_id += 1
            rec_copy["outbox_id"] = next_id
        prepared.append(rec_copy)

    return insert_records(conn, "streaming_outbox", prepared)
