"""
DuckDB connection manager and helpers.

Provides a singleton DuckDB connection for the generator and simple
table creation/insert utilities optimized for batch loads.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_conn: duckdb.DuckDBPyConnection | None = None

# Allowlist of valid table names to prevent SQL injection.
# These are the only table names that can be used in dynamic SQL queries.
# Any function that accepts a table name parameter will validate against this list.
# Attempting to use an unlisted table name will raise ValueError.
#
# To add a new table:
# 1. Add the table name to this frozenset
# 2. Ensure the table follows naming conventions (dim_* for dimensions,
#    fact_* for facts)
ALLOWED_TABLES: frozenset[str] = frozenset(
    {
        # Dimension tables
        "dim_geographies",
        "dim_stores",
        "dim_distribution_centers",
        "dim_trucks",
        "dim_customers",
        "dim_products",
        # Fact tables (current)
        "fact_dc_inventory_txn",
        "fact_truck_moves",
        "fact_truck_inventory",
        "fact_store_inventory_txn",
        "fact_receipts",
        "fact_receipt_lines",
        "fact_foot_traffic",
        "fact_ble_pings",
        "fact_marketing",
        "fact_online_order_headers",
        "fact_online_order_lines",
        # Fact tables (planned - see GitHub issues #7-#13)
        "fact_payments",
        "fact_stockouts",
        "fact_reorders",
        "fact_promotions",
        "fact_promo_lines",
        "fact_store_ops",
        "fact_customer_zone_changes",
        # System tables
        "streaming_outbox",
    }
)

# Internal temporary table name - uses double underscore prefix to avoid
# collision with user-provided table names
_INTERNAL_TMP_TABLE = "__rdg_tmp_df__"

# Internal staging table for pending shipments (excluded from exports/dashboard)
# Stores truck shipments scheduled beyond the generation end date
_STAGING_PENDING_SHIPMENTS = "_staging_pending_shipments"


def validate_table_name(table: str) -> str:
    """Validate that a table name is in the allowlist to prevent SQL injection.

    Args:
        table: The table name to validate

    Returns:
        The validated table name

    Raises:
        ValueError: If the table name is not in the allowlist
    """
    if table not in ALLOWED_TABLES:
        raise ValueError(
            f"Invalid table name: '{table}'. "
            f"Table must be one of: {sorted(ALLOWED_TABLES)}"
        )
    return table


# Pattern for valid SQL identifiers: alphanumeric and underscore,
# not starting with digit
_VALID_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_column_name(column: str) -> str:
    """Validate that a column name is a safe SQL identifier to prevent injection.

    Column names must:
    - Start with a letter or underscore
    - Contain only alphanumeric characters and underscores
    - Not be empty

    Args:
        column: The column name to validate

    Returns:
        The validated column name

    Raises:
        ValueError: If the column name contains invalid characters
    """
    col_str = str(column)
    if not col_str:
        raise ValueError("Column name cannot be empty")
    if not _VALID_IDENTIFIER_PATTERN.match(col_str):
        raise ValueError(
            f"Invalid column name: '{col_str}'. "
            "Column names must start with a letter or underscore and contain "
            "only alphanumeric characters and underscores."
        )
    return col_str


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
            new_conn = duckdb.connect(str(get_duckdb_path()))
            try:
                # Pragmas for fast local writes (best-effort; tolerate older DuckDB)
                try:
                    threads = os.cpu_count() or 2
                    new_conn.execute(f"PRAGMA threads={threads}")
                except Exception as e:
                    # Older DuckDB may not support PRAGMA threads
                    logger.debug(f"Failed to set PRAGMA threads={threads}: {e}")
                try:
                    new_conn.execute("PRAGMA temp_directory=':memory:'")
                except Exception as e:
                    logger.debug(f"Failed to set PRAGMA temp_directory: {e}")
                # Ensure streaming outbox exists for outbox-driven streaming
                try:
                    _ensure_outbox_table(new_conn)
                except Exception as e:
                    # Non-fatal if creation fails here; callers may retry
                    logger.warning(f"Failed to create outbox table during init: {e}")
                # Ensure fact_store_ops table exists with proper schema
                try:
                    ensure_fact_store_ops_table(new_conn)
                except Exception as e:
                    # Non-fatal if creation fails here; subsequent inserts will fail
                    # if table doesn't exist, but this allows partial initialization
                    logger.warning(
                        f"Failed to create fact_store_ops table during init: {e}"
                    )
                # Ensure fact_payments table exists with proper schema
                try:
                    ensure_fact_payments_table(new_conn)
                except Exception as e:
                    logger.warning(
                        f"Failed to create fact_payments table during init: {e}"
                    )
                # Ensure fact_receipt_lines table exists with proper schema
                # (promo_code must be VARCHAR, not INT)
                try:
                    ensure_fact_receipt_lines_table(new_conn)
                except Exception as e:
                    logger.warning(
                        f"Failed to create fact_receipt_lines table during init: {e}"
                    )
                # Only assign to global after successful initialization
                _conn = new_conn
            except Exception as e:
                # Clean up connection if any critical initialization fails
                logger.error(f"DuckDB connection initialization failed: {e}")
                try:
                    new_conn.close()
                except Exception as close_error:
                    # Log cleanup failure but don't mask the original exception
                    logger.warning(
                        f"Failed to close connection during cleanup: {close_error}"
                    )
                raise
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
                except Exception as e:
                    logger.warning(f"Failed to close DuckDB connection: {e}")
                _conn = None
        finally:
            path = get_duckdb_path()
            try:
                if path.exists():
                    path.unlink()
            except (OSError, PermissionError) as e:
                # Ignore delete failures; caller can handle
                logger.warning(f"Failed to delete DuckDB file at {path}: {e}")


def close_duckdb() -> None:
    """Close the global DuckDB connection without deleting the DB file."""
    global _conn
    with _lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception as e:
                logger.warning(f"Failed to close DuckDB connection: {e}")
            _conn = None


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    validated_table = validate_table_name(table)
    try:
        conn.execute(f"SELECT * FROM {validated_table} LIMIT 0")
        return True
    except duckdb.CatalogException:
        # Table doesn't exist
        return False
    except Exception as e:
        logger.warning(
            f"Unexpected error checking if table {validated_table} exists: {e}"
        )
        return False


def _current_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    validated_table = validate_table_name(table)
    try:
        cur = conn.execute(f"PRAGMA table_info('{validated_table}')")
        rows = cur.fetchall()
        return {str(r[1]).lower() for r in rows}  # name at index 1
    except duckdb.CatalogException:
        # Table doesn't exist
        return set()
    except Exception as e:
        logger.warning(f"Failed to get columns for table {validated_table}: {e}")
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


def _ensure_columns(
    conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame
) -> None:
    """Ensure all DataFrame columns exist in the DuckDB table.

    Adds missing columns with inferred types (conservative defaults) so that
    evolving schemas (e.g., later batches adding 'Source' or 'Balance') do not fail.
    """
    validated_table = validate_table_name(table)
    existing = _current_columns(conn, validated_table)
    for col in df.columns:
        if str(col).lower() not in existing:
            validated_col = validate_column_name(col)
            duck_type = _duck_type_from_series(df[col])
            try:
                conn.execute(
                    f"ALTER TABLE {validated_table} "
                    f"ADD COLUMN {validated_col} {duck_type}"
                )
            except Exception as e:
                # Best-effort; if it fails, proceed and let INSERT surface issues
                logger.debug(
                    f"Failed to add column {validated_col} to {validated_table}: {e}"
                )
    # No need to drop extra table columns; INSERT will only specify df columns


def insert_dataframe(
    conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame
) -> int:
    """Insert a pandas DataFrame into a DuckDB table.

    Creates the table if it doesn't exist, otherwise inserts into existing table.
    All table and column names are validated to prevent SQL injection.

    Args:
        conn: DuckDB connection
        table: Target table name (must be in ALLOWED_TABLES)
        df: DataFrame to insert

    Returns:
        Number of rows inserted

    Raises:
        ValueError: If table name is not in ALLOWED_TABLES or column names
            contain invalid characters (must be alphanumeric + underscore)
    """
    if df.empty:
        return 0
    validated_table = validate_table_name(table)
    # Validate all column names before any SQL operations
    validated_cols = [validate_column_name(c) for c in df.columns]
    col_list = ", ".join(validated_cols)
    # Register DataFrame with internal name to avoid collision with user tables
    conn.register(_INTERNAL_TMP_TABLE, df)
    try:
        if not _table_exists(conn, validated_table):
            # Create with explicit column list to ensure validation is applied
            # (SELECT * would bypass column name validation)
            conn.execute(
                f"CREATE TABLE {validated_table} AS "
                f"SELECT {col_list} FROM {_INTERNAL_TMP_TABLE}"
            )
        else:
            # Align columns by name to avoid positional mismatches
            # Ensure any new columns are added before INSERT
            _ensure_columns(conn, validated_table, df)
            conn.execute(
                f"INSERT INTO {validated_table} ({col_list}) "
                f"SELECT {col_list} FROM {_INTERNAL_TMP_TABLE}"
            )
    finally:
        conn.unregister(_INTERNAL_TMP_TABLE)
    return len(df)


def insert_records(
    conn: duckdb.DuckDBPyConnection, table: str, records: Iterable[dict]
) -> int:
    df = pd.DataFrame.from_records(list(records))
    return insert_dataframe(conn, table, df)


# ================================
# Fact Table Schema Helpers (DuckDB)
# ================================


def ensure_fact_store_ops_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the fact_store_ops table if it does not exist.

    This ensures the table schema is defined before any inserts,
    preventing runtime failures when the generator attempts to insert records.

    Architecture Note:
        - DuckDB fact_store_ops: Used for local batch generation and testing.
          Contains only core payload fields (trace_id, operation_time, store_id,
          operation_type) generated by store_ops_mixin.
        - KQL store_opened/store_closed tables: Used for streaming ingestion.
          Include envelope fields (event_type, ingest_timestamp, schema_version, etc.)
          which are added by the streaming layer when events are wrapped in
          EventEnvelope objects.

    The separation keeps local generation simple while streaming adds metadata.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_store_ops (
            trace_id VARCHAR,
            operation_time TIMESTAMP,
            store_id INTEGER,
            operation_type VARCHAR
        );
        """
    )
    # Backward compatibility: If table already existed with fewer columns (from
    # an older schema version), add any missing columns. This block is a no-op
    # for newly created tables but ensures existing databases are migrated.
    try:
        existing = _current_columns(conn, "fact_store_ops")

        def _ensure(col: str, type_sql: str) -> None:
            if col not in existing:
                try:
                    conn.execute(
                        f"ALTER TABLE fact_store_ops ADD COLUMN {col} {type_sql}"
                    )
                    existing.add(col)
                except Exception as e:
                    logger.debug(f"Failed to add column {col} to fact_store_ops: {e}")

        # Core payload fields - add if missing from older schema
        _ensure("trace_id", "VARCHAR")
        _ensure("operation_time", "TIMESTAMP")
        _ensure("store_id", "INTEGER")
        _ensure("operation_type", "VARCHAR")
    except Exception as e:
        logger.warning(f"Failed to ensure columns on fact_store_ops: {e}")


def ensure_fact_payments_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the fact_payments table if it does not exist.

    This ensures the table schema is defined before any inserts,
    with OrderIdExt explicitly set as VARCHAR to prevent type inference issues.

    Schema Design Notes:
    - order_id_ext: VARCHAR to support both receipt IDs (NULL) and
      online order IDs (strings)
    - Mutual exclusivity: Either receipt_id_ext OR order_id_ext is populated,
      never both
    - Without explicit schema, DuckDB would infer INT32 when first seeing NULLs
      from receipt payments, causing conversion errors for online order payments
    - Pre-creating schema prevents type inference and ensures data integrity
    - Column names use snake_case to match codebase convention
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_payments (
            event_ts TIMESTAMP,
            receipt_id_ext VARCHAR,
            order_id_ext VARCHAR,
            payment_method VARCHAR,
            amount_cents BIGINT,
            amount VARCHAR,
            transaction_id VARCHAR,
            processing_time_ms BIGINT,
            status VARCHAR,
            decline_reason VARCHAR,
            store_id BIGINT,
            customer_id BIGINT
        );
        """
    )


def ensure_fact_receipt_lines_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the fact_receipt_lines table if it does not exist.

    This ensures the table schema is defined before any inserts,
    with promo_code explicitly set as VARCHAR to prevent type inference issues.

    Schema Design Notes:
    - promo_code: VARCHAR to support promotional codes like 'SAVE20', 'BOGO50'
    - Without explicit schema, DuckDB would infer INT32 when first seeing NULLs
      (receipts without promotions), causing conversion errors when actual
      promo codes are inserted later
    - Pre-creating schema prevents type inference and ensures data integrity
    - If table exists with wrong promo_code type (INT), it will be migrated
    """
    # Check if table exists
    if _table_exists(conn, "fact_receipt_lines"):
        # Check if promo_code column has wrong type and fix it
        try:
            result = conn.execute(
                """
                SELECT data_type FROM information_schema.columns
                WHERE table_name = 'fact_receipt_lines' AND column_name = 'promo_code'
                """
            ).fetchone()
            if result:
                current_type = result[0].upper()
                if current_type != "VARCHAR" and "INT" in current_type:
                    logger.warning(
                        f"SCHEMA FIX: Dropping fact_receipt_lines table to migrate "
                        f"promo_code from {current_type} to VARCHAR. "
                        f"All existing data will be lost and must be regenerated."
                    )
                    # DuckDB doesn't support ALTER COLUMN TYPE directly, so we need
                    # to recreate the table. Since this is a schema fix, we drop
                    # and recreate (data will be regenerated).
                    conn.execute("DROP TABLE fact_receipt_lines")
        except Exception as e:
            logger.warning(f"Failed to check promo_code type, skipping migration: {e}")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_receipt_lines (
            receipt_id_ext VARCHAR,
            event_ts TIMESTAMP,
            product_id BIGINT,
            line_num INTEGER,
            quantity INTEGER,
            unit_price VARCHAR,
            ext_price VARCHAR,
            unit_cents BIGINT,
            ext_cents BIGINT,
            promo_code VARCHAR
        );
        """
    )


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
                            f"ALTER TABLE streaming_outbox ADD COLUMN {col} "
                            f"{type_sql} DEFAULT {default_sql}"
                        )
                    else:
                        conn.execute(
                            f"ALTER TABLE streaming_outbox ADD COLUMN {col} {type_sql}"
                        )
                    existing.add(col)
                except Exception as e:
                    logger.debug(f"Failed to add column {col} to streaming_outbox: {e}")

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
    except Exception as e:
        logger.warning(
            f"Failed to ensure backward compatibility columns on streaming_outbox: {e}"
        )
    # Lightweight indexes for draining
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_outbox_status_ts "
            "ON streaming_outbox(status, event_ts)"
        )
    except Exception as e:
        logger.debug(f"Failed to create index on streaming_outbox: {e}")


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

    Returns the row as a dict with keys: outbox_id, event_ts, message_type,
    payload, partition_key, trace_id.
    """
    _ensure_outbox_table(conn)
    # Select candidate id
    # Note: Using simple outbox_id ordering for stability
    # hash(outbox_id) was causing DuckDB internal errors
    row = conn.execute(
        "SELECT outbox_id FROM streaming_outbox "
        "WHERE status='pending' ORDER BY event_ts, outbox_id LIMIT 1"
    ).fetchone()
    if not row:
        return None
    oid = int(row[0])
    # Try to mark as processing with guard
    conn.execute(
        "UPDATE streaming_outbox SET status='processing', "
        "last_attempt_ts=now(), attempts=attempts+1 "
        "WHERE outbox_id=? AND status='pending'",
        [oid],
    )
    # NOTE: DuckDB may return rowcount=-1 (unknown) even for successful UPDATEs
    # Instead of checking rowcount, verify the UPDATE by fetching the row
    # Fetch full row and verify it's in 'processing' status
    cur = conn.execute(
        "SELECT outbox_id, event_ts, message_type, payload, partition_key, trace_id, status "
        "FROM streaming_outbox WHERE outbox_id=?",
        [oid],
    )
    r = cur.fetchone()
    if not r:
        return None
    # Check if the row is now in 'processing' status (index 6)
    if r[6] != 'processing':
        # Another connection got it first, try again
        return None
    # Return the row without status column
    cols = [d[0] for d in (cur.description or []) if d[0] != 'status']
    return {cols[i]: r[i] for i in range(len(cols))}


def outbox_ack_sent(conn: duckdb.DuckDBPyConnection, outbox_id: int) -> None:
    conn.execute(
        "UPDATE streaming_outbox SET status='sent', sent_ts=now() WHERE outbox_id=?",
        [outbox_id],
    )


def outbox_nack_retry(conn: duckdb.DuckDBPyConnection, outbox_id: int) -> None:
    """Return a processing row to pending for retry."""
    conn.execute(
        "UPDATE streaming_outbox SET status='pending' "
        "WHERE outbox_id=? AND status='processing'",
        [outbox_id],
    )


def outbox_insert_records(
    conn: duckdb.DuckDBPyConnection, records: Iterable[dict]
) -> int:
    """Insert outbox rows, assigning monotonically increasing outbox_id values.

    Ensures compatibility with DuckDB versions lacking IDENTITY columns by
    generating outbox_id in Python.
    """
    _ensure_outbox_table(conn)
    try:
        row = conn.execute(
            "SELECT COALESCE(MAX(outbox_id), 0) FROM streaming_outbox"
        ).fetchone()
        max_id = int(row[0] or 0)
    except Exception as e:
        logger.warning(f"Failed to get max outbox_id, using 0: {e}")
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


# ================================
# Pending Shipments Staging (DuckDB)
# ================================


def _ensure_pending_shipments_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the pending shipments staging table if it does not exist.

    This table stores truck shipments that are scheduled beyond the generation
    end date. They are NOT included in exports or dashboard counts, but will
    be picked up by streaming when it runs.

    Schema mirrors fact_truck_moves but adds scheduling metadata.
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_STAGING_PENDING_SHIPMENTS} (
            staging_id BIGINT PRIMARY KEY,
            -- Core shipment data (matches fact_truck_moves schema)
            event_ts TIMESTAMP,
            truck_id VARCHAR,
            dc_id INTEGER,
            store_id INTEGER,
            shipment_id VARCHAR,
            status VARCHAR,
            eta TIMESTAMP,
            etd TIMESTAMP,
            departure_time TIMESTAMP,
            actual_unload_duration INTEGER,
            trace_id VARCHAR,
            -- Scheduling metadata
            generation_end_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT now(),
            -- Full shipment info as JSON for restoration
            shipment_info_json VARCHAR
        );
        """
    )
    # Index for efficient retrieval by departure time
    try:
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_pending_shipments_departure "
            f"ON {_STAGING_PENDING_SHIPMENTS}(departure_time)"
        )
    except Exception as e:
        logger.debug(f"Failed to create index on pending shipments: {e}")


def pending_shipments_insert(
    conn: duckdb.DuckDBPyConnection,
    records: list[dict],
    generation_end_date: datetime | None = None,
) -> int:
    """Insert pending shipment records into staging table.

    Args:
        conn: DuckDB connection
        records: List of shipment records to stage
        generation_end_date: The end date of the generation run (for metadata)

    Returns:
        Number of records inserted
    """
    import json

    _ensure_pending_shipments_table(conn)

    if not records:
        return 0

    # Get max existing staging_id
    try:
        row = conn.execute(
            f"SELECT COALESCE(MAX(staging_id), 0) FROM {_STAGING_PENDING_SHIPMENTS}"
        ).fetchone()
        max_id = int(row[0] or 0)
    except Exception as e:
        logger.warning(f"Failed to get max staging_id, using 0: {e}")
        max_id = 0

    prepared: list[dict] = []
    next_id = max_id

    for rec in records:
        if rec is None:
            continue
        next_id += 1

        # Serialize full shipment info for restoration
        try:
            shipment_json = json.dumps(rec, default=str)
        except (TypeError, ValueError):
            shipment_json = json.dumps({k: str(v) for k, v in rec.items()})

        prepared.append(
            {
                "staging_id": next_id,
                "event_ts": rec.get("EventTS") or rec.get("event_ts"),
                "truck_id": str(rec.get("TruckId") or rec.get("truck_id", "")),
                "dc_id": rec.get("DCID") or rec.get("dc_id"),
                "store_id": rec.get("StoreID") or rec.get("store_id"),
                "shipment_id": rec.get("ShipmentId") or rec.get("shipment_id"),
                "status": rec.get("Status") or rec.get("status"),
                "eta": rec.get("ETA") or rec.get("eta"),
                "etd": rec.get("ETD") or rec.get("etd"),
                "departure_time": rec.get("DepartureTime") or rec.get("departure_time"),
                "actual_unload_duration": rec.get("ActualUnloadDuration")
                or rec.get("actual_unload_duration"),
                "trace_id": rec.get("TraceId") or rec.get("trace_id", ""),
                "generation_end_date": generation_end_date,
                "shipment_info_json": shipment_json,
            }
        )

    if not prepared:
        return 0

    # Insert using pandas DataFrame for consistency
    df = pd.DataFrame.from_records(prepared)
    conn.register(_INTERNAL_TMP_TABLE, df)
    try:
        # Check if table exists and has data
        try:
            conn.execute(f"SELECT 1 FROM {_STAGING_PENDING_SHIPMENTS} LIMIT 0")
            table_exists = True
        except duckdb.CatalogException:
            table_exists = False

        col_list = ", ".join(df.columns)
        if not table_exists:
            conn.execute(
                f"CREATE TABLE {_STAGING_PENDING_SHIPMENTS} AS "
                f"SELECT {col_list} FROM {_INTERNAL_TMP_TABLE}"
            )
        else:
            conn.execute(
                f"INSERT INTO {_STAGING_PENDING_SHIPMENTS} ({col_list}) "
                f"SELECT {col_list} FROM {_INTERNAL_TMP_TABLE}"
            )
    finally:
        conn.unregister(_INTERNAL_TMP_TABLE)

    logger.info(f"Staged {len(prepared)} pending shipments for future processing")
    return len(prepared)


def pending_shipments_get_ready(
    conn: duckdb.DuckDBPyConnection,
    up_to_time: datetime,
) -> list[dict]:
    """Get pending shipments ready to be processed.

    Retrieves shipments where departure_time <= up_to_time.

    Args:
        conn: DuckDB connection
        up_to_time: Process shipments with departure_time <= this time

    Returns:
        List of shipment records ready for processing
    """
    import json

    _ensure_pending_shipments_table(conn)

    try:
        cur = conn.execute(
            f"SELECT staging_id, shipment_info_json FROM {_STAGING_PENDING_SHIPMENTS} "
            f"WHERE departure_time <= ? ORDER BY departure_time",
            [up_to_time],
        )
        rows = cur.fetchall()
    except Exception as e:
        logger.warning(f"Failed to query pending shipments: {e}")
        return []

    results = []
    for staging_id, shipment_json in rows:
        try:
            rec = json.loads(shipment_json)
            rec["_staging_id"] = staging_id  # Include for deletion after processing
            results.append(rec)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(
                f"Failed to parse shipment JSON for staging_id={staging_id}: {e}"
            )

    return results


def pending_shipments_delete(
    conn: duckdb.DuckDBPyConnection,
    staging_ids: list[int],
) -> int:
    """Delete processed pending shipments from staging table.

    Args:
        conn: DuckDB connection
        staging_ids: List of staging_ids to delete

    Returns:
        Number of records deleted
    """
    if not staging_ids:
        return 0

    _ensure_pending_shipments_table(conn)

    try:
        # DuckDB supports IN with list
        placeholders = ", ".join(["?" for _ in staging_ids])
        result = conn.execute(
            f"DELETE FROM {_STAGING_PENDING_SHIPMENTS} "
            f"WHERE staging_id IN ({placeholders})",
            staging_ids,
        )
        deleted = result.rowcount
        logger.debug(f"Deleted {deleted} processed pending shipments")
        return deleted
    except Exception as e:
        logger.warning(f"Failed to delete pending shipments: {e}")
        return 0


def pending_shipments_count(conn: duckdb.DuckDBPyConnection) -> int:
    """Get count of pending shipments in staging table.

    Args:
        conn: DuckDB connection

    Returns:
        Number of pending shipments
    """
    _ensure_pending_shipments_table(conn)

    try:
        row = conn.execute(
            f"SELECT COUNT(*) FROM {_STAGING_PENDING_SHIPMENTS}"
        ).fetchone()
        return int(row[0] or 0)
    except Exception as e:
        logger.debug(f"Failed to count pending shipments: {e}")
        return 0


def pending_shipments_get_date_range(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[datetime | None, datetime | None]:
    """Get the min/max departure_time of pending shipments.

    Args:
        conn: DuckDB connection

    Returns:
        Tuple of (min_departure_time, max_departure_time), or (None, None) if empty
    """
    _ensure_pending_shipments_table(conn)

    try:
        row = conn.execute(
            f"SELECT MIN(departure_time), MAX(departure_time) "
            f"FROM {_STAGING_PENDING_SHIPMENTS}"
        ).fetchone()
        if row and row[0] is not None:
            return row[0], row[1]
    except Exception as e:
        logger.debug(f"Failed to get pending shipments date range: {e}")

    return None, None
