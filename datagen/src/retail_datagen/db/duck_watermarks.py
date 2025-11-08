"""
DuckDB watermark helpers for batch streaming.

Provides functions to track publication watermarks in DuckDB, mirroring the
previous watermark logic with a minimal schema.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Tuple

import duckdb

WATERMARK_TABLE = "fact_data_watermarks"


def _ensure_watermark_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            fact_table_name VARCHAR NOT NULL UNIQUE,
            earliest_unpublished_ts TIMESTAMP,
            latest_published_ts TIMESTAMP,
            last_purge_ts TIMESTAMP,
            created_at TIMESTAMP DEFAULT now(),
            updated_at TIMESTAMP DEFAULT now()
        );
        """
    )


def get_unpublished_data_range(
    conn: duckdb.DuckDBPyConnection, fact_table_name: str
) -> Tuple[datetime | None, datetime | None]:
    """
    Return (earliest_unpublished, latest_unpublished) for a DuckDB fact table.
    If no watermark exists, compute from data. Returns (None, None) if no data.
    """
    _ensure_watermark_table(conn)
    # Read watermark row
    wm = conn.execute(
        f"SELECT earliest_unpublished_ts, latest_published_ts FROM {WATERMARK_TABLE} WHERE fact_table_name=?",
        [fact_table_name],
    ).fetchone()

    table_exists = True
    try:
        # Will fail if table doesn't exist
        _ = conn.execute(f"PRAGMA table_info('{fact_table_name}')").fetchall()
    except Exception:
        table_exists = False

    if not table_exists:
        return None, None

    if wm is None or wm[0] is None:
        # No watermark or earliest_unpublished NULL → start at data min
        row = conn.execute(
            f"SELECT MIN(event_ts) FROM {fact_table_name}"
        ).fetchone()
        earliest = row[0]
        if earliest is None:
            return None, None
        # Latest unpublished = current max in table
        row2 = conn.execute(
            f"SELECT MAX(event_ts) FROM {fact_table_name}"
        ).fetchone()
        latest = row2[0] or earliest
        return earliest, latest

    earliest_unpublished = wm[0]
    row3 = conn.execute(
        f"SELECT MAX(event_ts) FROM {fact_table_name} WHERE event_ts >= ?",
        [earliest_unpublished],
    ).fetchone()
    latest_unpublished = row3[0]
    return earliest_unpublished, latest_unpublished


def update_publication_watermark(
    conn: duckdb.DuckDBPyConnection, fact_table_name: str, published_up_to_ts: datetime
) -> None:
    """
    Upsert latest_published_ts and advance earliest_unpublished_ts if needed.
    """
    _ensure_watermark_table(conn)
    row = conn.execute(
        f"SELECT earliest_unpublished_ts FROM {WATERMARK_TABLE} WHERE fact_table_name=?",
        [fact_table_name],
    ).fetchone()

    # Upsert
    if row is None:
        conn.execute(
            f"INSERT INTO {WATERMARK_TABLE} (fact_table_name, earliest_unpublished_ts, latest_published_ts, last_purge_ts, created_at, updated_at) VALUES (?, NULL, ?, NULL, now(), now())",
            [fact_table_name, published_up_to_ts],
        )
        return

    earliest_unpublished = row[0]
    # Advance earliest_unpublished beyond the published boundary
    new_earliest = earliest_unpublished
    if earliest_unpublished is not None and earliest_unpublished <= published_up_to_ts:
        new_earliest = published_up_to_ts + timedelta(seconds=1)

    conn.execute(
        f"UPDATE {WATERMARK_TABLE} SET latest_published_ts=?, earliest_unpublished_ts=?, updated_at=now() WHERE fact_table_name=?",
        [published_up_to_ts, new_earliest, fact_table_name],
    )
