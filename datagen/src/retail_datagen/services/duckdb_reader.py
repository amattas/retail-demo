"""
DuckDB reader service for export functionality.

Reads master and fact tables from DuckDB into pandas DataFrames, with optional
date filtering for fact tables.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, Tuple

import duckdb
import pandas as pd

from retail_datagen.db.duckdb_engine import get_duckdb_conn


MASTER_TABLES = [
    "dim_geographies",
    "dim_stores",
    "dim_distribution_centers",
    "dim_trucks",
    "dim_customers",
    "dim_products",
]

FACT_TABLES = [
    "fact_dc_inventory_txn",
    "fact_truck_moves",
    "fact_store_inventory_txn",
    "fact_receipts",
    "fact_receipt_lines",
    "fact_foot_traffic",
    "fact_ble_pings",
    "fact_marketing",
    "fact_online_order_headers",
    "fact_online_order_lines",
]


def _date_bounds(start_date: date | None, end_date: date | None) -> tuple[datetime | None, datetime | None]:
    if start_date is None and end_date is None:
        return None, None
    start_dt = datetime.combine(start_date or date.min, datetime.min.time())
    end_dt = datetime.combine(end_date or date.max, datetime.min.time()) + timedelta(days=1)
    return start_dt, end_dt


def read_all_master_tables() -> Dict[str, pd.DataFrame]:
    conn = get_duckdb_conn()
    result: Dict[str, pd.DataFrame] = {}
    for table in MASTER_TABLES:
        try:
            df = conn.execute(f"SELECT * FROM {table}").df()
        except Exception:
            df = pd.DataFrame()
        result[table] = df
    return result


def read_all_fact_tables(start_date: date | None = None, end_date: date | None = None) -> Dict[str, pd.DataFrame]:
    conn = get_duckdb_conn()
    start_dt, end_dt = _date_bounds(start_date, end_date)
    result: Dict[str, pd.DataFrame] = {}
    for table in FACT_TABLES:
        try:
            if start_dt is None:
                df = conn.execute(f"SELECT * FROM {table}").df()
            else:
                # Special-case tables without event_ts
                if table == "fact_online_order_lines":
                    df = conn.execute(
                        f"SELECT * FROM {table} WHERE coalesce(picked_ts, shipped_ts, delivered_ts) >= ? AND coalesce(picked_ts, shipped_ts, delivered_ts) < ?",
                        [start_dt, end_dt],
                    ).df()
                else:
                    df = conn.execute(
                        f"SELECT * FROM {table} WHERE event_ts >= ? AND event_ts < ?",
                        [start_dt, end_dt],
                    ).df()
        except Exception:
            df = pd.DataFrame()
        result[table] = df
    return result


def get_fact_table_date_range(table_name: str) -> Tuple[datetime | None, datetime | None]:
    """Return (min_event_ts, max_event_ts) for a DuckDB fact table.

    If the table does not exist or has no rows, returns (None, None).
    """
    conn = get_duckdb_conn()
    try:
        cur = conn.execute(
            f"SELECT MIN(event_ts) AS min_ts, MAX(event_ts) AS max_ts FROM {table_name}"
        )
        row = cur.fetchone()
        if not row:
            return None, None
        return row[0], row[1]
    except Exception:
        return None, None


def get_all_fact_table_date_ranges() -> Dict[str, Tuple[datetime | None, datetime | None]]:
    """Return date ranges for all known fact tables from DuckDB."""
    ranges: Dict[str, Tuple[datetime | None, datetime | None]] = {}
    for tbl in FACT_TABLES:
        ranges[tbl] = get_fact_table_date_range(tbl)
    return ranges
