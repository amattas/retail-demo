"""
DuckDB reader service for export functionality.

Reads master and fact tables from DuckDB into pandas DataFrames, with optional
date filtering for fact tables.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd

from retail_datagen.db.duckdb_engine import get_duckdb_conn, validate_table_name

logger = logging.getLogger(__name__)


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


def _date_bounds(
    start_date: date | None, end_date: date | None
) -> tuple[datetime | None, datetime | None]:
    if start_date is None and end_date is None:
        return None, None
    start_dt = datetime.combine(start_date or date.min, datetime.min.time())
    end_dt = datetime.combine(end_date or date.max, datetime.min.time()) + timedelta(
        days=1
    )
    return start_dt, end_dt


def read_all_master_tables() -> dict[str, pd.DataFrame]:
    conn = get_duckdb_conn()
    result: dict[str, pd.DataFrame] = {}
    for table in MASTER_TABLES:
        try:
            validated_table = validate_table_name(table)
            df = conn.execute(f"SELECT * FROM {validated_table}").df()
        except duckdb.CatalogException:
            logger.debug(f"Table {table} does not exist, returning empty DataFrame")
            df = pd.DataFrame()
        except Exception as e:
            logger.warning(
                f"Failed to read table {table}: {e}, returning empty DataFrame"
            )
            df = pd.DataFrame()
        result[table] = df
    return result


def read_all_fact_tables(
    start_date: date | None = None, end_date: date | None = None
) -> dict[str, pd.DataFrame]:
    conn = get_duckdb_conn()
    start_dt, end_dt = _date_bounds(start_date, end_date)
    result: dict[str, pd.DataFrame] = {}
    for table in FACT_TABLES:
        try:
            validated_table = validate_table_name(table)
            if start_dt is None:
                df = conn.execute(f"SELECT * FROM {validated_table}").df()
            else:
                # Special-case tables without event_ts
                if table == "fact_online_order_lines":
                    # Join with headers to get order creation timestamp
                    # for partitioning pending orders with NULL timestamps.
                    # Include: fulfilled orders in range OR pending orders
                    # created in range
                    query = f"""
                        SELECT l.*, h.event_ts as order_event_ts
                        FROM {validated_table} l
                        JOIN fact_online_order_headers h
                            ON l.order_id = h.order_id_ext
                        WHERE (
                            (COALESCE(l.picked_ts, l.shipped_ts, l.delivered_ts) >= ?
                             AND COALESCE(l.picked_ts, l.shipped_ts,
                                          l.delivered_ts) < ?)
                            OR
                            (l.picked_ts IS NULL
                             AND l.shipped_ts IS NULL
                             AND l.delivered_ts IS NULL
                             AND h.event_ts >= ?
                             AND h.event_ts < ?)
                        )
                    """
                    df = conn.execute(
                        query, [start_dt, end_dt, start_dt, end_dt]
                    ).df()
                else:
                    query = (
                        f"SELECT * FROM {validated_table} "
                        f"WHERE event_ts >= ? AND event_ts < ?"
                    )
                    df = conn.execute(query, [start_dt, end_dt]).df()
        except duckdb.CatalogException:
            logger.debug(f"Table {table} does not exist, returning empty DataFrame")
            df = pd.DataFrame()
        except Exception as e:
            logger.warning(
                f"Failed to read table {table}: {e}, returning empty DataFrame"
            )
            df = pd.DataFrame()
        result[table] = df
    return result


def get_fact_table_date_range(
    table_name: str,
) -> tuple[datetime | None, datetime | None]:
    """Return (min_event_ts, max_event_ts) for a DuckDB fact table.

    If the table does not exist or has no rows, returns (None, None).
    """
    validated_table = validate_table_name(table_name)
    conn = get_duckdb_conn()
    try:
        # Special handling for tables without event_ts column
        if table_name == "fact_online_order_lines":
            # Use coalesce of available timestamp columns
            query = (
                f"SELECT "
                f"MIN(coalesce(picked_ts, shipped_ts, delivered_ts)) AS min_ts, "
                f"MAX(coalesce(picked_ts, shipped_ts, delivered_ts)) AS max_ts "
                f"FROM {validated_table}"
            )
            cur = conn.execute(query)
        else:
            query = (
                f"SELECT MIN(event_ts) AS min_ts, MAX(event_ts) AS max_ts "
                f"FROM {validated_table}"
            )
            cur = conn.execute(query)
        row = cur.fetchone()
        if not row:
            return None, None
        return row[0], row[1]
    except duckdb.CatalogException:
        logger.debug(f"Table {validated_table} does not exist")
        return None, None
    except Exception as e:
        logger.warning(f"Failed to get date range for table {validated_table}: {e}")
        return None, None


def get_all_fact_table_date_ranges() -> dict[
    str, tuple[datetime | None, datetime | None]
]:
    """Return date ranges for all known fact tables from DuckDB."""
    ranges: dict[str, tuple[datetime | None, datetime | None]] = {}
    for tbl in FACT_TABLES:
        ranges[tbl] = get_fact_table_date_range(tbl)
    return ranges
