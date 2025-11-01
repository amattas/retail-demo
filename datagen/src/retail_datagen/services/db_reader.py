"""
Database reader service for data export functionality.

Provides efficient reading of master and fact tables from SQLite database
with support for:
- Chunked reading for large datasets
- Date-based filtering for fact tables
- Pandas DataFrame output
- Memory-efficient iteration

This service is designed for the export functionality to convert database
data to CSV/Parquet formats. All functions use async SQLAlchemy sessions
and return pandas DataFrames for compatibility with export formats.

Usage:
    >>> async with get_retail_session() as session:
    ...     # Read single master table
    ...     stores_df = await read_master_table(session, Store)
    ...
    ...     # Read all master tables
    ...     all_master = await read_all_master_tables(session)
    ...
    ...     # Read fact table with date filter
    ...     receipts_df = await read_fact_table(
    ...         session,
    ...         Receipt,
    ...         start_date=date(2024, 1, 1),
    ...         end_date=date(2024, 1, 31)
    ...     )
"""

import logging
from datetime import date, datetime

import pandas as pd
from sqlalchemy import Integer, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from retail_datagen.db.models.base import Base
from retail_datagen.db.models.facts import (
    BLEPing,
    DCInventoryTransaction,
    FootTraffic,
    MarketingImpression,
    OnlineOrder,
    Receipt,
    ReceiptLine,
    StoreInventoryTransaction,
    TruckMove,
)
from retail_datagen.db.models.master import (
    Customer,
    DistributionCenter,
    Geography,
    Product,
    Store,
    Truck,
)

logger = logging.getLogger(__name__)

# Default chunk size for reading large tables
DEFAULT_CHUNK_SIZE = 10000

# Master table mapping (table name -> ORM class)
MASTER_TABLES = {
    "dim_geographies": Geography,
    "dim_stores": Store,
    "dim_distribution_centers": DistributionCenter,
    "dim_trucks": Truck,
    "dim_customers": Customer,
    "dim_products": Product,
}

# Fact table mapping (table name -> ORM class)
FACT_TABLES = {
    "fact_dc_inventory_txn": DCInventoryTransaction,
    "fact_truck_moves": TruckMove,
    "fact_store_inventory_txn": StoreInventoryTransaction,
    "fact_receipts": Receipt,
    "fact_receipt_lines": ReceiptLine,
    "fact_foot_traffic": FootTraffic,
    "fact_ble_pings": BLEPing,
    "fact_marketing": MarketingImpression,
    "fact_online_orders": OnlineOrder,
}


def _convert_nullable_int_columns(
    df: pd.DataFrame, table_model: type[Base]
) -> pd.DataFrame:
    """
    Convert nullable integer columns to pandas nullable Int64 dtype.

    This prevents None values from being converted to float NaN when exporting to CSV.
    Identifies nullable integer columns from the SQLAlchemy model and converts them
    to use pandas' nullable integer dtype (Int64), which preserves None as <NA>
    and renders as empty string in CSV exports instead of "nan" or "1.0".

    Args:
        df: DataFrame to convert
        table_model: SQLAlchemy ORM model class

    Returns:
        DataFrame with nullable integer columns properly typed
    """
    if df.empty:
        return df

    # Identify nullable integer columns from the SQLAlchemy model
    nullable_int_cols = []
    for column in table_model.__table__.columns:
        # Check if column is Integer type and nullable
        if isinstance(column.type, Integer) and column.nullable:
            col_name = column.key
            if col_name in df.columns:
                nullable_int_cols.append(col_name)

    # Convert each nullable integer column to nullable Int64 dtype
    for col in nullable_int_cols:
        # Convert to nullable Int64 dtype, which preserves None as pd.NA
        # This prevents None -> NaN -> "nan" or "1.0" issues in CSV export
        df[col] = df[col].astype("Int64")
        logger.debug(f"Converted nullable integer column '{col}' to Int64 dtype")

    return df


async def get_table_row_count(
    session: AsyncSession,
    table_model: type[Base],
) -> int:
    """
    Get total row count for a table.

    Args:
        session: AsyncSession for database operations
        table_model: SQLAlchemy ORM model class

    Returns:
        Total number of rows in the table

    Example:
        >>> async with get_retail_session() as session:
        ...     count = await get_table_row_count(session, Store)
        ...     print(f"Total stores: {count}")
    """
    try:
        stmt = select(func.count()).select_from(table_model)
        result = await session.execute(stmt)
        count = result.scalar_one()
        logger.debug(f"Table {table_model.__tablename__} has {count} rows")
        return count
    except Exception as e:
        logger.error(f"Failed to count rows in {table_model.__tablename__}: {e}")
        raise


async def read_master_table(
    session: AsyncSession,
    table_model: type[Base],
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> pd.DataFrame:
    """
    Read a complete master table into a pandas DataFrame.

    Uses chunked reading for memory efficiency with large tables.
    All columns are included in the output.

    Args:
        session: AsyncSession for database operations
        table_model: SQLAlchemy ORM model class (e.g., Store, Customer)
        chunk_size: Number of rows to read per chunk (default: 10000)

    Returns:
        pandas DataFrame with all rows from the table

    Raises:
        ValueError: If table_model is not a valid master table
        Exception: For database errors

    Example:
        >>> async with get_retail_session() as session:
        ...     stores_df = await read_master_table(session, Store)
        ...     print(f"Read {len(stores_df)} stores")
    """
    table_name = table_model.__tablename__

    # Validate it's a master table
    if table_name not in MASTER_TABLES:
        raise ValueError(f"Invalid master table: {table_name}")

    logger.info(f"Reading master table: {table_name}")

    try:
        # Get total count for logging
        total_rows = await get_table_row_count(session, table_model)
        logger.info(f"Table {table_name} contains {total_rows} rows")

        if total_rows == 0:
            logger.warning(f"Table {table_name} is empty")
            return pd.DataFrame()

        # Read all data in chunks
        chunks = []
        offset = 0

        while offset < total_rows:
            # Build query with limit/offset for chunking
            stmt = select(table_model).limit(chunk_size).offset(offset)
            result = await session.execute(stmt)
            rows = result.scalars().all()

            if not rows:
                break

            # Convert ORM objects to dictionaries
            # Map database column names to Python attribute names via mapper
            column_to_attr = {}
            mapper = table_model.__mapper__
            for attr in mapper.attrs:
                if hasattr(attr, "columns") and len(attr.columns) > 0:
                    # Map database column name to Python attribute name
                    column_to_attr[attr.columns[0].name] = attr.key

            chunk_data = [
                {
                    column.key: getattr(row, column_to_attr.get(column.key, column.key))
                    for column in table_model.__table__.columns
                }
                for row in rows
            ]
            chunks.append(pd.DataFrame(chunk_data))

            offset += chunk_size
            logger.debug(
                f"Read chunk: offset={offset - chunk_size}, "
                f"size={len(rows)}, total_progress={offset}/{total_rows}"
            )

        # Combine all chunks
        if chunks:
            df = pd.concat(chunks, ignore_index=True)

            # Convert nullable integer foreign key columns to proper nullable Int64 dtype
            # This prevents None values from being converted to float NaN in CSV export
            df = _convert_nullable_int_columns(df, table_model)

            logger.info(f"Successfully read {len(df)} rows from {table_name}")
            return df
        else:
            logger.warning(f"No data read from {table_name}")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Failed to read master table {table_name}: {e}")
        raise


async def read_all_master_tables(
    session: AsyncSession,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, pd.DataFrame]:
    """
    Read all master dimension tables into a dictionary of DataFrames.

    Args:
        session: AsyncSession for database operations
        chunk_size: Number of rows to read per chunk (default: 10000)

    Returns:
        Dictionary mapping table names to DataFrames:
        {
            "dim_geographies": DataFrame,
            "dim_stores": DataFrame,
            "dim_distribution_centers": DataFrame,
            "dim_trucks": DataFrame,
            "dim_customers": DataFrame,
            "dim_products": DataFrame,
        }

    Example:
        >>> async with get_retail_session() as session:
        ...     all_master = await read_all_master_tables(session)
        ...     print(f"Read {len(all_master['dim_stores'])} stores")
        ...     print(f"Read {len(all_master['dim_customers'])} customers")
    """
    logger.info("Reading all master tables")
    result = {}

    for table_name, table_model in MASTER_TABLES.items():
        try:
            df = await read_master_table(session, table_model, chunk_size)
            result[table_name] = df
            logger.debug(f"Added {table_name} with {len(df)} rows to result")
        except Exception as e:
            logger.error(f"Failed to read master table {table_name}: {e}")
            # Continue reading other tables even if one fails
            result[table_name] = pd.DataFrame()

    total_rows = sum(len(df) for df in result.values())
    logger.info(f"Successfully read all master tables: {total_rows} total rows")
    return result


async def read_fact_table(
    session: AsyncSession,
    table_model: type[Base],
    start_date: date | None = None,
    end_date: date | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> pd.DataFrame:
    """
    Read a fact table with optional date filtering.

    Fact tables use event_ts column for temporal filtering. Date parameters
    are converted to datetime ranges (start of day to end of day).

    Args:
        session: AsyncSession for database operations
        table_model: SQLAlchemy ORM model class (e.g., Receipt, TruckMove)
        start_date: Optional start date for filtering (inclusive)
        end_date: Optional end date for filtering (inclusive)
        chunk_size: Number of rows to read per chunk (default: 10000)

    Returns:
        pandas DataFrame with filtered rows

    Raises:
        ValueError: If table_model is not a valid fact table or date range is invalid
        Exception: For database errors

    Example:
        >>> async with get_retail_session() as session:
        ...     # Read all receipts
        ...     all_receipts = await read_fact_table(session, Receipt)
        ...
        ...     # Read receipts for specific date range
        ...     jan_receipts = await read_fact_table(
        ...         session,
        ...         Receipt,
        ...         start_date=date(2024, 1, 1),
        ...         end_date=date(2024, 1, 31)
        ...     )
    """
    table_name = table_model.__tablename__

    # Validate it's a fact table
    if table_name not in FACT_TABLES:
        raise ValueError(f"Invalid fact table: {table_name}")

    # Validate date range
    if start_date and end_date and start_date > end_date:
        raise ValueError(
            f"Invalid date range: start_date ({start_date}) > end_date ({end_date})"
        )

    logger.info(
        f"Reading fact table: {table_name} "
        f"(start_date={start_date}, end_date={end_date})"
    )

    try:
        # Build base query
        stmt = select(table_model)

        # Add date filters if provided
        if start_date:
            start_datetime = datetime.combine(start_date, datetime.min.time())
            stmt = stmt.where(table_model.event_ts >= start_datetime)
            logger.debug(f"Filtering event_ts >= {start_datetime}")

        if end_date:
            # Include entire end date (up to 23:59:59.999999)
            end_datetime = datetime.combine(end_date, datetime.max.time())
            stmt = stmt.where(table_model.event_ts <= end_datetime)
            logger.debug(f"Filtering event_ts <= {end_datetime}")

        # Order by event_ts for consistent chunking
        stmt = stmt.order_by(table_model.event_ts)

        # Get total count for the filtered query
        count_stmt = select(func.count()).select_from(table_model)
        if start_date:
            start_datetime = datetime.combine(start_date, datetime.min.time())
            count_stmt = count_stmt.where(table_model.event_ts >= start_datetime)
        if end_date:
            end_datetime = datetime.combine(end_date, datetime.max.time())
            count_stmt = count_stmt.where(table_model.event_ts <= end_datetime)

        result = await session.execute(count_stmt)
        total_rows = result.scalar_one()
        logger.info(f"Table {table_name} contains {total_rows} matching rows")

        if total_rows == 0:
            logger.warning(
                f"No data in {table_name} for date range {start_date} to {end_date}"
            )
            return pd.DataFrame()

        # Read all data in chunks
        chunks = []
        offset = 0

        while offset < total_rows:
            # Build query with limit/offset for chunking
            chunk_stmt = stmt.limit(chunk_size).offset(offset)
            result = await session.execute(chunk_stmt)
            rows = result.scalars().all()

            if not rows:
                break

            # Convert ORM objects to dictionaries
            # Map database column names to Python attribute names via mapper
            column_to_attr = {}
            mapper = table_model.__mapper__
            for attr in mapper.attrs:
                if hasattr(attr, "columns") and len(attr.columns) > 0:
                    # Map database column name to Python attribute name
                    column_to_attr[attr.columns[0].name] = attr.key

            chunk_data = [
                {
                    column.key: getattr(row, column_to_attr.get(column.key, column.key))
                    for column in table_model.__table__.columns
                }
                for row in rows
            ]
            chunks.append(pd.DataFrame(chunk_data))

            offset += chunk_size
            logger.debug(
                f"Read chunk: offset={offset - chunk_size}, "
                f"size={len(rows)}, total_progress={offset}/{total_rows}"
            )

        # Combine all chunks
        if chunks:
            df = pd.concat(chunks, ignore_index=True)

            # Convert nullable integer foreign key columns to proper nullable Int64 dtype
            # This prevents None values from being converted to float NaN in CSV export
            df = _convert_nullable_int_columns(df, table_model)

            logger.info(
                f"Successfully read {len(df)} rows from {table_name} "
                f"(date_range: {start_date} to {end_date})"
            )
            return df
        else:
            logger.warning(f"No data read from {table_name}")
            return pd.DataFrame()

    except Exception as e:
        logger.error(
            f"Failed to read fact table {table_name} "
            f"(start_date={start_date}, end_date={end_date}): {e}"
        )
        raise


async def read_all_fact_tables(
    session: AsyncSession,
    start_date: date | None = None,
    end_date: date | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, pd.DataFrame]:
    """
    Read all fact tables with optional date filtering.

    Args:
        session: AsyncSession for database operations
        start_date: Optional start date for filtering (inclusive)
        end_date: Optional end date for filtering (inclusive)
        chunk_size: Number of rows to read per chunk (default: 10000)

    Returns:
        Dictionary mapping table names to DataFrames:
        {
            "fact_dc_inventory_txn": DataFrame,
            "fact_truck_moves": DataFrame,
            "fact_store_inventory_txn": DataFrame,
            "fact_receipts": DataFrame,
            "fact_receipt_lines": DataFrame,
            "fact_foot_traffic": DataFrame,
            "fact_ble_pings": DataFrame,
            "fact_marketing": DataFrame,
            "fact_online_orders": DataFrame,
        }

    Example:
        >>> async with get_retail_session() as session:
        ...     # Read all fact data
        ...     all_facts = await read_all_fact_tables(session)
        ...
        ...     # Read fact data for specific month
        ...     jan_facts = await read_all_fact_tables(
        ...         session,
        ...         start_date=date(2024, 1, 1),
        ...         end_date=date(2024, 1, 31)
        ...     )
        ...     print(f"January receipts: {len(jan_facts['fact_receipts'])}")
    """
    logger.info(
        f"Reading all fact tables (start_date={start_date}, end_date={end_date})"
    )
    result = {}

    for table_name, table_model in FACT_TABLES.items():
        try:
            df = await read_fact_table(
                session, table_model, start_date, end_date, chunk_size
            )
            result[table_name] = df
            logger.debug(f"Added {table_name} with {len(df)} rows to result")
        except Exception as e:
            logger.error(f"Failed to read fact table {table_name}: {e}")
            # Continue reading other tables even if one fails
            result[table_name] = pd.DataFrame()

    total_rows = sum(len(df) for df in result.values())
    logger.info(
        f"Successfully read all fact tables: {total_rows} total rows "
        f"(date_range: {start_date} to {end_date})"
    )
    return result


async def get_fact_table_date_range(
    session: AsyncSession,
    table_model: type[Base],
) -> tuple[datetime | None, datetime | None]:
    """
    Get the earliest and latest event_ts for a fact table.

    Useful for understanding data coverage and partition boundaries.

    Args:
        session: AsyncSession for database operations
        table_model: SQLAlchemy ORM model class (e.g., Receipt, TruckMove)

    Returns:
        Tuple of (earliest_ts, latest_ts)
        Both None if table is empty

    Raises:
        ValueError: If table_model is not a valid fact table

    Example:
        >>> async with get_retail_session() as session:
        ...     start, end = await get_fact_table_date_range(session, Receipt)
        ...     if start:
        ...         print(f"Receipts span from {start} to {end}")
    """
    table_name = table_model.__tablename__

    # Validate it's a fact table
    if table_name not in FACT_TABLES:
        raise ValueError(f"Invalid fact table: {table_name}")

    try:
        # Query for min and max event_ts
        stmt = select(
            func.min(table_model.event_ts),
            func.max(table_model.event_ts),
        )
        result = await session.execute(stmt)
        min_ts, max_ts = result.one()

        logger.debug(f"Date range for {table_name}: {min_ts} to {max_ts}")

        return min_ts, max_ts

    except Exception as e:
        logger.error(f"Failed to get date range for {table_name}: {e}")
        raise


async def get_all_fact_table_date_ranges(
    session: AsyncSession,
) -> dict[str, tuple[datetime | None, datetime | None]]:
    """
    Get date ranges for all fact tables.

    Args:
        session: AsyncSession for database operations

    Returns:
        Dictionary mapping table names to (earliest_ts, latest_ts) tuples

    Example:
        >>> async with get_retail_session() as session:
        ...     date_ranges = await get_all_fact_table_date_ranges(session)
        ...     for table, (start, end) in date_ranges.items():
        ...         if start:
        ...             print(f"{table}: {start} to {end}")
    """
    logger.info("Getting date ranges for all fact tables")
    result = {}

    for table_name, table_model in FACT_TABLES.items():
        try:
            date_range = await get_fact_table_date_range(session, table_model)
            result[table_name] = date_range
        except Exception as e:
            logger.error(f"Failed to get date range for {table_name}: {e}")
            result[table_name] = (None, None)

    return result
