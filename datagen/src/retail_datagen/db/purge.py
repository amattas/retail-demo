"""
Data purge system for fact tables.

Provides utilities to safely delete published data from retail.db after
Azure Event Hub publication, using watermark tracking to maintain data integrity.

Key Features:
- Watermark-based purge boundaries (never purge unpublished data)
- Safety buffer (keep recent hours even if published)
- Disk space reclamation via VACUUM
- Batch deletion for large datasets
- Comprehensive purge metrics and logging

Workflow:
1. Historical generation writes fact data to retail.db
2. Streaming publishes data to Azure Event Hub
3. After publication, update_publication_watermark() is called
4. Periodically, purge_published_data() removes old published data
5. Disk space is reclaimed via VACUUM

Safety Rules:
- NEVER purge unpublished data (respect watermark boundaries)
- ALWAYS keep buffer of recent hours (default 24h)
- Log all purge operations with row counts
- Support dry-run mode to preview deletions
"""

import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text, select, func
from sqlalchemy.ext.asyncio import AsyncSession

from retail_datagen.db.models.watermarks import FactDataWatermark
from retail_datagen.db.models.facts import (
    DCInventoryTransaction,
    TruckMove,
    StoreInventoryTransaction,
    Receipt,
    ReceiptLine,
    FootTraffic,
    BLEPing,
    MarketingImpression,
    OnlineOrder,
)

logger = logging.getLogger(__name__)

# Mapping of fact table names to ORM classes
FACT_TABLE_MAPPING = {
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

# Default purge settings
DEFAULT_BUFFER_HOURS = 24
BATCH_SIZE = 10000  # Delete in batches to avoid long-running transactions


async def update_publication_watermark(
    session: AsyncSession,
    fact_table_name: str,
    published_up_to_ts: datetime,
) -> None:
    """
    Update watermark after successfully publishing data to Azure Event Hub.

    Args:
        session: AsyncSession for retail.db
        fact_table_name: Name of fact table (e.g., "fact_receipts")
        published_up_to_ts: Timestamp up to which data has been published

    Updates:
        - latest_published_ts = published_up_to_ts
        - If earliest_unpublished_ts < published_up_to_ts:
            earliest_unpublished_ts = published_up_to_ts + 1 second

    Example:
        >>> async with get_retail_session() as session:
        ...     await update_publication_watermark(
        ...         session,
        ...         "fact_receipts",
        ...         datetime(2025, 1, 15, 12, 0, 0)
        ...     )
        ...     await session.commit()
    """
    # Get or create watermark
    stmt = select(FactDataWatermark).where(
        FactDataWatermark.fact_table_name == fact_table_name
    )
    result = await session.execute(stmt)
    watermark = result.scalar_one_or_none()

    if watermark is None:
        # Create new watermark
        watermark = FactDataWatermark(
            fact_table_name=fact_table_name,
            earliest_unpublished_ts=None,
            latest_published_ts=published_up_to_ts,
            last_purge_ts=None,
        )
        session.add(watermark)
        logger.info(
            f"Created watermark for {fact_table_name}: "
            f"latest_published={published_up_to_ts}"
        )
    else:
        # Update existing watermark
        watermark.latest_published_ts = published_up_to_ts

        # If earliest_unpublished is before published boundary, move it forward
        if (
            watermark.earliest_unpublished_ts is not None
            and watermark.earliest_unpublished_ts <= published_up_to_ts
        ):
            # Set to 1 second after published timestamp
            watermark.earliest_unpublished_ts = published_up_to_ts + timedelta(seconds=1)

        logger.info(
            f"Updated watermark for {fact_table_name}: "
            f"latest_published={published_up_to_ts}, "
            f"earliest_unpublished={watermark.earliest_unpublished_ts}"
        )


async def mark_data_unpublished(
    session: AsyncSession,
    fact_table_name: str,
    data_start_ts: datetime,
    data_end_ts: datetime,
) -> None:
    """
    Mark new data range as unpublished after fact generation.

    Args:
        session: AsyncSession for retail.db
        fact_table_name: Name of fact table
        data_start_ts: Start of new data range
        data_end_ts: End of new data range

    Updates:
        - If earliest_unpublished_ts is None or > data_start_ts:
            earliest_unpublished_ts = data_start_ts

    Example:
        >>> async with get_retail_session() as session:
        ...     await mark_data_unpublished(
        ...         session,
        ...         "fact_receipts",
        ...         datetime(2025, 1, 15, 0, 0, 0),
        ...         datetime(2025, 1, 15, 23, 59, 59)
        ...     )
        ...     await session.commit()
    """
    # Get or create watermark
    stmt = select(FactDataWatermark).where(
        FactDataWatermark.fact_table_name == fact_table_name
    )
    result = await session.execute(stmt)
    watermark = result.scalar_one_or_none()

    if watermark is None:
        # Create new watermark with unpublished data
        watermark = FactDataWatermark(
            fact_table_name=fact_table_name,
            earliest_unpublished_ts=data_start_ts,
            latest_published_ts=None,
            last_purge_ts=None,
        )
        session.add(watermark)
        logger.info(
            f"Created watermark for {fact_table_name}: "
            f"earliest_unpublished={data_start_ts}"
        )
    else:
        # Update earliest_unpublished if new data is earlier
        if (
            watermark.earliest_unpublished_ts is None
            or data_start_ts < watermark.earliest_unpublished_ts
        ):
            watermark.earliest_unpublished_ts = data_start_ts
            logger.info(
                f"Updated watermark for {fact_table_name}: "
                f"earliest_unpublished={data_start_ts}"
            )


async def get_unpublished_data_range(
    session: AsyncSession,
    fact_table_name: str,
) -> tuple[datetime | None, datetime | None]:
    """
    Get the time range of unpublished data.

    Args:
        session: AsyncSession for retail.db
        fact_table_name: Name of fact table

    Returns:
        (earliest_unpublished_ts, latest_unpublished_ts)
        Both None if no unpublished data exists

    Example:
        >>> async with get_retail_session() as session:
        ...     start, end = await get_unpublished_data_range(
        ...         session,
        ...         "fact_receipts"
        ...     )
        ...     if start:
        ...         print(f"Unpublished data from {start} to {end}")
    """
    # Get watermark
    stmt = select(FactDataWatermark).where(
        FactDataWatermark.fact_table_name == fact_table_name
    )
    result = await session.execute(stmt)
    watermark = result.scalar_one_or_none()

    if watermark is None or watermark.earliest_unpublished_ts is None:
        return None, None

    # Get the ORM class for this table
    table_class = FACT_TABLE_MAPPING.get(fact_table_name)
    if table_class is None:
        logger.error(f"Unknown fact table: {fact_table_name}")
        return None, None

    # Query max event_ts for unpublished data
    stmt = select(func.max(table_class.event_ts)).where(
        table_class.event_ts >= watermark.earliest_unpublished_ts
    )
    result = await session.execute(stmt)
    latest_ts = result.scalar_one_or_none()

    return watermark.earliest_unpublished_ts, latest_ts


async def purge_published_data(
    session: AsyncSession,
    fact_table_name: str,
    keep_buffer_hours: int = DEFAULT_BUFFER_HOURS,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Delete published data older than buffer period.

    Args:
        session: AsyncSession for retail.db
        fact_table_name: Name of fact table to purge
        keep_buffer_hours: Keep this many recent hours (safety buffer)
        dry_run: If True, report what would be deleted without deleting

    Returns:
        {
            "rows_deleted": int,
            "purge_cutoff_ts": datetime | None,
            "disk_space_freed_mb": float,
            "dry_run": bool,
        }

    Safety Rules:
        - Never purge unpublished data
        - Always keep buffer_hours of recent data
        - Purge only if cutoff < earliest_unpublished_ts

    Example:
        >>> async with get_retail_session() as session:
        ...     result = await purge_published_data(
        ...         session,
        ...         "fact_receipts",
        ...         keep_buffer_hours=24,
        ...         dry_run=True
        ...     )
        ...     print(f"Would delete {result['rows_deleted']} rows")
    """
    # Get watermark
    stmt = select(FactDataWatermark).where(
        FactDataWatermark.fact_table_name == fact_table_name
    )
    result = await session.execute(stmt)
    watermark = result.scalar_one_or_none()

    if watermark is None or watermark.latest_published_ts is None:
        logger.info(f"No published data to purge for {fact_table_name}")
        return {
            "rows_deleted": 0,
            "purge_cutoff_ts": None,
            "disk_space_freed_mb": 0.0,
            "dry_run": dry_run,
        }

    # Calculate purge cutoff (published - buffer)
    buffer = timedelta(hours=keep_buffer_hours)
    purge_cutoff = watermark.latest_published_ts - buffer

    # Safety check: Don't purge if cutoff is after earliest_unpublished
    if watermark.earliest_unpublished_ts is not None:
        if purge_cutoff >= watermark.earliest_unpublished_ts:
            logger.warning(
                f"Purge cutoff {purge_cutoff} would delete unpublished data "
                f"(earliest_unpublished={watermark.earliest_unpublished_ts}). "
                f"Adjusting cutoff to maintain safety."
            )
            # Adjust cutoff to 1 second before unpublished data
            purge_cutoff = watermark.earliest_unpublished_ts - timedelta(seconds=1)

    # Get the ORM class for this table
    table_class = FACT_TABLE_MAPPING.get(fact_table_name)
    if table_class is None:
        logger.error(f"Unknown fact table: {fact_table_name}")
        return {
            "rows_deleted": 0,
            "purge_cutoff_ts": None,
            "disk_space_freed_mb": 0.0,
            "dry_run": dry_run,
        }

    # Count rows to delete
    count_stmt = select(func.count()).select_from(table_class).where(
        table_class.event_ts < purge_cutoff
    )
    result = await session.execute(count_stmt)
    rows_to_delete = result.scalar_one()

    if rows_to_delete == 0:
        logger.info(f"No rows to purge for {fact_table_name} (cutoff={purge_cutoff})")
        return {
            "rows_deleted": 0,
            "purge_cutoff_ts": purge_cutoff,
            "disk_space_freed_mb": 0.0,
            "dry_run": dry_run,
        }

    if dry_run:
        logger.info(
            f"DRY RUN: Would delete {rows_to_delete} rows from {fact_table_name} "
            f"before {purge_cutoff}"
        )
        return {
            "rows_deleted": rows_to_delete,
            "purge_cutoff_ts": purge_cutoff,
            "disk_space_freed_mb": 0.0,  # Can't estimate without actual deletion
            "dry_run": True,
        }

    # Get database size before purge (for disk space calculation)
    size_before_stmt = text("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
    result = await session.execute(size_before_stmt)
    size_before_bytes = result.scalar_one()

    # Delete in batches to avoid long-running transactions
    total_deleted = 0
    while True:
        # Delete batch using SQLite's DELETE ... LIMIT syntax
        # Note: We need to use a subquery for the ID selection in SQLite
        delete_stmt = text(
            f"DELETE FROM {fact_table_name} "
            f"WHERE rowid IN ("
            f"  SELECT rowid FROM {fact_table_name} "
            f"  WHERE event_ts < :cutoff "
            f"  LIMIT :batch_size"
            f")"
        )
        result = await session.execute(
            delete_stmt,
            {"cutoff": purge_cutoff, "batch_size": BATCH_SIZE}
        )
        deleted = result.rowcount

        if deleted == 0:
            break

        total_deleted += deleted
        logger.debug(f"Deleted {deleted} rows from {fact_table_name} (total: {total_deleted})")

        # Commit batch
        await session.commit()

        # Exit if we've deleted fewer than batch size (last batch)
        if deleted < BATCH_SIZE:
            break

    # Update watermark last_purge_ts
    # Reload watermark in case it was updated elsewhere
    stmt = select(FactDataWatermark).where(
        FactDataWatermark.fact_table_name == fact_table_name
    )
    result = await session.execute(stmt)
    watermark = result.scalar_one()
    watermark.last_purge_ts = datetime.utcnow()
    await session.commit()

    # Run VACUUM to reclaim disk space
    # Note: VACUUM must be outside transaction
    await session.execute(text("VACUUM"))

    # Get database size after purge
    result = await session.execute(size_before_stmt)
    size_after_bytes = result.scalar_one()
    space_freed_mb = (size_before_bytes - size_after_bytes) / (1024 * 1024)

    logger.info(
        f"Purged {total_deleted} rows from {fact_table_name} "
        f"before {purge_cutoff}. Freed {space_freed_mb:.2f} MB."
    )

    return {
        "rows_deleted": total_deleted,
        "purge_cutoff_ts": purge_cutoff,
        "disk_space_freed_mb": space_freed_mb,
        "dry_run": False,
    }


async def purge_all_fact_tables(
    session: AsyncSession,
    keep_buffer_hours: int = DEFAULT_BUFFER_HOURS,
    dry_run: bool = False,
) -> dict[str, dict[str, Any]]:
    """
    Purge all fact tables in one operation.

    Args:
        session: AsyncSession for retail.db
        keep_buffer_hours: Keep this many recent hours (safety buffer)
        dry_run: If True, report what would be deleted without deleting

    Returns:
        Dictionary mapping table names to purge results

    Example:
        >>> async with get_retail_session() as session:
        ...     results = await purge_all_fact_tables(
        ...         session,
        ...         keep_buffer_hours=24,
        ...         dry_run=False
        ...     )
        ...     for table, result in results.items():
        ...         print(f"{table}: {result['rows_deleted']} rows deleted")
    """
    results = {}

    for fact_table_name in FACT_TABLE_MAPPING.keys():
        try:
            result = await purge_published_data(
                session,
                fact_table_name,
                keep_buffer_hours=keep_buffer_hours,
                dry_run=dry_run,
            )
            results[fact_table_name] = result
        except Exception as e:
            logger.error(f"Failed to purge {fact_table_name}: {e}")
            results[fact_table_name] = {
                "error": str(e),
                "rows_deleted": 0,
                "purge_cutoff_ts": None,
                "disk_space_freed_mb": 0.0,
                "dry_run": dry_run,
            }

    # Log summary
    total_deleted = sum(r.get("rows_deleted", 0) for r in results.values())
    total_freed_mb = sum(r.get("disk_space_freed_mb", 0.0) for r in results.values())

    if dry_run:
        logger.info(
            f"DRY RUN: Would delete {total_deleted} rows total across all tables"
        )
    else:
        logger.info(
            f"Purged {total_deleted} rows total across all tables. "
            f"Freed {total_freed_mb:.2f} MB."
        )

    return results


async def get_purge_candidates(
    session: AsyncSession,
    keep_buffer_hours: int = DEFAULT_BUFFER_HOURS,
) -> dict[str, dict[str, Any]]:
    """
    Get summary of data eligible for purging across all tables.

    Args:
        session: AsyncSession for retail.db
        keep_buffer_hours: Buffer hours to calculate purge cutoff

    Returns:
        {
            "fact_receipts": {
                "earliest_published": datetime | None,
                "latest_published": datetime | None,
                "purge_cutoff": datetime | None,
                "estimated_rows": int,
                "earliest_unpublished": datetime | None,
            },
            ...
        }

    Example:
        >>> async with get_retail_session() as session:
        ...     candidates = await get_purge_candidates(session)
        ...     for table, info in candidates.items():
        ...         if info['estimated_rows'] > 0:
        ...             print(f"{table}: {info['estimated_rows']} rows eligible")
    """
    candidates = {}

    for fact_table_name in FACT_TABLE_MAPPING.keys():
        try:
            # Get watermark
            stmt = select(FactDataWatermark).where(
                FactDataWatermark.fact_table_name == fact_table_name
            )
            result = await session.execute(stmt)
            watermark = result.scalar_one_or_none()

            if watermark is None or watermark.latest_published_ts is None:
                candidates[fact_table_name] = {
                    "earliest_published": None,
                    "latest_published": None,
                    "purge_cutoff": None,
                    "estimated_rows": 0,
                    "earliest_unpublished": None,
                }
                continue

            # Calculate purge cutoff
            buffer = timedelta(hours=keep_buffer_hours)
            purge_cutoff = watermark.latest_published_ts - buffer

            # Adjust cutoff if it would delete unpublished data
            if watermark.earliest_unpublished_ts is not None:
                if purge_cutoff >= watermark.earliest_unpublished_ts:
                    purge_cutoff = watermark.earliest_unpublished_ts - timedelta(seconds=1)

            # Get ORM class
            table_class = FACT_TABLE_MAPPING.get(fact_table_name)
            if table_class is None:
                continue

            # Count eligible rows
            count_stmt = select(func.count()).select_from(table_class).where(
                table_class.event_ts < purge_cutoff
            )
            result = await session.execute(count_stmt)
            estimated_rows = result.scalar_one()

            # Get earliest published timestamp
            min_stmt = select(func.min(table_class.event_ts))
            result = await session.execute(min_stmt)
            earliest_ts = result.scalar_one_or_none()

            candidates[fact_table_name] = {
                "earliest_published": earliest_ts,
                "latest_published": watermark.latest_published_ts,
                "purge_cutoff": purge_cutoff,
                "estimated_rows": estimated_rows,
                "earliest_unpublished": watermark.earliest_unpublished_ts,
            }

        except Exception as e:
            logger.error(f"Failed to get purge candidates for {fact_table_name}: {e}")
            candidates[fact_table_name] = {
                "error": str(e),
                "earliest_published": None,
                "latest_published": None,
                "purge_cutoff": None,
                "estimated_rows": 0,
                "earliest_unpublished": None,
            }

    return candidates


async def get_watermark_status(
    session: AsyncSession,
) -> dict[str, dict[str, Any]]:
    """
    Get watermark status for all fact tables.

    Args:
        session: AsyncSession for retail.db

    Returns:
        Dictionary mapping table names to watermark info:
        {
            "fact_receipts": {
                "earliest_unpublished_ts": datetime | None,
                "latest_published_ts": datetime | None,
                "last_purge_ts": datetime | None,
                "is_fully_published": bool,
                "publication_lag_seconds": float | None,
            },
            ...
        }

    Example:
        >>> async with get_retail_session() as session:
        ...     status = await get_watermark_status(session)
        ...     for table, info in status.items():
        ...         print(f"{table}: {info['is_fully_published']}")
    """
    status = {}

    # Get all watermarks
    stmt = select(FactDataWatermark)
    result = await session.execute(stmt)
    watermarks = result.scalars().all()

    # Create status for all fact tables
    for fact_table_name in FACT_TABLE_MAPPING.keys():
        watermark = next(
            (w for w in watermarks if w.fact_table_name == fact_table_name),
            None
        )

        if watermark is None:
            status[fact_table_name] = {
                "earliest_unpublished_ts": None,
                "latest_published_ts": None,
                "last_purge_ts": None,
                "is_fully_published": False,
                "publication_lag_seconds": None,
            }
        else:
            status[fact_table_name] = {
                "earliest_unpublished_ts": watermark.earliest_unpublished_ts,
                "latest_published_ts": watermark.latest_published_ts,
                "last_purge_ts": watermark.last_purge_ts,
                "is_fully_published": watermark.is_fully_published(),
                "publication_lag_seconds": watermark.get_publication_lag_seconds(),
            }

    return status
