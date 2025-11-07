"""
Database initialization and schema management.

Provides utilities for creating databases, tables, and handling
initial setup and teardown operations.
"""

import logging
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from retail_datagen.db.config import DatabaseConfig
from retail_datagen.db.engine import (
    get_facts_engine,
    get_master_engine,
    get_retail_engine,
)
from retail_datagen.db.migration import migrate_to_unified_db, needs_migration
from retail_datagen.db.models.base import Base

logger = logging.getLogger(__name__)


def ensure_database_directories() -> None:
    """
    Ensure all required database directories exist.

    Creates parent directories for master, facts, and retail databases if they
    don't already exist. This prevents file system errors during
    database creation.

    Example:
        >>> ensure_database_directories()
        >>> # data/ directory now exists
    """
    master_path = Path(DatabaseConfig.MASTER_DB_PATH)
    facts_path = Path(DatabaseConfig.FACTS_DB_PATH)
    retail_path = Path(DatabaseConfig.RETAIL_DB_PATH)

    # Create parent directories
    master_path.parent.mkdir(parents=True, exist_ok=True)
    facts_path.parent.mkdir(parents=True, exist_ok=True)
    retail_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Ensured database directories exist: {retail_path.parent}")


async def init_databases() -> None:
    """
    Initialize database tables with automatic migration support.

    This function:
    1. Ensures database directories exist
    2. Checks if migration from split to unified database is needed
    3. Performs migration if necessary (preserving all data)
    4. Creates database files if they don't exist
    5. Initializes engines and creates tables
    6. Verifies connectivity

    Migration is automatically triggered when:
    - retail.db doesn't exist AND
    - Either master.db or facts.db exists

    After migration, the system switches to unified retail.db mode.
    Original split databases are preserved as backups.

    This should be called during application startup.

    Example:
        >>> await init_databases()

    Raises:
        RuntimeError: If database migration fails
        Exception: If database initialization fails
    """
    logger.info("Initializing databases...")

    # Ensure directories exist
    ensure_database_directories()

    # Check if migration is needed (split → unified)
    if needs_migration():
        logger.info(
            "Detected split databases. Starting migration to unified database..."
        )

        try:
            result = await migrate_to_unified_db()

            if result["success"]:
                logger.info(
                    f"✓ Migration successful: {len(result['tables_migrated'])} tables migrated"
                )
                logger.info(
                    f"  Row counts: {sum(result['row_counts'].values())} total rows"
                )
                logger.info(f"  Duration: {result['duration_seconds']:.2f} seconds")
                logger.info(f"  Backups created: {len(result['backups_created'])}")
            else:
                logger.error(f"✗ Migration failed: {result['errors']}")
                raise RuntimeError(f"Database migration failed: {result['errors']}")
        except Exception as e:
            logger.error(f"Migration failed with exception: {e}")
            raise RuntimeError(f"Database migration failed: {e}") from e

    # Determine which mode we're in
    if DatabaseConfig.is_unified_mode():
        logger.info("Using unified retail database mode")
        retail_engine = get_retail_engine()

        # Create all tables in retail.db
        async with retail_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Lightweight, non-destructive schema migrations for new columns
        try:
            async with retail_engine.begin() as conn:
                # Helper to check if a column exists on a table
                async def _has_column(table: str, column: str) -> bool:
                    res = await conn.execute(text(f"PRAGMA table_info('{table}')"))
                    cols = [row[1] for row in res.fetchall()]
                    return column in cols

                # dim_stores.tax_rate (nullable FLOAT)
                if not await _has_column("dim_stores", "tax_rate"):
                    await conn.execute(
                        text("ALTER TABLE dim_stores ADD COLUMN tax_rate FLOAT")
                    )
                    logger.info("Added missing column dim_stores.tax_rate")

                # fact_store_inventory_txn.source (nullable TEXT)
                if not await _has_column("fact_store_inventory_txn", "source"):
                    await conn.execute(
                        text(
                            "ALTER TABLE fact_store_inventory_txn ADD COLUMN source TEXT"
                        )
                    )
                    logger.info(
                        "Added missing column fact_store_inventory_txn.source"
                    )

                # Online order lines: add per-line lifecycle fields if missing
                ool_cols = [
                    ("picked_ts", "DATETIME"),
                    ("shipped_ts", "DATETIME"),
                    ("delivered_ts", "DATETIME"),
                    ("fulfillment_status", "TEXT"),
                    ("fulfillment_mode", "TEXT"),
                    ("node_type", "TEXT"),
                    ("node_id", "INTEGER"),
                ]
                for col, typ in ool_cols:
                    if not await _has_column("fact_online_order_lines", col):
                        await conn.execute(
                            text(
                                f"ALTER TABLE fact_online_order_lines ADD COLUMN {col} {typ}"
                            )
                        )
                        logger.info(
                            f"Added missing column fact_online_order_lines.{col}"
                        )

                # Online order headers: ensure only desired columns remain
                # If deprecated columns exist, rebuild the table without them
                res = await conn.execute(text("PRAGMA table_info('fact_online_orders')"))
                hdr_cols = [row[1] for row in res.fetchall()]
                deprecated = {"fulfillment_status", "fulfillment_mode", "node_type", "node_id"}
                needs_rebuild = any(col in hdr_cols for col in deprecated)
                # Also ensure completed_ts exists
                if "completed_ts" not in hdr_cols:
                    needs_rebuild = True

                if needs_rebuild:
                    logger.info("Rebuilding fact_online_orders to drop deprecated columns and ensure completed_ts")
                    await conn.execute(text(
                        """
                        CREATE TABLE IF NOT EXISTS fact_online_orders_new (
                            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            order_id_ext VARCHAR(100),
                            customer_id INTEGER NOT NULL,
                            event_ts DATETIME NOT NULL,
                            completed_ts DATETIME,
                            subtotal_amount FLOAT NOT NULL,
                            tax_amount FLOAT NOT NULL,
                            total_amount FLOAT NOT NULL,
                            payment_method VARCHAR(50) NOT NULL
                        )
                        """
                    ))
                    # Copy data from old table (mapping columns if present)
                    copy_sql = (
                        "INSERT INTO fact_online_orders_new (order_id, order_id_ext, customer_id, event_ts, completed_ts, subtotal_amount, tax_amount, total_amount, payment_method) "
                        "SELECT order_id, order_id_ext, customer_id, event_ts, "
                        + ("completed_ts" if "completed_ts" in hdr_cols else "NULL")
                        + ", subtotal_amount, tax_amount, total_amount, payment_method FROM fact_online_orders"
                    )
                    await conn.execute(text(copy_sql))
                    await conn.execute(text("DROP TABLE fact_online_orders"))
                    await conn.execute(text("ALTER TABLE fact_online_orders_new RENAME TO fact_online_orders"))
                    # Recreate essential indexes
                    await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_online_order_hdr_event_customer ON fact_online_orders (event_ts, customer_id)"))
                    logger.info("Rebuilt fact_online_orders with new schema")
        except Exception as e:
            logger.warning(f"Non-critical schema migration step failed: {e}")

        # Test connectivity
        try:
            async with retail_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info(
                f"✓ Retail database initialized: {DatabaseConfig.RETAIL_DB_PATH}"
            )
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    else:
        logger.info("Using legacy split database mode")
        # Keep existing split database initialization logic
        master_engine = get_master_engine()
        facts_engine = get_facts_engine()

        # Create all tables in both databases
        async with master_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with facts_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Test connectivity
        try:
            async with master_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info(
                f"✓ Master database initialized: {DatabaseConfig.MASTER_DB_PATH}"
            )

            async with facts_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info(f"✓ Facts database initialized: {DatabaseConfig.FACTS_DB_PATH}")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    logger.info("Database initialization completed")


async def migrate_fact_schema() -> None:
    """
    Validate/auto-migrate facts database schema to match current models.

    - Ensures fact_receipts has 'receipt_id_ext' column and index.
    - (Extend here for future, non-destructive ALTERs.)
    """
    engine = get_facts_engine()
    from sqlalchemy import text

    async with engine.begin() as conn:
        # Ensure receipt_id_ext exists
        try:
            res = await conn.execute(text("PRAGMA table_info('fact_receipts')"))
            cols = [row[1] for row in res.fetchall()]
            if "receipt_id_ext" not in cols:
                await conn.execute(
                    text("ALTER TABLE fact_receipts ADD COLUMN receipt_id_ext TEXT")
                )
                await conn.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS ix_fact_receipts_ext ON fact_receipts (receipt_id_ext)"
                    )
                )
                logger.info(
                    "Migrated fact_receipts: added receipt_id_ext column + index"
                )
        except Exception as e:
            logger.warning(f"Schema check/migration for fact_receipts failed: {e}")


async def create_all_tables(metadata, engine: AsyncEngine) -> None:
    """
    Create all tables defined in metadata for a specific engine.

    Args:
        metadata: SQLAlchemy MetaData object containing table definitions
        engine: AsyncEngine to create tables in

    Example:
        >>> from retail_datagen.db.models import MasterBase
        >>> master_engine = get_master_engine()
        >>> await create_all_tables(MasterBase.metadata, master_engine)

    Note:
        This function uses SQLAlchemy's create_all() which is idempotent.
        It will not recreate existing tables.
    """
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    logger.info(f"Created all tables for engine: {engine.url}")


async def drop_all_tables(metadata, engine: AsyncEngine) -> None:
    """
    Drop all tables defined in metadata for a specific engine.

    WARNING: This is a destructive operation that will delete all data!

    Args:
        metadata: SQLAlchemy MetaData object containing table definitions
        engine: AsyncEngine to drop tables from

    Example:
        >>> from retail_datagen.db.models import FactsBase
        >>> facts_engine = get_facts_engine()
        >>> await drop_all_tables(FactsBase.metadata, facts_engine)
    """
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    logger.warning(f"Dropped all tables for engine: {engine.url}")


async def reset_database(metadata, engine: AsyncEngine) -> None:
    """
    Reset a database by dropping and recreating all tables.

    WARNING: This is a destructive operation that will delete all data!

    Args:
        metadata: SQLAlchemy MetaData object containing table definitions
        engine: AsyncEngine to reset

    Example:
        >>> from retail_datagen.db.models import FactsBase
        >>> facts_engine = get_facts_engine()
        >>> await reset_database(FactsBase.metadata, facts_engine)
    """
    logger.warning(f"Resetting database: {engine.url}")
    await drop_all_tables(metadata, engine)
    await create_all_tables(metadata, engine)
    logger.info(f"Database reset complete: {engine.url}")


async def vacuum_database(engine: AsyncEngine) -> None:
    """
    Run VACUUM on a SQLite database to reclaim space and optimize.

    VACUUM rebuilds the database file, repacking it into a minimal amount
    of disk space. This is useful after deleting large amounts of data.

    Args:
        engine: AsyncEngine to vacuum

    Example:
        >>> facts_engine = get_facts_engine()
        >>> await vacuum_database(facts_engine)

    Note:
        VACUUM requires exclusive access to the database and can take
        significant time on large databases.
    """
    logger.info(f"Running VACUUM on database: {engine.url}")
    async with engine.begin() as conn:
        await conn.execute(text("VACUUM"))
    logger.info(f"VACUUM complete: {engine.url}")


async def analyze_database(engine: AsyncEngine) -> None:
    """
    Run ANALYZE on a SQLite database to update query optimizer statistics.

    ANALYZE gathers statistics about table contents to help the query
    planner choose optimal execution plans.

    Args:
        engine: AsyncEngine to analyze

    Example:
        >>> master_engine = get_master_engine()
        >>> await analyze_database(master_engine)

    Note:
        This should be run periodically on databases with changing data
        patterns to maintain optimal query performance.
    """
    logger.info(f"Running ANALYZE on database: {engine.url}")
    async with engine.begin() as conn:
        await conn.execute(text("ANALYZE"))
    logger.info(f"ANALYZE complete: {engine.url}")


async def get_database_info(engine: AsyncEngine) -> dict:
    """
    Get information about a SQLite database.

    Args:
        engine: AsyncEngine to inspect

    Returns:
        Dictionary containing database information including:
        - page_count: Number of pages in database
        - page_size: Size of each page in bytes
        - file_size: Total file size in bytes
        - journal_mode: Current journal mode (e.g., WAL)
        - foreign_keys: Whether foreign key constraints are enabled

    Example:
        >>> info = await get_database_info(get_master_engine())
        >>> print(f"Database size: {info['file_size']} bytes")
    """
    async with engine.begin() as conn:
        # Get page count
        result = await conn.execute(text("PRAGMA page_count"))
        page_count = result.scalar()

        # Get page size
        result = await conn.execute(text("PRAGMA page_size"))
        page_size = result.scalar()

        # Get journal mode
        result = await conn.execute(text("PRAGMA journal_mode"))
        journal_mode = result.scalar()

        # Get foreign keys setting
        result = await conn.execute(text("PRAGMA foreign_keys"))
        foreign_keys = result.scalar()

    info = {
        "page_count": page_count,
        "page_size": page_size,
        "file_size": page_count * page_size if page_count and page_size else 0,
        "journal_mode": journal_mode,
        "foreign_keys": bool(foreign_keys),
    }

    logger.debug(f"Database info for {engine.url}: {info}")
    return info


async def check_database_integrity(engine: AsyncEngine) -> bool:
    """
    Check SQLite database integrity.

    Runs PRAGMA integrity_check to verify database structure is valid.

    Args:
        engine: AsyncEngine to check

    Returns:
        True if database passes integrity check, False otherwise

    Example:
        >>> is_valid = await check_database_integrity(get_master_engine())
        >>> if not is_valid:
        ...     logger.error("Database corruption detected!")
    """
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("PRAGMA integrity_check"))
            status = result.scalar()

        if status == "ok":
            logger.info(f"Database integrity check passed: {engine.url}")
            return True
        else:
            logger.error(f"Database integrity check failed: {engine.url} - {status}")
            return False

    except Exception as e:
        logger.error(f"Database integrity check error: {engine.url} - {e}")
        return False
