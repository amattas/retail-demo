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
from retail_datagen.db.engine import get_facts_engine, get_master_engine

logger = logging.getLogger(__name__)


def ensure_database_directories() -> None:
    """
    Ensure all required database directories exist.

    Creates parent directories for master and facts databases if they
    don't already exist. This prevents file system errors during
    database creation.

    Example:
        >>> ensure_database_directories()
        >>> # data/ directory now exists
    """
    master_path = Path(DatabaseConfig.MASTER_DB_PATH)
    facts_path = Path(DatabaseConfig.FACTS_DB_PATH)

    # Create parent directories
    master_path.parent.mkdir(parents=True, exist_ok=True)
    facts_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Ensured database directories exist: {master_path.parent}, {facts_path.parent}"
    )


async def init_databases() -> None:
    """
    Initialize both master and facts databases.

    This function:
    1. Ensures database directories exist
    2. Creates database files if they don't exist
    3. Initializes engines (which applies PRAGMAs)
    4. Verifies connectivity

    This should be called during application startup.

    Example:
        >>> await init_databases()

    Raises:
        Exception: If database initialization fails
    """
    logger.info("Initializing databases...")

    # Ensure directories exist
    ensure_database_directories()

    # Initialize engines (creates files and applies PRAGMAs)
    master_engine = get_master_engine()
    facts_engine = get_facts_engine()

    # Test connectivity
    try:
        async with master_engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info(f"Master database initialized: {DatabaseConfig.MASTER_DB_PATH}")

        async with facts_engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info(f"Facts database initialized: {DatabaseConfig.FACTS_DB_PATH}")

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


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
            if 'receipt_id_ext' not in cols:
                await conn.execute(text("ALTER TABLE fact_receipts ADD COLUMN receipt_id_ext TEXT"))
                await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fact_receipts_ext ON fact_receipts (receipt_id_ext)"))
                logger.info("Migrated fact_receipts: added receipt_id_ext column + index")
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
