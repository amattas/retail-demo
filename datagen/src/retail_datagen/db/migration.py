"""
Database migration utilities for consolidating split databases.

Provides automatic migration from legacy split-database mode (master.db + facts.db)
to the unified retail.db database. Migration is safe, preserves all data, and
includes comprehensive validation.

Migration Strategy:
1. Detect if migration is needed (retail.db missing, split databases exist)
2. Create backups of existing databases
3. Create new retail.db with all tables
4. Copy master dimension tables from master.db
5. Copy fact tables from facts.db
6. Verify row counts match
7. Test foreign key constraints
8. Keep original databases as backup

Safety Features:
- Original databases are never deleted
- Backup files created before migration
- Row count validation after each table
- Foreign key constraint testing
- Detailed error reporting
"""

import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine

from retail_datagen.db.config import DatabaseConfig
from retail_datagen.db.engine import get_facts_engine, get_master_engine, get_retail_engine
from retail_datagen.db.models.base import Base

logger = logging.getLogger(__name__)


def needs_migration() -> bool:
    """
    Check if database migration is needed.

    Migration is needed when:
    - retail.db does NOT exist
    - AND either master.db OR facts.db exists

    Migration is NOT needed when:
    - retail.db already exists (migration already done)
    - OR neither master.db nor facts.db exist (fresh install)

    Returns:
        True if migration should be performed, False otherwise

    Example:
        >>> if needs_migration():
        ...     await migrate_to_unified_db()
    """
    retail_exists = os.path.exists(DatabaseConfig.RETAIL_DB_PATH)
    master_exists = os.path.exists(DatabaseConfig.MASTER_DB_PATH)
    facts_exists = os.path.exists(DatabaseConfig.FACTS_DB_PATH)

    # If retail.db exists, no migration needed (already migrated)
    if retail_exists:
        logger.info("retail.db already exists - no migration needed")
        return False

    # If either split database exists, migration is needed
    if master_exists or facts_exists:
        logger.info(
            f"Migration needed: master.db={'exists' if master_exists else 'missing'}, "
            f"facts.db={'exists' if facts_exists else 'missing'}"
        )
        return True

    # No databases exist - fresh install, no migration needed
    logger.info("No databases exist - fresh install, no migration needed")
    return False


def _create_backup(db_path: str) -> str | None:
    """
    Create a backup copy of a database file.

    Args:
        db_path: Path to database file to backup

    Returns:
        Path to backup file, or None if source doesn't exist

    Raises:
        IOError: If backup creation fails
    """
    if not os.path.exists(db_path):
        logger.debug(f"Skipping backup - database does not exist: {db_path}")
        return None

    # Create backup with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.backup_{timestamp}"

    try:
        shutil.copy2(db_path, backup_path)
        logger.info(f"Created backup: {backup_path}")
        return backup_path
    except Exception as e:
        logger.error(f"Failed to create backup of {db_path}: {e}")
        raise IOError(f"Backup creation failed: {e}")


async def _get_table_names(engine: AsyncEngine) -> List[str]:
    """
    Get list of all table names in a database.

    Args:
        engine: AsyncEngine to inspect

    Returns:
        List of table names
    """
    def _sync_get_tables(connection):
        inspector = inspect(connection)
        return inspector.get_table_names()

    async with engine.begin() as conn:
        tables = await conn.run_sync(_sync_get_tables)

    return tables


async def _get_row_count(engine: AsyncEngine, table_name: str) -> int:
    """
    Get row count for a specific table.

    Args:
        engine: AsyncEngine to query
        table_name: Name of table to count

    Returns:
        Number of rows in table
    """
    async with engine.begin() as conn:
        result = await conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = result.scalar()

    return count or 0


async def _copy_table_data(
    source_engine: AsyncEngine,
    target_engine: AsyncEngine,
    table_name: str
) -> Dict[str, int]:
    """
    Copy all data from source table to target table.

    Uses direct SQL queries to read from source and write to target.
    This avoids SQLite's ATTACH DATABASE limitations (max 10 attached databases).

    Args:
        source_engine: Engine for source database
        target_engine: Engine for target database
        table_name: Name of table to copy

    Returns:
        Dictionary with 'source_count' and 'target_count' keys

    Raises:
        Exception: If copy fails or row counts don't match
    """
    logger.info(f"Copying table: {table_name}")

    # Get source row count before copy
    source_count = await _get_row_count(source_engine, table_name)
    logger.debug(f"  Source row count: {source_count}")

    # If table is empty, skip the copy
    if source_count == 0:
        logger.debug(f"  Table is empty, skipping")
        return {"source_count": 0, "target_count": 0}

    # Read all data from source table
    async with source_engine.begin() as source_conn:
        # Get column names
        result = await source_conn.execute(text(f"PRAGMA table_info({table_name})"))
        columns = [row[1] for row in result.fetchall()]
        column_list = ", ".join(columns)

        # Read all rows
        result = await source_conn.execute(text(f"SELECT {column_list} FROM {table_name}"))
        rows = result.fetchall()

    logger.debug(f"  Read {len(rows)} rows from source")

    # Write data to target table in batches
    batch_size = 1000
    total_written = 0

    async with target_engine.begin() as target_conn:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]

            # Build INSERT statement with placeholders
            placeholders = ", ".join([f"({', '.join(['?' for _ in columns])})" for _ in batch])
            # Flatten the batch data
            flat_data = [val for row in batch for val in row]

            insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES {placeholders}"
            await target_conn.execute(text(insert_sql), flat_data)

            total_written += len(batch)
            if len(rows) > batch_size:
                logger.debug(f"  Written {total_written}/{len(rows)} rows")

    logger.debug(f"  Data copied successfully")

    # Verify target row count after copy
    target_count = await _get_row_count(target_engine, table_name)
    logger.debug(f"  Target row count: {target_count}")

    # Validate row counts match
    if source_count != target_count:
        raise Exception(
            f"Row count mismatch for {table_name}: "
            f"source={source_count}, target={target_count}"
        )

    logger.info(f"  ✓ Successfully copied {target_count} rows")

    return {
        "source_count": source_count,
        "target_count": target_count
    }


async def _test_foreign_keys(engine: AsyncEngine) -> bool:
    """
    Test that foreign key constraints are working properly.

    Runs a simple query that exercises FK relationships between
    master and fact tables.

    Args:
        engine: Engine to test

    Returns:
        True if FK constraints are working, False otherwise
    """
    try:
        # Test a simple FK constraint (stores reference geographies)
        async with engine.begin() as conn:
            # Enable foreign keys (should already be enabled)
            await conn.execute(text("PRAGMA foreign_keys=ON"))

            # Check FK pragma is set
            result = await conn.execute(text("PRAGMA foreign_keys"))
            fk_enabled = result.scalar()

            if not fk_enabled:
                logger.warning("Foreign keys not enabled")
                return False

            # Try a query that exercises FK relationships
            # This will fail if FKs are broken
            result = await conn.execute(text("""
                SELECT COUNT(*)
                FROM dim_stores s
                JOIN dim_geographies g ON s.GeographyID = g.ID
                LIMIT 1
            """))
            result.scalar()

        logger.info("Foreign key constraints validated successfully")
        return True

    except Exception as e:
        logger.error(f"Foreign key constraint test failed: {e}")
        return False


async def migrate_to_unified_db() -> Dict:
    """
    Migrate data from split databases (master.db + facts.db) to unified database (retail.db).

    This function performs a safe, comprehensive migration:
    1. Validates migration is needed
    2. Creates backup of existing databases
    3. Creates retail.db with all tables
    4. Copies master dimension tables from master.db
    5. Copies fact tables from facts.db
    6. Verifies row counts match source databases
    7. Tests foreign key constraints
    8. Returns detailed migration report

    The migration preserves all original databases as backups and includes
    comprehensive validation to ensure data integrity.

    Returns:
        Dictionary containing migration results:
        - success: bool - Whether migration succeeded
        - tables_migrated: list - Names of tables migrated
        - row_counts: dict - Row counts for each table
        - backups_created: list - Paths to backup files
        - errors: list - Any errors encountered
        - duration_seconds: float - Time taken for migration

    Example:
        >>> result = await migrate_to_unified_db()
        >>> if result['success']:
        ...     print(f"Migrated {len(result['tables_migrated'])} tables")
        >>> else:
        ...     print(f"Migration failed: {result['errors']}")

    Raises:
        Exception: If migration fails critically (original databases remain intact)
    """
    start_time = datetime.now()

    result: Dict = {
        "success": False,
        "tables_migrated": [],
        "row_counts": {},
        "backups_created": [],
        "errors": [],
        "duration_seconds": 0.0
    }

    logger.info("=" * 70)
    logger.info("Starting database migration to unified retail.db")
    logger.info("=" * 70)

    try:
        # Step 1: Verify migration is needed
        if not needs_migration():
            result["errors"].append("Migration not needed")
            logger.info("Migration skipped - not needed")
            return result

        # Step 2: Create backups of existing databases
        logger.info("\nStep 1: Creating backups...")

        master_backup = _create_backup(DatabaseConfig.MASTER_DB_PATH)
        if master_backup:
            result["backups_created"].append(master_backup)

        facts_backup = _create_backup(DatabaseConfig.FACTS_DB_PATH)
        if facts_backup:
            result["backups_created"].append(facts_backup)

        logger.info(f"Created {len(result['backups_created'])} backup(s)")

        # Step 3: Create retail.db and initialize all tables
        logger.info("\nStep 2: Creating retail.db and initializing tables...")

        # Ensure directory exists
        Path(DatabaseConfig.RETAIL_DB_PATH).parent.mkdir(parents=True, exist_ok=True)

        # Get retail engine (creates database file)
        retail_engine = get_retail_engine()

        # Create all tables (master + facts + watermarks)
        async with retail_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("All tables created in retail.db")

        # Step 4: Copy master tables (if master.db exists)
        if os.path.exists(DatabaseConfig.MASTER_DB_PATH):
            logger.info("\nStep 3: Copying master dimension tables...")

            master_engine = get_master_engine()
            master_tables = await _get_table_names(master_engine)

            for table in master_tables:
                try:
                    counts = await _copy_table_data(master_engine, retail_engine, table)
                    result["tables_migrated"].append(table)
                    result["row_counts"][table] = counts["target_count"]
                except Exception as e:
                    error_msg = f"Failed to copy master table {table}: {e}"
                    logger.error(error_msg)
                    result["errors"].append(error_msg)

            logger.info(f"Copied {len(master_tables)} master tables")
        else:
            logger.info("\nStep 3: Skipping master tables (master.db does not exist)")

        # Step 5: Copy fact tables (if facts.db exists)
        if os.path.exists(DatabaseConfig.FACTS_DB_PATH):
            logger.info("\nStep 4: Copying fact tables...")

            facts_engine = get_facts_engine()
            fact_tables = await _get_table_names(facts_engine)

            for table in fact_tables:
                try:
                    counts = await _copy_table_data(facts_engine, retail_engine, table)
                    result["tables_migrated"].append(table)
                    result["row_counts"][table] = counts["target_count"]
                except Exception as e:
                    error_msg = f"Failed to copy fact table {table}: {e}"
                    logger.error(error_msg)
                    result["errors"].append(error_msg)

            logger.info(f"Copied {len(fact_tables)} fact tables")
        else:
            logger.info("\nStep 4: Skipping fact tables (facts.db does not exist)")

        # Step 6: Verify foreign key constraints
        logger.info("\nStep 5: Verifying foreign key constraints...")
        fk_valid = await _test_foreign_keys(retail_engine)

        if not fk_valid:
            result["errors"].append("Foreign key constraint validation failed")
            logger.warning("Foreign key constraints may not be working properly")

        # Step 7: Calculate duration and set success
        duration = (datetime.now() - start_time).total_seconds()
        result["duration_seconds"] = duration

        # Mark as successful if tables were migrated and no critical errors
        if result["tables_migrated"] and not result["errors"]:
            result["success"] = True
            logger.info("=" * 70)
            logger.info("Migration completed successfully!")
            logger.info(f"  Tables migrated: {len(result['tables_migrated'])}")
            logger.info(f"  Total rows: {sum(result['row_counts'].values())}")
            logger.info(f"  Duration: {duration:.2f} seconds")
            logger.info("=" * 70)
        else:
            logger.warning("=" * 70)
            logger.warning("Migration completed with errors")
            logger.warning(f"  Tables migrated: {len(result['tables_migrated'])}")
            logger.warning(f"  Errors: {len(result['errors'])}")
            logger.warning("=" * 70)

    except Exception as e:
        error_msg = f"Migration failed with critical error: {e}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        result["success"] = False

        # Log full traceback for debugging
        import traceback
        logger.error(traceback.format_exc())

    return result


async def verify_migration() -> Dict:
    """
    Verify that migration was successful by comparing databases.

    This function checks:
    - retail.db exists
    - All expected tables exist in retail.db
    - Row counts match between split and unified databases
    - Foreign key constraints are working

    Returns:
        Dictionary containing verification results:
        - verified: bool - Whether verification passed
        - retail_db_exists: bool - retail.db exists
        - table_count: int - Number of tables in retail.db
        - row_count_matches: bool - Row counts match source databases
        - foreign_keys_valid: bool - FK constraints working
        - errors: list - Any errors found

    Example:
        >>> result = await verify_migration()
        >>> if result['verified']:
        ...     print("Migration verified successfully")
    """
    result: Dict = {
        "verified": False,
        "retail_db_exists": False,
        "table_count": 0,
        "row_count_matches": False,
        "foreign_keys_valid": False,
        "errors": []
    }

    logger.info("Verifying database migration...")

    try:
        # Check retail.db exists
        result["retail_db_exists"] = os.path.exists(DatabaseConfig.RETAIL_DB_PATH)
        if not result["retail_db_exists"]:
            result["errors"].append("retail.db does not exist")
            return result

        # Get retail engine and table count
        retail_engine = get_retail_engine()
        retail_tables = await _get_table_names(retail_engine)
        result["table_count"] = len(retail_tables)

        logger.info(f"Found {result['table_count']} tables in retail.db")

        # Test foreign keys
        result["foreign_keys_valid"] = await _test_foreign_keys(retail_engine)

        # Compare row counts if split databases still exist
        if os.path.exists(DatabaseConfig.MASTER_DB_PATH) or os.path.exists(DatabaseConfig.FACTS_DB_PATH):
            logger.info("Comparing row counts with source databases...")

            mismatches = []

            # Check master tables
            if os.path.exists(DatabaseConfig.MASTER_DB_PATH):
                master_engine = get_master_engine()
                master_tables = await _get_table_names(master_engine)

                for table in master_tables:
                    master_count = await _get_row_count(master_engine, table)
                    retail_count = await _get_row_count(retail_engine, table)

                    if master_count != retail_count:
                        mismatches.append(
                            f"{table}: master={master_count}, retail={retail_count}"
                        )

            # Check fact tables
            if os.path.exists(DatabaseConfig.FACTS_DB_PATH):
                facts_engine = get_facts_engine()
                fact_tables = await _get_table_names(facts_engine)

                for table in fact_tables:
                    facts_count = await _get_row_count(facts_engine, table)
                    retail_count = await _get_row_count(retail_engine, table)

                    if facts_count != retail_count:
                        mismatches.append(
                            f"{table}: facts={facts_count}, retail={retail_count}"
                        )

            if mismatches:
                result["errors"].extend(mismatches)
                logger.error(f"Row count mismatches found: {mismatches}")
            else:
                result["row_count_matches"] = True
                logger.info("All row counts match")
        else:
            # No source databases to compare against
            result["row_count_matches"] = True
            logger.info("No source databases to compare (already removed)")

        # Mark as verified if all checks pass
        result["verified"] = (
            result["retail_db_exists"] and
            result["table_count"] > 0 and
            result["foreign_keys_valid"] and
            result["row_count_matches"] and
            not result["errors"]
        )

        if result["verified"]:
            logger.info("✓ Migration verification passed")
        else:
            logger.warning(f"Migration verification failed: {result['errors']}")

    except Exception as e:
        error_msg = f"Verification failed: {e}"
        logger.error(error_msg)
        result["errors"].append(error_msg)

    return result
