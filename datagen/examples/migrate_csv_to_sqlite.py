"""
Example: Migrate existing CSV data to SQLite databases.

This example demonstrates how to use the migration utilities to convert
existing CSV data (master dimensions and fact tables) into SQLite databases.

Prerequisites:
    - Master CSV files in data/master/
    - Fact CSV files in data/facts/ (partitioned by dt=YYYY-MM-DD)

Usage:
    python examples/migrate_csv_to_sqlite.py
"""

import asyncio
from pathlib import Path

from retail_datagen.db import (
    init_databases,
    migrate_master_data_from_csv,
    migrate_fact_data_from_csv,
    validate_foreign_keys,
    get_master_session,
    get_facts_session,
)


async def main():
    """
    Main migration workflow.
    """
    print("=== CSV to SQLite Migration Example ===\n")

    # Step 1: Initialize databases (create tables)
    print("Step 1: Initializing databases...")
    await init_databases()
    print("✓ Databases initialized\n")

    # Step 2: Migrate master data
    print("Step 2: Migrating master dimension data...")
    master_dir = Path("data/master")

    if not master_dir.exists():
        print(f"ERROR: Master directory not found: {master_dir}")
        print("Please generate master data first using the web UI or API")
        return

    async with get_master_session() as session:
        def progress_callback(table: str, loaded: int, total: int):
            if total > 0:
                pct = (loaded / total) * 100
                print(f"  [{table}] {loaded:,} / {total:,} rows ({pct:.1f}%)")
            else:
                print(f"  [{table}] {loaded:,} rows")

        master_results = await migrate_master_data_from_csv(
            master_csv_dir=master_dir,
            session=session,
            batch_size=10000,
            progress_callback=progress_callback,
        )

    print("\n✓ Master data migrated:")
    for table, count in master_results.items():
        print(f"    {table}: {count:,} rows")

    # Step 3: Migrate fact data (if exists)
    print("\nStep 3: Migrating fact data...")
    facts_dir = Path("data/facts")

    if not facts_dir.exists():
        print(f"  Facts directory not found: {facts_dir}")
        print("  Skipping fact data migration (generate historical data first)")
    else:
        async with get_facts_session() as session:
            fact_results = await migrate_fact_data_from_csv(
                facts_csv_dir=facts_dir,
                session=session,
                batch_size=10000,
                progress_callback=progress_callback,
            )

        print("\n✓ Fact data migrated:")
        for table, count in fact_results.items():
            print(f"    {table}: {count:,} rows")

    # Step 4: Validate foreign keys
    print("\nStep 4: Validating foreign key relationships...")
    async with get_master_session() as master_session:
        async with get_facts_session() as facts_session:
            validation_results = await validate_foreign_keys(
                master_session, facts_session
            )

    print("\n✓ Validation results:")
    for check, passed in validation_results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"    {status} - {check}")

    print("\n=== Migration Complete ===")
    print("\nDatabases created:")
    print("  - data/db/master.db (dimension tables)")
    print("  - data/db/facts.db (fact tables)")
    print("\nYou can now use SQLite-based operations for data generation and streaming.")


if __name__ == "__main__":
    asyncio.run(main())
