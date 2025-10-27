"""
CLI entry point for database management utilities.

Provides both migration and purge operations for retail data generator databases.

Migration Usage:
    python -m retail_datagen.db migrate --master
    python -m retail_datagen.db migrate --facts
    python -m retail_datagen.db migrate --all
    python -m retail_datagen.db migrate --validate

Purge Usage:
    python -m retail_datagen.db purge --table fact_receipts
    python -m retail_datagen.db purge --all
    python -m retail_datagen.db purge --dry-run
    python -m retail_datagen.db status
    python -m retail_datagen.db candidates
"""

import asyncio
import argparse
import sys
from pathlib import Path
from typing import Optional

from retail_datagen.db.migration import (
    migrate_master_data_from_csv,
    migrate_fact_data_from_csv,
    validate_foreign_keys,
)
from retail_datagen.db.purge import (
    purge_published_data,
    purge_all_fact_tables,
    get_purge_candidates,
    get_watermark_status,
    FACT_TABLE_MAPPING,
)
from retail_datagen.db.session import get_master_session, get_facts_session
from retail_datagen.db.init import init_databases


def progress_callback(table_name: str, rows_loaded: int, total_rows: int) -> None:
    """
    Progress callback for migration reporting.

    Args:
        table_name: Name of the table being migrated
        rows_loaded: Number of rows loaded so far
        total_rows: Total rows to load (-1 if unknown)
    """
    if total_rows > 0:
        pct = (rows_loaded / total_rows) * 100
        print(f"  [{table_name}] {rows_loaded:,} / {total_rows:,} rows ({pct:.1f}%)")
    else:
        print(f"  [{table_name}] {rows_loaded:,} rows loaded")


def format_datetime(dt) -> str:
    """Format datetime for CLI output."""
    if dt is None:
        return "N/A"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_duration(seconds: float | None) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds is None:
        return "N/A"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"


async def migrate_master_data(master_dir: Path, batch_size: int) -> None:
    """
    Migrate master dimension data from CSV to SQLite.

    Args:
        master_dir: Path to master CSV directory
        batch_size: Rows per batch
    """
    print(f"\n=== Migrating Master Data from {master_dir} ===\n")

    async with get_master_session() as session:
        results = await migrate_master_data_from_csv(
            master_csv_dir=master_dir,
            session=session,
            batch_size=batch_size,
            progress_callback=progress_callback,
        )

    print("\n=== Master Data Migration Summary ===")
    total_rows = 0
    for table_name, count in results.items():
        print(f"  {table_name}: {count:,} rows")
        total_rows += count
    print(f"\nTotal rows migrated: {total_rows:,}\n")


async def migrate_fact_data(facts_dir: Path, batch_size: int) -> None:
    """
    Migrate fact data from partitioned CSV to SQLite.

    Args:
        facts_dir: Path to facts CSV directory
        batch_size: Rows per batch
    """
    print(f"\n=== Migrating Fact Data from {facts_dir} ===\n")

    async with get_facts_session() as session:
        results = await migrate_fact_data_from_csv(
            facts_csv_dir=facts_dir,
            session=session,
            batch_size=batch_size,
            progress_callback=progress_callback,
        )

    print("\n=== Fact Data Migration Summary ===")
    total_rows = 0
    for table_name, count in results.items():
        print(f"  {table_name}: {count:,} rows")
        total_rows += count
    print(f"\nTotal rows migrated: {total_rows:,}\n")


async def validate_data() -> None:
    """
    Validate foreign key relationships across databases.
    """
    print("\n=== Validating Foreign Key Relationships ===\n")

    async with get_master_session() as master_session:
        async with get_facts_session() as facts_session:
            results = await validate_foreign_keys(master_session, facts_session)

    print("\n=== Validation Results ===")
    for check, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status} - {check}")
    print()


async def cmd_purge(args: argparse.Namespace) -> int:
    """
    Purge published data from fact tables.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    async with get_facts_session() as session:
        try:
            if args.all:
                # Purge all tables
                print(f"\n=== Purging All Fact Tables (buffer={args.buffer}h, dry_run={args.dry_run}) ===\n")
                results = await purge_all_fact_tables(
                    session,
                    keep_buffer_hours=args.buffer,
                    dry_run=args.dry_run,
                )

                # Display results
                print("\nPurge Results:")
                print("-" * 80)
                total_deleted = 0
                total_freed = 0.0

                for table_name, result in results.items():
                    if "error" in result:
                        print(f"  {table_name}: ERROR - {result['error']}")
                    else:
                        rows = result['rows_deleted']
                        freed = result['disk_space_freed_mb']
                        cutoff = format_datetime(result.get('purge_cutoff_ts'))

                        if rows > 0 or args.verbose:
                            print(f"  {table_name}:")
                            print(f"    Rows deleted: {rows:,}")
                            print(f"    Disk freed: {freed:.2f} MB")
                            print(f"    Cutoff: {cutoff}")

                        total_deleted += rows
                        total_freed += freed

                print("-" * 80)
                print(f"Total: {total_deleted:,} rows deleted, {total_freed:.2f} MB freed\n")

            elif args.table:
                # Purge single table
                table_name = args.table
                if table_name not in FACT_TABLE_MAPPING:
                    print(f"ERROR: Unknown table '{table_name}'")
                    print(f"Valid tables: {', '.join(FACT_TABLE_MAPPING.keys())}")
                    return 1

                print(f"\n=== Purging {table_name} (buffer={args.buffer}h, dry_run={args.dry_run}) ===\n")
                result = await purge_published_data(
                    session,
                    table_name,
                    keep_buffer_hours=args.buffer,
                    dry_run=args.dry_run,
                )

                # Display result
                print("Purge Result:")
                print("-" * 80)
                print(f"  Table: {table_name}")
                print(f"  Rows deleted: {result['rows_deleted']:,}")
                print(f"  Disk freed: {result['disk_space_freed_mb']:.2f} MB")
                print(f"  Cutoff: {format_datetime(result.get('purge_cutoff_ts'))}")
                print("-" * 80)
                print()

            else:
                print("ERROR: Must specify --table or --all")
                return 1

            return 0

        except Exception as e:
            print(f"ERROR: Purge failed: {e}")
            import traceback
            traceback.print_exc()
            return 1


async def cmd_status(args: argparse.Namespace) -> int:
    """
    Display watermark status for all fact tables.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    async with get_facts_session() as session:
        try:
            status = await get_watermark_status(session)

            print("\n=== Watermark Status ===")
            print("=" * 100)

            for table_name, info in status.items():
                print(f"\n{table_name}:")
                print(f"  Earliest unpublished: {format_datetime(info['earliest_unpublished_ts'])}")
                print(f"  Latest published:     {format_datetime(info['latest_published_ts'])}")
                print(f"  Last purge:           {format_datetime(info['last_purge_ts'])}")
                print(f"  Fully published:      {info['is_fully_published']}")

                lag = info['publication_lag_seconds']
                if lag is not None:
                    print(f"  Publication lag:      {format_duration(lag)}")
                else:
                    print(f"  Publication lag:      N/A")

            print("=" * 100)
            print()
            return 0

        except Exception as e:
            print(f"ERROR: Status check failed: {e}")
            import traceback
            traceback.print_exc()
            return 1


async def cmd_candidates(args: argparse.Namespace) -> int:
    """
    Display purge candidates for all fact tables.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    async with get_facts_session() as session:
        try:
            candidates = await get_purge_candidates(
                session,
                keep_buffer_hours=args.buffer,
            )

            print(f"\n=== Purge Candidates (buffer={args.buffer}h) ===")
            print("=" * 100)

            total_rows = 0
            for table_name, info in candidates.items():
                if "error" in info:
                    print(f"\n{table_name}: ERROR - {info['error']}")
                    continue

                rows = info['estimated_rows']
                if rows == 0 and not args.verbose:
                    continue

                print(f"\n{table_name}:")
                print(f"  Eligible rows:        {rows:,}")
                print(f"  Earliest published:   {format_datetime(info['earliest_published'])}")
                print(f"  Latest published:     {format_datetime(info['latest_published'])}")
                print(f"  Purge cutoff:         {format_datetime(info['purge_cutoff'])}")
                print(f"  Earliest unpublished: {format_datetime(info['earliest_unpublished'])}")

                total_rows += rows

            print("=" * 100)
            print(f"Total eligible rows: {total_rows:,}\n")
            return 0

        except Exception as e:
            print(f"ERROR: Candidate check failed: {e}")
            import traceback
            traceback.print_exc()
            return 1


async def cmd_migrate(args: argparse.Namespace) -> int:
    """
    Handle migrate subcommand.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Initialize databases if needed
        if args.init or args.all:
            print("\n=== Initializing Databases ===")
            await init_databases()
            print("Databases initialized successfully\n")

        # Migrate master data
        if args.master or args.all:
            master_dir = Path(args.master_dir)
            if not master_dir.exists():
                print(f"ERROR: Master directory not found: {master_dir}")
                return 1
            await migrate_master_data(master_dir, args.batch_size)

        # Migrate fact data
        if args.facts or args.all:
            facts_dir = Path(args.facts_dir)
            if not facts_dir.exists():
                print(f"ERROR: Facts directory not found: {facts_dir}")
                return 1
            await migrate_fact_data(facts_dir, args.batch_size)

        # Validate foreign keys
        if args.validate:
            await validate_data()

        print("=== Migration Complete ===\n")
        return 0

    except KeyboardInterrupt:
        print("\n\nMigration interrupted by user")
        return 1
    except Exception as e:
        print(f"\nERROR: Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


async def main(args: argparse.Namespace) -> int:
    """
    Main CLI entry point - routes to subcommands.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    if args.command == "migrate":
        return await cmd_migrate(args)
    elif args.command == "purge":
        return await cmd_purge(args)
    elif args.command == "status":
        return await cmd_status(args)
    elif args.command == "candidates":
        return await cmd_candidates(args)
    else:
        print("ERROR: No command specified. Use --help for usage information.")
        return 1


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Database management utilities for retail data generator",
        prog="python -m retail_datagen.db",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # ===== MIGRATE SUBCOMMAND =====
    migrate_parser = subparsers.add_parser(
        "migrate",
        help="Migrate data from CSV to SQLite databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate master data only
  python -m retail_datagen.db migrate --master

  # Migrate fact data only
  python -m retail_datagen.db migrate --facts

  # Migrate everything
  python -m retail_datagen.db migrate --all

  # Initialize databases before migration
  python -m retail_datagen.db migrate --all --init

  # Validate foreign keys
  python -m retail_datagen.db migrate --validate
        """,
    )
    migrate_parser.add_argument(
        "--master",
        action="store_true",
        help="Migrate master dimension tables",
    )
    migrate_parser.add_argument(
        "--facts",
        action="store_true",
        help="Migrate fact tables",
    )
    migrate_parser.add_argument(
        "--all",
        action="store_true",
        help="Migrate both master and fact tables",
    )
    migrate_parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate foreign key relationships",
    )
    migrate_parser.add_argument(
        "--init",
        action="store_true",
        help="Initialize databases before migration (create tables)",
    )
    migrate_parser.add_argument(
        "--master-dir",
        type=str,
        default="data/master",
        help="Path to master CSV directory (default: data/master)",
    )
    migrate_parser.add_argument(
        "--facts-dir",
        type=str,
        default="data/facts",
        help="Path to facts CSV directory (default: data/facts)",
    )
    migrate_parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Number of rows per batch insert (default: 10000)",
    )

    # ===== PURGE SUBCOMMAND =====
    purge_parser = subparsers.add_parser(
        "purge",
        help="Purge published data from fact tables",
    )
    purge_parser.add_argument(
        "--table",
        type=str,
        help="Purge specific table (e.g., fact_receipts)",
    )
    purge_parser.add_argument(
        "--all",
        action="store_true",
        help="Purge all fact tables",
    )
    purge_parser.add_argument(
        "--buffer",
        type=int,
        default=24,
        help="Keep this many hours of recent data (default: 24)",
    )
    purge_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be purged without deleting",
    )
    purge_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show all tables, including those with 0 rows deleted",
    )

    # ===== STATUS SUBCOMMAND =====
    status_parser = subparsers.add_parser(
        "status",
        help="Show watermark status for all fact tables",
    )

    # ===== CANDIDATES SUBCOMMAND =====
    candidates_parser = subparsers.add_parser(
        "candidates",
        help="Show purge candidates for all fact tables",
    )
    candidates_parser.add_argument(
        "--buffer",
        type=int,
        default=24,
        help="Buffer hours for calculating purge cutoff (default: 24)",
    )
    candidates_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show all tables, including those with 0 eligible rows",
    )

    args = parser.parse_args()

    # Validate migrate subcommand arguments
    if args.command == "migrate":
        if not any([args.master, args.facts, args.all, args.validate]):
            migrate_parser.error("At least one of --master, --facts, --all, or --validate is required")
    elif args.command == "purge":
        if not args.table and not args.all:
            purge_parser.error("Either --table or --all is required")
    elif args.command is None:
        parser.print_help()
        sys.exit(1)

    return args


if __name__ == "__main__":
    args = parse_args()
    exit_code = asyncio.run(main(args))
    sys.exit(exit_code)
