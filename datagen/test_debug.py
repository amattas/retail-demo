#!/usr/bin/env python
"""Debug script to test fact table generation."""
import asyncio
import tempfile
import os
from pathlib import Path
import json

# Add src to path
import sys
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.generators.master_generator import MasterDataGenerator
from retail_datagen.generators.fact_generator import FactDataGenerator
from retail_datagen.config.models import RetailConfig

async def test_fact_generation():
    """Test fact generation and check for missing tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create minimal config
        config_data = {
            "seed": 42,
            "volume": {
                "stores": 2,
                "dcs": 1,
                "trucks": 2,
                "total_customers": 100,
                "customers_per_day": 10,
                "items_per_ticket_mean": 5
            },
            "paths": {
                "dict": "data/dictionaries",
                "master": f"{tmpdir}/master",
                "facts": f"{tmpdir}/facts"
            },
            "historical": {
                "start_date": "2024-01-01",
                "end_date": "2024-01-01"
            },
            "realtime": {
                "emit_interval_ms": 500,
                "burst": 100
            },
            "stream": {
                "port": 8000,
                "host": "0.0.0.0",
                "hub": "retail-events"
            }
        }

        config_path = Path(tmpdir) / "config.json"
        with open(config_path, "w") as f:
            json.dump(config_data, f)

        config = RetailConfig.from_file(str(config_path))

        print("Step 1: Generating master data...")
        master_gen = MasterDataGenerator(config)
        master_stats = await master_gen.generate_all_master_data_async()
        print(f"Master data generated: {master_stats}")

        print("\nStep 2: Generating fact data...")
        # Track what's generated
        generated_tables = set()

        def progress_callback(update):
            if "table" in update and update.get("progress", 0) > 0:
                generated_tables.add(update["table"])

        fact_gen = FactDataGenerator(
            config,
            progress_callback=progress_callback
        )

        # Generate facts
        await fact_gen.generate_facts("2024-01-01", "2024-01-01")

        print(f"\nTables with progress reported: {sorted(generated_tables)}")

        # Check what files were actually created
        facts_dir = Path(tmpdir) / "facts"
        created_files = {}
        if facts_dir.exists():
            for table_dir in facts_dir.iterdir():
                if table_dir.is_dir():
                    files = list(table_dir.rglob("*.csv"))
                    created_files[table_dir.name] = len(files)
                    if files:
                        # Check file sizes
                        total_size = sum(f.stat().st_size for f in files)
                        print(f"  {table_dir.name}: {len(files)} files, {total_size} bytes total")
                        # Check first file for records
                        with open(files[0]) as f:
                            lines = f.readlines()
                            print(f"    First file has {len(lines)} lines (including header)")

        print(f"\nFiles created per table: {created_files}")

        # Expected tables
        expected_tables = [
            "receipts", "receipt_lines",
            "dc_inventory_txn", "truck_moves", "store_inventory_txn",
            "foot_traffic", "ble_pings", "marketing"
        ]

        missing_tables = set(expected_tables) - set(created_files.keys())
        if missing_tables:
            print(f"\n❌ MISSING TABLES: {sorted(missing_tables)}")

        empty_tables = [t for t, count in created_files.items() if count == 0]
        if empty_tables:
            print(f"\n⚠️  EMPTY TABLES: {sorted(empty_tables)}")

        return generated_tables, created_files, missing_tables

if __name__ == "__main__":
    generated, created, missing = asyncio.run(test_fact_generation())

    if missing:
        print("\n" + "="*60)
        print("BUG CONFIRMED: The following tables are NOT being generated:")
        for table in sorted(missing):
            print(f"  - {table}")
        sys.exit(1)
    else:
        print("\n✅ All expected tables were generated!")