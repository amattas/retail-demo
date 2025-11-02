#!/usr/bin/env python
"""Simple test to check which fact tables are generated."""
import tempfile
from pathlib import Path
import json
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.generators.master_generator import MasterDataGenerator
from retail_datagen.generators.fact_generator import FactDataGenerator
from retail_datagen.config.models import RetailConfig

def main():
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

        # Load dictionary data first
        master_gen._load_dictionary_data()

        # Generate master data synchronously (order matters!)
        master_gen.generate_geography_master()
        master_gen.generate_distribution_centers()  # Must be before stores
        master_gen.generate_stores()
        master_gen.generate_trucks()
        master_gen.generate_customers()
        master_gen.generate_products_master()
        master_gen.generate_dc_inventory_snapshots()
        master_gen.generate_store_inventory_snapshots()

        print("Master data generated successfully")

        print("\nStep 2: Generating fact data (synchronously)...")
        # Create a fact generator with None session (will use CSV output)
        fact_gen = FactDataGenerator(config, session=None)

        # Use the async generate method
        import asyncio
        from datetime import datetime
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 1)
        asyncio.run(fact_gen.generate_historical_data(start, end))

        print("\nStep 3: Checking generated files...")
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
                        print(f"  {table_dir.name}: {len(files)} files, {total_size:,} bytes total")
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

        if missing_tables:
            print("\n" + "="*60)
            print("BUG CONFIRMED: The following tables are NOT being generated:")
            for table in sorted(missing_tables):
                print(f"  - {table}")
            return 1
        else:
            print("\n✅ All expected tables were generated!")
            return 0

if __name__ == "__main__":
    sys.exit(main())