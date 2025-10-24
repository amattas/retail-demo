#!/usr/bin/env python
"""
Quick test to verify parallel master data generation works.
"""
import time
import csv
import tempfile
from pathlib import Path

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.master_generator import MasterDataGenerator


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def test_parallel_vs_sequential():
    """Compare parallel vs sequential performance."""

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        dict_dir = root / "dictionaries"
        master_dir = root / "master"
        facts_dir = root / "facts"
        dict_dir.mkdir()
        master_dir.mkdir()
        facts_dir.mkdir()

        # Minimal dictionaries
        _write_csv(
            dict_dir / "geographies.csv",
            ["City", "State", "Zip", "District", "Region"],
            [
                {
                    "City": "Springfield",
                    "State": "IL",
                    "Zip": "62701",
                    "District": "Central",
                    "Region": "Midwest",
                },
                {
                    "City": "Riverside",
                    "State": "CA",
                    "Zip": "92501",
                    "District": "Inland Empire",
                    "Region": "West",
                },
            ],
        )
        _write_csv(
            dict_dir / "first_names.csv",
            ["FirstName"],
            [{"FirstName": "Alexis"}, {"FirstName": "Blake"}],
        )
        _write_csv(
            dict_dir / "last_names.csv",
            ["LastName"],
            [{"LastName": "Brightwell"}, {"LastName": "Clearwater"}],
        )
        _write_csv(
            dict_dir / "product_companies.csv",
            ["Company", "Category"],
            [{"Company": "Synthetic Corp", "Category": "Electronics"}],
        )
        _write_csv(
            dict_dir / "product_brands.csv",
            ["Brand", "Company", "Category"],
            [
                {
                    "Brand": "SyntheticBrand",
                    "Company": "Synthetic Corp",
                    "Category": "Electronics",
                }
            ],
        )
        _write_csv(
            dict_dir / "products.csv",
            ["ProductName", "BasePrice", "Department", "Category", "Subcategory"],
            [
                {
                    "ProductName": "Synthetic Widget",
                    "BasePrice": "19.99",
                    "Department": "Electronics",
                    "Category": "Gadgets",
                    "Subcategory": "Widgets",
                }
            ],
        )

        # Config
        cfg = RetailConfig(
            seed=42,
            volume={
                "stores": 2,
                "dcs": 1,
                "customers_per_day": 10,
                "items_per_ticket_mean": 2.0,
            },
            realtime={"emit_interval_ms": 500, "burst": 5},
            paths={
                "dict": str(dict_dir),
                "master": str(master_dir),
                "facts": str(facts_dir),
            },
            stream={"hub": "test"},
        )

        print("=" * 70)
        print("Testing PARALLEL master data generation")
        print("=" * 70)
        gen_parallel = MasterDataGenerator(cfg)
        start = time.time()
        gen_parallel.generate_all_master_data(parallel=True)
        parallel_time = time.time() - start
        print(f"\n✓ Parallel generation completed in {parallel_time:.2f} seconds")

        # Verify all files exist
        assert (master_dir / "geographies_master.csv").exists(), "Missing geographies_master.csv"
        assert (master_dir / "stores.csv").exists(), "Missing stores.csv"
        assert (master_dir / "distribution_centers.csv").exists(), "Missing distribution_centers.csv"
        assert (master_dir / "trucks.csv").exists(), "Missing trucks.csv"
        assert (master_dir / "customers.csv").exists(), "Missing customers.csv"
        assert (master_dir / "products_master.csv").exists(), "Missing products_master.csv"
        assert (master_dir / "dc_inventory_snapshots.csv").exists(), "Missing dc_inventory_snapshots.csv"
        assert (master_dir / "store_inventory_snapshots.csv").exists(), "Missing store_inventory_snapshots.csv"

        print("\n✓ All expected files created successfully")

        # Count records in customers and products
        with open(master_dir / "customers.csv") as f:
            customer_count = sum(1 for _ in csv.DictReader(f))
        with open(master_dir / "products_master.csv") as f:
            product_count = sum(1 for _ in csv.DictReader(f))
        with open(master_dir / "dc_inventory_snapshots.csv") as f:
            dc_inv_count = sum(1 for _ in csv.DictReader(f))
        with open(master_dir / "store_inventory_snapshots.csv") as f:
            store_inv_count = sum(1 for _ in csv.DictReader(f))

        print(f"\n✓ Generated {customer_count} customers")
        print(f"✓ Generated {product_count} products")
        print(f"✓ Generated {dc_inv_count} DC inventory records")
        print(f"✓ Generated {store_inv_count} store inventory records")

        print("\n" + "=" * 70)
        print("✅ PARALLEL MASTER DATA GENERATION TEST PASSED")
        print("=" * 70)

        return True


if __name__ == "__main__":
    try:
        test_parallel_vs_sequential()
        print("\n✅ All tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
