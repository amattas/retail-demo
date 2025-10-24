"""
Integration test for master data generation on a tiny dictionary set.

Validates that trucks and inventory snapshots are generated and exported.
"""

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


def test_master_generation_exports_expected_files():
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

        gen = MasterDataGenerator(cfg)
        gen.generate_all_master_data()

        # Check critical outputs
        assert (master_dir / "geographies_master.csv").exists()
        assert (master_dir / "stores.csv").exists()
        assert (master_dir / "distribution_centers.csv").exists()
        assert (master_dir / "customers.csv").exists()
        assert (master_dir / "products_master.csv").exists()
        # New expectations
        assert (master_dir / "trucks.csv").exists()
        # Snapshots are exported under same master_dir
        assert any(
            p.name.startswith("dc_inventory_snapshots")
            for p in master_dir.glob("*.csv")
        )
        assert any(
            p.name.startswith("store_inventory_snapshots")
            for p in master_dir.glob("*.csv")
        )
