"""
Integration test for master data generation on a tiny dictionary set.

Validates that trucks and inventory snapshots are generated in SQLite database.
"""

import asyncio
import csv
import tempfile
from pathlib import Path

from sqlalchemy import select, func

from retail_datagen.config.models import RetailConfig
from retail_datagen.db.session import get_master_session
from retail_datagen.db.models.master import (
    Geography,
    Store,
    DistributionCenter,
    Truck,
    Customer,
    Product,
)
from retail_datagen.generators.master_generator import MasterDataGenerator


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def test_master_generation_exports_expected_files():
    """Test that master data generation creates records in SQLite database."""

    async def run_test():
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

            # Generate data using async method with database session
            # Use parallel=False for small test datasets to avoid race conditions
            async with get_master_session() as session:
                await gen.generate_all_master_data_async(session=session, parallel=False)

                # Verify data in database
                # Check geographies
                result = await session.execute(select(func.count()).select_from(Geography))
                geo_count = result.scalar()
                assert geo_count > 0, "Geographies should be generated"

                # Check stores
                result = await session.execute(select(func.count()).select_from(Store))
                store_count = result.scalar()
                assert store_count > 0, "Stores should be generated"

                # Check distribution centers
                result = await session.execute(select(func.count()).select_from(DistributionCenter))
                dc_count = result.scalar()
                assert dc_count > 0, "Distribution centers should be generated"

                # Check trucks
                result = await session.execute(select(func.count()).select_from(Truck))
                truck_count = result.scalar()
                assert truck_count > 0, "Trucks should be generated"

                # Check customers
                result = await session.execute(select(func.count()).select_from(Customer))
                customer_count = result.scalar()
                assert customer_count > 0, "Customers should be generated"

                # Check products
                result = await session.execute(select(func.count()).select_from(Product))
                product_count = result.scalar()
                assert product_count > 0, "Products should be generated"

    # Run the async test
    asyncio.run(run_test())
