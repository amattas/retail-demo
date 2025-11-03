"""Test script to verify stores have proper tax rates assigned."""

import sys
from pathlib import Path
from decimal import Decimal
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.master_generator import MasterDataGenerator


def test_store_tax_rates():
    """Test that stores are assigned proper tax rates."""
    print("=" * 70)
    print("Testing Store Tax Rate Assignment")
    print("=" * 70)

    # Load config
    config = RetailConfig.from_file(Path("config.json"))

    # Create generator (but don't generate yet)
    generator = MasterDataGenerator(config)

    # Load dictionary data (this loads tax rates)
    print("\n1. Loading dictionary data...")
    generator.load_dictionaries()
    print(f"   ✓ Loaded {len(generator._tax_rate_mapping)} tax rate entries")

    # Show sample of tax rate mapping
    print("\n2. Sample Tax Rate Mapping:")
    sample_items = list(generator._tax_rate_mapping.items())[:5]
    for (state, city), rate in sample_items:
        print(f"   ({state}, {city:20}) -> {rate:.4f} ({rate * 100:.2f}%)")

    # Check if stores already exist
    stores_file = config.paths.master / "stores.csv"

    if stores_file.exists():
        print(f"\n3. Loading existing stores from {stores_file}...")
        stores_df = pd.read_csv(stores_file)

        print(f"   ✓ Loaded {len(stores_df)} stores")

        # Check if tax_rate column exists
        if "tax_rate" in stores_df.columns:
            print("\n4. Analyzing Store Tax Rates:")

            # Count stores with tax rates
            has_rate = stores_df["tax_rate"].notna().sum()
            no_rate = stores_df["tax_rate"].isna().sum()

            print(f"   Stores with tax rate: {has_rate}")
            print(f"   Stores without tax rate: {no_rate}")

            if has_rate > 0:
                # Show statistics
                tax_rates = stores_df["tax_rate"].dropna()
                print(f"\n   Tax Rate Statistics:")
                print(f"     Min: {tax_rates.min():.4f} ({tax_rates.min() * 100:.2f}%)")
                print(f"     Max: {tax_rates.max():.4f} ({tax_rates.max() * 100:.2f}%)")
                print(
                    f"     Mean: {tax_rates.mean():.4f} ({tax_rates.mean() * 100:.2f}%)"
                )
                print(
                    f"     Median: {tax_rates.median():.4f} ({tax_rates.median() * 100:.2f}%)"
                )

                # Show sample of stores with rates
                print("\n5. Sample Stores with Tax Rates:")
                # Merge with geography to show location details
                geography_file = config.paths.master / "geography.csv"
                if geography_file.exists():
                    geo_df = pd.read_csv(geography_file)
                    stores_with_geo = stores_df.merge(
                        geo_df, left_on="GeographyID", right_on="ID", suffixes=("", "_geo")
                    )

                    # Show first 10 stores
                    for i, row in stores_with_geo.head(10).iterrows():
                        store_id = row["ID"]
                        store_num = row["StoreNumber"]
                        city = row["City"]
                        state = row["State"]
                        tax_rate = row["tax_rate"]

                        if pd.notna(tax_rate):
                            tax_pct = Decimal(str(tax_rate)) * 100
                            print(
                                f"   Store {store_id:3} ({store_num}): "
                                f"{city:20} {state} -> {tax_rate:.4f} ({tax_pct:.2f}%)"
                            )
                        else:
                            print(
                                f"   Store {store_id:3} ({store_num}): "
                                f"{city:20} {state} -> NO RATE"
                            )

                    # Verify rates match expectations
                    print("\n6. Validating Tax Rates Against Mapping:")
                    mismatches = []
                    for _, row in stores_with_geo.iterrows():
                        state = row["State"]
                        city = row["City"]
                        store_rate = row.get("tax_rate")

                        # Look up expected rate
                        key = (state, city)
                        expected_rate = generator._tax_rate_mapping.get(
                            key, Decimal("0.07407")
                        )

                        # Compare (allowing small floating point differences)
                        if pd.notna(store_rate):
                            store_rate_decimal = Decimal(str(store_rate))
                            if abs(store_rate_decimal - expected_rate) > Decimal("0.0001"):
                                mismatches.append(
                                    {
                                        "store_id": row["ID"],
                                        "city": city,
                                        "state": state,
                                        "expected": expected_rate,
                                        "actual": store_rate_decimal,
                                    }
                                )

                    if mismatches:
                        print(f"   ✗ Found {len(mismatches)} mismatches:")
                        for mm in mismatches[:5]:
                            print(
                                f"     Store {mm['store_id']}: {mm['city']}, {mm['state']} - "
                                f"Expected {mm['expected']:.4f}, Got {mm['actual']:.4f}"
                            )
                    else:
                        print(f"   ✓ All {len(stores_with_geo)} stores have correct tax rates!")

                else:
                    print("   ⚠ Geography file not found, can't show location details")

        else:
            print("\n4. ✗ Tax Rate Column Not Found in stores.csv")
            print("   Available columns:", list(stores_df.columns))

    else:
        print(f"\n3. ⚠ Stores file not found: {stores_file}")
        print("   Run master data generation first to create stores.")

    print("\n" + "=" * 70)
    print("Store Tax Rate Test Complete")
    print("=" * 70)


if __name__ == "__main__":
    test_store_tax_rates()
