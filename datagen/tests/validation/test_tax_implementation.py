"""Test script to verify tax rate implementation."""

import sys
from pathlib import Path
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.config.models import RetailConfig
from retail_datagen.shared.dictionary_loader import DictionaryLoader

def test_tax_rate_loading():
    """Test that tax rates load correctly."""
    print("=" * 60)
    print("Testing Tax Rate Implementation")
    print("=" * 60)

    # Load config
    config = RetailConfig.from_file(Path("config.json"))

    # Create dictionary loader
    loader = DictionaryLoader(config)

    # Load tax rates
    print("\n1. Loading tax rates from CSV...")
    tax_jurisdictions = loader.load_tax_rates()
    print(f"   ✓ Loaded {len(tax_jurisdictions)} tax jurisdictions")

    # Show structure
    print("\n2. Tax Jurisdiction Structure:")
    if tax_jurisdictions:
        sample = tax_jurisdictions[0]
        print(f"   StateCode: {sample.StateCode}")
        print(f"   County: {sample.County}")
        print(f"   City: {sample.City}")
        print(f"   CombinedRate: {sample.CombinedRate}")

    # Create mapping (simulating what MasterDataGenerator does)
    print("\n3. Creating (State, City) -> TaxRate mapping...")
    tax_rate_mapping = {}
    for tax_jurisdiction in tax_jurisdictions:
        key = (tax_jurisdiction.StateCode, tax_jurisdiction.City)
        tax_rate_mapping[key] = tax_jurisdiction.CombinedRate

    print(f"   ✓ Created mapping with {len(tax_rate_mapping)} entries")

    # Test lookups
    print("\n4. Testing Tax Rate Lookups:")
    test_cases = [
        ("CA", "Los Angeles"),
        ("CA", "San Francisco"),
        ("TX", "Houston"),
        ("NY", "New York City"),
        ("IL", "Chicago"),
    ]

    for state, city in test_cases:
        key = (state, city)
        rate = tax_rate_mapping.get(key, Decimal("0.07407"))
        status = "✓ Found" if key in tax_rate_mapping else "⚠ Default"
        print(f"   {status}: {city}, {state} -> {rate:.4f} ({rate * 100:.2f}%)")

    # Show sample of actual rates
    print("\n5. Sample of Actual Tax Rates (first 10):")
    for i, jurisdiction in enumerate(tax_jurisdictions[:10], 1):
        rate_pct = jurisdiction.CombinedRate * 100
        print(f"   {i:2}. {jurisdiction.City:20} {jurisdiction.StateCode} -> {jurisdiction.CombinedRate:.4f} ({rate_pct:.2f}%)")

    # Verify all rates are valid decimals between 0 and 0.15
    print("\n6. Validating All Tax Rates:")
    invalid_rates = []
    for jurisdiction in tax_jurisdictions:
        if not (Decimal("0") <= jurisdiction.CombinedRate <= Decimal("0.15")):
            invalid_rates.append(jurisdiction)

    if invalid_rates:
        print(f"   ✗ Found {len(invalid_rates)} invalid rates!")
        for jr in invalid_rates[:5]:
            print(f"     - {jr.City}, {jr.StateCode}: {jr.CombinedRate}")
    else:
        print(f"   ✓ All {len(tax_jurisdictions)} tax rates are valid (0-15%)")

    print("\n" + "=" * 60)
    print("Tax Rate Implementation Test Complete")
    print("=" * 60)

if __name__ == "__main__":
    test_tax_rate_loading()
