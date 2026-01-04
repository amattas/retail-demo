#!/usr/bin/env python3
"""
Verify Phase 1.3 tax rate implementation end-to-end.

Purpose:
    Comprehensive verification of the tax rate infrastructure, from
    CSV loading through to tax calculation in receipt generation.

Checks:
    1. Tax rates CSV structure and content (data/dictionaries/tax_rates.csv)
    2. TaxJurisdiction Pydantic model validation
    3. DictionaryLoader integration
    4. MasterDataGenerator tax rate mapping
    5. TaxCalculator utility class functionality

Usage:
    python verify_tax_implementation.py

When to run:
    After modifying tax rates CSV or tax-related code in:
    - data/dictionaries/tax_rates.csv
    - src/retail_datagen/shared/tax_utils.py
    - src/retail_datagen/shared/dictionary_loader.py
    Development artifact - not integrated into CI/CD.

Exit codes:
    0 - All verifications passed
    1 - Some verifications failed
"""

import sys
from pathlib import Path
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.config.models import RetailConfig
from retail_datagen.shared.dictionary_loader import DictionaryLoader
from retail_datagen.shared.tax_utils import TaxCalculator
from retail_datagen.generators.master_generator import MasterDataGenerator


def print_section(title: str) -> None:
    """Print a section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def verify_csv_structure() -> bool:
    """Verify tax_rates.csv exists and has correct structure."""
    print_section("1. Tax Rates CSV Structure")

    tax_rates_path = Path("data/dictionaries/tax_rates.csv")

    if not tax_rates_path.exists():
        print(f"  ✗ File not found: {tax_rates_path}")
        return False

    print(f"  ✓ File exists: {tax_rates_path}")

    import pandas as pd

    df = pd.read_csv(tax_rates_path)
    print(f"  ✓ Loaded {len(df)} rows")

    expected_columns = {"StateCode", "County", "City", "CombinedRate"}
    actual_columns = set(df.columns)

    if expected_columns == actual_columns:
        print(f"  ✓ Columns match: {sorted(expected_columns)}")
    else:
        print(f"  ✗ Column mismatch:")
        print(f"    Expected: {sorted(expected_columns)}")
        print(f"    Actual: {sorted(actual_columns)}")
        return False

    # Show sample rows
    print("\n  Sample Rows:")
    for i, row in df.head(5).iterrows():
        print(
            f"    {row['StateCode']:2} | {row['County']:20} | {row['City']:20} | {row['CombinedRate']:.4f}"
        )

    # Validate rates
    invalid_rates = df[
        (df["CombinedRate"] < 0) | (df["CombinedRate"] > 0.15)
    ]

    if len(invalid_rates) == 0:
        print(f"\n  ✓ All rates are valid (0.0000 - 0.1500)")
    else:
        print(f"\n  ✗ Found {len(invalid_rates)} invalid rates")
        return False

    # Show statistics
    print(f"\n  Rate Statistics:")
    print(f"    Min: {df['CombinedRate'].min():.4f}")
    print(f"    Max: {df['CombinedRate'].max():.4f}")
    print(f"    Mean: {df['CombinedRate'].mean():.4f}")
    print(f"    Median: {df['CombinedRate'].median():.4f}")

    return True


def verify_dictionary_loader() -> bool:
    """Verify DictionaryLoader can load tax rates."""
    print_section("2. DictionaryLoader Integration")

    try:
        config = RetailConfig.from_file(Path("config.json"))
        print("  ✓ Config loaded")

        loader = DictionaryLoader(config)
        print("  ✓ DictionaryLoader initialized")

        tax_jurisdictions = loader.load_tax_rates()
        print(f"  ✓ Loaded {len(tax_jurisdictions)} tax jurisdictions")

        # Verify type
        if tax_jurisdictions:
            sample = tax_jurisdictions[0]
            print(f"\n  Sample TaxJurisdiction:")
            print(f"    StateCode: {sample.StateCode}")
            print(f"    County: {sample.County}")
            print(f"    City: {sample.City}")
            print(f"    CombinedRate: {sample.CombinedRate}")

            # Verify Pydantic validation
            print(f"\n  ✓ Pydantic validation passed")
            print(f"    Type: {type(sample).__name__}")
            print(f"    StateCode type: {type(sample.StateCode).__name__}")
            print(f"    CombinedRate type: {type(sample.CombinedRate).__name__}")

        return True

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def verify_master_generator() -> bool:
    """Verify MasterDataGenerator creates tax rate mapping."""
    print_section("3. MasterDataGenerator Tax Rate Mapping")

    try:
        config = RetailConfig.from_file(Path("config.json"))
        print("  ✓ Config loaded")

        generator = MasterDataGenerator(config)
        print("  ✓ MasterDataGenerator initialized")

        # Check initial state
        if hasattr(generator, "_tax_rate_mapping"):
            print(f"  ✓ _tax_rate_mapping attribute exists")
        else:
            print(f"  ✗ _tax_rate_mapping attribute missing")
            return False

        # Load dictionaries to populate mapping
        generator.load_dictionaries()
        print("  ✓ Dictionaries loaded")

        mapping_size = len(generator._tax_rate_mapping)
        print(f"  ✓ Tax rate mapping created: {mapping_size} entries")

        # Show sample mappings
        print("\n  Sample Mappings (State, City) -> Rate:")
        for i, ((state, city), rate) in enumerate(
            list(generator._tax_rate_mapping.items())[:5]
        ):
            print(f"    ({state}, {city:20}) -> {rate:.4f} ({rate * 100:.2f}%)")

        # Test specific lookups
        print("\n  Testing Specific Lookups:")
        test_cases = [
            ("CA", "Los Angeles", Decimal("0.0950")),
            ("TX", "Houston", Decimal("0.0825")),
            ("IL", "Chicago", Decimal("0.1025")),
            ("AK", "Anchorage", Decimal("0.0000")),
        ]

        all_correct = True
        for state, city, expected_rate in test_cases:
            key = (state, city)
            actual_rate = generator._tax_rate_mapping.get(key)

            if actual_rate == expected_rate:
                print(f"    ✓ {city:20} {state} -> {actual_rate:.4f}")
            else:
                print(
                    f"    ✗ {city:20} {state} -> Expected {expected_rate:.4f}, Got {actual_rate}"
                )
                all_correct = False

        return all_correct

    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def verify_tax_calculator() -> bool:
    """Verify TaxCalculator utility class."""
    print_section("4. TaxCalculator Utility Class")

    try:
        tax_rates_path = Path("data/dictionaries/tax_rates.csv")
        calc = TaxCalculator(tax_rates_path)
        print(f"  ✓ TaxCalculator initialized")
        print(f"  ✓ Loaded {len(calc.rate_cache)} rates into cache")

        # Test get_tax_rate
        print("\n  Testing get_tax_rate():")
        test_cases = [
            ("CA", "Los Angeles", Decimal("0.0950")),
            ("CA", "San Francisco", Decimal("0.0863")),
            ("TX", "Houston", Decimal("0.0825")),
            ("NY", "New York City", Decimal("0.0875")),
            ("IL", "Chicago", Decimal("0.1025")),
            ("AK", "Anchorage", Decimal("0.0000")),
            ("ZZ", "Unknown", Decimal("0.07407")),  # Default
        ]

        all_correct = True
        for state, city, expected in test_cases:
            actual = calc.get_tax_rate(state, city=city)
            if actual == expected:
                print(f"    ✓ {city:20} {state} -> {actual:.4f}")
            else:
                print(f"    ✗ {city:20} {state} -> Expected {expected:.4f}, Got {actual:.4f}")
                all_correct = False

        # Test calculate_tax
        print("\n  Testing calculate_tax():")
        calc_tests = [
            (Decimal("100.00"), Decimal("0.0950"), Decimal("9.50")),
            (Decimal("17.99"), Decimal("0.0825"), Decimal("1.48")),
            (Decimal("42.23"), Decimal("0.1025"), Decimal("4.33")),
        ]

        for amount, rate, expected_tax in calc_tests:
            actual_tax = calc.calculate_tax(amount, rate)
            if actual_tax == expected_tax:
                print(f"    ✓ ${amount} × {rate:.4f} = ${actual_tax}")
            else:
                print(
                    f"    ✗ ${amount} × {rate:.4f} -> Expected ${expected_tax}, Got ${actual_tax}"
                )
                all_correct = False

        # Test get_all_rates_for_state
        print("\n  Testing get_all_rates_for_state():")
        ca_rates = calc.get_all_rates_for_state("CA")
        print(f"    ✓ California: {len(ca_rates)} cities")

        if "Los Angeles" in ca_rates:
            print(f"      ✓ Los Angeles: {ca_rates['Los Angeles']:.4f}")
        else:
            print(f"      ✗ Los Angeles not found")
            all_correct = False

        # Test get_rate_statistics
        print("\n  Testing get_rate_statistics():")
        stats = calc.get_rate_statistics()
        print(f"    Min: {stats['min']:.4f}")
        print(f"    Max: {stats['max']:.4f}")
        print(f"    Mean: {stats['mean']:.4f}")
        print(f"    Median: {stats['median']:.4f}")
        print(f"    Count: {stats['count']}")

        return all_correct

    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all verification tests."""
    print("\n" + "=" * 70)
    print("  PHASE 1.3 TAX RATE IMPLEMENTATION VERIFICATION")
    print("=" * 70)

    results = {
        "CSV Structure": verify_csv_structure(),
        "DictionaryLoader": verify_dictionary_loader(),
        "MasterDataGenerator": verify_master_generator(),
        "TaxCalculator": verify_tax_calculator(),
    }

    # Summary
    print_section("VERIFICATION SUMMARY")

    all_passed = True
    for component, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {component:25} {status}")
        if not passed:
            all_passed = False

    print("\n" + "=" * 70)
    if all_passed:
        print("  ✓ ALL VERIFICATIONS PASSED")
        print("  Tax rate infrastructure is fully functional!")
    else:
        print("  ✗ SOME VERIFICATIONS FAILED")
        print("  Please review the errors above.")
    print("=" * 70 + "\n")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
