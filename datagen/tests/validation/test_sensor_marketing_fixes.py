#!/usr/bin/env python3
"""
Validation script for BLE and marketing data pattern fixes.

Tests:
1. BLE pings have 25-40% customer_id match rate (target: 30%)
2. Marketing impressions have 3-7% customer_id resolution (target: 5%)
3. Marketing costs are within documented ranges per channel
"""

import asyncio
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.fact_generators import FactDataGenerator
from retail_datagen.shared.models import (
    Customer,
    DistributionCenter,
    ProductMaster,
    Store,
)


def load_master_data():
    """Load master data from CSV files."""
    print("Loading master data...")

    # Load stores
    stores = []
    stores_file = Path("data/master/StoreMaster.csv")
    if stores_file.exists():
        import csv
        with open(stores_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                stores.append(Store(
                    ID=int(row['ID']),
                    Name=row['Name'],
                    Address=row['Address'],
                    GeographyID=int(row['GeographyID']),
                    Format=row['Format'],
                    SqFt=int(row['SqFt']),
                ))

    # Load customers
    customers = []
    customers_file = Path("data/master/CustomerMaster.csv")
    if customers_file.exists():
        import csv
        with open(customers_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                customers.append(Customer(
                    ID=int(row['ID']),
                    FirstName=row['FirstName'],
                    LastName=row['LastName'],
                    Address=row['Address'],
                    GeographyID=int(row['GeographyID']),
                    LoyaltyCard=row['LoyaltyCard'],
                    Phone=row['Phone'],
                    BLEId=row['BLEId'],
                    AdId=row['AdId'],
                ))

    # Load products (sample)
    products = []
    products_file = Path("data/master/ProductMaster.csv")
    if products_file.exists():
        import csv
        from decimal import Decimal
        with open(products_file) as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 100:  # Just load first 100 for testing
                    break
                products.append(ProductMaster(
                    ID=int(row['ID']),
                    ProductName=row['ProductName'],
                    Brand=row['Brand'],
                    Company=row['Company'],
                    Department=row['Department'],
                    Category=row['Category'],
                    Subcategory=row['Subcategory'],
                    Cost=Decimal(row['Cost']),
                    SalePrice=Decimal(row['SalePrice']),
                    MSRP=Decimal(row['MSRP']),
                    BasePrice=Decimal(row['BasePrice']),
                    LaunchDate=datetime.fromisoformat(row['LaunchDate']),
                    Taxability=row['Taxability'],
                ))

    # Load DCs
    dcs = []
    dcs_file = Path("data/master/DistributionCenterMaster.csv")
    if dcs_file.exists():
        import csv
        with open(dcs_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                dcs.append(DistributionCenter(
                    ID=int(row['ID']),
                    Name=row['Name'],
                    Address=row['Address'],
                    GeographyID=int(row['GeographyID']),
                    Capacity=int(row['Capacity']),
                ))

    print(f"Loaded {len(stores)} stores, {len(customers)} customers, {len(products)} products, {len(dcs)} DCs")
    return stores, customers, products, dcs


async def test_ble_customer_matching():
    """Test BLE pings have 30% customer_id match rate."""
    print("\n" + "=" * 80)
    print("TEST 1: BLE Customer ID Matching")
    print("=" * 80)

    stores, customers, products, dcs = load_master_data()

    if not all([stores, customers, products, dcs]):
        print("❌ Master data not found. Run master data generation first.")
        return

    # Create config
    config = RetailConfig.load_from_file("config.json")

    # Create generator
    generator = FactDataGenerator(
        stores=stores,
        customers=customers[:1000],  # Use subset for testing
        products=products,
        distribution_centers=dcs,
        config=config,
    )

    # Generate one day of data
    test_date = datetime(2024, 6, 15, 12, 0, 0)  # Midday for better activity

    # Generate BLE pings directly
    sample_store = stores[0]
    sample_customer = customers[0]

    ble_pings = []
    for _ in range(100):  # Generate 100 customer visits
        customer = generator._rng.choice(customers[:1000])
        pings = generator._generate_ble_pings(sample_store, customer, test_date)
        ble_pings.extend(pings)

    # Analyze customer_id match rate
    total_pings = len(ble_pings)
    pings_with_customer_id = sum(1 for ping in ble_pings if ping.get('CustomerId') is not None)
    match_rate = (pings_with_customer_id / total_pings * 100) if total_pings > 0 else 0

    print("\nResults:")
    print(f"  Total BLE pings generated: {total_pings}")
    print(f"  Pings with customer_id: {pings_with_customer_id}")
    print(f"  Match rate: {match_rate:.1f}%")
    print("  Expected: 25-40% (target: 30%)")

    # Sample data
    print("\nSample BLE pings (first 5):")
    for i, ping in enumerate(ble_pings[:5]):
        has_customer = "✓" if ping.get('CustomerId') else "✗"
        print(f"  {i+1}. BLEId: {ping['CustomerBLEId'][:20]}... | CustomerId: {ping.get('CustomerId', 'None'):>6} {has_customer}")

    # Validation
    if 25 <= match_rate <= 40:
        print(f"\n✅ PASS: Match rate {match_rate:.1f}% is within expected range (25-40%)")
        return True
    else:
        print(f"\n❌ FAIL: Match rate {match_rate:.1f}% is outside expected range (25-40%)")
        return False


async def test_marketing_customer_resolution():
    """Test marketing impressions have 5% customer_id resolution."""
    print("\n" + "=" * 80)
    print("TEST 2: Marketing Customer ID Resolution")
    print("=" * 80)

    stores, customers, products, dcs = load_master_data()

    if not all([stores, customers, products, dcs]):
        print("❌ Master data not found. Run master data generation first.")
        return

    # Create config
    config = RetailConfig.load_from_file("config.json")

    # Create generator
    generator = FactDataGenerator(
        stores=stores,
        customers=customers[:1000],  # Use subset for testing
        products=products,
        distribution_centers=dcs,
        config=config,
    )

    # Generate marketing activity for one day
    test_date = datetime(2024, 6, 15)
    multiplier = 1.0

    marketing_records = generator._generate_marketing_activity(test_date, multiplier)

    # Analyze customer_id resolution rate
    total_impressions = len(marketing_records)
    impressions_with_customer_id = sum(1 for rec in marketing_records if rec.get('CustomerId') is not None)
    resolution_rate = (impressions_with_customer_id / total_impressions * 100) if total_impressions > 0 else 0

    print("\nResults:")
    print(f"  Total marketing impressions: {total_impressions}")
    print(f"  Impressions with customer_id: {impressions_with_customer_id}")
    print(f"  Resolution rate: {resolution_rate:.1f}%")
    print("  Expected: 3-7% (target: 5%)")

    # Sample data
    print("\nSample marketing impressions (first 5 with customer_id):")
    resolved_samples = [rec for rec in marketing_records if rec.get('CustomerId') is not None][:5]
    for i, rec in enumerate(resolved_samples):
        print(f"  {i+1}. AdId: {rec['CustomerAdId'][:20]}... | CustomerId: {rec['CustomerId']} | Channel: {rec['Channel']}")

    # Validation
    if total_impressions == 0:
        print("\n⚠️  SKIP: No marketing impressions generated (campaigns may not be active)")
        return None
    elif 3 <= resolution_rate <= 7:
        print(f"\n✅ PASS: Resolution rate {resolution_rate:.1f}% is within expected range (3-7%)")
        return True
    else:
        print(f"\n❌ FAIL: Resolution rate {resolution_rate:.1f}% is outside expected range (3-7%)")
        return False


async def test_marketing_cost_ranges():
    """Test marketing costs are within documented ranges."""
    print("\n" + "=" * 80)
    print("TEST 3: Marketing Cost Ranges")
    print("=" * 80)

    stores, customers, products, dcs = load_master_data()

    if not all([stores, customers, products, dcs]):
        print("❌ Master data not found. Run master data generation first.")
        return

    # Create config
    config = RetailConfig.load_from_file("config.json")

    # Create generator
    generator = FactDataGenerator(
        stores=stores,
        customers=customers[:1000],
        products=products,
        distribution_centers=dcs,
        config=config,
    )

    # Generate marketing activity for multiple days to get good sample
    marketing_records = []
    for day_offset in range(7):  # 7 days
        test_date = datetime(2024, 6, 15) + timedelta(days=day_offset)
        records = generator._generate_marketing_activity(test_date, multiplier=1.0)
        marketing_records.extend(records)

    # Analyze costs by channel
    costs_by_channel = defaultdict(list)
    for rec in marketing_records:
        channel = rec['Channel']
        cost = float(rec['Cost'])
        costs_by_channel[channel].append(cost)

    # Expected ranges (from task requirements)
    expected_ranges = {
        'EMAIL': (0.05, 0.50),
        'SOCIAL': (1.00, 5.00),
        'DISPLAY': (2.00, 10.00),
        'SEARCH': (0.50, 3.00),
    }

    print(f"\nResults ({len(marketing_records)} total impressions):")
    all_pass = True

    for channel in sorted(costs_by_channel.keys()):
        costs = costs_by_channel[channel]
        min_cost = min(costs)
        max_cost = max(costs)
        avg_cost = statistics.mean(costs)
        median_cost = statistics.median(costs)

        print(f"\n  {channel}:")
        print(f"    Count: {len(costs)}")
        print(f"    Min: ${min_cost:.4f}")
        print(f"    Max: ${max_cost:.4f}")
        print(f"    Avg: ${avg_cost:.4f}")
        print(f"    Median: ${median_cost:.4f}")

        if channel in expected_ranges:
            expected_min, expected_max = expected_ranges[channel]
            print(f"    Expected range: ${expected_min:.2f} - ${expected_max:.2f}")

            # Check if costs are within expected range
            within_range = all(expected_min <= cost <= expected_max for cost in costs)
            if within_range:
                print("    ✅ All costs within expected range")
            else:
                out_of_range = sum(1 for cost in costs if cost < expected_min or cost > expected_max)
                print(f"    ❌ {out_of_range}/{len(costs)} costs outside expected range")
                all_pass = False

    # Overall validation
    if len(marketing_records) == 0:
        print("\n⚠️  SKIP: No marketing impressions generated")
        return None
    elif all_pass:
        print("\n✅ PASS: All marketing costs within expected ranges")
        return True
    else:
        print("\n❌ FAIL: Some marketing costs outside expected ranges")
        return False


async def main():
    """Run all validation tests."""
    print("=" * 80)
    print("BLE and Marketing Data Pattern Validation")
    print("=" * 80)

    results = []

    # Run tests
    result1 = await test_ble_customer_matching()
    results.append(("BLE Customer Matching", result1))

    result2 = await test_marketing_customer_resolution()
    results.append(("Marketing Customer Resolution", result2))

    result3 = await test_marketing_cost_ranges()
    results.append(("Marketing Cost Ranges", result3))

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    for test_name, result in results:
        if result is True:
            status = "✅ PASS"
        elif result is False:
            status = "❌ FAIL"
        else:
            status = "⚠️  SKIP"
        print(f"  {status}: {test_name}")

    passed = sum(1 for _, r in results if r is True)
    total = sum(1 for _, r in results if r is not None)

    print(f"\nTests passed: {passed}/{total}")

    if total > 0 and passed == total:
        print("\n🎉 All tests passed!")
        return 0
    elif total == 0:
        print("\n⚠️  No tests could run (missing master data)")
        return 2
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
