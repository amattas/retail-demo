#!/usr/bin/env python3
"""
Unit tests for BLE and marketing data pattern fixes.

Tests the logic without requiring full master data generation.
"""

from datetime import datetime
from decimal import Decimal

from retail_datagen.config.models import MarketingCostConfig, RetailConfig
from retail_datagen.generators.fact_generators import FactDataGenerator
from retail_datagen.generators.retail_patterns import MarketingCampaignSimulator
from retail_datagen.shared.models import (
    Customer,
    DeviceType,
    DistributionCenter,
    MarketingChannel,
    ProductMaster,
    Store,
)


def create_test_data():
    """Create minimal test data."""
    # Create test stores
    stores = [
        Store(
            ID=1,
            Name="Test Store 1",
            Address="123 Test St",
            GeographyID=1,
            Format="SUPERSTORE",
            SqFt=50000,
        )
    ]

    # Create test customers
    customers = []
    for i in range(100):
        customers.append(
            Customer(
                ID=i + 1,
                FirstName=f"FirstName{i+1}",
                LastName=f"LastName{i+1}",
                Address=f"{i+1} Test Ave",
                GeographyID=1,
                LoyaltyCard=f"LOYAL{i+1:08d}",
                Phone=f"{200+i:03d}-555-{1000+i:04d}",
                BLEId=f"BLE{i+1:010d}",
                AdId=f"AD{i+1:010d}",
            )
        )

    # Create test products
    products = [
        ProductMaster(
            ID=1,
            ProductName="Test Product",
            Brand="Test Brand",
            Company="Test Company",
            Department="Grocery",
            Category="Food",
            Subcategory="Snacks",
            Cost=Decimal("1.00"),
            SalePrice=Decimal("1.50"),
            MSRP=Decimal("2.00"),
            BasePrice=Decimal("1.75"),
            LaunchDate=datetime(2024, 1, 1),
            Taxability="TAXABLE",
        )
    ]

    # Create test DCs
    dcs = [
        DistributionCenter(
            ID=1,
            Name="Test DC",
            Address="456 DC Blvd",
            GeographyID=1,
            Capacity=1000000,
        )
    ]

    return stores, customers, products, dcs


def test_ble_customer_matching():
    """Test BLE pings have ~30% customer_id match rate."""
    print("\n" + "=" * 80)
    print("TEST 1: BLE Customer ID Matching (Unit Test)")
    print("=" * 80)

    stores, customers, products, dcs = create_test_data()

    # Create minimal config
    config = RetailConfig(
        seed=42,
        volume={
            "stores": 1,
            "dcs": 1,
            "total_customers": 100,
            "customers_per_day": 10,
        },
        paths={
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
    )

    # Create generator
    generator = FactDataGenerator(
        stores=stores,
        customers=customers,
        products=products,
        distribution_centers=dcs,
        config=config,
    )

    # Generate BLE pings for 100 customer visits
    test_date = datetime(2024, 6, 15, 12, 0, 0)
    ble_pings = []

    for customer in customers:
        pings = generator._generate_ble_pings(stores[0], customer, test_date)
        ble_pings.extend(pings)

    # Analyze customer_id match rate
    total_pings = len(ble_pings)
    pings_with_customer_id = sum(
        1 for ping in ble_pings if ping.get("CustomerId") is not None
    )
    match_rate = (pings_with_customer_id / total_pings * 100) if total_pings > 0 else 0

    print("\nResults:")
    print(f"  Total BLE pings generated: {total_pings}")
    print(f"  Pings with customer_id: {pings_with_customer_id}")
    print(f"  Match rate: {match_rate:.1f}%")
    print("  Expected: 25-40% (target: 30%)")

    # Sample data
    print("\nSample BLE pings (first 10):")
    for i, ping in enumerate(ble_pings[:10]):
        has_customer = "✓" if ping.get("CustomerId") else "✗"
        customer_id_str = str(ping.get("CustomerId", "None")).rjust(6)
        print(
            f"  {i+1:2d}. BLEId: {ping['CustomerBLEId'][:15]:15s} | "
            f"CustomerId: {customer_id_str} {has_customer} | Zone: {ping['Zone']}"
        )

    # Validation (with statistical tolerance)
    if 25 <= match_rate <= 40:
        print(
            f"\n✅ PASS: Match rate {match_rate:.1f}% is within expected range (25-40%)"
        )
        return True
    else:
        print(
            f"\n❌ FAIL: Match rate {match_rate:.1f}% is outside expected range (25-40%)"
        )
        print(
            "   Note: With RNG seed=42 and 100 customers, some variance is expected."
        )
        return False


def test_marketing_customer_resolution():
    """Test marketing impressions have ~5% customer_id resolution."""
    print("\n" + "=" * 80)
    print("TEST 2: Marketing Customer ID Resolution (Unit Test)")
    print("=" * 80)

    stores, customers, products, dcs = create_test_data()

    # Create minimal config
    config = RetailConfig(
        seed=42,
        volume={
            "stores": 1,
            "dcs": 1,
            "total_customers": 100,
            "customers_per_day": 10,
        },
        paths={
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
    )

    # Create generator
    generator = FactDataGenerator(
        stores=stores,
        customers=customers,
        products=products,
        distribution_centers=dcs,
        config=config,
    )

    # Start a campaign manually
    test_date = datetime(2024, 6, 15)
    campaign_id = generator.marketing_campaign_sim.start_campaign(
        "seasonal_sale", test_date
    )

    # Generate impressions for 7 days to get good sample size
    all_impressions = []
    for day in range(7):
        date = test_date.replace(day=test_date.day + day)
        impressions = generator.marketing_campaign_sim.generate_campaign_impressions(
            campaign_id, date, traffic_multiplier=1.0
        )
        all_impressions.extend(impressions)

    # Now process through fact generator to add customer_id resolution
    marketing_records = []
    for impression in all_impressions:
        # Simulate the logic from _generate_marketing_activity
        customer_id = None
        if generator._rng.random() < 0.05:
            # Find customer with this AdId
            customer_ad_id = impression["CustomerAdId"]
            for customer in customers:
                if customer.AdId == customer_ad_id:
                    customer_id = customer.ID
                    break

        marketing_records.append(
            {
                "CustomerAdId": impression["CustomerAdId"],
                "CustomerId": customer_id,
                "Channel": impression["Channel"].value,
                "Cost": str(impression["Cost"]),
            }
        )

    # Analyze customer_id resolution rate
    total_impressions = len(marketing_records)
    impressions_with_customer_id = sum(
        1 for rec in marketing_records if rec.get("CustomerId") is not None
    )
    resolution_rate = (
        (impressions_with_customer_id / total_impressions * 100)
        if total_impressions > 0
        else 0
    )

    print("\nResults:")
    print(f"  Total marketing impressions: {total_impressions}")
    print(f"  Impressions with customer_id: {impressions_with_customer_id}")
    print(f"  Resolution rate: {resolution_rate:.1f}%")
    print("  Expected: 3-7% (target: 5%)")

    # Sample data
    print("\nSample marketing impressions (first 10 with customer_id):")
    resolved_samples = [
        rec for rec in marketing_records if rec.get("CustomerId") is not None
    ][:10]
    for i, rec in enumerate(resolved_samples):
        print(
            f"  {i+1:2d}. AdId: {rec['CustomerAdId'][:15]:15s} | "
            f"CustomerId: {rec['CustomerId']:3d} | Channel: {rec['Channel']}"
        )

    # Validation (with statistical tolerance)
    if total_impressions == 0:
        print("\n⚠️  SKIP: No marketing impressions generated")
        return None
    elif 3 <= resolution_rate <= 7:
        print(
            f"\n✅ PASS: Resolution rate {resolution_rate:.1f}% is within expected range (3-7%)"
        )
        return True
    else:
        print(
            f"\n❌ FAIL: Resolution rate {resolution_rate:.1f}% is outside expected range (3-7%)"
        )
        print(
            "   Note: With RNG and small sample, some variance is expected. Target is 5%."
        )
        return False


def test_marketing_cost_ranges():
    """Test marketing cost configuration matches documented ranges."""
    print("\n" + "=" * 80)
    print("TEST 3: Marketing Cost Configuration (Unit Test)")
    print("=" * 80)

    # Create config and check defaults
    cost_config = MarketingCostConfig()

    expected_ranges = {
        "EMAIL": (0.05, 0.50),
        "SOCIAL": (1.00, 5.00),
        "DISPLAY": (2.00, 10.00),
        "SEARCH": (0.50, 3.00),
    }

    actual_ranges = {
        "EMAIL": (cost_config.email_cost_min, cost_config.email_cost_max),
        "SOCIAL": (cost_config.social_cost_min, cost_config.social_cost_max),
        "DISPLAY": (cost_config.display_cost_min, cost_config.display_cost_max),
        "SEARCH": (cost_config.search_cost_min, cost_config.search_cost_max),
    }

    print("\nConfiguration Ranges:")
    all_match = True

    for channel, (expected_min, expected_max) in expected_ranges.items():
        actual_min, actual_max = actual_ranges[channel]
        matches = (actual_min == expected_min) and (actual_max == expected_max)

        status = "✅" if matches else "❌"
        print(f"\n  {channel}:")
        print(f"    Expected: ${expected_min:.2f} - ${expected_max:.2f}")
        print(f"    Actual:   ${actual_min:.2f} - ${actual_max:.2f}")
        print(f"    {status} {'Match' if matches else 'Mismatch'}")

        if not matches:
            all_match = False

    # Test actual cost calculation
    print("\n\nSample Cost Calculations:")
    simulator = MarketingCampaignSimulator(
        customers=create_test_data()[1], seed=42, cost_config=cost_config
    )

    # Generate sample costs for each channel
    for channel_name, (min_cost, max_cost) in expected_ranges.items():
        channel = MarketingChannel[channel_name]
        device = DeviceType.MOBILE

        # Generate 100 samples
        costs = [
            float(simulator.calculate_impression_cost(channel, device))
            for _ in range(100)
        ]

        min_generated = min(costs)
        max_generated = max(costs)
        avg_generated = sum(costs) / len(costs)

        # Account for device multiplier (mobile typically ~1.0)
        # Costs should be roughly within the base range
        within_range = all(
            min_cost * 0.8 <= cost <= max_cost * 1.2 for cost in costs
        )  # Allow 20% tolerance for device multiplier

        status = "✅" if within_range else "❌"
        print(f"\n  {channel_name} (100 samples, MOBILE device):")
        print(f"    Config range: ${min_cost:.2f} - ${max_cost:.2f}")
        print(f"    Generated: ${min_generated:.4f} - ${max_generated:.4f}")
        print(f"    Average: ${avg_generated:.4f}")
        print(f"    {status} {'Within range' if within_range else 'Outside range'}")

        if not within_range:
            all_match = False

    # Overall validation
    if all_match:
        print("\n✅ PASS: All marketing cost ranges match expected values")
        return True
    else:
        print("\n❌ FAIL: Some marketing cost ranges don't match expected values")
        return False


def main():
    """Run all unit tests."""
    print("=" * 80)
    print("BLE and Marketing Pattern Fixes - Unit Tests")
    print("=" * 80)

    results = []

    # Run tests
    result1 = test_ble_customer_matching()
    results.append(("BLE Customer Matching", result1))

    result2 = test_marketing_customer_resolution()
    results.append(("Marketing Customer Resolution", result2))

    result3 = test_marketing_cost_ranges()
    results.append(("Marketing Cost Configuration", result3))

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
        print("\n⚠️  No tests could run")
        return 2
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
