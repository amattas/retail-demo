#!/usr/bin/env python3
"""
Validate foot traffic generation and conversion rate logic.

Purpose:
    Validates that foot traffic data generation produces realistic
    aggregate counts with proper receipt-to-traffic conversion rates.

Checks:
    1. Aggregate counts (not individual pings with Count=1)
    2. Traffic > receipts with proper conversion rates (10-35%)
    3. Temporal patterns (peak vs off-peak hours)
    4. Sensor distribution (5 sensors per store with proportional traffic)

Usage:
    python validate_foot_traffic.py

When to run:
    After modifying _generate_foot_traffic() in src/retail_datagen/generators/fact_generator.py.
    Development artifact - not integrated into CI/CD.

Exit codes:
    0 - All validation checks passed
    1 - Some validation checks failed
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.fact_generator import FactDataGenerator
from retail_datagen.shared.models import Store, Geography


def create_test_stores() -> list[Store]:
    """Create 3 test stores for validation."""
    geo = Geography(
        GeoID=1,
        County="Test County",
        State="CA",
        StateFIPS=6,
        DCID=1,
    )

    stores = [
        Store(
            ID=1,
            Name="Test Store 1",
            GeoID=1,
            County="Test County",
            State="CA",
            DCID=1,
            daily_traffic_multiplier=Decimal("1.0"),  # Average store
        ),
        Store(
            ID=2,
            Name="Test Store 2 (High Traffic)",
            GeoID=1,
            County="Test County",
            State="CA",
            DCID=1,
            daily_traffic_multiplier=Decimal("1.5"),  # High traffic store
        ),
        Store(
            ID=3,
            Name="Test Store 3 (Low Traffic)",
            GeoID=1,
            County="Test County",
            State="CA",
            DCID=1,
            daily_traffic_multiplier=Decimal("0.7"),  # Low traffic store
        ),
    ]

    return stores


def validate_foot_traffic():
    """Run validation tests on foot traffic generation."""
    print("=" * 80)
    print("FOOT TRAFFIC VALIDATION")
    print("=" * 80)
    print()

    # Create minimal config
    config = RetailConfig(
        seed=42,
        volume={
            "stores": 3,
            "dcs": 1,
            "total_customers": 1000,
            "customers_per_day": 200,  # Per store
        },
        paths={
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
        historical={"start_date": "2024-01-01"},
        realtime={"emit_interval_ms": 500, "burst": 10},
    )

    # Create generator (we'll only use its foot traffic method)
    generator = FactDataGenerator(config)
    stores = create_test_stores()

    # Test different time periods
    test_dates = [
        datetime(2024, 1, 15, 10, 0),  # Monday 10am - moderate
        datetime(2024, 1, 15, 12, 0),  # Monday 12pm - peak
        datetime(2024, 1, 15, 18, 0),  # Monday 6pm - peak
        datetime(2024, 1, 15, 8, 0),   # Monday 8am - early
        datetime(2024, 1, 20, 14, 0),  # Saturday 2pm - weekend
    ]

    print("Test Scenarios:")
    print("-" * 80)
    print()

    all_results = []

    for test_datetime in test_dates:
        hour = test_datetime.hour
        day_name = test_datetime.strftime("%A")
        is_weekend = test_datetime.weekday() >= 5

        print(f"\n{day_name} {test_datetime.strftime('%I:%M %p')}")
        print("-" * 40)

        for store in stores:
            # Simulate different receipt counts based on store and time
            base_receipts = 10
            if store.daily_traffic_multiplier > 1.0:
                base_receipts = 15
            elif store.daily_traffic_multiplier < 1.0:
                base_receipts = 6

            # Peak hours get more receipts
            if hour in [12, 13, 17, 18, 19]:
                base_receipts = int(base_receipts * 1.5)
            elif hour in [8, 9, 20, 21]:
                base_receipts = int(base_receipts * 0.7)

            receipt_count = base_receipts

            # Generate foot traffic
            traffic_records = generator._generate_foot_traffic(
                store, test_datetime, receipt_count
            )

            # Validate results
            total_traffic = sum(r["Count"] for r in traffic_records)
            sensor_count = len(traffic_records)

            # Calculate conversion rate
            conversion_rate = receipt_count / total_traffic if total_traffic > 0 else 0

            # Store for summary
            all_results.append({
                "datetime": test_datetime,
                "store": store.Name,
                "receipts": receipt_count,
                "total_traffic": total_traffic,
                "conversion_rate": conversion_rate,
                "sensor_count": sensor_count,
            })

            print(f"\n  {store.Name}:")
            print(f"    Receipts: {receipt_count}")
            print(f"    Total Foot Traffic: {total_traffic}")
            print(f"    Conversion Rate: {conversion_rate:.1%}")
            print(f"    Sensors: {sensor_count}")

            # Check for individual pings (should not exist)
            individual_pings = [r for r in traffic_records if r["Count"] == 1]
            if individual_pings:
                print(f"    ⚠️  WARNING: Found {len(individual_pings)} individual pings (Count=1)")
            else:
                print(f"    ✅ All records are aggregates (Count > 1 or Count = 0)")

            # Show sensor breakdown
            print(f"\n    Sensor Breakdown:")
            for record in traffic_records:
                zone = record["Zone"]
                count = record["Count"]
                dwell = record["Dwell"]
                print(f"      {zone:20s}: Count={count:4d}, Dwell={dwell:3d}s")

    # Summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print()

    # Group by time period
    peak_hours = [r for r in all_results if r["datetime"].hour in [12, 13, 17, 18, 19]]
    moderate_hours = [r for r in all_results if r["datetime"].hour in [10, 11, 14, 15, 16]]
    early_hours = [r for r in all_results if r["datetime"].hour in [8, 9, 20, 21]]

    def print_stats(label, results):
        if not results:
            return
        avg_conversion = sum(r["conversion_rate"] for r in results) / len(results)
        min_conversion = min(r["conversion_rate"] for r in results)
        max_conversion = max(r["conversion_rate"] for r in results)
        print(f"\n{label}:")
        print(f"  Average Conversion: {avg_conversion:.1%}")
        print(f"  Range: {min_conversion:.1%} - {max_conversion:.1%}")

    print_stats("Peak Hours (12pm, 1pm, 5-7pm)", peak_hours)
    print_stats("Moderate Hours (10-11am, 2-4pm)", moderate_hours)
    print_stats("Early/Late Hours (8-9am, 8-9pm)", early_hours)

    # Validation checks
    print("\n" + "=" * 80)
    print("VALIDATION CHECKS")
    print("=" * 80)
    print()

    all_passed = True

    # Check 1: All records are aggregates
    individual_ping_count = sum(
        1 for r in all_results
        if r["sensor_count"] > 0 and r["total_traffic"] == r["sensor_count"]
    )
    if individual_ping_count == 0:
        print("✅ PASS: All records are aggregates (not individual Count=1 pings)")
    else:
        print(f"❌ FAIL: Found scenarios with individual pings")
        all_passed = False

    # Check 2: Traffic > receipts
    traffic_greater = all(r["total_traffic"] >= r["receipts"] for r in all_results)
    if traffic_greater:
        print("✅ PASS: Foot traffic always >= receipts")
    else:
        print("❌ FAIL: Some scenarios have traffic < receipts")
        all_passed = False

    # Check 3: Conversion rates in range (10-30%)
    conversion_in_range = all(
        0.10 <= r["conversion_rate"] <= 0.35 for r in all_results if r["total_traffic"] > 0
    )
    if conversion_in_range:
        print("✅ PASS: All conversion rates in 10-35% range")
    else:
        print("❌ FAIL: Some conversion rates outside expected range")
        all_passed = False

    # Check 4: 5 sensors per store per hour
    five_sensors = all(r["sensor_count"] == 5 for r in all_results if r["total_traffic"] > 0)
    if five_sensors:
        print("✅ PASS: All scenarios have exactly 5 sensors")
    else:
        print("❌ FAIL: Some scenarios don't have 5 sensors")
        all_passed = False

    # Check 5: Peak hours have higher conversion than early hours
    if peak_hours and early_hours:
        avg_peak_conversion = sum(r["conversion_rate"] for r in peak_hours) / len(peak_hours)
        avg_early_conversion = sum(r["conversion_rate"] for r in early_hours) / len(early_hours)

        if avg_peak_conversion > avg_early_conversion:
            print("✅ PASS: Peak hours have higher conversion than early hours")
        else:
            print("❌ FAIL: Peak hours conversion not higher than early hours")
            all_passed = False

    print()
    if all_passed:
        print("🎉 ALL VALIDATION CHECKS PASSED")
    else:
        print("⚠️  SOME VALIDATION CHECKS FAILED")

    print()
    print("=" * 80)

    return all_passed


if __name__ == "__main__":
    success = validate_foot_traffic()
    sys.exit(0 if success else 1)
