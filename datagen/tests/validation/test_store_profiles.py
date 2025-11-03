#!/usr/bin/env python3
"""
Quick validation script for store profile variability.

This script verifies that:
1. Store profiles are being assigned correctly
2. Traffic multipliers create variability in transaction volumes
3. Distribution is non-uniform (coefficient of variation > 0.5)
"""

import sys
from pathlib import Path
from decimal import Decimal
from collections import Counter

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.shared.models import Store, GeographyMaster
from retail_datagen.shared.store_profiles import (
    StoreProfiler,
    StoreVolumeClass,
    StoreFormat,
)


def create_test_data(num_stores=100):
    """Create test stores and geographies."""
    # Create geographies
    geographies = [
        GeographyMaster(
            ID=i,
            City=f"City{i}",
            State="CA" if i % 2 == 0 else "TX",
            Zip=f"{90000 + i:05d}",
            District="Metro" if i % 3 == 0 else "Rural",
            Region="West" if i % 2 == 0 else "South",
        )
        for i in range(1, 26)
    ]

    # Create stores
    stores = []
    for i in range(1, num_stores + 1):
        geo_id = ((i - 1) % len(geographies)) + 1
        stores.append(
            Store(
                ID=i,
                StoreNumber=f"ST{i:03d}",
                Address=f"{i}00 Main St",
                GeographyID=geo_id,
            )
        )

    return stores, geographies


def main():
    """Run validation tests."""
    print("=" * 70)
    print("Store Profile Variability Validation")
    print("=" * 70)

    # Create test data
    print("\n1. Creating test data...")
    stores, geographies = create_test_data(num_stores=200)
    print(f"   Created {len(stores)} stores across {len(geographies)} geographies")

    # Create profiler and assign profiles
    print("\n2. Assigning store profiles...")
    profiler = StoreProfiler(stores, geographies, seed=42)
    profiles = profiler.assign_profiles()
    print(f"   Assigned profiles to {len(profiles)} stores")

    # Analyze volume class distribution
    print("\n3. Volume Class Distribution:")
    volume_classes = [p.volume_class for p in profiles.values()]
    volume_counts = Counter(volume_classes)
    total = len(volume_classes)

    for vol_class in StoreVolumeClass:
        count = volume_counts.get(vol_class, 0)
        pct = (count / total) * 100 if total > 0 else 0
        print(f"   {vol_class.value:20s}: {count:3d} ({pct:5.1f}%)")

    # Analyze store format distribution
    print("\n4. Store Format Distribution:")
    store_formats = [p.store_format for p in profiles.values()]
    format_counts = Counter(store_formats)

    for fmt in StoreFormat:
        count = format_counts.get(fmt, 0)
        pct = (count / total) * 100 if total > 0 else 0
        print(f"   {fmt.value:20s}: {count:3d} ({pct:5.1f}%)")

    # Analyze traffic multipliers
    print("\n5. Traffic Multiplier Analysis:")
    multipliers = [float(p.daily_traffic_multiplier) for p in profiles.values()]

    min_mult = min(multipliers)
    max_mult = max(multipliers)
    mean_mult = sum(multipliers) / len(multipliers)

    print(f"   Minimum:  {min_mult:.3f}")
    print(f"   Maximum:  {max_mult:.3f}")
    print(f"   Mean:     {mean_mult:.3f}")
    print(f"   Range:    {max_mult - min_mult:.3f}")

    # Calculate coefficient of variation
    variance = sum((m - mean_mult) ** 2 for m in multipliers) / len(multipliers)
    std_dev = variance ** 0.5
    cv = std_dev / mean_mult

    print(f"   Std Dev:  {std_dev:.3f}")
    print(f"   CV:       {cv:.3f}")

    # Check variability
    print("\n6. Variability Checks:")
    checks_passed = 0
    total_checks = 0

    # Check 1: Multiple volume classes
    total_checks += 1
    unique_volume_classes = len(set(volume_classes))
    if unique_volume_classes >= 3:
        print(f"   ✓ Multiple volume classes: {unique_volume_classes} classes found")
        checks_passed += 1
    else:
        print(f"   ✗ Multiple volume classes: Only {unique_volume_classes} classes found (expected >= 3)")

    # Check 2: Coefficient of variation
    total_checks += 1
    if cv >= 0.5:
        print(f"   ✓ Coefficient of variation: {cv:.3f} (>= 0.5)")
        checks_passed += 1
    else:
        print(f"   ✗ Coefficient of variation: {cv:.3f} (expected >= 0.5)")

    # Check 3: Range of multipliers
    total_checks += 1
    mult_range = max_mult - min_mult
    if mult_range >= 2.0:
        print(f"   ✓ Multiplier range: {mult_range:.3f} (>= 2.0)")
        checks_passed += 1
    else:
        print(f"   ✗ Multiplier range: {mult_range:.3f} (expected >= 2.0)")

    # Check 4: Flagship stores have high multipliers
    total_checks += 1
    flagship_profiles = [
        p for p in profiles.values()
        if p.volume_class == StoreVolumeClass.FLAGSHIP
    ]
    if flagship_profiles:
        flagship_mults = [float(p.daily_traffic_multiplier) for p in flagship_profiles]
        min_flagship = min(flagship_mults)
        if min_flagship >= 2.0:
            print(f"   ✓ Flagship multipliers: min={min_flagship:.3f} (>= 2.0)")
            checks_passed += 1
        else:
            print(f"   ✗ Flagship multipliers: min={min_flagship:.3f} (expected >= 2.0)")
    else:
        print(f"   - Flagship multipliers: No flagship stores in dataset")
        # Don't count this check
        total_checks -= 1

    # Check 5: Kiosk stores have low multipliers
    total_checks += 1
    kiosk_profiles = [
        p for p in profiles.values()
        if p.volume_class == StoreVolumeClass.KIOSK
    ]
    if kiosk_profiles:
        kiosk_mults = [float(p.daily_traffic_multiplier) for p in kiosk_profiles]
        max_kiosk = max(kiosk_mults)
        if max_kiosk <= 0.5:
            print(f"   ✓ Kiosk multipliers: max={max_kiosk:.3f} (<= 0.5)")
            checks_passed += 1
        else:
            print(f"   ✗ Kiosk multipliers: max={max_kiosk:.3f} (expected <= 0.5)")
    else:
        print(f"   - Kiosk multipliers: No kiosk stores in dataset")
        # Don't count this check
        total_checks -= 1

    # Sample stores by volume class
    print("\n7. Sample Stores by Volume Class:")
    for vol_class in StoreVolumeClass:
        class_profiles = [
            (sid, p) for sid, p in profiles.items()
            if p.volume_class == vol_class
        ]
        if class_profiles:
            # Show first 2 stores of each class
            for i, (store_id, profile) in enumerate(class_profiles[:2]):
                mult = float(profile.daily_traffic_multiplier)
                fmt = profile.store_format.value
                hrs = profile.operating_hours.value
                basket = profile.avg_basket_size
                print(f"   Store {store_id:3d} ({vol_class.value:15s}): "
                      f"mult={mult:.2f}, format={fmt:12s}, hours={hrs:12s}, basket={basket:.1f}")
            if len(class_profiles) > 2:
                print(f"   ... and {len(class_profiles) - 2} more {vol_class.value} stores")

    # Summary
    print("\n" + "=" * 70)
    print(f"RESULTS: {checks_passed}/{total_checks} checks passed")
    print("=" * 70)

    if checks_passed == total_checks:
        print("\n✓ All variability checks passed!")
        print("Store profiles are creating realistic variability in transaction volumes.")
        return 0
    else:
        print(f"\n✗ {total_checks - checks_passed} checks failed")
        print("Store profiles may not be creating sufficient variability.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
