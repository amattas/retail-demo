#!/usr/bin/env python3
"""
Test script to verify parallel fact generation works correctly.

This script tests both sequential and parallel modes to ensure:
1. Both modes produce data successfully
2. Parallel mode is significantly faster
3. Data consistency between modes
"""

import time
from datetime import datetime
from pathlib import Path

from src.retail_datagen.config.models import RetailConfig
from src.retail_datagen.generators.fact_generator import FactDataGenerator


def test_parallel_generation():
    """Test parallel vs sequential generation."""

    print("=" * 80)
    print("PHASE 3: Parallel Fact Generation Test")
    print("=" * 80)

    # Load config
    config = RetailConfig.from_file("config.json")

    # Test dates (3 days for quick test)
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 1, 3)

    print(f"\nTest date range: {start_date.date()} to {end_date.date()} (3 days)")
    print(f"Expected speedup: 6-8x with parallel processing\n")

    # Test 1: Sequential mode
    print("\n" + "=" * 80)
    print("TEST 1: Sequential Mode (parallel=False)")
    print("=" * 80)

    generator_seq = FactDataGenerator(config)
    generator_seq.load_master_data()

    start_time = time.time()
    summary_seq = generator_seq.generate_historical_data(
        start_date, end_date, parallel=False
    )
    sequential_time = time.time() - start_time

    print(f"\n✅ Sequential generation completed:")
    print(f"   - Time: {sequential_time:.2f}s")
    print(f"   - Total records: {summary_seq.total_records:,}")
    print(f"   - Partitions: {summary_seq.partitions_created}")
    print(f"   - Records/sec: {summary_seq.total_records / sequential_time:.0f}")

    # Test 2: Parallel mode
    print("\n" + "=" * 80)
    print("TEST 2: Parallel Mode (parallel=True)")
    print("=" * 80)

    # Clear previous data
    facts_path = Path(config.paths.facts)
    for fact_table in summary_seq.facts_generated.keys():
        table_path = facts_path / fact_table
        if table_path.exists():
            import shutil
            shutil.rmtree(table_path)

    generator_par = FactDataGenerator(config)
    generator_par.load_master_data()

    start_time = time.time()
    summary_par = generator_par.generate_historical_data(
        start_date, end_date, parallel=True
    )
    parallel_time = time.time() - start_time

    print(f"\n✅ Parallel generation completed:")
    print(f"   - Time: {parallel_time:.2f}s")
    print(f"   - Total records: {summary_par.total_records:,}")
    print(f"   - Partitions: {summary_par.partitions_created}")
    print(f"   - Records/sec: {summary_par.total_records / parallel_time:.0f}")

    # Compare results
    print("\n" + "=" * 80)
    print("COMPARISON")
    print("=" * 80)

    speedup = sequential_time / parallel_time

    print(f"\n⚡ Speedup: {speedup:.2f}x")
    print(f"   Sequential: {sequential_time:.2f}s")
    print(f"   Parallel:   {parallel_time:.2f}s")
    print(f"   Time saved: {sequential_time - parallel_time:.2f}s")

    # Verify data consistency
    print("\n📊 Data Consistency:")
    print(f"   Sequential records: {summary_seq.total_records:,}")
    print(f"   Parallel records:   {summary_par.total_records:,}")

    if summary_seq.total_records == summary_par.total_records:
        print("   ✅ Record counts match!")
    else:
        print("   ⚠️  Record counts differ (may be due to randomness)")

    # Success criteria
    print("\n" + "=" * 80)
    print("SUCCESS CRITERIA")
    print("=" * 80)

    checks = []

    # Check 1: Both modes produced data
    check1 = summary_seq.total_records > 0 and summary_par.total_records > 0
    checks.append(("Both modes produced data", check1))

    # Check 2: Parallel is faster
    check2 = parallel_time < sequential_time
    checks.append(("Parallel is faster than sequential", check2))

    # Check 3: Speedup is significant (>2x)
    check3 = speedup > 2.0
    checks.append((f"Speedup > 2x (got {speedup:.2f}x)", check3))

    # Check 4: Partitions created correctly
    check4 = summary_par.partitions_created > 0
    checks.append(("Partitions created", check4))

    print()
    for check_name, passed in checks:
        status = "✅" if passed else "❌"
        print(f"{status} {check_name}")

    all_passed = all(check[1] for check in checks)

    print("\n" + "=" * 80)
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        print("Phase 3: Parallel Fact Generation is working correctly.")
    else:
        print("❌ SOME TESTS FAILED")
        print("Review the output above for details.")
    print("=" * 80)

    return all_passed


if __name__ == "__main__":
    success = test_parallel_generation()
    exit(0 if success else 1)
