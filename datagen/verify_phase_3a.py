#!/usr/bin/env python3
"""
Verification script for Phase 3A implementation.

Checks:
1. File syntax is valid (imports correctly)
2. All async methods exist
3. Backward compatibility maintained
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def verify_imports():
    """Verify that the updated file imports correctly."""
    print("✓ Checking imports...")
    try:
        from retail_datagen.generators.master_generator import MasterDataGenerator
        print("  ✓ MasterDataGenerator imports successfully")
        return True
    except Exception as e:
        print(f"  ✗ Import failed: {e}")
        return False

def verify_async_methods():
    """Verify all async methods exist."""
    print("\n✓ Checking async methods...")
    from retail_datagen.generators.master_generator import MasterDataGenerator

    required_methods = [
        "generate_all_master_data_async",
        "generate_geography_master_async",
        "generate_distribution_centers_async",
        "generate_stores_async",
        "generate_trucks_async",
        "generate_customers_async",
        "generate_products_master_async",
        "generate_dc_inventory_snapshots_async",
        "generate_store_inventory_snapshots_async",
        "_insert_to_db",
        "_map_pydantic_to_db_columns",
        "_run_async_in_thread",
    ]

    all_exist = True
    for method_name in required_methods:
        if hasattr(MasterDataGenerator, method_name):
            print(f"  ✓ {method_name} exists")
        else:
            print(f"  ✗ {method_name} missing")
            all_exist = False

    return all_exist

def verify_backward_compatibility():
    """Verify backward-compatible methods still exist."""
    print("\n✓ Checking backward compatibility...")
    from retail_datagen.generators.master_generator import MasterDataGenerator

    legacy_methods = [
        "generate_all_master_data",
        "generate_geography_master",
        "generate_distribution_centers",
        "generate_stores",
        "generate_trucks",
        "generate_customers",
        "generate_products_master",
        "generate_dc_inventory_snapshots",
        "generate_store_inventory_snapshots",
    ]

    all_exist = True
    for method_name in legacy_methods:
        if hasattr(MasterDataGenerator, method_name):
            print(f"  ✓ {method_name} exists (backward compatible)")
        else:
            print(f"  ✗ {method_name} missing (BREAKING CHANGE)")
            all_exist = False

    return all_exist

def verify_method_signatures():
    """Verify method signatures are correct."""
    print("\n✓ Checking method signatures...")
    import inspect
    from retail_datagen.generators.master_generator import MasterDataGenerator

    # Check generate_all_master_data_async signature
    sig = inspect.signature(MasterDataGenerator.generate_all_master_data_async)
    params = list(sig.parameters.keys())
    expected_params = ['self', 'session', 'export_csv', 'output_dir', 'parallel']

    if params == expected_params:
        print(f"  ✓ generate_all_master_data_async signature correct")
        return True
    else:
        print(f"  ✗ generate_all_master_data_async signature incorrect")
        print(f"    Expected: {expected_params}")
        print(f"    Got: {params}")
        return False

def main():
    """Run all verification checks."""
    print("="* 60)
    print("Phase 3A Implementation Verification")
    print("="* 60)

    checks = [
        ("Imports", verify_imports),
        ("Async Methods", verify_async_methods),
        ("Backward Compatibility", verify_backward_compatibility),
        ("Method Signatures", verify_method_signatures),
    ]

    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n✗ {name} check failed with exception: {e}")
            results.append((name, False))

    print("\n" + "="* 60)
    print("Summary")
    print("="* 60)

    all_passed = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {name}")
        if not passed:
            all_passed = False

    print("="* 60)

    if all_passed:
        print("\n✅ All checks passed! Phase 3A implementation is valid.")
        return 0
    else:
        print("\n❌ Some checks failed. Please review the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
