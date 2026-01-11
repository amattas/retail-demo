#!/usr/bin/env python3
"""
Validation script for Bronze layer shortcuts.

Verifies that all required shortcuts exist in the Bronze schema (cusn).
Run this after executing 00-create-bronze-shortcuts.ipynb to ensure
the Bronze layer is complete before proceeding to Silver transformation.

Usage:
    python validate-bronze-shortcuts.py

Expected Shortcuts:
    - 24 ADLSv2 parquet shortcuts (6 dimensions + 18 facts)
    - 18 Eventhouse streaming event shortcuts
    - Total: 42 shortcuts

Exit Codes:
    0: All shortcuts valid
    1: Missing shortcuts or validation errors
"""

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import sys

# Expected Bronze shortcuts
BRONZE_SCHEMA = "cusn"

# ADLSv2 Parquet Shortcuts (24 total)
EXPECTED_BATCH_SHORTCUTS = [
    # Dimensions (6)
    "dim_geographies",
    "dim_stores",
    "dim_distribution_centers",
    "dim_trucks",
    "dim_customers",
    "dim_products",

    # Facts (18)
    "fact_receipts",
    "fact_receipt_lines",
    "fact_store_inventory_txn",
    "fact_dc_inventory_txn",
    "fact_truck_moves",
    "fact_truck_inventory",
    "fact_foot_traffic",
    "fact_ble_pings",
    "fact_customer_zone_changes",
    "fact_marketing",
    "fact_online_order_headers",
    "fact_online_order_lines",
    "fact_payments",
    "fact_store_ops",
    "fact_stockouts",
    "fact_promotions",
    "fact_promo_lines",
    "fact_reorders",
]

# Eventhouse Streaming Shortcuts (18 total)
EXPECTED_STREAMING_SHORTCUTS = [
    # Transaction events (3)
    "receipt_created",
    "receipt_line_added",
    "payment_processed",

    # Inventory events (3)
    "inventory_updated",
    "stockout_detected",
    "reorder_triggered",

    # Customer events (3)
    "customer_entered",
    "customer_zone_changed",
    "ble_ping_detected",

    # Operational events (4)
    "truck_arrived",
    "truck_departed",
    "store_opened",
    "store_closed",

    # Marketing events (2)
    "ad_impression",
    "promotion_applied",

    # Omnichannel events (3)
    "online_order_created",
    "online_order_picked",
    "online_order_shipped",
]


def check_shortcut_exists(spark, schema, table_name):
    """
    Check if a shortcut exists and is readable.

    Returns:
        (exists: bool, row_count: int, error: str)
    """
    try:
        df = spark.table(f"{schema}.{table_name}")
        count = df.count()
        return True, count, None
    except AnalysisException as e:
        if "does not exist" in str(e).lower():
            return False, 0, "Table does not exist"
        elif "permission" in str(e).lower():
            return True, 0, "Permission denied"
        else:
            return True, 0, f"Analysis error: {str(e)[:100]}"
    except Exception as e:
        return True, 0, f"Unexpected error: {str(e)[:100]}"


def validate_bronze_shortcuts():
    """Main validation function."""
    print("="*80)
    print("BRONZE LAYER SHORTCUT VALIDATION")
    print("="*80)
    print(f"Schema: {BRONZE_SCHEMA}")
    print(f"Expected batch shortcuts: {len(EXPECTED_BATCH_SHORTCUTS)}")
    print(f"Expected streaming shortcuts: {len(EXPECTED_STREAMING_SHORTCUTS)}")
    print(f"Total expected: {len(EXPECTED_BATCH_SHORTCUTS) + len(EXPECTED_STREAMING_SHORTCUTS)}")
    print()

    # Initialize Spark
    spark = SparkSession.builder.appName("ValidateBronzeShortcuts").getOrCreate()

    # Validate batch shortcuts
    print("BATCH SHORTCUTS (ADLSv2 Parquet)")
    print("-"*80)
    batch_missing = []
    batch_errors = []

    for table in EXPECTED_BATCH_SHORTCUTS:
        exists, count, error = check_shortcut_exists(spark, BRONZE_SCHEMA, table)

        if not exists:
            print(f"  ✗ {table:40s} MISSING")
            batch_missing.append(table)
        elif error:
            print(f"  ⚠ {table:40s} ERROR: {error}")
            batch_errors.append((table, error))
        else:
            print(f"  ✓ {table:40s} {count:>10,} rows")

    print()

    # Validate streaming shortcuts
    print("STREAMING SHORTCUTS (Eventhouse)")
    print("-"*80)
    streaming_missing = []
    streaming_errors = []

    for table in EXPECTED_STREAMING_SHORTCUTS:
        exists, count, error = check_shortcut_exists(spark, BRONZE_SCHEMA, table)

        if not exists:
            print(f"  ✗ {table:40s} MISSING")
            streaming_missing.append(table)
        elif error:
            print(f"  ⚠ {table:40s} ERROR: {error}")
            streaming_errors.append((table, error))
        else:
            print(f"  ✓ {table:40s} {count:>10,} rows")

    print()

    # Summary
    print("="*80)
    print("VALIDATION SUMMARY")
    print("="*80)

    total_expected = len(EXPECTED_BATCH_SHORTCUTS) + len(EXPECTED_STREAMING_SHORTCUTS)
    total_missing = len(batch_missing) + len(streaming_missing)
    total_errors = len(batch_errors) + len(streaming_errors)
    total_valid = total_expected - total_missing - total_errors

    print(f"Total shortcuts expected: {total_expected}")
    print(f"  ✓ Valid:   {total_valid}")
    print(f"  ✗ Missing: {total_missing}")
    print(f"  ⚠ Errors:  {total_errors}")
    print()

    # Detailed error reporting
    if batch_missing:
        print("MISSING BATCH SHORTCUTS:")
        for table in batch_missing:
            print(f"  - {BRONZE_SCHEMA}.{table}")
        print()
        print("ACTION: Run 00-create-bronze-shortcuts.ipynb to create ADLSv2 parquet shortcuts")
        print()

    if streaming_missing:
        print("MISSING STREAMING SHORTCUTS:")
        for table in streaming_missing:
            print(f"  - {BRONZE_SCHEMA}.{table}")
        print()
        print("ACTION: Manually create Eventhouse shortcuts via Fabric Portal:")
        print("  1. Navigate to Lakehouse in Fabric workspace")
        print("  2. Right-click Tables → New shortcut → Eventhouse")
        print("  3. Select Eventhouse KQL database")
        print("  4. Select event tables listed above")
        print("  5. Target location: Tables/cusn/<table_name>")
        print()

    if batch_errors or streaming_errors:
        print("SHORTCUT ERRORS:")
        for table, error in batch_errors + streaming_errors:
            print(f"  - {BRONZE_SCHEMA}.{table}: {error}")
        print()
        print("ACTION: Check permissions, network connectivity, and source availability")
        print()

    # Exit status
    if total_missing > 0 or total_errors > 0:
        print("❌ VALIDATION FAILED")
        print()
        print("Bronze layer is incomplete. Do not proceed to Silver transformation")
        print("until all shortcuts are created and accessible.")
        return 1
    else:
        print("✅ VALIDATION PASSED")
        print()
        print("Bronze layer is complete with all 42 shortcuts.")
        print("Safe to proceed with Silver transformation (02-onelake-to-silver.ipynb)")
        return 0


if __name__ == "__main__":
    try:
        exit_code = validate_bronze_shortcuts()
        sys.exit(exit_code)
    except Exception as e:
        print(f"\n❌ VALIDATION ERROR: {e}")
        sys.exit(1)
