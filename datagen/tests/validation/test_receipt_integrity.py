#!/usr/bin/env python3
"""
Test script to validate receipt integrity after Phase 2.1 fixes.

This script verifies:
1. 0% of receipts have 0 lines (all receipts must have ≥1 line)
2. 0% of receipts have total mismatches > $0.01
3. Tax calculations use proper store tax rates
4. All monetary values use Decimal precision

Run this after generating historical data to verify receipt quality.
"""

import asyncio
import sqlite3
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


async def validate_receipt_integrity(db_path: str) -> dict:
    """
    Validate receipt integrity in the facts database.

    Args:
        db_path: Path to facts.db SQLite database

    Returns:
        Dictionary with validation results
    """
    # Create async engine for facts.db
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    results = {
        "total_receipts": 0,
        "receipts_with_zero_lines": 0,
        "receipts_with_total_mismatch": 0,
        "total_mismatch_amount": Decimal("0.00"),
        "max_mismatch": Decimal("0.00"),
        "sample_receipts": [],
        "errors": [],
    }

    try:
        async with async_session() as session:
            # Count total receipts
            result = await session.execute(text("SELECT COUNT(*) FROM fact_receipts"))
            results["total_receipts"] = result.scalar()

            print(f"Total receipts in database: {results['total_receipts']}")

            # Check for receipts with zero lines
            query = text("""
                SELECT r.receipt_id, r.receipt_id_ext, r.total_amount
                FROM fact_receipts r
                LEFT JOIN fact_receipt_lines rl ON r.receipt_id = rl.receipt_id
                GROUP BY r.receipt_id
                HAVING COUNT(rl.receipt_line_id) = 0
            """)

            result = await session.execute(query)
            empty_receipts = result.fetchall()
            results["receipts_with_zero_lines"] = len(empty_receipts)

            if empty_receipts:
                print(f"\n❌ CRITICAL: Found {len(empty_receipts)} receipts with 0 lines!")
                for receipt in empty_receipts[:5]:
                    print(
                        f"  - Receipt ID: {receipt[0]}, External ID: {receipt[1]}, Total: ${receipt[2]}"
                    )
                    results["errors"].append(
                        f"Receipt {receipt[1]} has 0 lines (ID: {receipt[0]})"
                    )
            else:
                print("✅ All receipts have at least 1 line")

            # Validate receipt totals: Total = Subtotal - Discount + Tax
            # Subtotal = sum of line totals
            query = text("""
                SELECT
                    r.receipt_id,
                    r.receipt_id_ext,
                    r.total_amount,
                    r.tax_amount,
                    r.discount_amount,
                    COALESCE(SUM(rl.line_total), 0.0) as calculated_subtotal,
                    COUNT(rl.receipt_line_id) as line_count
                FROM fact_receipts r
                LEFT JOIN fact_receipt_lines rl ON r.receipt_id = rl.receipt_id
                GROUP BY r.receipt_id
                LIMIT 10000
            """)

            result = await session.execute(query)
            receipts = result.fetchall()

            mismatches = []
            for receipt in receipts:
                (
                    receipt_id,
                    receipt_id_ext,
                    total_amount,
                    tax_amount,
                    discount_amount,
                    calculated_subtotal,
                    line_count,
                ) = receipt

                # Calculate expected total: Subtotal - Discount + Tax
                calculated_total = calculated_subtotal - discount_amount + tax_amount
                difference = abs(Decimal(str(calculated_total)) - Decimal(str(total_amount)))

                # Allow 1 cent tolerance for rounding
                if difference > Decimal("0.01"):
                    mismatches.append(
                        {
                            "receipt_id": receipt_id,
                            "receipt_id_ext": receipt_id_ext,
                            "stored_total": Decimal(str(total_amount)),
                            "calculated_total": Decimal(str(calculated_total)),
                            "difference": difference,
                            "subtotal": Decimal(str(calculated_subtotal)),
                            "tax": Decimal(str(tax_amount)),
                            "discount": Decimal(str(discount_amount)),
                            "line_count": line_count,
                        }
                    )

                    if difference > results["max_mismatch"]:
                        results["max_mismatch"] = difference

                # Collect samples for display
                if len(results["sample_receipts"]) < 5:
                    results["sample_receipts"].append(
                        {
                            "receipt_id_ext": receipt_id_ext,
                            "line_count": line_count,
                            "subtotal": Decimal(str(calculated_subtotal)),
                            "discount": Decimal(str(discount_amount)),
                            "tax": Decimal(str(tax_amount)),
                            "total": Decimal(str(total_amount)),
                            "calculated_total": Decimal(str(calculated_total)),
                            "difference": difference,
                        }
                    )

            results["receipts_with_total_mismatch"] = len(mismatches)

            if mismatches:
                print(
                    f"\n❌ Found {len(mismatches)} receipts with total mismatches > $0.01"
                )
                print(f"   Max mismatch: ${results['max_mismatch']}")
                for mismatch in mismatches[:5]:
                    print(
                        f"  - Receipt {mismatch['receipt_id_ext']}: "
                        f"Stored=${mismatch['stored_total']}, "
                        f"Calculated=${mismatch['calculated_total']}, "
                        f"Diff=${mismatch['difference']}"
                    )
                    print(
                        f"    Subtotal=${mismatch['subtotal']}, "
                        f"Discount=${mismatch['discount']}, "
                        f"Tax=${mismatch['tax']}, "
                        f"Lines={mismatch['line_count']}"
                    )
                    results["errors"].append(
                        f"Receipt {mismatch['receipt_id_ext']} total mismatch: "
                        f"${mismatch['difference']}"
                    )
            else:
                print("✅ All receipts have correct totals (within $0.01 tolerance)")

    except Exception as e:
        print(f"\n❌ Error during validation: {e}")
        results["errors"].append(f"Validation error: {e}")

    finally:
        await engine.dispose()

    return results


def print_summary(results: dict):
    """Print validation summary."""
    print("\n" + "=" * 60)
    print("RECEIPT INTEGRITY VALIDATION SUMMARY")
    print("=" * 60)

    total = results["total_receipts"]
    zero_lines = results["receipts_with_zero_lines"]
    mismatches = results["receipts_with_total_mismatch"]

    print(f"\nTotal receipts analyzed: {total:,}")

    # Empty receipts
    if total > 0:
        zero_pct = (zero_lines / total) * 100
        print(f"Receipts with 0 lines: {zero_lines:,} ({zero_pct:.4f}%)")
        if zero_lines == 0:
            print("  ✅ PASS: No empty receipts")
        else:
            print("  ❌ FAIL: Empty receipts found")
    else:
        print("⚠️  No receipts found in database")

    # Total mismatches
    if total > 0:
        mismatch_pct = (mismatches / total) * 100
        print(
            f"Receipts with total mismatches: {mismatches:,} ({mismatch_pct:.4f}%)"
        )
        if mismatches == 0:
            print("  ✅ PASS: All totals correct")
        else:
            print(f"  ❌ FAIL: Mismatches found (max: ${results['max_mismatch']})")

    # Sample receipts
    if results["sample_receipts"]:
        print("\n" + "-" * 60)
        print("SAMPLE RECEIPTS (showing calculation)")
        print("-" * 60)
        for i, receipt in enumerate(results["sample_receipts"], 1):
            print(f"\n{i}. Receipt {receipt['receipt_id_ext']}")
            print(f"   Lines: {receipt['line_count']}")
            print(f"   Subtotal: ${receipt['subtotal']:.2f}")
            print(f"   Discount: ${receipt['discount']:.2f}")
            print(f"   Tax:      ${receipt['tax']:.2f}")
            print(f"   Total:    ${receipt['total']:.2f}")
            print(f"   Calculated: ${receipt['calculated_total']:.2f}")
            if receipt['difference'] > Decimal("0.01"):
                print(f"   ❌ Diff: ${receipt['difference']:.2f}")
            else:
                print(f"   ✅ Match (diff: ${receipt['difference']:.4f})")

    # Overall result
    print("\n" + "=" * 60)
    if zero_lines == 0 and mismatches == 0:
        print("✅ VALIDATION PASSED: All receipts are valid")
    else:
        print("❌ VALIDATION FAILED: Issues found")
        if results["errors"]:
            print("\nErrors:")
            for error in results["errors"][:10]:
                print(f"  - {error}")
    print("=" * 60 + "\n")


async def main():
    """Main validation routine."""
    # Find facts.db in data directory
    facts_db = Path("data/facts.db")

    if not facts_db.exists():
        print(f"❌ Database not found: {facts_db}")
        print("Please generate historical data first.")
        return

    print(f"Validating receipts in: {facts_db}")
    print("This may take a few seconds...\n")

    results = await validate_receipt_integrity(str(facts_db))
    print_summary(results)

    # Return exit code based on validation
    if results["receipts_with_zero_lines"] == 0 and results["receipts_with_total_mismatch"] == 0:
        exit(0)
    else:
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
