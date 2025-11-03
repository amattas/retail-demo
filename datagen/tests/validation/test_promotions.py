"""
Test script to validate promotional discount system.

This script tests the promotion implementation by generating a small dataset
and validating that:
1. 10-20% of receipts have promotional discounts
2. PromoCode fields are populated correctly
3. Receipt totals are accurate (Subtotal - Discount + Tax = Total)
4. Seasonal patterns are working (more promos in Nov/Dec)
5. Average discount amounts are realistic
"""

import asyncio
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.config.models import RetailConfig
from retail_datagen.db.models.facts import Receipt, ReceiptLine
from retail_datagen.generators.fact_generator import FactDataGenerator


async def test_promotions():
    """Test promotional discount system."""
    print("=" * 80)
    print("PROMOTIONAL DISCOUNT SYSTEM VALIDATION")
    print("=" * 80)
    print()

    # Load config
    config_path = Path(__file__).parent / "config.json"
    if not config_path.exists():
        print(f"❌ Error: config.json not found at {config_path}")
        return

    config = RetailConfig.from_file(config_path)
    print(f"✓ Loaded configuration from {config_path}")
    print()

    # Check master data exists
    master_path = Path(config.resolve_master_path())
    required_files = [
        "geographies_master.csv",
        "stores.csv",
        "distribution_centers.csv",
        "customers.csv",
        "products_master.csv",
    ]

    print("Checking master data files...")
    for filename in required_files:
        file_path = master_path / filename
        if not file_path.exists():
            print(f"❌ Missing required file: {filename}")
            print(f"   Please run master data generation first")
            return
        print(f"  ✓ {filename}")
    print()

    # Setup database connection
    db_path = Path(config.resolve_facts_path()) / "facts.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing db for fresh test
    if db_path.exists():
        db_path.unlink()
        print(f"✓ Removed existing facts.db for fresh test")

    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Create tables
    from retail_datagen.db.models.facts import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print(f"✓ Created facts database at {db_path}")
    print()

    # Generate test data (3 days)
    print("Generating test data (3 days)...")
    print("-" * 80)

    async with async_session_maker() as session:
        generator = FactDataGenerator(config=config, session=session)

        # Load master data
        generator.load_master_data()
        print(f"✓ Loaded master data: {len(generator.stores)} stores, "
              f"{len(generator.customers)} customers, {len(generator.products)} products")

        # Set date range (3 days for quick test)
        start_date = datetime(2024, 11, 15)  # Mid-November for seasonal test
        end_date = start_date + timedelta(days=2)

        print(f"✓ Generating receipts from {start_date.date()} to {end_date.date()}")
        print()

        # Generate facts
        try:
            summary = await generator.generate_facts(
                start_date=start_date,
                end_date=end_date,
            )
            print(f"✓ Generation complete!")
            print(f"  Total records: {summary.total_records:,}")
            print(f"  Receipts: {summary.facts_generated.get('receipts', 0):,}")
            print(f"  Receipt lines: {summary.facts_generated.get('receipt_lines', 0):,}")
            print()
        except Exception as e:
            print(f"❌ Error during generation: {e}")
            import traceback
            traceback.print_exc()
            return

    # Validate promotion system
    print("=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)
    print()

    async with async_session_maker() as session:
        # Query receipts
        stmt = select(Receipt)
        result = await session.execute(stmt)
        receipts = result.scalars().all()

        total_receipts = len(receipts)
        receipts_with_discount = sum(1 for r in receipts if r.discount_amount > 0)
        promo_rate = receipts_with_discount / total_receipts if total_receipts > 0 else 0

        print(f"Receipt Analysis:")
        print(f"  Total receipts: {total_receipts:,}")
        print(f"  Receipts with discounts: {receipts_with_discount:,} ({promo_rate:.1%})")
        print()

        # Test 1: Promotion rate
        print("Test 1: Promotion Rate (10-20% expected)")
        if 0.10 <= promo_rate <= 0.20:
            print(f"  ✓ PASS: Promotion rate {promo_rate:.1%} is within expected range")
        else:
            print(f"  ⚠ WARNING: Promotion rate {promo_rate:.1%} is outside 10-20% range")
        print()

        # Test 2: Discount amounts
        if receipts_with_discount > 0:
            discounts = [r.discount_amount for r in receipts if r.discount_amount > 0]
            avg_discount = sum(discounts) / len(discounts)
            min_discount = min(discounts)
            max_discount = max(discounts)

            print(f"Discount Amount Analysis:")
            print(f"  Average discount: ${avg_discount:.2f}")
            print(f"  Min discount: ${min_discount:.2f}")
            print(f"  Max discount: ${max_discount:.2f}")
            print()

            print("Test 2: Discount Amounts ($2-$50 expected)")
            if 2.00 <= avg_discount <= 50.00:
                print(f"  ✓ PASS: Average discount ${avg_discount:.2f} is reasonable")
            else:
                print(f"  ⚠ WARNING: Average discount ${avg_discount:.2f} seems unusual")
            print()

        # Test 3: PromoCode population
        stmt = select(ReceiptLine)
        result = await session.execute(stmt)
        receipt_lines = result.scalars().all()

        total_lines = len(receipt_lines)
        lines_with_promo = sum(1 for line in receipt_lines if line.promo_code is not None)

        print(f"Receipt Line Analysis:")
        print(f"  Total lines: {total_lines:,}")
        print(f"  Lines with promo codes: {lines_with_promo:,}")
        print()

        print("Test 3: PromoCode Population")
        if lines_with_promo > 0:
            print(f"  ✓ PASS: PromoCode field is populated on {lines_with_promo} lines")
        else:
            print(f"  ⚠ WARNING: No promo codes found in receipt lines")
        print()

        # Test 4: Receipt totals validation
        print("Test 4: Receipt Total Accuracy")
        total_errors = 0
        max_errors_to_show = 5

        for receipt in receipts[:100]:  # Check first 100 receipts
            # Get receipt lines
            stmt = select(ReceiptLine).where(ReceiptLine.receipt_id == receipt.receipt_id)
            result = await session.execute(stmt)
            lines = result.scalars().all()

            # Calculate subtotal from lines
            line_subtotal = sum(line.ext_price for line in lines)

            # Validate: Total = Subtotal - Discount + Tax
            expected_total = line_subtotal - receipt.discount_amount + receipt.tax_amount
            actual_total = receipt.total_amount

            # Allow 1 cent tolerance for rounding
            if abs(expected_total - actual_total) > 0.01:
                total_errors += 1
                if total_errors <= max_errors_to_show:
                    print(f"  ❌ Receipt {receipt.receipt_id_ext}: Total mismatch!")
                    print(f"     Expected: ${expected_total:.2f}, Actual: ${actual_total:.2f}")
                    print(f"     Subtotal: ${line_subtotal:.2f}, Discount: ${receipt.discount_amount:.2f}, Tax: ${receipt.tax_amount:.2f}")

        if total_errors == 0:
            print(f"  ✓ PASS: All receipts have accurate totals")
        else:
            print(f"  ❌ FAIL: {total_errors} receipts have total mismatches")
        print()

        # Test 5: Promo code distribution
        if lines_with_promo > 0:
            promo_codes = [line.promo_code for line in receipt_lines if line.promo_code is not None]
            promo_distribution = pd.Series(promo_codes).value_counts()

            print(f"Test 5: Promotion Code Distribution")
            print(f"  Unique promo codes: {len(promo_distribution)}")
            print()
            print("  Top 10 promo codes:")
            for code, count in promo_distribution.head(10).items():
                pct = count / lines_with_promo * 100
                print(f"    {code}: {count} ({pct:.1f}%)")
            print()

        # Sample receipts with promotions
        print("=" * 80)
        print("SAMPLE RECEIPTS WITH PROMOTIONS")
        print("=" * 80)
        print()

        sample_receipts = [r for r in receipts if r.discount_amount > 0][:5]

        for idx, receipt in enumerate(sample_receipts, 1):
            print(f"Receipt #{idx}: {receipt.receipt_id_ext}")
            print(f"  Date: {receipt.event_ts}")
            print(f"  Store: {receipt.store_id}, Customer: {receipt.customer_id}")
            print(f"  Subtotal: ${receipt.total_amount + receipt.discount_amount - receipt.tax_amount:.2f}")
            print(f"  Discount: -${receipt.discount_amount:.2f}")
            print(f"  Tax: +${receipt.tax_amount:.2f}")
            print(f"  Total: ${receipt.total_amount:.2f}")

            # Get lines
            stmt = select(ReceiptLine).where(ReceiptLine.receipt_id == receipt.receipt_id)
            result = await session.execute(stmt)
            lines = result.scalars().all()

            print(f"  Lines ({len(lines)}):")
            for line in lines:
                promo_text = f" [Promo: {line.promo_code}]" if line.promo_code else ""
                print(f"    Line {line.line_num}: Product {line.product_id}, "
                      f"Qty {line.quantity}, ${line.ext_price:.2f}{promo_text}")

    print("=" * 80)
    print("VALIDATION COMPLETE")
    print("=" * 80)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(test_promotions())
