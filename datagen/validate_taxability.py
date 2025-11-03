#!/usr/bin/env python3
"""
Validation script for Phase 1.4: Product Taxability Implementation

This script validates that product taxability flags are correctly assigned
during product generation and provides distribution statistics.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.master_generator import MasterDataGenerator
from retail_datagen.shared.models import ProductTaxability


async def validate_taxability():
    """Validate product taxability implementation."""
    print("=" * 80)
    print("Phase 1.4: Product Taxability Validation")
    print("=" * 80)
    print()

    # Load configuration
    config = RetailConfig.from_yaml("config.yaml")

    # Override to generate fewer products for faster testing
    config.volume.total_products = 1000

    print(f"Generating {config.volume.total_products} test products...")
    print()

    # Initialize generator
    generator = MasterDataGenerator(config)
    generator._load_dictionary_data()

    # Generate only products (skip other master data)
    generator.generate_products_master()

    print()
    print("=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)
    print()

    # Analyze taxability distribution
    taxability_counts = {
        ProductTaxability.TAXABLE: 0,
        ProductTaxability.NON_TAXABLE: 0,
        ProductTaxability.REDUCED_RATE: 0,
    }

    products_by_category = {}

    for product in generator.products_master:
        taxability_counts[product.taxability] += 1

        # Track by department for analysis
        dept = product.Department
        if dept not in products_by_category:
            products_by_category[dept] = {
                ProductTaxability.TAXABLE: 0,
                ProductTaxability.NON_TAXABLE: 0,
                ProductTaxability.REDUCED_RATE: 0,
            }
        products_by_category[dept][product.taxability] += 1

    # Calculate percentages
    total = len(generator.products_master)
    taxable_pct = (taxability_counts[ProductTaxability.TAXABLE] / total) * 100
    non_taxable_pct = (taxability_counts[ProductTaxability.NON_TAXABLE] / total) * 100
    reduced_pct = (taxability_counts[ProductTaxability.REDUCED_RATE] / total) * 100

    # Print overall distribution
    print("1. OVERALL TAXABILITY DISTRIBUTION")
    print("-" * 80)
    print(f"Total Products: {total:,}")
    print()
    print(f"TAXABLE:         {taxability_counts[ProductTaxability.TAXABLE]:>6,} ({taxable_pct:>5.1f}%)")
    print(f"NON_TAXABLE:     {taxability_counts[ProductTaxability.NON_TAXABLE]:>6,} ({non_taxable_pct:>5.1f}%)")
    print(f"REDUCED_RATE:    {taxability_counts[ProductTaxability.REDUCED_RATE]:>6,} ({reduced_pct:>5.1f}%)")
    print()

    # Print by department
    print("2. TAXABILITY BY DEPARTMENT")
    print("-" * 80)
    for dept in sorted(products_by_category.keys()):
        counts = products_by_category[dept]
        dept_total = sum(counts.values())
        print(f"\n{dept} ({dept_total} products):")

        for tax_type in [ProductTaxability.NON_TAXABLE, ProductTaxability.REDUCED_RATE, ProductTaxability.TAXABLE]:
            count = counts[tax_type]
            pct = (count / dept_total) * 100 if dept_total > 0 else 0
            if count > 0:
                print(f"  {tax_type.value:20s}: {count:>4} ({pct:>5.1f}%)")

    print()
    print("3. SAMPLE PRODUCTS (10 random samples)")
    print("-" * 80)

    import random
    sample_products = random.sample(generator.products_master, min(10, len(generator.products_master)))

    for i, product in enumerate(sample_products, 1):
        print(f"\n{i}. {product.ProductName}")
        print(f"   Department: {product.Department}")
        print(f"   Category:   {product.Category}")
        print(f"   Taxability: {product.taxability.value}")
        print(f"   Price:      ${product.SalePrice:.2f}")

    print()
    print("=" * 80)
    print("4. VALIDATION CHECKS")
    print("=" * 80)
    print()

    # Validation checks
    checks_passed = []
    checks_failed = []

    # Check 1: All products have taxability assigned
    if all(hasattr(p, 'taxability') and p.taxability is not None for p in generator.products_master):
        checks_passed.append("✓ All products have taxability assigned")
    else:
        checks_failed.append("✗ Some products missing taxability field")

    # Check 2: Grocery products are NON_TAXABLE
    grocery_products = [p for p in generator.products_master if p.Department.lower() == 'grocery']
    if grocery_products:
        non_taxable_grocery = sum(1 for p in grocery_products if p.taxability == ProductTaxability.NON_TAXABLE)
        grocery_non_taxable_pct = (non_taxable_grocery / len(grocery_products)) * 100

        if grocery_non_taxable_pct >= 95:  # Allow 5% margin for edge cases
            checks_passed.append(f"✓ Grocery products are NON_TAXABLE ({grocery_non_taxable_pct:.1f}%)")
        else:
            checks_failed.append(f"✗ Grocery products should be mostly NON_TAXABLE (only {grocery_non_taxable_pct:.1f}%)")

    # Check 3: Distribution is reasonable
    if 50 <= taxable_pct <= 80 and 15 <= non_taxable_pct <= 40:
        checks_passed.append(f"✓ Overall distribution is reasonable")
    else:
        checks_failed.append(f"✗ Distribution seems off (TAXABLE: {taxable_pct:.1f}%, NON_TAXABLE: {non_taxable_pct:.1f}%)")

    # Check 4: No None/null taxability values
    none_count = sum(1 for p in generator.products_master if p.taxability is None)
    if none_count == 0:
        checks_passed.append("✓ No null/None taxability values")
    else:
        checks_failed.append(f"✗ Found {none_count} products with null taxability")

    # Print validation results
    for check in checks_passed:
        print(check)

    for check in checks_failed:
        print(check)

    print()

    if checks_failed:
        print("⚠️  VALIDATION FAILED - Some checks did not pass")
        return False
    else:
        print("✅ VALIDATION PASSED - All checks successful!")
        return True


if __name__ == "__main__":
    success = asyncio.run(validate_taxability())
    sys.exit(0 if success else 1)
