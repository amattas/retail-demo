#!/usr/bin/env python3
"""
Test script to validate online order lifecycle implementation.

This script generates a small sample of online orders and validates:
1. Multi-line support (orders with multiple products)
2. Status progression (created -> picked -> shipped -> delivered)
3. Proper financial calculations (Subtotal + Tax = Total)
4. TenderType distribution
5. Timing constraints (picked after created, shipped after picked, etc.)
"""

import asyncio

# Add src to path
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.config.models import RetailConfig
from retail_datagen.generators.fact_generators import FactDataGenerator


async def test_online_orders():
    """Test online order generation and validate results."""
    print("="*80)
    print("ONLINE ORDER LIFECYCLE TEST")
    print("="*80)

    # Load configuration
    config_path = Path(__file__).parent / "config.json"
    config = RetailConfig.load_from_file(str(config_path))

    # Reduce volume for testing
    config.volume.online_orders_per_day = 5  # Generate just 5 orders per day

    # Initialize generator
    print("\n1. Initializing FactDataGenerator...")
    generator = FactDataGenerator(config=config, seed=42)

    # Load master data
    print("2. Loading master data...")
    await generator.load_master_data()

    print(f"   - Loaded {len(generator.customers)} customers")
    print(f"   - Loaded {len(generator.stores)} stores")
    print(f"   - Loaded {len(generator.distribution_centers)} DCs")
    print(f"   - Loaded {len(generator.products)} products")

    # Generate orders for a single date
    test_date = datetime(2024, 1, 15)
    print(f"\n3. Generating online orders for {test_date.date()}...")

    orders, store_txn, dc_txn = generator._generate_online_orders(test_date)

    print(f"   - Generated {len(orders)} order records")
    print(f"   - Generated {len(store_txn)} store inventory transactions")
    print(f"   - Generated {len(dc_txn)} DC inventory transactions")

    # Convert to DataFrame for analysis
    df = pd.DataFrame(orders)

    # Analyze results
    print("\n" + "="*80)
    print("VALIDATION RESULTS")
    print("="*80)

    # 1. Multi-line support
    print("\n1. MULTI-LINE ORDER SUPPORT:")
    order_counts = df.groupby('OrderId').size()
    unique_orders = len(order_counts.unique())
    products_per_order = df.groupby(['OrderId', 'FulfillmentStatus']).ProductID.nunique()
    print(f"   - Unique orders: {len(order_counts) // 4}")  # Divide by 4 statuses
    print("   - Products per order (at created status):")
    created_df = df[df['FulfillmentStatus'] == 'created']
    products_in_orders = created_df.groupby('OrderId')['ProductID'].count()
    print(f"     Min: {products_in_orders.min()}, Max: {products_in_orders.max()}, Mean: {products_in_orders.mean():.2f}")

    # 2. Status progression
    print("\n2. STATUS PROGRESSION:")
    status_counts = df['FulfillmentStatus'].value_counts()
    print("   Status distribution:")
    for status, count in status_counts.items():
        print(f"     - {status}: {count} records")

    # Check that all statuses are present
    expected_statuses = {'created', 'picked', 'shipped', 'delivered'}
    actual_statuses = set(df['FulfillmentStatus'].unique())
    if expected_statuses == actual_statuses:
        print("   ✓ All expected statuses present")
    else:
        print(f"   ✗ Missing statuses: {expected_statuses - actual_statuses}")

    # 3. Financial calculations
    print("\n3. FINANCIAL CALCULATIONS:")
    financial_errors = []
    for _, row in df.iterrows():
        subtotal = Decimal(row['Subtotal'])
        tax = Decimal(row['Tax'])
        total = Decimal(row['Total'])
        calculated_total = subtotal + tax

        if abs(total - calculated_total) > Decimal('0.01'):
            financial_errors.append(row['OrderId'])

    if not financial_errors:
        print(f"   ✓ All {len(df)} records have correct Total = Subtotal + Tax")
    else:
        print(f"   ✗ {len(financial_errors)} records have incorrect totals")
        print(f"     Error in orders: {financial_errors[:5]}")

    # 4. TenderType distribution
    print("\n4. TENDER TYPE DISTRIBUTION:")
    tender_counts = df[df['FulfillmentStatus'] == 'created']['TenderType'].value_counts()
    total_orders = len(df[df['FulfillmentStatus'] == 'created'])
    print("   Tender type distribution:")
    for tender, count in tender_counts.items():
        pct = (count / total_orders) * 100
        print(f"     - {tender}: {count} ({pct:.1f}%)")

    # Check for new tender types
    expected_tenders = {'CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'OTHER'}
    actual_tenders = set(df['TenderType'].unique())
    if actual_tenders.issubset(expected_tenders):
        print("   ✓ All tender types are valid")
    else:
        print(f"   ✗ Unexpected tender types: {actual_tenders - expected_tenders}")

    # 5. Timing constraints
    print("\n5. TIMING CONSTRAINTS:")
    timing_errors = []
    for order_id in df['OrderId'].unique():
        order_records = df[df['OrderId'] == order_id].sort_values('EventTS')
        timestamps = {
            row['FulfillmentStatus']: pd.to_datetime(row['EventTS'])
            for _, row in order_records.iterrows()
        }

        # Check ordering
        if 'created' in timestamps and 'picked' in timestamps:
            if timestamps['picked'] <= timestamps['created']:
                timing_errors.append(f"{order_id}: picked not after created")

        if 'picked' in timestamps and 'shipped' in timestamps:
            if timestamps['shipped'] <= timestamps['picked']:
                timing_errors.append(f"{order_id}: shipped not after picked")

        if 'shipped' in timestamps and 'delivered' in timestamps:
            if timestamps['delivered'] <= timestamps['shipped']:
                timing_errors.append(f"{order_id}: delivered not after shipped")

    if not timing_errors:
        print("   ✓ All order statuses follow correct time progression")
    else:
        print(f"   ✗ {len(timing_errors)} timing errors found:")
        for error in timing_errors[:5]:
            print(f"     - {error}")

    # 6. Sample orders
    print("\n" + "="*80)
    print("SAMPLE ORDERS (First 2 complete lifecycles)")
    print("="*80)

    sample_orders = df['OrderId'].unique()[:2]
    for order_id in sample_orders:
        order_df = df[df['OrderId'] == order_id].sort_values('EventTS')
        print(f"\nOrder: {order_id}")
        print(f"  Customer: {order_df.iloc[0]['CustomerID']}")
        print(f"  Fulfillment: {order_df.iloc[0]['FulfillmentMode']} from {order_df.iloc[0]['NodeType']} {order_df.iloc[0]['NodeID']}")
        print(f"  Tender: {order_df.iloc[0]['TenderType']}")
        print(f"  Subtotal: ${order_df.iloc[0]['Subtotal']}, Tax: ${order_df.iloc[0]['Tax']}, Total: ${order_df.iloc[0]['Total']}")
        print("  Products:")

        created_records = order_df[order_df['FulfillmentStatus'] == 'created']
        for _, row in created_records.iterrows():
            print(f"    - Product {row['ProductID']}: Qty {row['Qty']}")

        print("  Status progression:")
        for status in ['created', 'picked', 'shipped', 'delivered']:
            status_records = order_df[order_df['FulfillmentStatus'] == status]
            if not status_records.empty:
                ts = pd.to_datetime(status_records.iloc[0]['EventTS'])
                print(f"    - {status}: {ts}")

    # 7. Inventory transactions
    print("\n" + "="*80)
    print("INVENTORY TRANSACTIONS")
    print("="*80)
    print(f"Store transactions: {len(store_txn)}")
    print(f"DC transactions: {len(dc_txn)}")

    if store_txn:
        print("\nSample store transaction:")
        print(f"  {store_txn[0]}")

    if dc_txn:
        print("\nSample DC transaction:")
        print(f"  {dc_txn[0]}")

    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)


if __name__ == "__main__":
    asyncio.run(test_online_orders())
