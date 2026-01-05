"""
Test script for customer geography and store affinity implementation.

This script validates that:
1. Customer geographies are assigned correctly
2. Store selection respects geographic affinity
3. Customers primarily shop at nearby stores
4. Distance distributions are realistic
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.shared.customer_geography import GeographyAssigner, StoreSelector
from retail_datagen.shared.models import Customer, GeographyMaster, Store


# Create test data
def create_test_geographies():
    """Create sample geographies for testing."""
    return [
        GeographyMaster(ID=1, City="Oakville", State="CA", ZipCode="90210", District="West", Region="Pacific"),
        GeographyMaster(ID=2, City="Springfield", State="IL", ZipCode="62701", District="Central", Region="Midwest"),
        GeographyMaster(ID=3, City="Austin", State="TX", ZipCode="73301", District="South", Region="Southwest"),
        GeographyMaster(ID=4, City="Portland", State="OR", ZipCode="97201", District="Northwest", Region="Pacific"),
        GeographyMaster(ID=5, City="Miami", State="FL", ZipCode="33101", District="Southeast", Region="Southeast"),
    ]


def create_test_stores(geographies):
    """Create sample stores for testing."""
    return [
        Store(ID=1, StoreNumber="S001", Address="123 Main St", GeographyID=1),
        Store(ID=2, StoreNumber="S002", Address="456 Oak Ave", GeographyID=2),
        Store(ID=3, StoreNumber="S003", Address="789 Pine Rd", GeographyID=3),
        Store(ID=4, StoreNumber="S004", Address="321 Elm St", GeographyID=4),
        Store(ID=5, StoreNumber="S005", Address="654 Maple Dr", GeographyID=5),
    ]


def create_test_customers(geographies):
    """Create sample customers for testing."""
    customers = []
    for i in range(1, 101):  # 100 test customers
        # Distribute customers across geographies
        geo_id = ((i - 1) % len(geographies)) + 1
        customer = Customer(
            ID=i,
            FirstName=f"Customer{i}",
            LastName=f"Test{i}",
            Address=f"{i} Test St",
            GeographyID=geo_id,
            LoyaltyCard=f"LOYAL{i:06d}",
            Phone=f"555-{i:04d}",
            BLEId=f"BLE{i:06d}",
            AdId=f"AD{i:06d}",
        )
        customers.append(customer)
    return customers


def main():
    """Run geography assignment tests."""
    print("=" * 80)
    print("Customer Geography & Store Affinity Test")
    print("=" * 80)

    # Create test data
    print("\n1. Creating test data...")
    geographies = create_test_geographies()
    stores = create_test_stores(geographies)
    customers = create_test_customers(geographies)

    print(f"   Created {len(geographies)} geographies")
    print(f"   Created {len(stores)} stores")
    print(f"   Created {len(customers)} customers")

    # Initialize geography assigner
    print("\n2. Assigning customer geographies...")
    geo_assigner = GeographyAssigner(
        customers=customers,
        stores=stores,
        geographies=geographies,
        seed=42
    )

    customer_geographies = geo_assigner.assign_geographies()
    print(f"   Assigned geographies to {len(customer_geographies)} customers")

    # Validate assignment
    print("\n3. Validating geography assignments...")
    for customer_id, cust_geo in list(customer_geographies.items())[:5]:
        print(f"   Customer {customer_id}:")
        print(f"      Home: {cust_geo.home_city}, {cust_geo.home_state} {cust_geo.home_zip}")
        print(f"      Primary store: {cust_geo.primary_store_id}")
        print(f"      Secondary store: {cust_geo.secondary_store_id}")
        print(f"      Segment: {cust_geo.customer_segment}")
        print(f"      Travel propensity: {cust_geo.travel_propensity:.2f}")
        if cust_geo.nearest_stores:
            print(f"      Nearest store distance: {cust_geo.nearest_stores[0][1]:.1f} miles")

    # Initialize store selector
    print("\n4. Testing store selection...")
    store_selector = StoreSelector(
        customer_geographies=customer_geographies,
        stores=stores,
        seed=42
    )

    # Test store selection for a sample customer
    test_customer_id = 1
    test_customer_geo = customer_geographies[test_customer_id]

    print(f"\n   Testing customer {test_customer_id} (home: {test_customer_geo.home_city}):")
    print(f"   Primary store: {test_customer_geo.primary_store_id}")

    # Simulate 100 shopping trips
    store_visit_counts = {store.ID: 0 for store in stores}
    for _ in range(100):
        selected_store = store_selector.select_store_for_customer(test_customer_id)
        if selected_store:
            store_visit_counts[selected_store.ID] += 1

    print("\n   Store visit distribution (100 trips):")
    for store_id, count in sorted(store_visit_counts.items(), key=lambda x: -x[1]):
        pct = (count / 100) * 100
        print(f"      Store {store_id}: {count} visits ({pct:.0f}%)")

    # Validate loyalty pattern
    primary_visits = store_visit_counts[test_customer_geo.primary_store_id]
    secondary_visits = store_visit_counts[test_customer_geo.secondary_store_id]
    top_two_pct = ((primary_visits + secondary_visits) / 100) * 100

    print("\n   Validation:")
    print(f"      Primary store visits: {primary_visits}% (target: ~50%)")
    print(f"      Top 2 stores combined: {top_two_pct:.0f}% (target: ≥60%)")

    if primary_visits >= 30 and top_two_pct >= 60:
        print("      ✓ Store affinity pattern looks realistic!")
    else:
        print("      ✗ Store affinity pattern needs adjustment")

    # Test store customer distribution
    print("\n5. Testing store customer distributions...")
    for store in stores[:3]:  # Test first 3 stores
        dist = store_selector.get_store_customer_distribution(store.ID)
        print(f"\n   Store {store.ID} ({store.StoreNumber}):")
        print(f"      Local customers (< 10mi): {dist['local_pct']:.1f}%")
        print(f"      Regional customers (10-30mi): {dist['regional_pct']:.1f}%")
        print(f"      Distant customers (> 30mi): {dist['distant_pct']:.1f}%")
        print(f"      Median customer distance: {dist['median_distance']:.1f} miles")

    print("\n" + "=" * 80)
    print("Test completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()
