import pytest
pytest.skip("Legacy ORM-based truck lifecycle test deprecated; DuckDB-only path active.", allow_module_level=True)

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.config.models import RetailConfig
from retail_datagen.db.session import get_retail_session, get_facts_session
from sqlalchemy import select, func, text
from retail_datagen.db.models.master import Truck as TruckModel
from retail_datagen.db.models.facts import (
    TruckMove,
    DCInventoryTransaction,
    StoreInventoryTransaction,
)


async def validate_truck_assignment():
    """Validate that 85% of trucks are assigned to DCs."""
    print("\n" + "=" * 80)
    print("1. VALIDATING TRUCK ASSIGNMENT RATIO")
    print("=" * 80)

    async with get_retail_session() as session:
        # Count trucks by assignment type
        total_query = select(func.count()).select_from(TruckModel)
        assigned_query = select(func.count()).select_from(TruckModel).where(
            TruckModel.DCID.isnot(None)
        )
        pool_query = select(func.count()).select_from(TruckModel).where(
            TruckModel.DCID.is_(None)
        )

        total = (await session.execute(total_query)).scalar()
        assigned = (await session.execute(assigned_query)).scalar()
        pool = (await session.execute(pool_query)).scalar()

        assigned_pct = (assigned / total * 100) if total > 0 else 0
        pool_pct = (pool / total * 100) if total > 0 else 0

        print(f"\nTotal Trucks: {total}")
        print(f"  - Assigned to DCs: {assigned} ({assigned_pct:.1f}%)")
        print(f"  - Pool (DCID=NULL): {pool} ({pool_pct:.1f}%)")

        # Check if assignment ratio is correct (allow 5% tolerance)
        if 80 <= assigned_pct <= 90:
            print("\n✓ PASS: Assignment ratio is within expected range (85% ± 5%)")
            return True
        else:
            print(f"\n✗ FAIL: Assignment ratio {assigned_pct:.1f}% is outside expected range (80-90%)")
            return False


async def validate_truck_status_progression():
    """Validate that truck statuses progress through all lifecycle stages."""
    print("\n" + "=" * 80)
    print("2. VALIDATING TRUCK STATUS LIFECYCLE")
    print("=" * 80)

    async with get_facts_session() as session:
        # Count records by status
        status_query = text("""
            SELECT Status, COUNT(*) as count
            FROM fact_truck_moves
            GROUP BY Status
            ORDER BY Status
        """)

        result = await session.execute(status_query)
        status_counts = {row[0]: row[1] for row in result}

        print("\nStatus Distribution:")
        expected_statuses = ['SCHEDULED', 'LOADING', 'IN_TRANSIT', 'ARRIVED', 'UNLOADING', 'COMPLETED']

        all_statuses_present = True
        for status in expected_statuses:
            count = status_counts.get(status, 0)
            print(f"  - {status}: {count}")
            if count == 0:
                all_statuses_present = False

        # Check for any unexpected statuses
        unexpected = set(status_counts.keys()) - set(expected_statuses)
        if unexpected:
            print(f"\nWarning: Unexpected statuses found: {unexpected}")

        if all_statuses_present:
            print("\n✓ PASS: All expected statuses are present")
            return True
        else:
            missing = [s for s in expected_statuses if status_counts.get(s, 0) == 0]
            print(f"\n✗ FAIL: Missing statuses: {missing}")
            return False


async def validate_inventory_transactions():
    """Validate DC OUTBOUND and Store INBOUND transactions are generated."""
    print("\n" + "=" * 80)
    print("3. VALIDATING INVENTORY TRANSACTIONS")
    print("=" * 80)

    async with get_facts_session() as session:
        # Count DC OUTBOUND transactions
        dc_outbound_query = text("""
            SELECT COUNT(*) as count
            FROM fact_dc_inventory_txn
            WHERE Reason = 'OUTBOUND_SHIPMENT'
        """)

        dc_outbound_count = (await session.execute(dc_outbound_query)).scalar()

        # Count Store INBOUND transactions
        store_inbound_query = text("""
            SELECT COUNT(*) as count
            FROM fact_store_inventory_txn
            WHERE Reason = 'INBOUND_SHIPMENT'
        """)

        store_inbound_count = (await session.execute(store_inbound_query)).scalar()

        print(f"\nDC Inventory Transactions:")
        print(f"  - OUTBOUND_SHIPMENT: {dc_outbound_count}")

        print(f"\nStore Inventory Transactions:")
        print(f"  - INBOUND_SHIPMENT: {store_inbound_count}")

        if dc_outbound_count > 0 and store_inbound_count > 0:
            print("\n✓ PASS: Both DC OUTBOUND and Store INBOUND transactions are present")
            return True
        else:
            print("\n✗ FAIL: Missing inventory transactions")
            if dc_outbound_count == 0:
                print("  - No DC OUTBOUND transactions found")
            if store_inbound_count == 0:
                print("  - No Store INBOUND transactions found")
            return False


async def validate_shipment_linking():
    """Validate that inventory transactions link to truck shipments via shipment_id."""
    print("\n" + "=" * 80)
    print("4. VALIDATING SHIPMENT LINKING")
    print("=" * 80)

    async with get_facts_session() as session:
        # Check DC outbound transactions with shipment_id in Source
        dc_linked_query = text("""
            SELECT COUNT(*) as count
            FROM fact_dc_inventory_txn
            WHERE Reason = 'OUTBOUND_SHIPMENT'
              AND Source IS NOT NULL
              AND Source LIKE 'SHIP%'
        """)

        dc_linked = (await session.execute(dc_linked_query)).scalar()

        # Check Store inbound transactions with shipment_id in Source
        store_linked_query = text("""
            SELECT COUNT(*) as count
            FROM fact_store_inventory_txn
            WHERE Reason = 'INBOUND_SHIPMENT'
              AND Source IS NOT NULL
              AND Source LIKE 'SHIP%'
        """)

        store_linked = (await session.execute(store_linked_query)).scalar()

        # Verify shipment IDs match between tables
        matching_query = text("""
            SELECT COUNT(DISTINCT dc.Source) as count
            FROM fact_dc_inventory_txn dc
            INNER JOIN fact_store_inventory_txn store
              ON dc.Source = store.Source
            WHERE dc.Reason = 'OUTBOUND_SHIPMENT'
              AND store.Reason = 'INBOUND_SHIPMENT'
        """)

        matching_shipments = (await session.execute(matching_query)).scalar()

        print(f"\nLinked Transactions:")
        print(f"  - DC OUTBOUND with shipment_id: {dc_linked}")
        print(f"  - Store INBOUND with shipment_id: {store_linked}")
        print(f"  - Matching shipment IDs: {matching_shipments}")

        if dc_linked > 0 and store_linked > 0 and matching_shipments > 0:
            print("\n✓ PASS: Shipment linking is working correctly")
            return True
        else:
            print("\n✗ FAIL: Shipment linking has issues")
            return False


async def show_sample_lifecycle():
    """Show sample truck lifecycle for 5 trucks."""
    print("\n" + "=" * 80)
    print("5. SAMPLE TRUCK LIFECYCLE (5 trucks)")
    print("=" * 80)

    async with get_facts_session() as session:
        # Get 5 sample shipments with all their lifecycle events
        sample_query = text("""
            SELECT
                ShipmentId,
                EventTS,
                Status,
                TruckId,
                DCID,
                StoreID
            FROM fact_truck_moves
            WHERE ShipmentId IN (
                SELECT DISTINCT ShipmentId
                FROM fact_truck_moves
                LIMIT 5
            )
            ORDER BY ShipmentId, EventTS
        """)

        result = await session.execute(sample_query)
        rows = result.fetchall()

        if not rows:
            print("\nNo truck movements found!")
            return False

        current_shipment = None
        for row in rows:
            shipment_id, event_ts, status, truck_id, dc_id, store_id = row

            if shipment_id != current_shipment:
                if current_shipment is not None:
                    print()  # Blank line between shipments
                print(f"\nShipment: {shipment_id} | Truck: {truck_id} | DC {dc_id} → Store {store_id}")
                print("-" * 80)
                current_shipment = shipment_id

            print(f"  {event_ts.strftime('%Y-%m-%d %H:%M')} | {status}")

        return True


async def main():
    """Run all validation checks."""
    print("\n" + "=" * 80)
    print("TRUCK LIFECYCLE VALIDATION")
    print("=" * 80)

    results = []

    # Run all validations
    results.append(await validate_truck_assignment())
    results.append(await validate_truck_status_progression())
    results.append(await validate_inventory_transactions())
    results.append(await validate_shipment_linking())
    await show_sample_lifecycle()

    # Summary
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)

    passed = sum(results)
    total = len(results)

    print(f"\nTests Passed: {passed}/{total}")

    if passed == total:
        print("\n✓ ALL TESTS PASSED!")
        return 0
    else:
        print(f"\n✗ {total - passed} TEST(S) FAILED")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
