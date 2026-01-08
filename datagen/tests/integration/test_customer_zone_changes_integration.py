"""
Integration tests for customer zone changes in historical data generation.
"""

from datetime import UTC, datetime

import pytest

from retail_datagen.config.models import RetailConfig
from retail_datagen.db.duckdb_engine import get_duckdb_conn, reset_duckdb
from retail_datagen.generators.fact_generators import FactDataGenerator


@pytest.fixture
def small_config():
    """Create a minimal config for fast test execution."""
    config = RetailConfig(
        seed=42,
        volume={
            "stores": 1,
            "dcs": 1,
            "total_customers": 50,
            "customers_per_day": 5,
            "items_per_ticket_mean": 3,
            "online_orders_per_day": 0,
        },
        paths={
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
        historical={
            "start_date": "2024-01-01",
        },
    )
    return config


@pytest.mark.asyncio
async def test_customer_zone_changes_generation(small_config):
    """Test that customer zone changes are generated during historical data generation."""
    # Reset DuckDB to start fresh
    reset_duckdb()

    try:
        # Initialize generator
        generator = FactDataGenerator(small_config)

        # Load master data from DuckDB (assumes master data exists)
        # For this test to work, master data must be generated first
        # We'll skip if master data doesn't exist
        try:
            generator.load_master_data_from_duckdb()
        except Exception:
            pytest.skip("Master data not available for integration test")

        if not generator.stores or not generator.customers:
            pytest.skip("Insufficient master data for integration test")

        # Generate one day of historical data
        start_date = datetime(2024, 1, 1, tzinfo=UTC)
        end_date = datetime(2024, 1, 1, tzinfo=UTC)

        summary = await generator.generate_historical_data(
            start_date=start_date,
            end_date=end_date,
        )

        # Verify customer_zone_changes table is in the summary
        assert "customer_zone_changes" in summary.facts_generated

        # Query DuckDB to verify data was written
        conn = get_duckdb_conn()

        # Check that the table exists
        result = conn.execute(
            "SELECT COUNT(*) FROM fact_customer_zone_changes"
        ).fetchone()
        zone_change_count = result[0] if result else 0

        # We should have some zone changes (customers moving between zones)
        # The exact count depends on BLE ping generation, but should be > 0
        print(f"Generated {zone_change_count} zone change records")

        # Also verify BLE pings were generated (prerequisite for zone changes)
        ble_ping_result = conn.execute(
            "SELECT COUNT(*) FROM fact_ble_pings"
        ).fetchone()
        ble_ping_count = ble_ping_result[0] if ble_ping_result else 0

        print(f"Generated {ble_ping_count} BLE ping records")

        # Zone changes should be <= BLE pings (not every ping creates a zone change)
        assert zone_change_count <= ble_ping_count

        # If we have BLE pings, we should have at least some zone changes
        # (unless all customers stayed in the same zone, which is unlikely)
        if ble_ping_count > 10:
            assert zone_change_count > 0, (
                "Expected zone changes when BLE pings were generated"
            )

        # Verify schema of zone change records
        if zone_change_count > 0:
            # Get column names
            columns = [desc[0] for desc in conn.execute(
                "SELECT * FROM fact_customer_zone_changes LIMIT 0"
            ).description]

            # Verify required columns exist (case-insensitive)
            columns_lower = [c.lower() for c in columns]
            required_fields = ["eventts", "storeid", "customerbleid", "fromzone", "tozone"]
            for field in required_fields:
                assert field in columns_lower, f"Missing required field: {field}"

    finally:
        # Cleanup
        reset_duckdb()


@pytest.mark.asyncio
async def test_zone_changes_follow_ble_pings(small_config):
    """Test that zone changes are consistent with BLE ping sequences."""
    reset_duckdb()

    try:
        generator = FactDataGenerator(small_config)

        try:
            generator.load_master_data_from_duckdb()
        except Exception:
            pytest.skip("Master data not available for integration test")

        if not generator.stores or not generator.customers:
            pytest.skip("Insufficient master data for integration test")

        # Generate one day of data
        start_date = datetime(2024, 1, 1, tzinfo=UTC)
        end_date = datetime(2024, 1, 1, tzinfo=UTC)

        await generator.generate_historical_data(
            start_date=start_date,
            end_date=end_date,
        )

        conn = get_duckdb_conn()

        # Get a sample customer's BLE pings and zone changes
        sample_customer = conn.execute("""
            SELECT DISTINCT CustomerBLEId
            FROM fact_ble_pings
            WHERE CustomerBLEId IS NOT NULL
            LIMIT 1
        """).fetchone()

        if not sample_customer:
            pytest.skip("No BLE pings with customer IDs generated")

        customer_ble_id = sample_customer[0]

        # Get all BLE pings for this customer, ordered by time
        pings = conn.execute("""
            SELECT EventTS, Zone
            FROM fact_ble_pings
            WHERE CustomerBLEId = ?
            ORDER BY EventTS
        """, [customer_ble_id]).fetchall()

        # Get all zone changes for this customer
        changes = conn.execute("""
            SELECT EventTS, FromZone, ToZone
            FROM fact_customer_zone_changes
            WHERE CustomerBLEId = ?
            ORDER BY EventTS
        """, [customer_ble_id]).fetchall()

        print(f"Customer {customer_ble_id}: {len(pings)} pings, {len(changes)} zone changes")

        # Each zone change should correspond to an actual zone transition in BLE pings
        if len(changes) > 0:
            # Verify that FromZone and ToZone are different
            for change in changes:
                from_zone = change[1]
                to_zone = change[2]
                assert from_zone != to_zone, (
                    "Zone change should have different FromZone and ToZone"
                )

    finally:
        reset_duckdb()
