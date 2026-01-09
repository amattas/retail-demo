"""
Integration tests for store operations fact generation.
"""

from datetime import datetime

import pytest

from retail_datagen.config.models import (
    HistoricalConfig,
    PathsConfig,
    RetailConfig,
    VolumeConfig,
)
from retail_datagen.db.duckdb_engine import get_duckdb_conn, reset_duckdb
from retail_datagen.generators.fact_generator import FactDataGenerator


@pytest.fixture(scope="module")
def test_config():
    """Create a test configuration."""
    return RetailConfig(
        seed=42,
        volume=VolumeConfig(
            stores=3,
            dcs=1,
            total_customers=100,
            customers_per_day=10,
            items_per_ticket_mean=3.0,
        ),
        paths=PathsConfig(
            dict="data/dictionaries",
            master="data/master",
            facts="data/facts",
        ),
        historical=HistoricalConfig(
            start_date="2024-01-01",
        ),
    )


@pytest.mark.asyncio
async def test_store_ops_generation_integration(test_config):
    """Test that store_ops table is generated correctly in integration."""
    # Reset database
    reset_duckdb()

    # Create generator
    generator = FactDataGenerator(test_config)

    # Load or generate master data
    try:
        generator.load_master_data_from_duckdb()
    except Exception:
        # Master data doesn't exist, skip test
        pytest.skip("Master data not available")

    if not generator.stores:
        pytest.skip("No stores available for testing")

    # Generate for a small date range
    start_date = datetime(2024, 1, 15)
    end_date = datetime(2024, 1, 17)  # 3 days

    # Set filter to only generate store_ops
    generator.set_included_tables(["store_ops"])

    summary = await generator.generate_historical_data(start_date, end_date)

    # Verify summary
    assert "store_ops" in summary.facts_generated
    assert summary.facts_generated["store_ops"] > 0

    # Verify in database
    conn = get_duckdb_conn()
    result = conn.execute("SELECT COUNT(*) FROM fact_store_ops").fetchone()
    count = result[0]

    # Should have 2 operations per store per day (open + close)
    expected_count = len(generator.stores) * 3 * 2  # stores * days * operations
    assert count == expected_count, f"Expected {expected_count} records, got {count}"

    # Verify data structure
    result = conn.execute(
        "SELECT store_id, operation_type, event_ts FROM fact_store_ops ORDER BY store_id, event_ts LIMIT 10"
    ).fetchall()
    assert len(result) > 0

    # Verify operation types
    operation_types = conn.execute(
        "SELECT DISTINCT operation_type FROM fact_store_ops"
    ).fetchall()
    operation_types_set = {row[0] for row in operation_types}
    assert operation_types_set == {"opened", "closed"}

    # Verify each store has operations
    stores_with_ops = conn.execute(
        "SELECT DISTINCT store_id FROM fact_store_ops"
    ).fetchall()
    store_ids = {row[0] for row in stores_with_ops}
    expected_store_ids = {store.ID for store in generator.stores}
    assert store_ids == expected_store_ids

    # Cleanup
    reset_duckdb()


@pytest.mark.asyncio
async def test_store_ops_respects_operating_hours(test_config):
    """Test that store operations respect configured operating hours."""
    # Reset database
    reset_duckdb()

    # Create generator
    generator = FactDataGenerator(test_config)

    # Load master data
    try:
        generator.load_master_data_from_duckdb()
    except Exception:
        pytest.skip("Master data not available")

    if not generator.stores:
        pytest.skip("No stores available for testing")

    # Generate for a single day
    start_date = datetime(2024, 1, 15)
    end_date = datetime(2024, 1, 15)

    generator.set_included_tables(["store_ops"])

    await generator.generate_historical_data(start_date, end_date)

    # Verify operations respect store hours
    conn = get_duckdb_conn()

    for store in generator.stores:
        # Get operations for this store
        result = conn.execute(
            """
            SELECT operation_type, HOUR(event_ts) as hour
            FROM fact_store_ops
            WHERE store_id = ?
            ORDER BY event_ts
            """,
            [store.ID],
        ).fetchall()

        if len(result) == 2:  # Should have open and close
            opened_type, opened_hour = result[0]
            closed_type, closed_hour = result[1]

            assert opened_type == "opened"
            assert closed_type == "closed"

            # Verify hours are reasonable (between 0-23)
            assert 0 <= opened_hour <= 23
            assert 0 <= closed_hour <= 23

            # Closed hour should be after or equal to open hour
            # (or close to midnight for 24-hour stores)
            if closed_hour != 23:  # Not a late-night close
                assert closed_hour >= opened_hour

    # Cleanup
    reset_duckdb()


@pytest.mark.asyncio
async def test_store_ops_christmas_closure(test_config):
    """Test that stores are closed on Christmas Day."""
    # Reset database
    reset_duckdb()

    # Create generator
    generator = FactDataGenerator(test_config)

    # Load master data
    try:
        generator.load_master_data_from_duckdb()
    except Exception:
        pytest.skip("Master data not available")

    if not generator.stores:
        pytest.skip("No stores available for testing")

    # Generate for Christmas Day
    christmas = datetime(2024, 12, 25)

    generator.set_included_tables(["store_ops"])

    await generator.generate_historical_data(christmas, christmas)

    # Verify no operations on Christmas
    conn = get_duckdb_conn()
    result = conn.execute(
        """
        SELECT COUNT(*)
        FROM fact_store_ops
        WHERE DATE(event_ts) = '2024-12-25'
        """
    ).fetchone()

    count = result[0]
    assert count == 0, f"Expected no operations on Christmas, got {count}"

    # Cleanup
    reset_duckdb()
