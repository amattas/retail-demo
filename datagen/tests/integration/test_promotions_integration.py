"""
Integration tests for fact_promotions table generation.

Tests end-to-end promotion tracking from receipt generation through
database persistence, verifying correct integration with existing
fact tables and promotion engine.
"""

from datetime import datetime
from decimal import Decimal

import pytest
import pytest_asyncio


@pytest.fixture
def small_test_config(temp_data_dirs):
    """Small configuration for fast integration tests."""
    from retail_datagen.config.models import RetailConfig

    config_data = {
        "seed": 42,
        "volume": {
            "stores": 2,
            "dcs": 1,
            "total_customers": 100,
            "customers_per_day": 10,
            "items_per_ticket_mean": 2.0,
        },
        "paths": {
            "dictionaries": temp_data_dirs["dict"],
            "master": temp_data_dirs["master"],
            "facts": temp_data_dirs["facts"],
        },
        "historical": {
            "start_date": "2024-01-01",
        },
        "realtime": {
            "emit_interval_ms": 500,
            "burst": 100,
        },
        "stream": {
            "hub": "test-hub",
        },
    }

    return RetailConfig(**config_data)


@pytest_asyncio.fixture
async def generator_with_master_data(small_test_config, set_test_mode):
    """Create generator with minimal master data."""
    from retail_datagen.generators.fact_generator import FactDataGenerator
    from retail_datagen.generators.master_generator import MasterDataGenerator

    # Generate master data first
    master_gen = MasterDataGenerator(small_test_config)
    await master_gen.generate_all_master_data()

    # Create fact generator
    generator = FactDataGenerator(small_test_config)

    # Load master data
    generator.load_master_data_from_duckdb()

    # Ensure we have data to work with
    assert len(generator.stores) > 0
    assert len(generator.customers) > 0
    assert len(generator.products) > 0

    yield generator

    # Cleanup
    from retail_datagen.db.duckdb_engine import reset_duckdb

    reset_duckdb()


@pytest.mark.asyncio
class TestPromotionsIntegration:
    """Test fact_promotions integration with receipts and database."""

    async def test_promotions_generated_with_receipts(self, generator_with_master_data):
        """Test that promotions are generated when receipts have discounts."""
        generator = generator_with_master_data

        # Set tables to generate only what we need for this test
        generator.set_included_tables(
            ["receipts", "receipt_lines", "promotions", "promo_lines"]
        )

        # Generate 1 day of data (should include some promotions)
        start_date = datetime(2024, 11, 28)  # Black Friday period
        end_date = start_date

        summary = await generator.generate_historical_data(start_date, end_date)

        # Verify generation completed
        assert summary.total_records > 0

        # Check that receipts were generated
        assert summary.facts_generated.get("receipts", 0) > 0

        # Check if any promotions were generated
        # Note: Not all receipts have promotions, but in Black Friday period we should get some
        promo_count = summary.facts_generated.get("promotions", 0)
        promo_line_count = summary.facts_generated.get("promo_lines", 0)

        # Either both should be 0 (no promos) or both > 0 (with promos)
        assert (promo_count == 0 and promo_line_count == 0) or (
            promo_count > 0 and promo_line_count > 0
        )

        # If we have promotions, verify promo_lines >= promotions
        # (each promotion should have at least one line)
        if promo_count > 0:
            assert promo_line_count >= promo_count

    async def test_promotion_data_structure(self, generator_with_master_data):
        """Test that promotion records have correct structure."""
        generator = generator_with_master_data

        # Generate a small batch
        start_date = datetime(2024, 11, 28)  # Black Friday
        end_date = start_date

        generator.set_included_tables(
            ["receipts", "receipt_lines", "promotions", "promo_lines"]
        )

        await generator.generate_historical_data(start_date, end_date)

        # Query promotions from DuckDB
        conn = generator._duckdb_conn
        promotions_df = conn.execute("SELECT * FROM fact_promotions LIMIT 10").fetchdf()

        if len(promotions_df) > 0:
            # Verify column structure
            expected_cols = {
                "event_ts",
                "receipt_id_ext",
                "promo_code",
                "discount_amount",
                "discount_cents",
                "discount_type",
                "product_count",
                "product_ids",
                "store_id",
                "customer_id",
            }

            actual_cols = set(promotions_df.columns)
            assert expected_cols.issubset(actual_cols), (
                f"Missing columns: {expected_cols - actual_cols}"
            )

            # Verify data types and constraints
            for _, row in promotions_df.iterrows():
                assert row["promo_code"] is not None
                assert row["discount_type"] in ["PERCENTAGE", "FIXED_AMOUNT", "BOGO"]
                assert row["discount_cents"] > 0
                assert row["product_count"] > 0
                assert row["store_id"] is not None
                assert row["customer_id"] is not None

    async def test_promo_lines_link_to_promotions(self, generator_with_master_data):
        """Test that promo_lines correctly link to promotions."""
        generator = generator_with_master_data

        start_date = datetime(2024, 11, 28)
        end_date = start_date

        generator.set_included_tables(
            ["receipts", "receipt_lines", "promotions", "promo_lines"]
        )

        await generator.generate_historical_data(start_date, end_date)

        # Query both tables
        conn = generator._duckdb_conn

        # Get promotions and their lines
        result = conn.execute(
            """
            SELECT
                p.receipt_id_ext,
                p.promo_code,
                p.product_count,
                COUNT(pl.line_number) as line_count
            FROM fact_promotions p
            LEFT JOIN fact_promo_lines pl
                ON p.receipt_id_ext = pl.receipt_id_ext
                AND p.promo_code = pl.promo_code
            GROUP BY p.receipt_id_ext, p.promo_code, p.product_count
            LIMIT 10
            """
        ).fetchdf()

        if len(result) > 0:
            # Each promotion should have lines matching its product_count
            for _, row in result.iterrows():
                assert row["line_count"] == row["product_count"]

    async def test_promotions_only_for_discounted_receipts(
        self, generator_with_master_data
    ):
        """Test that promotions are only generated for receipts with discounts."""
        generator = generator_with_master_data

        start_date = datetime(2024, 1, 15)  # Regular day (lower promo rate)
        end_date = start_date

        generator.set_included_tables(
            ["receipts", "receipt_lines", "promotions", "promo_lines"]
        )

        await generator.generate_historical_data(start_date, end_date)

        # Query receipts and promotions
        conn = generator._duckdb_conn

        # Verify: All receipts with promotions have discount_amount > 0
        result = conn.execute(
            """
            SELECT
                r.receipt_id_ext,
                r.discount_amount,
                COUNT(p.promo_code) as promo_count
            FROM fact_receipts r
            LEFT JOIN fact_promotions p ON r.receipt_id_ext = p.receipt_id_ext
            GROUP BY r.receipt_id_ext, r.discount_amount
            HAVING promo_count > 0
            """
        ).fetchdf()

        if len(result) > 0:
            # All receipts with promotions should have discount_amount > 0
            for _, row in result.iterrows():
                discount = Decimal(str(row["discount_amount"]))
                assert discount > Decimal("0.00")
