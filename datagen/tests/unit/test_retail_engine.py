import pytest

pytest.skip("Legacy SQLite/SQLAlchemy engine tests are deprecated; DuckDB-only path active.", allow_module_level=True)

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from retail_datagen.db.config import DatabaseConfig
from retail_datagen.db.engine import (
    get_retail_engine,
    dispose_engines,
    check_engine_health,
)


class TestRetailEngine:
    """Tests for unified retail database engine factory."""

    @pytest.mark.asyncio
    async def test_get_retail_engine_returns_engine(self):
        """Test that get_retail_engine() returns an AsyncEngine instance."""
        engine = get_retail_engine()
        assert isinstance(engine, AsyncEngine)
        assert engine is not None

    @pytest.mark.asyncio
    async def test_get_retail_engine_singleton(self):
        """Test that get_retail_engine() returns the same instance (singleton)."""
        engine1 = get_retail_engine()
        engine2 = get_retail_engine()
        assert engine1 is engine2

    @pytest.mark.asyncio
    async def test_retail_engine_uses_correct_path(self):
        """Test that retail engine uses RETAIL_DB_PATH from config."""
        engine = get_retail_engine()
        # The engine URL should contain the retail database path
        assert DatabaseConfig.RETAIL_DB_PATH in str(engine.url)

    @pytest.mark.asyncio
    async def test_retail_engine_foreign_keys_enabled(self):
        """Test that foreign keys are enabled via PRAGMA."""
        engine = get_retail_engine()
        async with engine.begin() as conn:
            result = await conn.execute(text("PRAGMA foreign_keys"))
            fk_status = result.scalar()
            assert fk_status == 1, "Foreign keys should be enabled"

    @pytest.mark.asyncio
    async def test_retail_engine_wal_mode(self):
        """Test that WAL journal mode is enabled."""
        engine = get_retail_engine()
        async with engine.begin() as conn:
            result = await conn.execute(text("PRAGMA journal_mode"))
            journal_mode = result.scalar()
            assert journal_mode.upper() == "WAL"

    @pytest.mark.asyncio
    async def test_retail_engine_can_execute_queries(self):
        """Test that retail engine can execute basic queries."""
        engine = get_retail_engine()
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT 1 as test_value"))
            value = result.scalar()
            assert value == 1

    @pytest.mark.asyncio
    async def test_check_engine_health_retail(self):
        """Test engine health check with retail engine."""
        engine = get_retail_engine()
        is_healthy = await check_engine_health(engine)
        assert is_healthy is True

    @pytest.mark.asyncio
    async def test_dispose_engines_includes_retail(self):
        """Test that dispose_engines() properly disposes retail engine."""
        # Get the engine to initialize it
        engine = get_retail_engine()
        assert engine is not None

        # Dispose all engines
        await dispose_engines()

        # Get a new engine - should be a different instance
        new_engine = get_retail_engine()
        assert new_engine is not engine

        # Clean up
        await dispose_engines()

    @pytest.mark.asyncio
    async def test_retail_engine_pragmas_applied(self):
        """Test that all configured PRAGMAs are properly applied."""
        engine = get_retail_engine()

        async with engine.begin() as conn:
            # Check critical PRAGMAs
            pragmas_to_check = {
                "foreign_keys": 1,
                "synchronous": "NORMAL",
                "temp_store": "MEMORY",
            }

            for pragma, expected_value in pragmas_to_check.items():
                result = await conn.execute(text(f"PRAGMA {pragma}"))
                actual_value = result.scalar()

                # Handle string comparisons (case-insensitive)
                if isinstance(expected_value, str):
                    assert actual_value.upper() == expected_value.upper(), (
                        f"PRAGMA {pragma} should be {expected_value}, got {actual_value}"
                    )
                else:
                    assert actual_value == expected_value, (
                        f"PRAGMA {pragma} should be {expected_value}, got {actual_value}"
                    )


@pytest.fixture(scope="function", autouse=True)
async def cleanup_engines():
    """Ensure engines are disposed after each test."""
    yield
    await dispose_engines()
