"""
Unit tests for DuckDB reader service.
"""

from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
import duckdb
from retail_datagen.services import duckdb_reader as db_reader


def test_read_all_master_tables_smoke():
    data = db_reader.read_all_master_tables()
    assert set(data.keys()) == {
        "dim_geographies",
        "dim_stores",
        "dim_distribution_centers",
        "dim_trucks",
        "dim_customers",
        "dim_products",
    }
    for df in data.values():
        assert isinstance(df, pd.DataFrame)


def test_read_all_fact_tables_smoke():
    result = db_reader.read_all_fact_tables()
    assert set(result.keys()) == set(db_reader.FACT_TABLES)
    for df in result.values():
        assert isinstance(df, pd.DataFrame)


def test_get_all_fact_table_date_ranges_shape():
    ranges = db_reader.get_all_fact_table_date_ranges()
    assert set(ranges.keys()) == set(db_reader.FACT_TABLES)
    for rng in ranges.values():
        assert isinstance(rng, tuple) and len(rng) == 2


# ================================
# Exception Handling Tests
# ================================


def test_read_table_handles_catalog_exception(caplog):
    """Test that CatalogException for missing table is logged appropriately."""
    with patch('retail_datagen.services.duckdb_reader.duckdb.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.side_effect = duckdb.CatalogException("Table does not exist")

        # Should return empty DataFrame instead of raising
        result = db_reader.read_table("nonexistent_table")

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert "does not exist" in caplog.text.lower() or "catalog" in caplog.text.lower()


def test_read_master_table_handles_missing_table(caplog):
    """Test that missing master table is handled gracefully."""
    with patch('retail_datagen.services.duckdb_reader.duckdb.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.side_effect = duckdb.CatalogException("Table 'dim_geographies' does not exist")

        # Should return empty dict or handle gracefully
        result = db_reader.read_all_master_tables()

        # At minimum, should not crash
        assert isinstance(result, dict)


def test_read_fact_table_handles_missing_table(caplog):
    """Test that missing fact table is handled gracefully."""
    with patch('retail_datagen.services.duckdb_reader.duckdb.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.side_effect = duckdb.CatalogException("Table does not exist")

        # Should return empty DataFrame instead of crashing
        result = db_reader.read_table("fact_sales")

        assert isinstance(result, pd.DataFrame)
        assert result.empty

