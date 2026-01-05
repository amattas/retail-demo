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


def test_read_all_master_tables_handles_catalog_exception(caplog):
    """Test that CatalogException for missing master table is logged appropriately."""
    import logging
    caplog.set_level(logging.DEBUG)

    # Mock get_duckdb_conn to return a connection that raises CatalogException
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = duckdb.CatalogException("Table does not exist")

    with patch('retail_datagen.services.duckdb_reader.get_duckdb_conn', return_value=mock_conn):
        # Should return dict with empty DataFrames instead of raising
        result = db_reader.read_all_master_tables()

        assert isinstance(result, dict)
        # Should have entries for all master tables
        for table_name in db_reader.MASTER_TABLES:
            assert table_name in result
            assert isinstance(result[table_name], pd.DataFrame)
            assert result[table_name].empty

        # Should log debug message about missing tables
        assert "does not exist" in caplog.text.lower()


def test_read_all_master_tables_handles_unexpected_exception(caplog):
    """Test that unexpected exceptions are handled gracefully."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Unexpected database error")

    with patch('retail_datagen.services.duckdb_reader.get_duckdb_conn', return_value=mock_conn):
        result = db_reader.read_all_master_tables()

        assert isinstance(result, dict)
        for table_name in db_reader.MASTER_TABLES:
            assert table_name in result
            assert isinstance(result[table_name], pd.DataFrame)
            assert result[table_name].empty

        # Should log warning for unexpected errors
        assert "failed to read table" in caplog.text.lower()


def test_read_all_fact_tables_handles_catalog_exception(caplog):
    """Test that missing fact tables are handled gracefully."""
    import logging
    caplog.set_level(logging.DEBUG)

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = duckdb.CatalogException("Table does not exist")

    with patch('retail_datagen.services.duckdb_reader.get_duckdb_conn', return_value=mock_conn):
        result = db_reader.read_all_fact_tables()

        assert isinstance(result, dict)
        for table_name in db_reader.FACT_TABLES:
            assert table_name in result
            assert isinstance(result[table_name], pd.DataFrame)
            assert result[table_name].empty

        assert "does not exist" in caplog.text.lower()


def test_get_fact_table_date_range_handles_catalog_exception(caplog):
    """Test that date range query for missing table returns None."""
    import logging
    caplog.set_level(logging.DEBUG)

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = duckdb.CatalogException("Table does not exist")

    with patch('retail_datagen.services.duckdb_reader.get_duckdb_conn', return_value=mock_conn):
        # Use a valid table name from the allowlist
        result = db_reader.get_fact_table_date_range("fact_receipts")

        assert result == (None, None)
        assert "does not exist" in caplog.text.lower()


def test_get_fact_table_date_range_handles_unexpected_exception(caplog):
    """Test that unexpected exceptions in date range query are logged."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Unexpected error")

    with patch('retail_datagen.services.duckdb_reader.get_duckdb_conn', return_value=mock_conn):
        # Use a valid table name from the allowlist
        result = db_reader.get_fact_table_date_range("fact_receipts")

        assert result == (None, None)
        assert "failed to get date range" in caplog.text.lower()


def test_get_fact_table_date_range_rejects_invalid_table_name():
    """Test that invalid table names are rejected with ValueError."""
    with pytest.raises(ValueError) as exc_info:
        db_reader.get_fact_table_date_range("malicious_table; DROP TABLE users;--")

    assert "Invalid table name" in str(exc_info.value)
