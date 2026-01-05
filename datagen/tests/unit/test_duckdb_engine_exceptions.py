"""
Unit tests for DuckDB engine exception handling.

Tests verify that exception handlers properly log errors and maintain
system stability when failures occur.
"""

from unittest.mock import patch, MagicMock, PropertyMock
import pytest
import duckdb
from pathlib import Path
from retail_datagen.db import duckdb_engine


def test_reset_duckdb_handles_close_failure(caplog):
    """Test that connection close failures are logged during reset."""
    # Set up a mock connection that will fail on close
    mock_conn = MagicMock()
    mock_conn.close.side_effect = Exception("Failed to close connection")

    # Patch the global _conn variable to use our mock
    with patch.object(duckdb_engine, '_conn', mock_conn):
        # Patch Path.exists to return False so we don't try to delete files
        with patch.object(Path, 'exists', return_value=False):
            # Should not raise, but should log warning
            duckdb_engine.reset_duckdb()

            assert "Failed to close" in caplog.text or "close" in caplog.text.lower()


def test_reset_duckdb_handles_file_deletion_failure(caplog):
    """Test that file deletion failures are logged during reset."""
    # Patch _conn to None so we skip connection close
    with patch.object(duckdb_engine, '_conn', None):
        # Patch get_duckdb_path to return a known path
        test_path = Path("/tmp/test.db")
        with patch.object(duckdb_engine, 'get_duckdb_path', return_value=test_path):
            # Mock Path.exists to return True and unlink to fail
            with patch.object(Path, 'exists', return_value=True):
                with patch.object(Path, 'unlink', side_effect=OSError("Permission denied")):
                    # Should not raise, but should log warning
                    duckdb_engine.reset_duckdb()

                    assert "Permission denied" in caplog.text or "failed" in caplog.text.lower()


def test_close_duckdb_handles_exception(caplog):
    """Test that close_duckdb logs exceptions but doesn't raise."""
    # Set up a mock connection that will fail on close
    mock_conn = MagicMock()
    mock_conn.close.side_effect = Exception("Connection close error")

    # Patch the global _conn variable
    with patch.object(duckdb_engine, '_conn', mock_conn):
        # Should not raise
        duckdb_engine.close_duckdb()

        assert "close" in caplog.text.lower() or "error" in caplog.text.lower()


def test_table_exists_handles_catalog_exception(caplog):
    """Test that _table_exists handles CatalogException gracefully."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = duckdb.CatalogException("Table does not exist")

    # Use a valid table name from the allowlist
    result = duckdb_engine._table_exists(mock_conn, "dim_geographies")

    assert result is False
    assert mock_conn.execute.called


def test_table_exists_handles_unexpected_exception(caplog):
    """Test that _table_exists logs unexpected exceptions."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Unexpected error")

    # Use a valid table name from the allowlist
    result = duckdb_engine._table_exists(mock_conn, "dim_geographies")

    assert result is False
    assert "Unexpected error checking if table" in caplog.text


def test_table_exists_rejects_invalid_table_name():
    """Test that _table_exists rejects table names not in allowlist."""
    mock_conn = MagicMock()

    # Invalid table name should raise ValueError
    with pytest.raises(ValueError) as exc_info:
        duckdb_engine._table_exists(mock_conn, "malicious_table; DROP TABLE users;--")

    assert "Invalid table name" in str(exc_info.value)
    # Connection should not be called
    assert not mock_conn.execute.called


def test_current_columns_handles_catalog_exception():
    """Test that _current_columns returns empty set for missing table."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = duckdb.CatalogException("Table does not exist")

    # Use a valid table name from the allowlist
    result = duckdb_engine._current_columns(mock_conn, "dim_geographies")

    assert result == set()


def test_current_columns_handles_unexpected_exception(caplog):
    """Test that _current_columns logs unexpected exceptions."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Unexpected error")

    # Use a valid table name from the allowlist
    result = duckdb_engine._current_columns(mock_conn, "dim_geographies")

    assert result == set()
    assert "Failed to get columns for table" in caplog.text


def test_validate_table_name_accepts_valid_tables():
    """Test that validate_table_name accepts all tables in allowlist."""
    for table in duckdb_engine.ALLOWED_TABLES:
        result = duckdb_engine.validate_table_name(table)
        assert result == table


def test_validate_table_name_rejects_sql_injection():
    """Test that validate_table_name blocks SQL injection attempts."""
    injection_attempts = [
        "users; DROP TABLE users;--",
        "' OR '1'='1",
        "table_name UNION SELECT * FROM secrets",
        "../../../etc/passwd",
    ]

    for attempt in injection_attempts:
        with pytest.raises(ValueError) as exc_info:
            duckdb_engine.validate_table_name(attempt)
        assert "Invalid table name" in str(exc_info.value)


def test_validate_column_name_accepts_valid_names():
    """Test that validate_column_name accepts valid SQL identifiers."""
    valid_names = [
        "id",
        "user_name",
        "firstName",
        "column123",
        "_private",
        "_underscore_prefix",
        "CamelCase",
        "UPPER_CASE",
    ]

    for name in valid_names:
        result = duckdb_engine.validate_column_name(name)
        assert result == name


def test_validate_column_name_rejects_sql_injection():
    """Test that validate_column_name blocks SQL injection attempts."""
    injection_attempts = [
        "col; DROP TABLE users;--",
        "col' OR '1'='1",
        "col UNION SELECT * FROM secrets",
        "col--comment",
        "123startsWithNumber",
        "has space",
        "has-dash",
        "has.dot",
    ]

    for attempt in injection_attempts:
        with pytest.raises(ValueError) as exc_info:
            duckdb_engine.validate_column_name(attempt)
        assert "Invalid column name" in str(exc_info.value)


def test_validate_column_name_rejects_empty():
    """Test that validate_column_name rejects empty strings."""
    with pytest.raises(ValueError) as exc_info:
        duckdb_engine.validate_column_name("")
    assert "empty" in str(exc_info.value).lower()
