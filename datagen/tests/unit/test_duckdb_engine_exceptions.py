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

    # Should return False, not raise
    result = duckdb_engine._table_exists(mock_conn, "nonexistent_table")

    assert result is False
    assert mock_conn.execute.called


def test_table_exists_handles_unexpected_exception(caplog):
    """Test that _table_exists logs unexpected exceptions."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Unexpected error")

    # Should return False and log warning
    result = duckdb_engine._table_exists(mock_conn, "test_table")

    assert result is False
    assert "Unexpected error checking if table" in caplog.text


def test_current_columns_handles_catalog_exception():
    """Test that _current_columns returns empty set for missing table."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = duckdb.CatalogException("Table does not exist")

    # Should return empty set, not raise
    result = duckdb_engine._current_columns(mock_conn, "nonexistent_table")

    assert result == set()


def test_current_columns_handles_unexpected_exception(caplog):
    """Test that _current_columns logs unexpected exceptions."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = RuntimeError("Unexpected error")

    # Should return empty set and log warning
    result = duckdb_engine._current_columns(mock_conn, "test_table")

    assert result == set()
    assert "Failed to get columns for table" in caplog.text
