"""
Unit tests for DuckDB engine exception handling.

Tests verify that exception handlers properly log errors and maintain
system stability when failures occur.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from retail_datagen.db import duckdb_engine


def test_reset_duckdb_handles_close_failure(caplog):
    """Test that connection close failures are logged during reset."""
    # Set up a mock connection that will fail on close
    mock_conn = MagicMock()
    mock_conn.close.side_effect = Exception("Failed to close connection")

    # Patch the global _conn variable to use our mock
    with patch.object(duckdb_engine, "_conn", mock_conn):
        # Patch Path.exists to return False so we don't try to delete files
        with patch.object(Path, "exists", return_value=False):
            # Should not raise, but should log warning
            duckdb_engine.reset_duckdb()

            assert "Failed to close" in caplog.text or "close" in caplog.text.lower()


def test_reset_duckdb_handles_file_deletion_failure(caplog):
    """Test that file deletion failures are logged during reset."""
    # Patch _conn to None so we skip connection close
    with patch.object(duckdb_engine, "_conn", None):
        # Patch get_duckdb_path to return a known path
        test_path = Path("/tmp/test.db")
        with patch.object(duckdb_engine, "get_duckdb_path", return_value=test_path):
            # Mock Path.exists to return True and unlink to fail
            with patch.object(Path, "exists", return_value=True):
                with patch.object(
                    Path, "unlink", side_effect=OSError("Permission denied")
                ):
                    # Should not raise, but should log warning
                    duckdb_engine.reset_duckdb()

                    assert (
                        "Permission denied" in caplog.text
                        or "failed" in caplog.text.lower()
                    )


def test_close_duckdb_handles_exception(caplog):
    """Test that close_duckdb logs exceptions but doesn't raise."""
    # Set up a mock connection that will fail on close
    mock_conn = MagicMock()
    mock_conn.close.side_effect = Exception("Connection close error")

    # Patch the global _conn variable
    with patch.object(duckdb_engine, "_conn", mock_conn):
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


def test_insert_dataframe_validates_columns():
    """Integration test: Verify malicious column names are blocked."""
    import pandas as pd

    # Create a DataFrame with a malicious column name
    malicious_df = pd.DataFrame(
        {
            "valid_col": [1, 2, 3],
            "col; DROP TABLE users;--": [4, 5, 6],  # SQL injection attempt
        }
    )

    mock_conn = MagicMock()

    # Should raise ValueError before any SQL is executed
    with pytest.raises(ValueError) as exc_info:
        duckdb_engine.insert_dataframe(mock_conn, "dim_geographies", malicious_df)

    assert "Invalid column name" in str(exc_info.value)
    # Verify no SQL was executed
    assert not mock_conn.execute.called
    assert not mock_conn.register.called


def test_insert_dataframe_validates_table_and_columns():
    """Integration test: Both table and column validation work together."""
    import pandas as pd

    valid_df = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["a", "b"],
        }
    )

    mock_conn = MagicMock()

    # Invalid table name should raise
    with pytest.raises(ValueError) as exc_info:
        duckdb_engine.insert_dataframe(mock_conn, "malicious_table", valid_df)
    assert "Invalid table name" in str(exc_info.value)

    # Valid table but invalid column should raise
    bad_col_df = pd.DataFrame({"123invalid": [1]})
    with pytest.raises(ValueError) as exc_info:
        duckdb_engine.insert_dataframe(mock_conn, "dim_geographies", bad_col_df)
    assert "Invalid column name" in str(exc_info.value)


def test_get_duckdb_conn_tolerates_pragma_failures(caplog):
    """Test that PRAGMA failures are tolerated and connection still succeeds.

    The initialization code uses best-effort for PRAGMA settings to maintain
    compatibility with older DuckDB versions. Failures are logged but don't
    prevent connection success.
    """
    import logging

    caplog.set_level(logging.DEBUG)

    mock_conn = MagicMock()

    # Ensure _conn starts as None
    with patch.object(duckdb_engine, "_conn", None):
        # Mock duckdb.connect to return our mock connection
        with patch("duckdb.connect", return_value=mock_conn):
            # Make PRAGMA execute fail
            mock_conn.execute.side_effect = RuntimeError("PRAGMA not supported")

            # Should NOT raise - PRAGMA failures are tolerated
            result = duckdb_engine.get_duckdb_conn()

            # Connection should still be returned
            assert result is mock_conn

            # Failure should be logged (debug level for PRAGMA, warning for outbox)
            assert "PRAGMA" in caplog.text or "outbox" in caplog.text.lower()


def test_get_duckdb_conn_tolerates_outbox_creation_failure(caplog):
    """Test that outbox table creation failures are logged but tolerated."""
    mock_conn = MagicMock()

    # Ensure _conn starts as None
    with patch.object(duckdb_engine, "_conn", None):
        # Mock duckdb.connect to return our mock connection
        with patch("duckdb.connect", return_value=mock_conn):
            # First two execute calls succeed (PRAGMAs), third fails (outbox)
            mock_conn.execute.side_effect = [
                None,  # PRAGMA threads
                None,  # PRAGMA temp_directory
                RuntimeError("Failed to create outbox"),  # _ensure_outbox_table
            ]

            # Should NOT raise - outbox failures are tolerated
            result = duckdb_engine.get_duckdb_conn()

            # Connection should still be returned
            assert result is mock_conn

            # Warning should be logged for outbox failure
            assert "outbox" in caplog.text.lower()


def test_ensure_columns_validates_column_names():
    """Test that _ensure_columns validates column names before adding them."""
    import pandas as pd

    mock_conn = MagicMock()
    # Mock _current_columns to return empty set (no existing columns)
    mock_conn.execute.return_value.fetchall.return_value = []

    # Create a DataFrame with a malicious column name
    malicious_df = pd.DataFrame(
        {
            "valid_col": [1, 2, 3],
            "col; DROP TABLE users;--": [4, 5, 6],  # SQL injection attempt
        }
    )

    # Should raise ValueError when trying to add malicious column
    with pytest.raises(ValueError) as exc_info:
        duckdb_engine._ensure_columns(mock_conn, "dim_geographies", malicious_df)

    assert "Invalid column name" in str(exc_info.value)


def test_ensure_columns_allows_valid_column_names():
    """Test that _ensure_columns allows valid column names to be added."""
    import pandas as pd

    mock_conn = MagicMock()
    # Mock PRAGMA table_info to return empty (no existing columns)
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = []
    mock_conn.execute.return_value = mock_cur

    # Create a DataFrame with valid column names only
    valid_df = pd.DataFrame(
        {
            "new_column_1": [1, 2, 3],
            "AnotherColumn": [4, 5, 6],
            "_private_col": [7, 8, 9],
        }
    )

    # Should not raise - columns are valid
    duckdb_engine._ensure_columns(mock_conn, "dim_geographies", valid_df)

    # Verify ALTER TABLE was called for each column
    alter_calls = [
        call for call in mock_conn.execute.call_args_list if "ALTER TABLE" in str(call)
    ]
    assert len(alter_calls) == 3
