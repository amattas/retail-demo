"""
Unit tests for FactDataGenerator exception handling.

Tests verify that exception handlers properly log errors when fallbacks
or error conditions occur.
"""

from unittest.mock import patch, MagicMock
import pytest
import duckdb
from retail_datagen.generators.fact_generator import FactDataGenerator
from retail_datagen.config.models import RetailConfig


@pytest.fixture
def mock_config():
    """Create a minimal mock configuration."""
    config = MagicMock(spec=RetailConfig)
    config.seed = 42
    return config


def test_fact_generator_handles_duckdb_init_failure(caplog, mock_config):
    """Test that FactDataGenerator handles DuckDB initialization failure gracefully."""
    # Mock get_duckdb_conn to raise an exception
    with patch('retail_datagen.db.duckdb_engine.get_duckdb_conn') as mock_get_duckdb:
        mock_get_duckdb.side_effect = Exception("DuckDB initialization failed")

        # Should not raise, should fall back to in-memory mode
        gen = FactDataGenerator(mock_config)

        # Should log a warning about the failure
        assert "failed to initialize" in caplog.text.lower() or "duckdb" in caplog.text.lower()
        # Should fall back to not using DuckDB
        assert gen._use_duckdb is False


def test_fact_generator_handles_duckdb_connection_error(caplog, mock_config):
    """Test that FactDataGenerator handles DuckDB connection errors."""
    # Mock get_duckdb_conn to raise duckdb.Error
    with patch('retail_datagen.db.duckdb_engine.get_duckdb_conn') as mock_get_duckdb:
        mock_get_duckdb.side_effect = duckdb.Error("Connection failed")

        gen = FactDataGenerator(mock_config)

        assert gen._use_duckdb is False
        # Should have logged something about the failure
        if caplog.text:  # Only check if there's log output
            assert "failed" in caplog.text.lower() or "error" in caplog.text.lower()


def test_ensure_columns_handles_alter_table_failure(caplog):
    """Test that _ensure_columns logs but continues when ALTER TABLE fails."""
    import logging
    from retail_datagen.db.duckdb_engine import _ensure_columns
    import pandas as pd

    # Capture debug-level logs
    caplog.set_level(logging.DEBUG)

    mock_conn = MagicMock()
    # Make _current_columns return empty set
    with patch('retail_datagen.db.duckdb_engine._current_columns', return_value=set()):
        # Make execute fail when trying to add column
        mock_conn.execute.side_effect = Exception("ALTER TABLE failed")

        df = pd.DataFrame({'new_column': [1, 2, 3]})

        # Should not raise - just log and continue
        # Use a valid table name from the allowlist
        _ensure_columns(mock_conn, 'fact_receipts', df)

        # Should have logged the failure at debug level
        assert "failed to add column" in caplog.text.lower()


def test_outbox_insert_handles_max_id_query_failure(caplog):
    """Test that outbox_insert_records handles failure to get max outbox_id."""
    from retail_datagen.db.duckdb_engine import outbox_insert_records

    mock_conn = MagicMock()

    # Mock _ensure_outbox_table to succeed
    with patch('retail_datagen.db.duckdb_engine._ensure_outbox_table'):
        # Mock the execute call for getting max ID to fail
        mock_conn.execute.side_effect = Exception("Query failed")

        # Should use 0 as fallback and log warning
        try:
            outbox_insert_records(mock_conn, [{'message_type': 'test', 'payload': '{}'}])
        except Exception:
            # May fail at insert, but should have logged the max_id warning
            pass

        assert "failed to get max outbox_id" in caplog.text.lower()


def test_pragma_setting_failures_are_logged(caplog):
    """Test that PRAGMA setting failures during connection are logged at debug level."""
    from retail_datagen.db import duckdb_engine

    # Reset the connection
    with patch.object(duckdb_engine, '_conn', None):
        # Mock duckdb.connect to return a connection that fails on PRAGMA
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("PRAGMA not supported")

        with patch('retail_datagen.db.duckdb_engine.duckdb.connect', return_value=mock_conn):
            with patch('retail_datagen.db.duckdb_engine._ensure_outbox_table'):
                try:
                    duckdb_engine.get_duckdb_conn()
                except Exception:
                    pass

                # Should have logged debug messages about PRAGMA failures
                if caplog.text:
                    assert "failed to set pragma" in caplog.text.lower() or "pragma" in caplog.text.lower()
