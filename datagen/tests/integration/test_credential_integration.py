"""
Integration tests for credential management across the system.

Tests the integration of credential utilities with config models,
Azure client, and event streamer.
"""

import os
from unittest.mock import patch

from retail_datagen.config.models import RealtimeConfig, RetailConfig


class TestCredentialIntegration:
    """Integration tests for credential management system."""

    def test_env_var_loading_in_config(self):
        """Test that environment variable is loaded by RealtimeConfig."""
        test_conn_str = (
            "Endpoint=sb://test.servicebus.windows.net/;"
            "SharedAccessKeyName=TestKey;"
            "SharedAccessKey=VGVzdFNlY3JldDEyM1Rlc3RTZWNyZXQxMjNUZXN0U2VjcmV0MTIz;"
            "EntityPath=test-hub"
        )

        with patch.dict(os.environ, {"AZURE_EVENTHUB_CONNECTION_STRING": test_conn_str}):
            # Create config with empty azure_connection_string
            config = RealtimeConfig(
                emit_interval_ms=500,
                burst=100,
                azure_connection_string="",  # Empty - should load from env
            )

            # Should load from environment variable
            assert config.azure_connection_string == test_conn_str

    def test_config_value_takes_precedence_over_empty_env(self):
        """Test that non-empty config value is preserved."""
        config_conn_str = (
            "Endpoint=sb://config.servicebus.windows.net/;"
            "SharedAccessKeyName=ConfigKey;"
            "SharedAccessKey=Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0;"
            "EntityPath=config-hub"
        )

        # Clear environment variable
        with patch.dict(os.environ, {"AZURE_EVENTHUB_CONNECTION_STRING": ""}, clear=True):
            config = RealtimeConfig(
                emit_interval_ms=500,
                burst=100,
                azure_connection_string=config_conn_str,
            )

            # Should keep config value
            assert config.azure_connection_string == config_conn_str

    def test_get_connection_string_method_env_priority(self):
        """Test get_connection_string() method respects priority."""
        env_conn_str = (
            "Endpoint=sb://env.servicebus.windows.net/;"
            "SharedAccessKeyName=EnvKey;"
            "SharedAccessKey=RW52U2VjcmV0RW52U2VjcmV0RW52U2VjcmV0RW52U2VjcmV0RW52U2VjcmV0;"
            "EntityPath=env-hub"
        )
        config_conn_str = (
            "Endpoint=sb://config.servicebus.windows.net/;"
            "SharedAccessKeyName=ConfigKey;"
            "SharedAccessKey=Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0;"
            "EntityPath=config-hub"
        )

        with patch.dict(os.environ, {"AZURE_EVENTHUB_CONNECTION_STRING": env_conn_str}):
            config = RealtimeConfig(
                emit_interval_ms=500,
                burst=100,
                azure_connection_string=config_conn_str,
            )

            # get_connection_string() should return env var value
            connection = config.get_connection_string()
            assert connection == env_conn_str

    def test_get_connection_string_fallback_to_config(self):
        """Test get_connection_string() falls back to config when env var empty."""
        config_conn_str = (
            "Endpoint=sb://config.servicebus.windows.net/;"
            "SharedAccessKeyName=ConfigKey;"
            "SharedAccessKey=Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0;"
            "EntityPath=config-hub"
        )

        # Clear environment variable
        with patch.dict(os.environ, {}, clear=True):
            config = RealtimeConfig(
                emit_interval_ms=500,
                burst=100,
                azure_connection_string=config_conn_str,
            )

            connection = config.get_connection_string()
            assert connection == config_conn_str

    def test_retail_config_loads_connection_string(self):
        """Test that full RetailConfig properly loads connection string."""
        test_conn_str = (
            "Endpoint=sb://retail.servicebus.windows.net/;"
            "SharedAccessKeyName=RetailKey;"
            "SharedAccessKey=UmV0YWlsU2VjcmV0UmV0YWlsU2VjcmV0UmV0YWlsU2VjcmV0UmV0YWlsU2VjcmV0;"
            "EntityPath=retail-hub"
        )

        with patch.dict(os.environ, {"AZURE_EVENTHUB_CONNECTION_STRING": test_conn_str}):
            config = RetailConfig(
                seed=42,
                volume={
                    "stores": 10,
                    "dcs": 2,
                    "total_customers": 1000,
                    "total_products": 100,
                    "customers_per_day": 50,
                    "items_per_ticket_mean": 5.0,
                },
                realtime={
                    "emit_interval_ms": 500,
                    "burst": 100,
                    "azure_connection_string": "",  # Empty - should load from env
                },
                paths={"dict": "data/dictionaries", "master": "data/master", "facts": "data/facts"},
                stream={"hub": "test-hub"},
            )

            # Verify env var was loaded
            assert config.realtime.azure_connection_string == test_conn_str

    def test_config_preserves_additional_realtime_fields(self):
        """Test that all RealtimeConfig fields are preserved."""
        config = RealtimeConfig(
            emit_interval_ms=1000,
            burst=200,
            azure_connection_string="test",
            max_batch_size=512,
            batch_timeout_ms=2000,
            retry_attempts=5,
            backoff_multiplier=3.0,
            circuit_breaker_enabled=False,
            monitoring_interval=60,
            max_buffer_size=20000,
            enable_dead_letter_queue=False,
        )

        assert config.emit_interval_ms == 1000
        assert config.burst == 200
        assert config.max_batch_size == 512
        assert config.batch_timeout_ms == 2000
        assert config.retry_attempts == 5
        assert config.backoff_multiplier == 3.0
        assert config.circuit_breaker_enabled is False
        assert config.monitoring_interval == 60
        assert config.max_buffer_size == 20000
        assert config.enable_dead_letter_queue is False

    def test_empty_connection_string_returns_empty(self):
        """Test that empty connection string in all sources returns empty."""
        with patch.dict(os.environ, {}, clear=True):
            config = RealtimeConfig(
                emit_interval_ms=500,
                burst=100,
                azure_connection_string="",
            )

            connection = config.get_connection_string()
            assert connection == ""


class TestSecurityWarnings:
    """Tests for security warning behavior."""

    def test_security_warning_fires_when_credential_in_config_only(self, caplog):
        """Test that security warning fires when credential is in config but not env var."""
        import logging
        caplog.set_level(logging.WARNING)

        config_conn_str = (
            "Endpoint=sb://config.servicebus.windows.net/;"
            "SharedAccessKeyName=ConfigKey;"
            "SharedAccessKey=Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0Q29uZmlnU2VjcmV0;"
            "EntityPath=config-hub"
        )

        # No env var set - credential must be from config
        with patch.dict(os.environ, {}, clear=True):
            RealtimeConfig(
                emit_interval_ms=500,
                burst=100,
                azure_connection_string=config_conn_str,
            )

            # Security warning should be logged
            assert "SECURITY WARNING" in caplog.text
            assert "azure_connection_string" in caplog.text.lower() or "connection string" in caplog.text.lower()

    def test_no_security_warning_when_credential_from_env_var(self, caplog):
        """Test that no security warning fires when credential comes from env var."""
        import logging
        caplog.set_level(logging.WARNING)

        env_conn_str = (
            "Endpoint=sb://env.servicebus.windows.net/;"
            "SharedAccessKeyName=EnvKey;"
            "SharedAccessKey=RW52U2VjcmV0RW52U2VjcmV0RW52U2VjcmV0RW52U2VjcmV0RW52U2VjcmV0;"
            "EntityPath=env-hub"
        )

        # Env var set, config empty - credential from env var (safe)
        with patch.dict(os.environ, {"AZURE_EVENTHUB_CONNECTION_STRING": env_conn_str}):
            RealtimeConfig(
                emit_interval_ms=500,
                burst=100,
                azure_connection_string="",  # Empty - will load from env
            )

            # No security warning should be logged
            assert "SECURITY WARNING" not in caplog.text

    def test_no_security_warning_when_empty_credential(self, caplog):
        """Test that no security warning fires when no credential is set."""
        import logging
        caplog.set_level(logging.WARNING)

        # No credential set anywhere
        with patch.dict(os.environ, {}, clear=True):
            RealtimeConfig(
                emit_interval_ms=500,
                burst=100,
                azure_connection_string="",
            )

            # No security warning should be logged (nothing to warn about)
            assert "SECURITY WARNING" not in caplog.text
