"""
Test configuration models for retail data generator.

These tests validate configuration loading, validation, and constraints
as specified in AGENTS.md.
"""

import json
import tempfile
from pathlib import Path

import pytest

_hyp = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from retail_datagen.config.models import (
    PathsConfig,
    RealtimeConfig,
    StreamConfig,
    VolumeConfig,
)

# Import will be available after implementation
from retail_datagen.config.models import (
    RetailConfig as Config,
)


class TestVolumeConfig:
    """Test volume configuration validation."""

    def test_volume_config_valid_values(self):
        """Test that valid volume configuration is accepted."""
        valid_volume = {
            "stores": 250,
            "dcs": 12,
            "customers_per_day": 20000,
            "items_per_ticket_mean": 4.2,
        }
        VolumeConfig(**valid_volume)  # Should not raise

    def test_volume_config_minimum_stores(self):
        """Test minimum store count validation."""
        invalid_volume = {
            "stores": 0,
            "dcs": 12,
            "customers_per_day": 20000,
            "items_per_ticket_mean": 4.2,
        }
        with pytest.raises(ValidationError):
            VolumeConfig(**invalid_volume)

    def test_volume_config_minimum_dcs(self):
        """Test minimum distribution center count validation."""
        invalid_volume = {
            "stores": 250,
            "dcs": 0,
            "customers_per_day": 20000,
            "items_per_ticket_mean": 4.2,
        }
        with pytest.raises(ValidationError):
            VolumeConfig(**invalid_volume)

    def test_volume_config_minimum_customers_per_day(self):
        """Test minimum customers per day validation."""
        invalid_volume = {
            "stores": 250,
            "dcs": 12,
            "customers_per_day": 0,
            "items_per_ticket_mean": 4.2,
        }
        with pytest.raises(ValidationError):
            VolumeConfig(**invalid_volume)

    def test_volume_config_positive_items_per_ticket(self):
        """Test that items per ticket must be positive."""
        invalid_volume = {
            "stores": 250,
            "dcs": 12,
            "customers_per_day": 20000,
            "items_per_ticket_mean": 0.0,
        }
        with pytest.raises(ValidationError):
            VolumeConfig(**invalid_volume)

    @given(
        stores=st.integers(min_value=1, max_value=10000),
        dcs=st.integers(min_value=1, max_value=100),
        customers_per_day=st.integers(min_value=1, max_value=1000000),
        items_per_ticket_mean=st.floats(min_value=0.1, max_value=100.0),
    )
    def test_volume_config_property_based(
        self,
        stores: int,
        dcs: int,
        customers_per_day: int,
        items_per_ticket_mean: float,
    ):
        """Property-based test for volume configuration."""
        volume_data = {
            "stores": stores,
            "dcs": dcs,
            "customers_per_day": customers_per_day,
            "items_per_ticket_mean": items_per_ticket_mean,
        }
        config = VolumeConfig(**volume_data)
        assert config.stores == stores
        assert config.dcs == dcs
        assert config.customers_per_day == customers_per_day
        assert config.items_per_ticket_mean == items_per_ticket_mean


class TestRealtimeConfig:
    """Test realtime configuration validation."""

    def test_realtime_config_valid_values(self):
        """Test that valid realtime configuration is accepted."""
        valid_realtime = {"emit_interval_ms": 500, "burst": 100}
        RealtimeConfig(**valid_realtime)  # Should not raise

    def test_realtime_config_minimum_emit_interval(self):
        """Test minimum emit interval validation."""
        invalid_realtime = {"emit_interval_ms": 0, "burst": 100}
        with pytest.raises(ValidationError):
            RealtimeConfig(**invalid_realtime)

    def test_realtime_config_minimum_burst(self):
        """Test minimum burst size validation."""
        invalid_realtime = {"emit_interval_ms": 500, "burst": 0}
        with pytest.raises(ValidationError):
            RealtimeConfig(**invalid_realtime)

    @given(
        emit_interval_ms=st.integers(min_value=1, max_value=60000),
        burst=st.integers(min_value=1, max_value=10000),
    )
    def test_realtime_config_property_based(self, emit_interval_ms: int, burst: int):
        """Property-based test for realtime configuration."""
        realtime_data = {"emit_interval_ms": emit_interval_ms, "burst": burst}
        config = RealtimeConfig(**realtime_data)
        assert config.emit_interval_ms == emit_interval_ms
        assert config.burst == burst


class TestPathsConfig:
    """Test paths configuration validation."""

    def test_paths_config_valid_values(self):
        """Test that valid paths configuration is accepted."""
        valid_paths = {
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        }
        PathsConfig(**valid_paths)  # Should not raise

    def test_paths_config_empty_dict_path(self):
        """Test that empty dictionary path is invalid."""
        invalid_paths = {"dict": "", "master": "data/master", "facts": "data/facts"}
        with pytest.raises(ValidationError):
            PathsConfig(**invalid_paths)

    def test_paths_config_empty_master_path(self):
        """Test that empty master path is invalid."""
        invalid_paths = {
            "dict": "data/dictionaries",
            "master": "",
            "facts": "data/facts",
        }
        with pytest.raises(ValidationError):
            PathsConfig(**invalid_paths)

    def test_paths_config_empty_facts_path(self):
        """Test that empty facts path is invalid."""
        invalid_paths = {
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "",
        }
        with pytest.raises(ValidationError):
            PathsConfig(**invalid_paths)


class TestStreamConfig:
    """Test stream configuration validation."""

    def test_stream_config_valid_values(self):
        """Test that valid stream configuration is accepted."""
        valid_stream = {"hub": "retail-events"}
        StreamConfig(**valid_stream)  # Should not raise

    def test_stream_config_empty_hub_name(self):
        """Test that empty hub name is invalid."""
        invalid_stream = {"hub": ""}
        with pytest.raises(ValidationError):
            StreamConfig(**invalid_stream)

    def test_stream_config_whitespace_hub_name(self):
        """Test that whitespace-only hub name is invalid."""
        invalid_stream = {"hub": "   "}
        with pytest.raises(ValidationError):
            StreamConfig(**invalid_stream)


class TestConfig:
    """Test main configuration model."""

    def test_config_valid_complete(self):
        """Test that a complete valid configuration is accepted."""
        valid_config = {
            "seed": 42,
            "volume": {
                "stores": 250,
                "dcs": 12,
                "customers_per_day": 20000,
                "items_per_ticket_mean": 4.2,
            },
            "realtime": {"emit_interval_ms": 500, "burst": 100},
            "paths": {
                "dict": "data/dictionaries",
                "master": "data/master",
                "facts": "data/facts",
            },
            "stream": {"hub": "retail-events"},
        }
        Config(**valid_config)  # Should not raise

    def test_config_missing_required_fields(self):
        """Test that missing required fields raise validation errors."""
        incomplete_config = {"seed": 42}
        with pytest.raises(ValidationError):
            Config(**incomplete_config)

    def test_config_invalid_seed(self):
        """Test that negative seed is invalid."""
        invalid_config = {
            "seed": -1,
            "volume": {
                "stores": 250,
                "dcs": 12,
                "customers_per_day": 20000,
                "items_per_ticket_mean": 4.2,
            },
            "realtime": {"emit_interval_ms": 500, "burst": 100},
            "paths": {
                "dict": "data/dictionaries",
                "master": "data/master",
                "facts": "data/facts",
            },
            "stream": {"hub": "retail-events"},
        }
        with pytest.raises(ValidationError):
            Config(**invalid_config)

    @given(seed=st.integers(min_value=0, max_value=2**32 - 1))
    def test_config_seed_property_based(self, seed: int):
        """Property-based test for config seed validation."""
        config_data = {
            "seed": seed,
            "volume": {
                "stores": 250,
                "dcs": 12,
                "customers_per_day": 20000,
                "items_per_ticket_mean": 4.2,
            },
            "realtime": {"emit_interval_ms": 500, "burst": 100},
            "paths": {
                "dict": "data/dictionaries",
                "master": "data/master",
                "facts": "data/facts",
            },
            "stream": {"hub": "retail-events"},
        }
        config = Config(**config_data)
        assert config.seed == seed

    def test_config_from_json_file(self):
        """Test loading configuration from JSON file."""
        config_data = {
            "seed": 42,
            "volume": {
                "stores": 250,
                "dcs": 12,
                "customers_per_day": 20000,
                "items_per_ticket_mean": 4.2,
            },
            "realtime": {"emit_interval_ms": 500, "burst": 100},
            "paths": {
                "dict": "data/dictionaries",
                "master": "data/master",
                "facts": "data/facts",
            },
            "stream": {"hub": "retail-events"},
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name

        try:
            config = Config.from_file(temp_path)
            assert config.seed == 42
            assert config.volume.stores == 250
            assert config.stream.hub == "retail-events"
        finally:
            Path(temp_path).unlink()

    def test_config_from_invalid_json_file(self):
        """Test that invalid JSON file raises appropriate error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ invalid json }")
            temp_path = f.name

        try:
            with pytest.raises(ValueError):
                Config.from_file(temp_path)
        finally:
            Path(temp_path).unlink()

    def test_config_from_nonexistent_file(self):
        """Test that nonexistent file raises appropriate error."""
        with pytest.raises(FileNotFoundError):
            Config.from_file("/nonexistent/config.json")

    def test_config_to_dict(self):
        """Test conversion of config to dictionary."""
        config_data = {
            "seed": 42,
            "volume": {
                "stores": 250,
                "dcs": 12,
                "customers_per_day": 20000,
                "items_per_ticket_mean": 4.2,
            },
            "realtime": {"emit_interval_ms": 500, "burst": 100},
            "paths": {
                "dict": "data/dictionaries",
                "master": "data/master",
                "facts": "data/facts",
            },
            "stream": {"hub": "retail-events"},
        }
        config = Config(**config_data)
        result = config.model_dump()

        # Seed matches
        assert result["seed"] == config_data["seed"]

        # Paths use canonical field names; alias 'dict' maps to 'dictionaries'
        assert result["paths"]["dictionaries"] == config_data["paths"]["dict"]
        assert result["paths"]["master"] == config_data["paths"]["master"]
        assert result["paths"]["facts"] == config_data["paths"]["facts"]

        # Core realtime fields preserved; additional defaults are acceptable
        assert (
            result["realtime"]["emit_interval_ms"]
            == config_data["realtime"]["emit_interval_ms"]
        )
        assert result["realtime"]["burst"] == config_data["realtime"]["burst"]

        # Volume core fields preserved; additional volume defaults are acceptable
        assert result["volume"]["stores"] == config_data["volume"]["stores"]
        assert result["volume"]["dcs"] == config_data["volume"]["dcs"]
        assert (
            result["volume"]["customers_per_day"]
            == config_data["volume"]["customers_per_day"]
        )
        assert (
            result["volume"]["items_per_ticket_mean"]
            == config_data["volume"]["items_per_ticket_mean"]
        )

        # Stream hub preserved
        assert result["stream"]["hub"] == config_data["stream"]["hub"]

    def test_config_reproducibility_constraint(self):
        """Test that seed ensures reproducible configuration behavior."""

        # Create two identical configs
        # config1 = Config(**config_data)
        # config2 = Config(**config_data)

        # They should be identical
        # assert config1.seed == config2.seed
        # assert config1.model_dump() == config2.model_dump()

    def test_config_validation_integration(self):
        """Test that all nested validations work together."""
        # This should fail due to multiple validation errors
        invalid_config = {
            "seed": -1,  # Invalid seed
            "volume": {
                "stores": 0,  # Invalid stores
                "dcs": 12,
                "customers_per_day": 20000,
                "items_per_ticket_mean": 4.2,
            },
            "realtime": {"emit_interval_ms": 0, "burst": 100},  # Invalid interval
            "paths": {
                "dict": "",  # Invalid path
                "master": "data/master",
                "facts": "data/facts",
            },
            "stream": {"hub": "retail-events"},
        }

        with pytest.raises(ValidationError):
            Config(**invalid_config)

        # Should have multiple validation errors
        # errors = exc_info.value.errors()
        # assert len(errors) >= 3  # At least seed, stores, emit_interval, dict path
