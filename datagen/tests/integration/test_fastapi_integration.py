"""
Basic FastAPI import and configuration smoke tests.
Skips if FastAPI is not installed.
"""

import json
from pathlib import Path

import pytest

pytest.importorskip("fastapi")


def test_imports_and_config(tmp_path: Path):
    from retail_datagen.api.models import (  # noqa: F401
        HistoricalDataRequest,
        MasterDataRequest,
    )

    # Create a minimal valid config and ensure RetailConfig accepts it
    from retail_datagen.config.models import RetailConfig
    from retail_datagen.main import app  # noqa: F401

    config_data = {
        "seed": 42,
        "volume": {
            "stores": 10,
            "dcs": 2,
            "customers_per_day": 100,
            "items_per_ticket_mean": 4.2,
        },
        "realtime": {"emit_interval_ms": 500, "burst": 100},
        "paths": {
            "dictionaries": "data/dictionaries",
            "master": str(tmp_path / "master"),
            "facts": str(tmp_path / "facts"),
        },
        "stream": {"hub": "retail-events"},
    }
    cfg = RetailConfig(**config_data)
    p = tmp_path / "config.json"
    p.write_text(json.dumps(cfg.model_dump(), indent=2))
    assert p.exists()
