from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from retail_setup.config.generation import GenerationConfig, load_generation_config


def test_defaults_are_valid():
    cfg = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 3, 31))
    assert cfg.store_type == "supercenter"
    assert cfg.store_count == 50
    assert cfg.seed == 42
    assert cfg.silver_db == "ag"
    assert cfg.gold_db == "au"


def test_end_before_start_rejected():
    with pytest.raises(ValidationError, match="end_date"):
        GenerationConfig(start_date=date(2025, 3, 1), end_date=date(2025, 1, 1))


def test_store_type_must_exist_on_disk():
    with pytest.raises(ValidationError, match="store_type"):
        GenerationConfig(
            start_date=date(2025, 1, 1), end_date=date(2025, 1, 31), store_type="bogus"
        )


def test_yaml_round_trip(tmp_path: Path):
    p = tmp_path / "config.yaml"
    p.write_text(
        "store_type: grocery\nstart_date: 2025-01-01\nend_date: 2025-02-28\n"
        "store_count: 10\nseed: 7\n"
    )
    cfg = load_generation_config(p)
    assert cfg.store_type == "grocery"
    assert cfg.store_count == 10


def test_scale_defaults_derive_from_store_count():
    cfg = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31),
                           store_count=40)
    assert cfg.dc_count == 4          # ~1 DC per 10 stores, min 1
    assert cfg.customer_count == 40_000  # 1000 per store
    assert cfg.transactions_per_store_day == 400


def test_scale_overrides_respected():
    cfg = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31),
                           store_count=40, dc_count=2, customer_count=500,
                           transactions_per_store_day=50)
    assert (cfg.dc_count, cfg.customer_count, cfg.transactions_per_store_day) == (2, 500, 50)
