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


def test_months_derives_window_ending_yesterday():
    from datetime import timedelta

    cfg = GenerationConfig(store_type="grocery", months=3)
    yesterday = date.today() - timedelta(days=1)
    assert cfg.end_date == yesterday
    assert cfg.months == 3
    assert cfg.start_date < cfg.end_date
    # ~3 calendar months earlier (allow the trailing partial day from inclusivity)
    assert 89 <= (cfg.end_date - cfg.start_date).days <= 93


def test_months_required_when_dates_absent():
    with pytest.raises(ValidationError, match="months"):
        GenerationConfig(store_type="grocery")


def test_explicit_dates_take_precedence_over_months():
    cfg = GenerationConfig(
        store_type="grocery",
        months=6,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
    )
    assert cfg.start_date == date(2025, 1, 1)
    assert cfg.end_date == date(2025, 1, 31)


def test_subtract_months_clamps_day():
    from retail_setup.config.generation import _subtract_months

    assert _subtract_months(date(2025, 3, 31), 1) == date(2025, 2, 28)
    assert _subtract_months(date(2025, 1, 15), 1) == date(2024, 12, 15)
    assert _subtract_months(date(2024, 3, 31), 1) == date(2024, 2, 29)  # leap year


def test_yaml_round_trip_months(tmp_path: Path):
    from datetime import timedelta

    p = tmp_path / "config.yaml"
    p.write_text("store_type: grocery\nmonths: 2\nstore_count: 10\nseed: 7\n")
    cfg = load_generation_config(p)
    assert cfg.months == 2
    assert cfg.store_count == 10
    assert cfg.end_date == date.today() - timedelta(days=1)


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
    cfg = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31), store_count=40)
    assert cfg.dc_count == 4  # ~1 DC per 10 stores, min 1
    # identified yearly visits / (active share x supercenter trips per customer)
    assert cfg.customer_count == round(40 * 400 * 365 * 1.0525 * 0.55 / (0.75 * 8))
    assert cfg.transactions_per_store_day == 400


def test_scale_overrides_respected():
    cfg = GenerationConfig(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        store_count=40,
        dc_count=2,
        customer_count=8000,
        transactions_per_store_day=50,
    )
    assert (cfg.dc_count, cfg.customer_count, cfg.transactions_per_store_day) == (2, 8000, 50)


def test_store_volumes_derive_from_the_store_type_profile():
    def cfg(store_type: str, **overrides) -> GenerationConfig:
        return GenerationConfig(
            store_type=store_type,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            store_count=10,
            **overrides,
        )

    supercenter, luxury = cfg("supercenter"), cfg("luxury")
    assert (supercenter.transactions_per_store_day, luxury.transactions_per_store_day) == (400, 40)
    # Online orders are the profile's online_order_share of all orders: 12% of
    # supercenter orders, 25% of luxury orders (mean traffic multiplier 1.0525).
    assert supercenter.online_orders_per_day == round(10 * 400 * 1.0525 * 0.12 / 0.88)
    assert luxury.online_orders_per_day == round(10 * 40 * 1.0525 * 0.25 / 0.75)
    explicit = cfg("luxury", transactions_per_store_day=100)
    assert explicit.online_orders_per_day == round(10 * 100 * 1.0525 * 0.25 / 0.75)
    assert cfg("luxury", online_orders_per_day=3).online_orders_per_day == 3


def test_mean_traffic_multiplier_matches_the_volume_classes():
    from retail_setup.config.generation import MEAN_TRAFFIC_MULTIPLIER
    from retail_setup.generation.dims import VOLUME_CLASSES

    expected = sum(share * (lo + hi) / 2 for _, share, (lo, hi) in VOLUME_CLASSES)
    assert MEAN_TRAFFIC_MULTIPLIER == pytest.approx(expected)


def test_customer_count_floored_at_minimum():
    from retail_setup.config.generation import MIN_CUSTOMER_COUNT

    # Small stores derive fewer than the minimum -> floored.
    small = GenerationConfig(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        store_count=1,
        transactions_per_store_day=100,
    )
    assert small.customer_count == MIN_CUSTOMER_COUNT  # ~3,500 derived -> floored
    # An explicitly tiny override is lifted too, so the churn model can train.
    explicit = GenerationConfig(
        start_date=date(2025, 1, 1), end_date=date(2025, 1, 31), store_count=1, customer_count=10
    )
    assert explicit.customer_count == MIN_CUSTOMER_COUNT
    # The floor is a no-op once the derived value reaches it.
    big = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31), store_count=50)
    assert big.customer_count == round(50 * 400 * 365 * 1.0525 * 0.55 / (0.75 * 8))


def test_explicit_dictionary_root(tmp_path):
    # a fake root with one valid store type
    import json
    import shutil

    from retail_setup.dictionaries.loader import default_dictionary_root

    src = default_dictionary_root()
    shutil.copytree(src / "_shared", tmp_path / "_shared")
    shutil.copytree(src / "grocery", tmp_path / "mini")
    profile = json.loads((tmp_path / "mini" / "profile.json").read_text())
    profile["store_type"] = "mini"
    (tmp_path / "mini" / "profile.json").write_text(json.dumps(profile))

    cfg = GenerationConfig(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        store_type="mini",
        dictionary_root=str(tmp_path),
    )
    assert cfg.store_type == "mini"
    assert cfg.resolved_dictionary_root == tmp_path


def test_unknown_type_in_explicit_root_rejected(tmp_path):
    (tmp_path / "_shared").mkdir()
    with pytest.raises(ValidationError, match="store_type"):
        GenerationConfig(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            store_type="grocery",
            dictionary_root=str(tmp_path),
        )


def test_online_orders_per_day_is_bounded():
    from datetime import date

    from pydantic import ValidationError

    from retail_setup.config.generation import MAX_ONLINE_ORDERS_PER_DAY, GenerationConfig

    window = {"start_date": date(2025, 1, 1), "end_date": date(2025, 1, 2)}
    assert (
        GenerationConfig(
            **window, online_orders_per_day=MAX_ONLINE_ORDERS_PER_DAY
        ).online_orders_per_day
        == MAX_ONLINE_ORDERS_PER_DAY
    )
    with pytest.raises(ValidationError, match="online_orders_per_day"):
        GenerationConfig(**window, online_orders_per_day=MAX_ONLINE_ORDERS_PER_DAY + 1)
