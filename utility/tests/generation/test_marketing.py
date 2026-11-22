from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.marketing import CHANNEL_COSTS, generate_marketing
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 6, 2),
        end_date=date(2025, 6, 8),
        store_count=4,
        dc_count=1,
        customer_count=200,
        seed=31,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    return cfg, dims, generate_marketing(spark, dims, cfg)


def test_contract_columns(setup):
    *_, mk = setup
    assert mk.columns == column_names("fact_marketing")


def test_ids_and_costs(setup):
    *_, mk = setup
    n = mk.count()
    assert n > 0
    assert mk.select("impression_id_ext").distinct().count() == n
    assert mk.filter(~F.col("campaign_id").startswith("CAMP")).count() == 0
    # Real CPMs: cost (four decimals) within the channel band x device multiplier,
    # $0.002-$0.03 an impression, and cost_cents rounds it at random.
    rows = mk.select("channel", "cost", "cost_cents", "campaign_id", "creative_id").collect()
    for r in rows:
        lo, hi = CHANNEL_COSTS[r.channel]
        cost = float(r.cost)
        assert lo * 0.8 - 1e-4 <= cost <= hi * 1.2 + 1e-4, r
        assert 0.002 <= cost <= 0.03, r
        assert r.cost_cents in (int(cost * 100), int(cost * 100) + 1), r
    dollars = sum(float(r.cost) for r in rows)
    assert abs(sum(r.cost_cents for r in rows) / 100 - dollars) < 0.05 * dollars
    # A small creative pool per campaign and channel.
    pools: dict[tuple[str, str], set[str]] = {}
    for r in rows:
        pools.setdefault((r.campaign_id, r.channel), set()).add(r.creative_id)
    assert max(len(pool) for pool in pools.values()) <= 4


def test_crm_match_share(setup):
    *_, mk = setup
    share = mk.filter(F.col("customer_id").isNotNull()).count() / mk.count()
    assert 0.01 < share < 0.12  # 5% nominal


def test_device_mix(setup):
    *_, mk = setup
    mob = mk.filter("device = 'MOBILE'").count() / mk.count()
    assert 0.45 < mob < 0.75
