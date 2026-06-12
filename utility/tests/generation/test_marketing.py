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
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 6, 2),
                           end_date=date(2025, 6, 8), store_count=4, dc_count=1,
                           customer_count=200, seed=31)
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
    # cost_cents within channel band x device multiplier envelope
    for r in mk.select("channel", "cost_cents").collect():
        lo, hi = CHANNEL_COSTS[r.channel]
        assert lo * 100 * 0.8 - 1 <= r.cost_cents <= hi * 100 * 1.2 + 1, r


def test_crm_match_share(setup):
    *_, mk = setup
    share = mk.filter(F.col("customer_id").isNotNull()).count() / mk.count()
    assert 0.01 < share < 0.12  # 5% nominal


def test_device_mix(setup):
    *_, mk = setup
    mob = mk.filter("device = 'MOBILE'").count() / mk.count()
    assert 0.45 < mob < 0.75
