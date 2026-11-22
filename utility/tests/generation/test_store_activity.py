from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.store_activity import generate_foot_traffic, generate_store_ops
from retail_setup.generation.schemas import column_names
from retail_setup.generation.traffic import generate_hourly_visits
from retail_setup.generation.vocabulary import ble_layout


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 12, 23),
        end_date=date(2025, 12, 27),
        store_count=2,
        dc_count=1,
        customer_count=200,
        seed=9,
        transactions_per_store_day=40,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    return cfg, dims, sales


def test_store_ops(spark, setup):
    cfg, dims, _ = setup
    ops = generate_store_ops(spark, dims, cfg)
    assert ops.columns == column_names("fact_store_ops")
    # 5 days minus Christmas = 4 op-days x 2 stores x 2 events
    assert ops.count() == 4 * 2 * 2
    assert ops.filter(F.col("event_date") == date(2025, 12, 25)).count() == 0
    per = ops.groupBy("store_id", "event_date").count().collect()
    assert all(r["count"] == 2 for r in per)
    assert {r.operation_type for r in ops.select("operation_type").distinct().collect()} == {
        "opened",
        "closed",
    }


def test_receipts_skip_christmas(setup):
    _, _, sales = setup
    assert sales["fact_receipts"].filter(F.col("event_date") == date(2025, 12, 25)).count() == 0


def test_foot_traffic_counts_zone_passages(spark, setup):
    cfg, dims, sales = setup
    profile = load_dictionaries(default_dictionary_root(), "grocery").profile
    layout = ble_layout(profile)
    ft = generate_foot_traffic(spark, sales["fact_receipts"], dims, cfg)
    assert ft.columns == column_names("fact_foot_traffic")
    rows = ft.collect()
    # The store type's own zones, every one counted each open store-hour.
    assert {r.zone for r in rows} == set(layout.zones)
    assert all(r["count"] >= 0 for r in rows)
    assert all(layout.dwell[r.zone][0] <= r.dwell_seconds <= layout.dwell[r.zone][1] for r in rows)
    counts = {}
    for r in rows:
        counts.setdefault((r.store_id, r.event_ts), {})[r.zone] = r["count"]
    visits = {
        (r.store_id, r.hour_ts): r
        for r in generate_hourly_visits(spark, dims["dim_stores"], profile, cfg).collect()
    }
    assert set(counts) == set(visits)
    for key, zones in counts.items():
        # Passages, not shares: every visitor passes the entrance and the exit,
        # checkout counts the hour's buyers, departments at most every visitor.
        assert zones["ENTRANCE"] == zones["EXIT"] == visits[key].visitors
        assert zones["CHECKOUT"] == visits[key].n_receipts
        assert all(zones[zone] <= visits[key].visitors for zone in layout.middle)
    browsed = sum(sum(zones[z] for z in layout.middle) for zones in counts.values())
    entered = sum(zones["ENTRANCE"] for zones in counts.values())
    # Each visitor browses 1-3 department zones (2 on average).
    assert 1.6 <= browsed / entered <= 2.4, browsed / entered
    # Checkout covers every purchase of the store-hour.
    receipts = (
        sales["fact_receipts"]
        .groupBy("store_id", F.date_trunc("hour", "event_ts").alias("hr"))
        .count()
        .collect()
    )
    assert receipts
    assert all(counts[(r.store_id, r.hr)]["CHECKOUT"] >= r["count"] for r in receipts)


def test_foot_traffic_covers_open_hours_without_receipts(spark, setup):
    cfg, dims, sales = setup
    ft = generate_foot_traffic(spark, sales["fact_receipts"], dims, cfg)
    rc = (
        sales["fact_receipts"]
        .groupBy("store_id", F.date_trunc("hour", "event_ts").alias("hr"))
        .count()
    )
    tt = ft.filter(F.col("zone") == "ENTRANCE").select(
        "store_id", F.col("event_ts").alias("hr"), F.col("count").alias("traffic")
    )
    # every receipt store-hour still has a foot-traffic row (coverage preserved)
    assert rc.join(tt, ["store_id", "hr"], "left_anti").count() == 0
    # and open store-hours with no receipts now emit browsing traffic
    assert tt.join(rc, ["store_id", "hr"], "left_anti").count() > 0
