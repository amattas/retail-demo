from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.store_activity import generate_foot_traffic, generate_store_ops
from retail_setup.generation.schemas import column_names

ZONES = ["ENTRANCE_MAIN", "ENTRANCE_SIDE", "AISLES_A", "AISLES_B", "CHECKOUT"]


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 12, 23),
                           end_date=date(2025, 12, 27), store_count=2, dc_count=1,
                           customer_count=200, seed=9, transactions_per_store_day=40)
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
    assert {r.operation_type for r in ops.select("operation_type").distinct().collect()} == \
        {"opened", "closed"}


def test_foot_traffic(spark, setup):
    cfg, dims, sales = setup
    ft = generate_foot_traffic(spark, sales["fact_receipts"], dims, cfg)
    assert ft.columns == column_names("fact_foot_traffic")
    assert {r.zone for r in ft.select("zone").distinct().collect()} <= set(ZONES)
    assert ft.filter(F.col("count") < 0).count() == 0
    assert ft.filter((F.col("dwell_seconds") < 30) | (F.col("dwell_seconds") > 420)).count() == 0
    # traffic exceeds receipts for matching store-hours
    rc = sales["fact_receipts"].groupBy(
        "store_id", F.date_trunc("hour", "event_ts").alias("hr")).count()
    tt = ft.groupBy("store_id", F.col("event_ts").alias("hr")) \
           .agg(F.sum("count").alias("traffic"))
    j = rc.join(tt, ["store_id", "hr"])
    assert j.count() > 0
    assert j.filter(F.col("traffic") < F.col("count")).count() == 0


def test_foot_traffic_covers_open_hours_without_receipts(spark, setup):
    cfg, dims, sales = setup
    ft = generate_foot_traffic(spark, sales["fact_receipts"], dims, cfg)
    rc = sales["fact_receipts"].groupBy(
        "store_id", F.date_trunc("hour", "event_ts").alias("hr")).count()
    tt = ft.groupBy("store_id", F.col("event_ts").alias("hr")) \
           .agg(F.sum("count").alias("traffic"))
    # every receipt store-hour still has a foot-traffic row (coverage preserved)
    assert rc.join(tt, ["store_id", "hr"], "left_anti").count() == 0
    # and open store-hours with no receipts now emit browsing traffic
    assert tt.join(rc, ["store_id", "hr"], "left_anti").count() > 0
