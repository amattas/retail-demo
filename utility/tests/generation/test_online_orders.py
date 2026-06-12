from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.online_orders import generate_online_orders
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 4, 7),
                           end_date=date(2025, 4, 13), store_count=3, dc_count=2,
                           customer_count=300, seed=13, online_orders_per_day=40)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    return cfg, dims, generate_online_orders(spark, dims, dicts.profile, cfg)


def test_contract_columns(setup):
    _, _, g = setup
    assert g["fact_online_order_headers"].columns == column_names("fact_online_order_headers")
    assert g["fact_online_order_lines"].columns == column_names("fact_online_order_lines")
    assert g["payments"].columns == column_names("fact_payments")


def test_volume_and_ids(setup):
    cfg, _, g = setup
    h = g["fact_online_order_headers"]
    n = h.count()
    assert 0.4 * 7 * cfg.online_orders_per_day < n < 2.0 * 7 * cfg.online_orders_per_day
    assert h.select("order_id_ext").distinct().count() == n
    assert h.filter(~F.col("order_id_ext").startswith("ONL")).count() == 0


def test_lines_link_and_money(setup):
    _, _, g = setup
    h, l = g["fact_online_order_headers"], g["fact_online_order_lines"]
    assert l.join(h, l.order_id == h.order_id_ext, "left_anti").count() == 0
    live = l.filter(F.col("fulfillment_status") != "CANCELLED")
    sums = live.groupBy("order_id").agg(F.sum("ext_cents").alias("s"))
    j = h.join(sums, h.order_id_ext == sums.order_id)
    assert j.filter(F.col("subtotal_cents") != F.col("s")).count() == 0
    assert h.filter(F.col("total_cents") != F.col("subtotal_cents") + F.col("tax_cents")).count() == 0


def test_fulfillment_modes_and_nodes(setup):
    _, dims, g = setup
    l = g["fact_online_order_lines"]
    modes = {r.fulfillment_mode for r in l.select("fulfillment_mode").distinct().collect()}
    assert modes <= {"SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"}
    dc_ids = {r.ID for r in dims["dim_distribution_centers"].select("ID").collect()}
    st_ids = {r.ID for r in dims["dim_stores"].select("ID").collect()}
    bad_dc = l.filter((F.col("node_type") == "DC") & ~F.col("node_id").isin(*dc_ids))
    bad_st = l.filter((F.col("node_type") == "STORE") & ~F.col("node_id").isin(*st_ids))
    assert bad_dc.count() == 0 and bad_st.count() == 0


def test_lifecycle_ordering(setup):
    _, _, g = setup
    l = g["fact_online_order_lines"].filter(F.col("fulfillment_status") == "DELIVERED")
    assert l.count() > 0
    assert l.filter(F.col("picked_ts") < F.col("event_ts")).count() == 0
    shipped = l.filter(F.col("shipped_ts").isNotNull())
    assert shipped.filter(F.col("shipped_ts") < F.col("picked_ts")).count() == 0
    assert shipped.filter(F.col("delivered_ts") < F.col("shipped_ts")).count() == 0


def test_cancellations_zeroed_no_payment(setup):
    _, _, g = setup
    h, pay = g["fact_online_order_headers"], g["payments"]
    cancelled = h.join(
        g["fact_online_order_lines"].filter("fulfillment_status = 'CANCELLED'")
        .select(F.col("order_id").alias("order_id_ext")).distinct(),
        "order_id_ext")
    assert cancelled.filter(F.col("total_cents") != 0).count() == 0
    assert pay.join(cancelled.select("order_id_ext"), "order_id_ext").count() == 0
    # payments basics
    assert pay.filter(F.col("receipt_id_ext").isNotNull()).count() == 0
    assert pay.filter(F.col("store_id").isNotNull()).count() == 0
