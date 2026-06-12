from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.sensors import generate_ble
from retail_setup.generation.schemas import column_names

BLE_ZONES = {"ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"}


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 7, 7),
                           end_date=date(2025, 7, 9), store_count=2, dc_count=1,
                           customer_count=150, seed=17, transactions_per_store_day=30)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    pings, zc = generate_ble(spark, sales["fact_receipts"], dims, cfg)
    return sales, pings, zc


def test_contract_columns(setup):
    _, pings, zc = setup
    assert pings.columns == column_names("fact_ble_pings")
    assert zc.columns == column_names("fact_customer_zone_changes")


def test_ping_properties(setup):
    sales, pings, _ = setup
    n_receipts = sales["fact_receipts"].count()
    # 2-5 zones x 2-5 pings per visit => 4-25 pings per receipt
    assert 4 * n_receipts <= pings.count() <= 25 * n_receipts
    assert pings.filter((F.col("rssi") < -80) | (F.col("rssi") > -29)).count() == 0
    assert {r.zone for r in pings.select("zone").distinct().collect()} <= BLE_ZONES
    known = pings.filter(F.col("customer_id").isNotNull()).count() / pings.count()
    assert 0.15 < known < 0.45  # 30% of visits nominal
    anon = pings.filter(F.col("customer_ble_id").startswith("ANON-"))
    assert anon.filter(F.col("customer_id").isNotNull()).count() == 0


def test_zone_changes_derive_from_pings(setup):
    _, pings, zc = setup
    assert zc.count() > 0
    assert zc.filter(F.col("from_zone") == F.col("to_zone")).count() == 0
    # every (store, ble) in zone-changes exists in pings
    zk = zc.select("store_id", "customer_ble_id").distinct()
    pk = pings.select("store_id", "customer_ble_id").distinct()
    assert zk.join(pk, ["store_id", "customer_ble_id"], "left_anti").count() == 0
