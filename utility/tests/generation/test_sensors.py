from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.sensors import generate_ble
from retail_setup.generation.schemas import column_names
from retail_setup.generation.vocabulary import ble_layout, ble_zones_for_profile


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 7, 7),
        end_date=date(2025, 7, 9),
        store_count=2,
        dc_count=1,
        customer_count=150,
        seed=17,
        transactions_per_store_day=30,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    pings, zc = generate_ble(spark, sales["fact_receipts"], dims, cfg, dicts.profile)
    return sales, pings, zc, dicts.profile


def test_contract_columns(setup):
    _, pings, zc, _ = setup
    assert pings.columns == column_names("fact_ble_pings")
    assert zc.columns == column_names("fact_customer_zone_changes")


def _visits(pings):
    """Each visit's pings in time order: (key, buyer, zones in order, customer IDs)."""
    rows = pings.select("trace_id", "event_ts", "zone", "customer_id").collect()
    visits = {}
    for row in rows:
        key, seq = row.trace_id.removeprefix("TRC-BLE-").rsplit("-", 1)
        visits.setdefault(key, []).append((int(seq), row.event_ts, row.zone, row.customer_id))
    return {key: sorted(entries) for key, entries in visits.items()}


def test_ping_properties(setup):
    sales, pings, _, profile = setup
    assert pings.filter((F.col("rssi") < -80) | (F.col("rssi") > -29)).count() == 0
    # The store type's own zones (grocery: produce, bakery, deli, ...).
    zones = {r.zone for r in pings.select("zone").distinct().collect()}
    assert zones <= set(ble_zones_for_profile(profile))
    assert {"PRODUCE", "BAKERY", "DELI"} & zones
    anon = pings.filter(F.col("customer_ble_id").startswith("ANON-"))
    assert anon.filter(F.col("customer_id").isNotNull()).count() == 0
    visits = _visits(pings)
    buyers = {key for key in visits if key.startswith("RCP")}
    browsers = set(visits) - buyers
    n_receipts = sales["fact_receipts"].count()
    # A quarter of visits are detected, buyers and non-buyers alike.
    assert 0.15 * n_receipts <= len(buyers) <= 0.35 * n_receipts
    assert len(browsers) > len(buyers)
    known = sum(1 for key in buyers if visits[key][0][3] is not None) / len(buyers)
    assert 0.15 < known < 0.45  # 30% of buying visits nominal


def test_ble_paths_run_entrance_departments_checkout_exit(setup):
    _, pings, _, profile = setup
    layout = ble_layout(profile)
    for key, entries in _visits(pings).items():
        times = [ts for _, ts, _, _ in entries]
        assert times == sorted(times), key
        path = [
            zone for i, (_, _, zone, _) in enumerate(entries) if i == 0 or zone != entries[i - 1][2]
        ]
        assert len(path) == len(set(path)), (key, path)
        assert path[0] == layout.entry and path[-1] == layout.exit, (key, path)
        middle = path[1:-1]
        if key.startswith("RCP"):
            # Buyers pay at checkout, the last zone before the exit.
            assert middle[-1] == layout.checkout, (key, path)
            middle = middle[:-1]
        else:
            assert layout.checkout not in path, (key, path)
        assert 1 <= len(middle) <= 3 and set(middle) <= set(layout.middle), (key, path)


def test_buyers_pay_at_checkout_between_browsing_and_the_exit(setup):
    sales, pings, _, profile = setup
    layout = ble_layout(profile)
    purchased = {r.receipt_id_ext: r.event_ts for r in sales["fact_receipts"].collect()}
    for key, entries in _visits(pings).items():
        if not key.startswith("RCP"):
            continue
        paid = purchased[key]
        at_checkout = [ts for _, ts, zone, _ in entries if zone == layout.checkout]
        assert min(at_checkout) <= paid <= max(at_checkout), key
        assert all(
            ts <= paid for _, ts, zone, _ in entries if zone not in (layout.checkout, layout.exit)
        )
        assert all(ts >= paid for _, ts, zone, _ in entries if zone == layout.exit)


def test_zone_changes_follow_the_path(setup):
    _, pings, zc, profile = setup
    layout = ble_layout(profile)
    assert zc.count() > 0
    assert zc.filter(F.col("from_zone") == F.col("to_zone")).count() == 0
    assert zc.filter(F.col("to_zone") == layout.entry).count() == 0
    assert zc.filter(F.col("from_zone") == layout.exit).count() == 0
    # every (store, ble) in zone-changes exists in pings
    zk = zc.select("store_id", "customer_ble_id").distinct()
    pk = pings.select("store_id", "customer_ble_id").distinct()
    assert zk.join(pk, ["store_id", "customer_ble_id"], "left_anti").count() == 0


def test_ble_visits_stay_within_store_hours_and_history(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 7, 7),
        end_date=date(2025, 7, 7),
        store_count=1,
        customer_count=5000,
        seed=29,
    )
    receipt_rows = [
        (
            f"RCP-OPEN-{index}",
            1,
            datetime(2025, 7, 7, 7, 0),
            date(2025, 7, 7),
            1,
            "SALE",
        )
        for index in range(30)
    ] + [
        (
            f"RCP-CLOSE-{index}",
            1,
            datetime(2025, 7, 7, 21, 59),
            date(2025, 7, 7),
            1,
            "SALE",
        )
        for index in range(30)
    ]
    receipts = spark.createDataFrame(
        receipt_rows,
        "receipt_id_ext string, store_id long, event_ts timestamp, "
        "event_date date, customer_id long, receipt_type string",
    )
    dims = {
        "dim_customers": spark.createDataFrame(
            [(1, "BLE000001")],
            "ID long, BLEId string",
        ),
        "dim_stores": spark.createDataFrame(
            [(1, 1, 1.0, "7-22")],
            "ID long, GeographyID long, daily_traffic_multiplier double, operating_hours string",
        ),
    }

    pings, changes = generate_ble(spark, receipts, dims, cfg)

    opens = F.to_timestamp(F.lit("2025-07-07 07:00:00"))
    closes = F.to_timestamp(F.lit("2025-07-07 22:00:00"))
    assert pings.filter(F.col("event_ts") < opens).count() == 0
    assert pings.filter(F.col("event_ts") >= closes).count() == 0
    assert pings.filter(F.col("event_date") != F.lit(cfg.start_date)).count() == 0
    assert changes.filter(F.col("event_ts") < opens).count() == 0
    assert changes.filter(F.col("event_ts") >= closes).count() == 0
    assert changes.filter(F.col("event_date") != F.lit(cfg.start_date)).count() == 0
