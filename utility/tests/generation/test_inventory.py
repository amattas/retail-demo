from dataclasses import asdict
from datetime import date, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.fulfillment import _EVENT_SCHEMA, resolve_fulfillment
from retail_setup.generation.inventory import (
    _reorders,
    _shipments,
    _split_reorders_by_capacity,
    _truck_inventory,
    _truck_moves,
    generate_inventory_chain,
)
from retail_setup.generation.inventory_balances import TXN_COLS, with_balances
from retail_setup.generation.inventory_policy import Demand, simulate_sku
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.returns import generate_returns
from retail_setup.generation.runtime import seeded_draws
from retail_setup.generation.schemas import column_names

TABLES = ["fact_store_inventory_txn", "fact_dc_inventory_txn", "fact_truck_moves",
          "fact_truck_inventory", "fact_reorders", "fact_stockouts"]


def test_dc_count_rejects_fixed_width_shipment_id_overflow():
    with pytest.raises(ValueError, match="less than or equal to 999"):
        GenerationConfig(
            store_type="grocery",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            store_count=1,
            dc_count=1000,
        )


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 8, 4),
                           end_date=date(2025, 8, 10), store_count=2, dc_count=1,
                           customer_count=200, seed=23, transactions_per_store_day=40)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    fulfilled = resolve_fulfillment(sales, dims, cfg)
    sales = fulfilled.sales
    rets = generate_returns(spark, sales, dims, cfg, return_headers=fulfilled.return_headers)
    inv = generate_inventory_chain(spark, sales, rets, dims, cfg, fulfillment=fulfilled)
    return cfg, dims, sales, inv


@pytest.fixture(scope="module")
def seeded_logistics(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 8, 4),
        end_date=date(2025, 8, 10),
        store_count=1,
        dc_count=1,
        seed=23,
        truck_capacity=100,
    )
    events = spark.createDataFrame([
        asdict(event)
        for product in range(1, 11)
        for event in simulate_sku(
            [Demand(datetime(2025, 8, 4, 12) + timedelta(days=day),
                    f"R-{product}-{day}", 1, 30) for day in range(7)],
            store_id=1, product_id=product, start_date=cfg.start_date,
            end_date=cfg.end_date, seed=cfg.seed, capacity=cfg.truck_capacity,
        )
    ], _EVENT_SCHEMA).withColumn("event_date", F.to_date("event_ts"))
    store_dc = spark.createDataFrame(
        [(1, 1)],
        "store_id long, dc_id long",
    )
    dims = {
        "dim_trucks": spark.createDataFrame(
            [(1, 1.0), (2, None)],
            "ID long, DCID double",
        )
    }
    reorders = _reorders(events, store_dc)
    planned = events.filter("kind = 'SHIPMENT'").join(store_dc, "store_id")
    shipments = _shipments(spark, planned, dims, cfg)
    return (
        reorders,
        _truck_moves(shipments),
        _truck_inventory(shipments),
    )


def test_contract_columns(setup):
    *_, inv = setup
    for t in TABLES:
        assert inv[t].columns == column_names(t), t


def test_sales_txns_mirror_receipt_lines(setup):
    _, _, sales, inv = setup
    txn = inv["fact_store_inventory_txn"].filter("txn_type = 'SALE'")
    assert txn.count() == sales["fact_receipt_lines"].count()
    assert txn.filter(F.col("quantity") >= 0).count() == 0


def test_balances_are_running(setup):
    *_, inv = setup
    txn = inv["fact_store_inventory_txn"]
    # spot-check one (store, product): balance deltas equal quantities
    key = txn.groupBy("store_id", "product_id").count().orderBy(F.desc("count")).first()
    seq = (txn.filter((F.col("store_id") == key.store_id)
                      & (F.col("product_id") == key.product_id))
           .orderBy("event_ts", "trace_id").collect())
    running = 0
    for r in seq:
        running += r.quantity
        assert r.balance == running, (r.event_ts, r.balance, running)


def test_truck_lifecycle(seeded_logistics):
    _, moves, truck_inventory = seeded_logistics
    assert moves.count() > 0, "fixed seed must produce shipment lifecycle rows"
    per = moves.groupBy("shipment_id").agg(
        F.collect_set("status").alias("statuses"), F.count("*").alias("n"))
    for r in per.collect():
        assert set(r.statuses) == {"SCHEDULED", "LOADING", "IN_TRANSIT", "ARRIVED",
                                   "UNLOADING", "COMPLETED"}
        assert r.n == 6
    done = moves.filter("status = 'COMPLETED'")
    assert done.filter(F.col("actual_unload_duration") < 30).count() == 0
    # load/unload pairs exist per shipment product
    loads = (
        truck_inventory.filter("action = 'LOAD'")
        .groupBy("shipment_id", "product_id")
        .count()
    )
    unloads = (
        truck_inventory.filter("action = 'UNLOAD'")
        .groupBy("shipment_id", "product_id")
        .count()
    )
    assert loads.join(unloads, ["shipment_id", "product_id"], "left_anti").count() == 0


def test_shipments_respect_capacity_when_one_reorder_exceeds_it(spark):
    reorders = spark.createDataFrame(
        [
            (1, date(2025, 8, 4), 1, 10, 60),
            (1, date(2025, 8, 4), 1, 20, 10),
        ],
        "store_id long, event_date date, dc_id long, product_id long, "
        "reorder_quantity long",
    )

    split = _split_reorders_by_capacity(reorders, capacity=25)
    leg_totals = (
        split.groupBy("store_id", "event_date", "leg")
        .agg(F.sum("reorder_quantity").alias("quantity"))
    )
    assert leg_totals.filter(F.col("quantity") > 25).count() == 0
    assert split.filter(F.col("product_id") == 10).count() == 3
    assert split.agg(F.sum("reorder_quantity")).first()[0] == 70


def test_reorder_priorities(seeded_logistics):
    ro, _, _ = seeded_logistics
    assert ro.count() > 0, "fixed seed must produce reorders"
    assert {r.priority for r in ro.select("priority").distinct().collect()} <= \
        {"NORMAL", "HIGH", "URGENT"}
    rows = ro.collect()
    assert {row.product_id for row in rows} == set(range(1, 11))
    assert all(row.reorder_quantity > 0 and row.reorder_quantity % 12 == 0 for row in rows)
    assert all(0 <= row.current_quantity <= row.reorder_point for row in rows)
    assert any(row.current_quantity > 0 for row in rows)
    assert any(row.priority != "URGENT" for row in rows)


def test_stockouts_mutually_exclusive(setup):
    *_, inv = setup
    so = inv["fact_stockouts"]
    both = so.filter(F.col("StoreID").isNotNull() & F.col("DCID").isNotNull())
    neither = so.filter(F.col("StoreID").isNull() & F.col("DCID").isNull())
    assert both.count() == 0 and neither.count() == 0


def _inventory_txns(spark, quantities):
    rows = [
        (
            1,
            1,
            quantity,
            "SALE",
            f"sale-{index}",
            datetime(2025, 1, index + 1, 12),
            date(2025, 1, index + 1),
            f"trace-{index}",
        )
        for index, quantity in enumerate(quantities)
    ]
    return spark.createDataFrame(rows, schema=", ".join([
        "node_id long",
        "product_id long",
        "quantity long",
        "txn_type string",
        "source string",
        "event_ts timestamp",
        "event_date date",
        "trace_id string",
    ])).select(*TXN_COLS)


def _opening_stock(balanced):
    return balanced.filter(F.col("txn_type") == "INITIAL").select("quantity").first()[0]


def test_opening_stock_stays_within_seed_range(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 2),
        store_count=1,
        seed=37,
    )
    balanced = with_balances(
        _inventory_txns(spark, [-10, -1000]),
        40,
        120,
        "ST",
        seeded_draws(cfg.seed),
        cfg,
    )
    assert 40 <= _opening_stock(balanced) <= 120


def test_history_extension_preserves_opening_stock(spark):
    short_cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        store_count=1,
        seed=41,
    )
    long_cfg = short_cfg.model_copy(update={"end_date": date(2025, 1, 2)})
    short = with_balances(
        _inventory_txns(spark, [-10]),
        40,
        120,
        "ST",
        seeded_draws(short_cfg.seed),
        short_cfg,
    )
    extended = with_balances(
        _inventory_txns(spark, [-10, -1000]),
        40,
        120,
        "ST",
        seeded_draws(long_cfg.seed),
        long_cfg,
    )
    assert _opening_stock(short) == _opening_stock(extended)
