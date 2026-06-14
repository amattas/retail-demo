from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.inventory import generate_inventory_chain
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.returns import generate_returns
from retail_setup.generation.schemas import column_names

TABLES = ["fact_store_inventory_txn", "fact_dc_inventory_txn", "fact_truck_moves",
          "fact_truck_inventory", "fact_reorders", "fact_stockouts"]


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 8, 4),
                           end_date=date(2025, 8, 10), store_count=2, dc_count=1,
                           customer_count=200, seed=23, transactions_per_store_day=40)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    rets = generate_returns(spark, sales, dims, cfg)
    inv = generate_inventory_chain(spark, sales, rets, dims, cfg)
    return cfg, dims, sales, inv


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


def test_truck_lifecycle(setup):
    *_, inv = setup
    moves = inv["fact_truck_moves"]
    if moves.count() == 0:
        pytest.skip("no shipments triggered in this window")
    per = moves.groupBy("shipment_id").agg(
        F.collect_set("status").alias("statuses"), F.count("*").alias("n"))
    for r in per.collect():
        assert set(r.statuses) == {"SCHEDULED", "LOADING", "IN_TRANSIT", "ARRIVED",
                                   "UNLOADING", "COMPLETED"}
        assert r.n == 6
    done = moves.filter("status = 'COMPLETED'")
    assert done.filter(F.col("actual_unload_duration") < 30).count() == 0
    # load/unload pairs exist per shipment product
    ti = inv["fact_truck_inventory"]
    loads = ti.filter("action = 'LOAD'").groupBy("shipment_id", "product_id").count()
    unloads = ti.filter("action = 'UNLOAD'").groupBy("shipment_id", "product_id").count()
    assert loads.join(unloads, ["shipment_id", "product_id"], "left_anti").count() == 0


def test_reorder_priorities(setup):
    *_, inv = setup
    ro = inv["fact_reorders"]
    if ro.count() == 0:
        pytest.skip("no reorders in window")
    assert {r.priority for r in ro.select("priority").distinct().collect()} <= \
        {"NORMAL", "HIGH", "URGENT"}
    assert ro.filter((F.col("reorder_quantity") < 50) | (F.col("reorder_quantity") > 200)).count() == 0
    assert ro.filter((F.col("reorder_point") < 5) | (F.col("reorder_point") > 20)).count() == 0


def test_stockouts_mutually_exclusive(setup):
    *_, inv = setup
    so = inv["fact_stockouts"]
    both = so.filter(F.col("StoreID").isNotNull() & F.col("DCID").isNotNull())
    neither = so.filter(F.col("StoreID").isNull() & F.col("DCID").isNull())
    assert both.count() == 0 and neither.count() == 0
