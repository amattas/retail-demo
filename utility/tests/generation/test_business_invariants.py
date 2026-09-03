"""IMP-010 adversarial tests: shared business invariants must hold on generated
data and must actively flag deliberately corrupted inputs (no vacuous passes)."""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.invariants import InvariantReport, _run_business_invariants
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.returns import build_return_headers, generate_returns


@pytest.fixture(scope="module")
def generated(spark):
    # A full quarter so products launched mid-history (30% of the catalog) are
    # present and the pre-launch enforcement path is genuinely exercised.
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 1, 1),
                           end_date=date(2025, 3, 31), store_count=4, dc_count=1,
                           customer_count=400, seed=11, transactions_per_store_day=80,
                           return_rate=0.05)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    rets = generate_returns(spark, sales, dims, cfg)
    t = {
        "fact_receipts": sales["fact_receipts"].unionByName(rets["fact_receipts"]),
        "fact_receipt_lines": sales["fact_receipt_lines"].unionByName(
            rets["fact_receipt_lines"]),
        "dim_stores": dims["dim_stores"],
        "dim_products": dims["dim_products"],
    }
    return cfg, dims, sales, rets, t


def test_business_invariants_pass_on_generated_data(generated):
    _, _, _, _, t = generated
    r = InvariantReport()
    _run_business_invariants(r, t)
    assert r.passed, r.failures
    # every business check must have actually run
    names = set(r.checks)
    assert "fact_receipts within store operating hours" in names
    assert "fact_receipt_lines no pre-launch sales" in names
    assert "fact_receipts no return before first sale" in names


def test_no_pre_launch_sales_on_generated_data(generated):
    _, dims, sales, _, _ = generated
    launch = dims["dim_products"].select(
        F.col("ID").alias("product_id"), F.to_date("LaunchDate").alias("_ld"))
    bad = (sales["fact_receipt_lines"]
           .filter(F.col("quantity") > 0)
           .join(launch, "product_id")
           .filter(F.col("event_date") < F.col("_ld")))
    assert bad.count() == 0


def test_sales_within_operating_hours_on_generated_data(generated):
    _, dims, sales, _, _ = generated
    from retail_setup.generation.receipts import _open_close_cols
    o, c = _open_close_cols(F.col("operating_hours"))
    checked = (sales["fact_receipts"]
               .select("store_id", F.hour("event_ts").alias("h"))
               .join(dims["dim_stores"].select(
                   F.col("ID").alias("store_id"), "operating_hours"), "store_id")
               .withColumn("o", o).withColumn("c", c))
    assert checked.filter((F.col("h") < F.col("o")) | (F.col("h") >= F.col("c"))).count() == 0


def test_returns_after_sale_on_generated_data(generated):
    cfg, _, sales, _, _ = generated
    hdr = build_return_headers(sales, cfg)
    assert hdr.count() > 0
    assert hdr.filter(F.col("event_date") <= F.col("orig_event_date")).count() == 0


def _minimal(spark, receipts_rows, stores_rows, line_rows, product_rows):
    # Build event_ts from a string via to_timestamp (as the generator does) so
    # hour() is evaluated in the session timezone, not shifted by the driver's
    # local zone the way a naive python datetime would be.
    receipts = spark.createDataFrame(receipts_rows,
        "store_id long, event_ts string, event_date date, receipt_type string") \
        .withColumn("event_ts", F.to_timestamp("event_ts"))
    stores = spark.createDataFrame(stores_rows, "ID long, operating_hours string")
    lines = spark.createDataFrame(line_rows,
        "product_id long, quantity int, event_date date")
    products = spark.createDataFrame(product_rows, "ID long, LaunchDate timestamp")
    return {"fact_receipts": receipts, "dim_stores": stores,
            "fact_receipt_lines": lines, "dim_products": products}


def test_hours_invariant_catches_closed_sale(spark):
    # a store open 07:00-22:00 with a SALE stamped at 03:00 must be flagged
    t = _minimal(
        spark,
        [(1, "2025-01-02 03:00:00", date(2025, 1, 2), "SALE")],
        [(1, "7-22")],
        [(1, 1, date(2025, 1, 2))],
        [(1, datetime(2024, 1, 1, 0, 0))],
    )
    r = InvariantReport()
    _run_business_invariants(r, t)
    assert not r.passed
    assert any("operating hours" in f for f in r.failures)


def test_pre_launch_invariant_catches_early_sale(spark):
    # product launches 2025-06-01 but is sold 2025-01-02 -> flagged
    t = _minimal(
        spark,
        [(1, "2025-01-02 10:00:00", date(2025, 1, 2), "SALE")],
        [(1, "24h")],
        [(1, 1, date(2025, 1, 2))],
        [(1, datetime(2025, 6, 1, 0, 0))],
    )
    r = InvariantReport()
    _run_business_invariants(r, t)
    assert not r.passed
    assert any("pre-launch" in f for f in r.failures)
