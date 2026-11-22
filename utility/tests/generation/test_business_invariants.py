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
from retail_setup.generation.timezones import (
    localize_by_store,
    localize_receipt_lines,
    with_local_hour,
)


@pytest.fixture(scope="module")
def generated(spark):
    # A full quarter so products launched mid-history (30% of the catalog) are
    # present and the pre-launch enforcement path is genuinely exercised.
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 3, 31),
        store_count=4,
        dc_count=1,
        customer_count=400,
        seed=11,
        transactions_per_store_day=80,
        return_rate=0.05,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    # Eager checkpoints, as the engine checkpoints stage outputs: every check
    # would otherwise regenerate the quarter's sales and returns.
    dims = {
        name: frame.localCheckpoint()
        for name, frame in generate_dimensions(spark, dicts, cfg).items()
    }
    sales = {
        name: frame.localCheckpoint()
        for name, frame in generate_receipts_group(spark, dims, dicts.profile, cfg).items()
    }
    rets = {
        name: frame.localCheckpoint()
        for name, frame in generate_returns(spark, sales, dims, cfg).items()
    }
    # Publish the way the engine does: store-local generation, UTC storage.
    receipts = localize_by_store(
        sales["fact_receipts"].unionByName(rets["fact_receipts"]), dims, ["event_ts"]
    )
    t = {
        "fact_receipts": with_local_hour(receipts, dims),
        "fact_receipt_lines": localize_receipt_lines(
            sales["fact_receipt_lines"].unionByName(rets["fact_receipt_lines"]), dims
        ),
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
    assert "fact_receipts local_hour matches the store time zone" in names
    assert "fact_receipts returns link their sale" in names
    assert "fact_receipts returns post after their sale" in names
    assert "fact_receipts returns refund at most the sale" in names


def test_no_pre_launch_sales_on_generated_data(generated):
    _, dims, sales, _, _ = generated
    launch = dims["dim_products"].select(
        F.col("ID").alias("product_id"), F.to_date("LaunchDate").alias("_ld")
    )
    bad = (
        sales["fact_receipt_lines"]
        .filter(F.col("quantity") > 0)
        .join(launch, "product_id")
        .filter(F.col("event_date") < F.col("_ld"))
    )
    assert bad.count() == 0


def test_sales_within_operating_hours_on_generated_data(generated):
    _, dims, sales, _, _ = generated
    from retail_setup.generation.receipts import _open_close_cols

    o, c = _open_close_cols(F.col("operating_hours"))
    checked = (
        sales["fact_receipts"]
        .select("store_id", F.hour("event_ts").alias("h"))
        .join(
            dims["dim_stores"].select(F.col("ID").alias("store_id"), "operating_hours"), "store_id"
        )
        .withColumn("o", o)
        .withColumn("c", c)
    )
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
    stores = spark.createDataFrame(stores_rows, "ID long, operating_hours string, TimeZone string")
    receipts = with_local_hour(
        spark.createDataFrame(
            receipts_rows, "store_id long, event_ts string, event_date date, receipt_type string"
        )
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("local_hour", F.lit(None).cast("int")),
        {"dim_stores": stores},
    )
    lines = spark.createDataFrame(line_rows, "product_id long, quantity int, event_date date")
    products = spark.createDataFrame(product_rows, "ID long, LaunchDate timestamp")
    return {
        "fact_receipts": receipts,
        "dim_stores": stores,
        "fact_receipt_lines": lines,
        "dim_products": products,
    }


def test_hours_invariant_catches_closed_sale(spark):
    # a store open 07:00-22:00 with a SALE stamped at 03:00 must be flagged
    t = _minimal(
        spark,
        [(1, "2025-01-02 03:00:00", date(2025, 1, 2), "SALE")],
        [(1, "7-22", "UTC")],
        [(1, 1, date(2025, 1, 2))],
        [(1, datetime(2024, 1, 1, 0, 0))],
    )
    r = InvariantReport()
    _run_business_invariants(r, t)
    assert not r.passed
    assert any("operating hours" in f for f in r.failures)


def test_hours_invariant_evaluates_store_local_time(spark):
    # A Los Angeles store open 07:00-22:00. 12:00 UTC is 04:00 local (closed);
    # 03:00 UTC is 19:00 local the previous evening (open). A UTC-hour check
    # would get both wrong.
    def failures(utc_ts: str) -> list[str]:
        t = _minimal(
            spark,
            [(1, utc_ts, date(2025, 1, 2), "SALE")],
            [(1, "7-22", "America/Los_Angeles")],
            [(1, 1, date(2025, 1, 2))],
            [(1, datetime(2024, 1, 1, 0, 0))],
        )
        r = InvariantReport()
        _run_business_invariants(r, t)
        return r.failures

    assert any("operating hours" in f for f in failures("2025-01-02 12:00:00"))
    assert failures("2025-01-02 03:00:00") == []


def test_pre_launch_invariant_catches_early_sale(spark):
    # product launches 2025-06-01 but is sold 2025-01-02 -> flagged
    t = _minimal(
        spark,
        [(1, "2025-01-02 10:00:00", date(2025, 1, 2), "SALE")],
        [(1, "24h", "UTC")],
        [(1, 1, date(2025, 1, 2))],
        [(1, datetime(2025, 6, 1, 0, 0))],
    )
    r = InvariantReport()
    _run_business_invariants(r, t)
    assert not r.passed
    assert any("pre-launch" in f for f in r.failures)


def test_return_invariants_catch_unlinked_or_same_day_returns(spark):
    receipts = spark.createDataFrame(
        [
            ("S1", 1, 7, "2025-01-02 10:00:00", "SALE", 500, None),
            ("R1", 1, 7, "2025-01-02 09:00:00", "RETURN", -100, "S1"),
            ("R2", 1, 8, "2025-01-05 15:00:00", "RETURN", -100, "S1"),
            ("R3", 1, 7, "2025-01-05 15:00:00", "RETURN", -900, "S1"),
        ],
        "receipt_id_ext string, store_id long, customer_id long, event_ts string, "
        "receipt_type string, total_cents long, original_receipt_id_ext string",
    ).withColumn("event_ts", F.to_timestamp("event_ts"))
    from retail_setup.generation.invariants import _return_link_checks

    r = InvariantReport()
    _return_link_checks(r, receipts)
    assert {failure.split(":")[0] for failure in r.failures} == {
        "fact_receipts returns link their sale",
        "fact_receipts returns post after their sale",
        "fact_receipts returns refund at most the sale",
    }
