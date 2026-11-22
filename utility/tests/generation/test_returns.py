from datetime import date
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.returns import build_return_headers, generate_returns
import retail_setup.generation.returns as returns_module
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 12, 20),
        end_date=date(2025, 12, 28),
        store_count=3,
        dc_count=1,
        customer_count=300,
        seed=5,
        transactions_per_store_day=60,
        return_rate=0.05,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    # Eager checkpoints keep every assertion from regenerating the sales.
    dims = {name: frame.localCheckpoint() for name, frame in dims.items()}
    sales = {
        name: frame.localCheckpoint()
        for name, frame in generate_receipts_group(spark, dims, dicts.profile, cfg).items()
    }
    rets = {
        name: frame.localCheckpoint()
        for name, frame in generate_returns(spark, sales, dims, cfg).items()
    }
    return cfg, sales, rets, dims


def test_contract_columns(setup):
    _, _, rets, _ = setup
    for t in ["fact_receipts", "fact_receipt_lines", "fact_payments"]:
        assert rets[t].columns == column_names(t)


def test_return_semantics(setup):
    _, sales, rets, _ = setup
    r = rets["fact_receipts"]
    assert r.count() > 0
    assert r.filter(F.col("receipt_type") != "RETURN").count() == 0
    assert r.filter(~F.col("receipt_id_ext").startswith("RET")).count() == 0
    assert r.filter(F.col("total_cents") >= 0).count() == 0  # strictly negative
    # A return keeps the sale's customer, refunds to the sale's tender and links it.
    sold = sales["fact_receipts"].select(
        F.col("receipt_id_ext").alias("original_receipt_id_ext"),
        F.col("customer_id").alias("_customer"),
        F.col("payment_method").alias("_tender"),
        F.col("event_ts").alias("_sold_at"),
        F.col("total_cents").alias("_sold_total"),
    )
    linked = r.join(sold, "original_receipt_id_ext")
    assert linked.count() == r.count()
    assert linked.filter(~F.col("customer_id").eqNullSafe(F.col("_customer"))).count() == 0
    assert r.filter(F.col("customer_id").isNotNull()).count() > 0
    assert linked.filter(F.col("tender_type") != F.col("_tender")).count() == 0
    assert linked.filter(F.col("payment_method") != F.col("_tender")).count() == 0
    assert len({row.tender_type for row in r.select("tender_type").distinct().collect()}) > 1
    assert linked.filter(F.col("event_ts") <= F.col("_sold_at")).count() == 0
    # Partial: never more than the sale, and some returns take only part of it.
    assert linked.filter(-F.col("total_cents") > F.col("_sold_total")).count() == 0
    assert linked.filter(-F.col("total_cents") < F.col("_sold_total")).count() > 0
    # return ids unique and disjoint from sales ids
    n = r.count()
    assert r.select("receipt_id_ext").distinct().count() == n
    overlap = r.join(sales["fact_receipts"], "receipt_id_ext", "inner")
    assert overlap.count() == 0


def test_returns_skip_christmas(setup):
    _, _, rets, _ = setup
    assert rets["fact_receipts"].filter(F.col("event_date") == date(2025, 12, 25)).count() == 0


def test_return_discount_amount_matches_cents(setup):
    _, _, rets, _ = setup
    headers = rets["fact_receipts"]
    assert headers.filter(F.col("discount_cents") != 0).count() > 0
    expected = F.format_string("%.2f", F.col("discount_cents") / F.lit(100.0))
    assert headers.filter(F.col("discount_amount") != expected).count() == 0


def test_return_lines_negative_and_linked(setup):
    _, _, rets, _ = setup
    lines = rets["fact_receipt_lines"]
    assert lines.filter(F.col("quantity") >= 0).count() == 0
    # Lines keep their own promo code; receipt_type marks the return.
    assert lines.filter(F.col("promo_code") == "RETURN").count() == 0
    orphans = lines.join(rets["fact_receipts"], "receipt_id_ext", "left_anti")
    assert orphans.count() == 0


def test_return_rate_and_dec26_spike(setup):
    cfg, sales, rets, _ = setup
    by_day_sales = {
        r["event_date"]: r["count"]
        for r in sales["fact_receipts"].groupBy("event_date").count().collect()
    }
    total_rets = rets["fact_receipts"].count()
    total_rate = total_rets / sum(by_day_sales.values())
    assert 0.02 < total_rate < 0.10  # 5% nominal incl. one 6x day, 10% cap

    # The Dec-26 spike is keyed on the *originating sale* day (returns post
    # 1..N days later, so a by-return-date view would smear the spike). Measure
    # it against the sale day via the return-header helper, which retains the
    # originating sale date.
    hdr = build_return_headers(sales, cfg)
    by_sale_day = {
        r["orig_event_date"]: r["count"] for r in hdr.groupBy("orig_event_date").count().collect()
    }
    dec26 = date(2025, 12, 26)
    other = [by_sale_day.get(d, 0) / by_day_sales[d] for d in by_day_sales if d != dec26]
    assert by_sale_day.get(dec26, 0) / by_day_sales[dec26] > 2 * (sum(other) / len(other))


def test_returns_posted_strictly_after_sale(setup):
    cfg, sales, _, _ = setup
    hdr = build_return_headers(sales, cfg)
    # No return may post on or before its originating sale's day (IMP-010).
    assert hdr.filter(F.col("event_date") <= F.col("orig_event_date")).count() == 0
    assert hdr.filter(F.col("event_date") > F.lit(cfg.end_date)).count() == 0


def test_return_payment_negative_approved(setup):
    _, sales, rets, _ = setup
    pay = rets["fact_payments"]
    assert pay.count() == rets["fact_receipts"].count()
    assert pay.filter(F.col("amount_cents") >= 0).count() == 0
    assert pay.filter(F.col("status") != "APPROVED").count() == 0
    # Transaction IDs derive from purchase IDs, so sales and refunds never collide.
    every = pay.unionByName(sales["fact_payments"])
    assert every.select("transaction_id").distinct().count() == every.count()


def test_returns_fall_inside_store_hours(setup):
    _, _, rets, dims = setup
    hours = {r.ID: r.operating_hours for r in dims["dim_stores"].collect()}
    # local_hour is the hour of the (still store-local) event_ts in the session zone.
    rows = rets["fact_receipts"].collect()
    for row in rows:
        opening = hours[row.store_id]
        open_h, close_h = (0, 24) if opening == "24h" else map(int, opening.split("-"))
        assert open_h <= row.local_hour < close_h, (row.receipt_id_ext, row.local_hour)
    assert len({row.local_hour for row in rows}) > 3


def test_returned_lines_follow_category_return_rates(spark):
    from retail_setup.generation.returns import return_weight, returned_lines_column
    from retail_setup.generation.runtime import seeded_draws

    lines = F.array(
        F.struct(
            F.lit(1).alias("line_num"),
            F.lit(3).alias("quantity"),
            return_weight(F.lit("Apparel")).alias("weight"),
        ),
        F.struct(
            F.lit(2).alias("line_num"),
            F.lit(1).alias("quantity"),
            return_weight(F.lit("Fresh")).alias("weight"),
        ),
    )
    rows = (
        spark.range(4000)
        .select(
            returned_lines_column(seeded_draws(3), F.col("id").cast("string"), lines).alias("r")
        )
        .collect()
    )
    apparel = sum(any(x.line_num == 1 for x in row.r) for row in rows) / len(rows)
    fresh = sum(any(x.line_num == 2 for x in row.r) for row in rows) / len(rows)
    assert all(row.r for row in rows)  # never an empty return
    assert apparel > 0.85 and fresh < 0.3, (apparel, fresh)
    units = [x.quantity for row in rows for x in row.r if x.line_num == 1]
    assert set(units) == {1, 2, 3}


def test_low_volume_returns_accumulate_fractional_daily_rates(spark):
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        store_count=1,
        customer_count=5000,
        seed=31,
        return_rate=0.05,
    )
    rows = [
        (
            f"RCP-LOW-{offset:02d}",
            date(2025, 1, 1 + offset),
            1,
            100,
            8,
            108,
            0,
            "SALE",
        )
        for offset in range(20)
    ]
    sales = {
        "fact_receipts": spark.createDataFrame(
            rows,
            "receipt_id_ext string, event_date date, store_id long, "
            "subtotal_cents long, tax_cents long, total_cents long, "
            "discount_cents long, receipt_type string",
        )
    }

    first = build_return_headers(sales, cfg)
    second = build_return_headers(sales, cfg)

    assert first.count() == 1
    assert (
        first.select("orig_receipt_id_ext").first()[0]
        == second.select("orig_receipt_id_ext").first()[0]
    )


@pytest.mark.parametrize(
    ("sale_date", "end_date", "expected"),
    [
        (date(2025, 12, 11), date(2025, 12, 30), date(2025, 12, 24)),
        (date(2025, 12, 12), date(2025, 12, 30), date(2025, 12, 26)),
        (date(2025, 12, 20), date(2025, 12, 25), date(2025, 12, 24)),
        (date(2025, 12, 24), date(2025, 12, 25), None),
    ],
)
def test_christmas_adjustment_stays_within_purchase_and_history_window(
    spark,
    monkeypatch,
    sale_date,
    end_date,
    expected,
):
    max_delay = min(returns_module.RETURN_WINDOW_DAYS, (end_date - sale_date).days)
    holiday_delay = (date(2025, 12, 25) - sale_date).days
    draw = (holiday_delay - 0.5) / max_delay
    monkeypatch.setattr(
        returns_module,
        "seeded_draws",
        lambda _: SimpleNamespace(u=lambda _columns, _salt: F.lit(draw)),
    )
    cfg = GenerationConfig(
        store_type="grocery",
        start_date=sale_date,
        end_date=end_date,
        store_count=1,
        seed=1,
        return_rate=0.1,
    )
    sales = spark.range(10).select(
        F.concat(F.lit("R"), F.col("id")).alias("receipt_id_ext"),
        F.lit(sale_date).alias("event_date"),
        F.lit(1).cast("long").alias("store_id"),
        F.lit(100).cast("long").alias("subtotal_cents"),
        F.lit(10).cast("long").alias("tax_cents"),
        F.lit(110).cast("long").alias("total_cents"),
        F.lit(0).cast("long").alias("discount_cents"),
        F.lit("SALE").alias("receipt_type"),
    )
    dates = [
        row.event_date for row in build_return_headers({"fact_receipts": sales}, cfg).collect()
    ]
    assert dates == ([] if expected is None else [expected])
    assert all(0 < (value - sale_date).days <= 14 and value <= end_date for value in dates)
