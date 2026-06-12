from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.returns import generate_returns
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 12, 20),
                           end_date=date(2025, 12, 28), store_count=3, dc_count=1,
                           customer_count=300, seed=5, transactions_per_store_day=60,
                           return_rate=0.05)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    rets = generate_returns(spark, sales, dims, cfg)
    return cfg, sales, rets


def test_contract_columns(setup):
    _, _, rets = setup
    for t in ["fact_receipts", "fact_receipt_lines", "fact_payments"]:
        assert rets[t].columns == column_names(t)


def test_return_semantics(setup):
    _, sales, rets = setup
    r = rets["fact_receipts"]
    assert r.count() > 0
    assert r.filter(F.col("receipt_type") != "RETURN").count() == 0
    assert r.filter(~F.col("receipt_id_ext").startswith("RET")).count() == 0
    assert r.filter(F.col("total_cents") >= 0).count() == 0  # strictly negative
    assert r.filter(F.col("customer_id").isNotNull()).count() == 0
    assert r.filter(F.col("tender_type") != "CREDIT_CARD").count() == 0
    # return ids unique and disjoint from sales ids
    n = r.count()
    assert r.select("receipt_id_ext").distinct().count() == n
    overlap = r.join(sales["fact_receipts"], "receipt_id_ext", "inner")
    assert overlap.count() == 0


def test_return_lines_negative_and_linked(setup):
    _, _, rets = setup
    lines = rets["fact_receipt_lines"]
    assert lines.filter(F.col("quantity") >= 0).count() == 0
    assert lines.filter(F.col("promo_code") != "RETURN").count() == 0
    orphans = lines.join(rets["fact_receipts"], "receipt_id_ext", "left_anti")
    assert orphans.count() == 0


def test_return_rate_and_dec26_spike(setup):
    cfg, sales, rets = setup
    by_day_sales = {r["event_date"]: r["count"] for r in
                    sales["fact_receipts"].groupBy("event_date").count().collect()}
    by_day_rets = {r["event_date"]: r["count"] for r in
                   rets["fact_receipts"].groupBy("event_date").count().collect()}
    total_rate = sum(by_day_rets.values()) / sum(by_day_sales.values())
    assert 0.02 < total_rate < 0.10  # 5% nominal incl. one 6x day, 10% cap
    dec26 = date(2025, 12, 26)
    other = [by_day_rets.get(d, 0) / by_day_sales[d] for d in by_day_sales if d != dec26]
    assert by_day_rets.get(dec26, 0) / by_day_sales[dec26] > 2 * (sum(other) / len(other))


def test_return_payment_negative_approved(setup):
    _, _, rets = setup
    pay = rets["fact_payments"]
    assert pay.count() == rets["fact_receipts"].count()
    assert pay.filter(F.col("amount_cents") >= 0).count() == 0
    assert pay.filter(F.col("status") != "APPROVED").count() == 0
