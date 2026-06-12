from datetime import date

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def cfg():
    return GenerationConfig(
        store_type="grocery", start_date=date(2025, 3, 3), end_date=date(2025, 3, 9),
        store_count=3, dc_count=1, customer_count=300, seed=7,
        transactions_per_store_day=40,
    )


@pytest.fixture(scope="module")
def dicts():
    return load_dictionaries(default_dictionary_root(), "grocery")


@pytest.fixture(scope="module")
def group(spark, cfg, dicts):
    dims = generate_dimensions(spark, dicts, cfg)
    return generate_receipts_group(spark, dims, dicts.profile, cfg)


def test_contract_columns(group):
    for t in ["fact_receipts", "fact_receipt_lines", "fact_payments"]:
        assert group[t].columns == column_names(t), t


def test_receipt_ids_unique_and_formatted(group):
    df = group["fact_receipts"]
    n = df.count()
    assert n > 0
    assert df.select("receipt_id_ext").distinct().count() == n
    r = df.first()
    assert r.receipt_id_ext.startswith("RCP") and len(r.receipt_id_ext) == 25


def test_event_fields_populated(group):
    from pyspark.sql import functions as F
    for t in ["fact_receipts", "fact_receipt_lines", "fact_payments"]:
        df = group[t]
        assert df.filter(F.col("event_ts").isNull() | F.col("event_date").isNull()).count() == 0, t


def test_lines_fk_and_math(group, spark):
    from pyspark.sql import functions as F
    lines, receipts = group["fact_receipt_lines"], group["fact_receipts"]
    # every line belongs to a receipt
    orphans = lines.join(receipts, "receipt_id_ext", "left_anti")
    assert orphans.count() == 0
    # ext_cents = unit_cents*quantity - discount  (>= 0)
    bad = lines.filter(F.col("ext_cents") > F.col("unit_cents") * F.col("quantity"))
    assert bad.count() == 0
    assert lines.filter(F.col("ext_cents") < 0).count() == 0
    # header subtotal equals sum of line ext_cents
    sums = lines.groupBy("receipt_id_ext").agg(F.sum("ext_cents").alias("line_sum"))
    joined = receipts.join(sums, "receipt_id_ext")
    mismatch = joined.filter(F.col("subtotal_cents") != F.col("line_sum"))
    assert mismatch.count() == 0
    # total = subtotal + tax (discounts already applied at line level)
    bad_total = receipts.filter(
        F.col("total_cents") != F.col("subtotal_cents") + F.col("tax_cents"))
    assert bad_total.count() == 0


def test_payments_one_per_receipt(group):
    from pyspark.sql import functions as F
    pay, receipts = group["fact_payments"], group["fact_receipts"]
    assert pay.count() == receipts.count()
    assert pay.filter(F.col("order_id_ext").isNotNull()).count() == 0  # in-store only here
    joined = pay.join(receipts.select("receipt_id_ext", "total_cents"), "receipt_id_ext")
    assert joined.filter(F.col("amount_cents") != F.col("total_cents")).count() == 0
    cash_declines = pay.filter((F.col("payment_method") == "CASH") &
                               (F.col("status") == "DECLINED"))
    assert cash_declines.count() == 0
    declined = pay.filter(F.col("status") == "DECLINED")
    assert declined.filter(F.col("decline_reason").isNull()).count() == 0


def test_tender_mix_roughly_matches(group):
    pay = group["fact_payments"]
    n = pay.count()
    cc = pay.filter("payment_method = 'CREDIT_CARD'").count()
    assert 0.25 < cc / n < 0.55  # 40% nominal, loose bounds for small n


def test_volume_in_expected_range(group, cfg):
    n_days, n_stores = 7, 3
    expected = cfg.transactions_per_store_day * n_days * n_stores
    actual = group["fact_receipts"].count()
    assert 0.4 * expected < actual < 2.0 * expected  # weights+multipliers move it


def test_determinism(spark, cfg, dicts):
    dims = generate_dimensions(spark, dicts, cfg)
    a = generate_receipts_group(spark, dims, dicts.profile, cfg)
    b = generate_receipts_group(spark, dims, dicts.profile, cfg)
    assert sorted(r.receipt_id_ext for r in a["fact_receipts"].collect()) == \
           sorted(r.receipt_id_ext for r in b["fact_receipts"].collect())


def test_different_seeds_differ(spark, dicts):
    def gen(seed):
        cfg = GenerationConfig(
            store_type="grocery", start_date=date(2025, 3, 3),
            end_date=date(2025, 3, 4), store_count=2, dc_count=1,
            customer_count=100, seed=seed, transactions_per_store_day=20,
        )
        dims = generate_dimensions(spark, dicts, cfg)
        g = generate_receipts_group(spark, dims, dicts.profile, cfg)
        return {r.receipt_id_ext for r in g["fact_receipts"].collect()}

    assert gen(7) != gen(8)


def test_unknown_department_raises(spark, cfg, dicts):
    dims = generate_dimensions(spark, dicts, cfg)
    bad_profile = dicts.profile.model_copy(
        update={"department_weights": {**dicts.profile.department_weights, "Bogus": 0.1}}
    )
    with pytest.raises(ValueError, match="Bogus"):
        generate_receipts_group(spark, dims, bad_profile, cfg)


def test_subtotal_mirrors_subtotal_amount(group):
    from pyspark.sql import functions as F
    receipts = group["fact_receipts"]
    assert receipts.filter(F.col("Subtotal") != F.col("subtotal_amount")).count() == 0
