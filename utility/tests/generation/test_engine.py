from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.engine import generate_all
from retail_setup.generation.invariants import run_invariants
from retail_setup.generation.schemas import TABLES, column_names


@pytest.fixture(scope="module")
def result(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 9, 1),
                           end_date=date(2025, 9, 7), store_count=2, dc_count=1,
                           customer_count=200, seed=99, transactions_per_store_day=30,
                           online_orders_per_day=20, return_rate=0.05)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    return cfg, generate_all(spark, dicts, cfg)


def test_all_contract_tables_present(result):
    _, out = result
    fact_tables = [t for t in TABLES if t.startswith("fact_")]
    for t in fact_tables:
        assert t in out.tables, t
        assert out.tables[t].columns == column_names(t), t
    for t in ["dim_stores", "dim_products", "dim_date"]:
        assert t in out.tables


def test_unions_applied(result):
    _, out = result
    receipts = out.tables["fact_receipts"]
    assert receipts.filter("receipt_type = 'RETURN'").count() > 0
    assert receipts.filter("receipt_type = 'SALE'").count() > 0
    pay = out.tables["fact_payments"]
    assert pay.filter(F.col("order_id_ext").isNotNull()).count() > 0   # online
    assert pay.filter(F.col("receipt_id_ext").isNotNull()).count() > 0  # in-store + returns
    both = pay.filter(F.col("order_id_ext").isNotNull() & F.col("receipt_id_ext").isNotNull())
    assert both.count() == 0


def test_invariants_pass_and_report(result, spark):
    _, out = result
    report = run_invariants(spark, out.tables)
    assert report.passed, report.failures
    assert report.row_counts["fact_receipts"] > 0
    assert len(report.checks) >= 10


def test_invariants_catch_violations(result, spark):
    _, out = result
    broken = dict(out.tables)
    broken["fact_receipt_lines"] = out.tables["fact_receipt_lines"].withColumn(
        "receipt_id_ext", F.lit("RCPBOGUS"))
    report = run_invariants(spark, broken)
    assert not report.passed
    assert any("fact_receipt_lines" in f for f in report.failures)
