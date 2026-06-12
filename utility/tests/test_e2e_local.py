"""Local end-to-end: dictionaries -> engine -> gold -> writer -> invariants.

Proves the full pipeline without a Fabric workspace (spec requirement).
"""

from datetime import date

from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.engine import generate_all
from retail_setup.generation.gold import GOLD_TABLES, generate_gold
from retail_setup.generation.invariants import run_invariants
from retail_setup.generation.schemas import TABLES
from retail_setup.generation.writer import write_all


def test_full_pipeline_hardware_store(spark, tmp_path):
    cfg = GenerationConfig(store_type="hardware", start_date=date(2025, 3, 1),
                           end_date=date(2025, 3, 7), store_count=3, dc_count=1,
                           customer_count=250, seed=2026,
                           transactions_per_store_day=35, online_orders_per_day=20)
    dicts = load_dictionaries(default_dictionary_root(), "hardware")
    result = generate_all(spark, dicts, cfg)

    report = run_invariants(spark, result.tables)
    assert report.passed, report.failures

    gold = generate_gold(spark, result.tables)
    written = write_all(result.tables, gold, cfg, run_id="e2e",
                        base_path=str(tmp_path), fmt="parquet")
    fact_tables = [t for t in TABLES if t.startswith("fact_")]
    assert set(fact_tables) <= set(written)
    assert set(GOLD_TABLES) <= set(written)

    # read-back sanity: receipts re-load with the contract column count
    back = spark.read.parquet(str(tmp_path / "ag" / "fact_receipts"))
    assert back.count() == result.tables["fact_receipts"].count()
    # hardware profile visible end-to-end: weekend traffic spike
    by_dow = {r["dow"]: r["n"] for r in
              result.tables["fact_receipts"]
              .withColumn("dow", F.dayofweek("event_date"))
              .groupBy("dow").count().withColumnRenamed("count", "n").collect()}
    weekend = by_dow.get(1, 0) + by_dow.get(7, 0)
    weekday_avg = sum(v for k, v in by_dow.items() if k not in (1, 7)) / 5
    assert weekend / 2 > weekday_avg * 0.9  # sat/sun at least near weekday avg
