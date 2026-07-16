from datetime import date

import pytest

from retail_setup.generation.writer import write_table


def test_write_table_parquet_roundtrip(spark, tmp_path):
    df = spark.range(5).withColumnRenamed("id", "ID")
    # format override lets unit tests avoid delta-spark; Fabric uses the default
    write_table(df, location=str(tmp_path / "t_demo"), fmt="parquet")
    back = spark.read.parquet(str(tmp_path / "t_demo"))
    assert back.count() == 5


def test_write_table_to_catalog_signature():
    import inspect
    from retail_setup.generation.writer import write_to_lakehouse

    params = list(inspect.signature(write_to_lakehouse).parameters)
    assert params == ["df", "lakehouse", "schema", "table"]


def test_write_all_writes_everything_and_run_log(spark, tmp_path):
    from retail_setup.config.generation import GenerationConfig
    from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
    from retail_setup.generation.engine import generate_all
    from retail_setup.generation.gold import generate_gold
    from retail_setup.generation.writer import write_all

    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 11, 3),
        end_date=date(2025, 11, 4),
        store_count=2,
        dc_count=1,
        customer_count=100,
        seed=3,
        transactions_per_store_day=15,
        online_orders_per_day=8,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    result = generate_all(spark, dicts, cfg)
    gold = generate_gold(spark, result.tables)

    written = write_all(
        result.tables, gold, cfg, run_id="testrun", base_path=str(tmp_path), fmt="parquet"
    )
    # silver tables under <base>/ag/<table>, gold under <base>/au/<table>
    assert (tmp_path / "ag" / "fact_receipts").exists()
    assert (tmp_path / "au" / "tender_mix_daily").exists()
    assert (tmp_path / "ag" / "dim_date").exists()
    log = spark.read.parquet(str(tmp_path / "ag" / "setup_run_log"))
    completed = log.filter("run_id = 'testrun' AND status = 'COMPLETED'")
    assert completed.filter("table_name != '__run__'").count() == len(written)
    assert completed.filter("table_name = '__run__'").count() == 1
    assert log.filter("run_id = 'testrun' AND status = 'STARTED'").count() == 1
    cols = set(log.columns)
    assert {
        "run_id",
        "store_type",
        "seed",
        "start_date",
        "end_date",
        "table_name",
        "row_count",
        "status",
        "error",
        "generated_at",
    } <= cols


def test_write_all_appends_runs_and_rejects_duplicate_run_id(spark, tmp_path):
    from retail_setup.config.generation import GenerationConfig
    from retail_setup.generation.writer import write_all

    cfg = GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 11, 3),
        end_date=date(2025, 11, 3),
        store_count=1,
        seed=7,
    )
    tables = {"example": spark.range(2)}
    base_path = str(tmp_path)

    write_all(tables, {}, cfg, run_id="run-a", base_path=base_path, fmt="parquet")
    write_all(tables, {}, cfg, run_id="run-b", base_path=base_path, fmt="parquet")

    log = spark.read.parquet(str(tmp_path / "ag" / "setup_run_log"))
    assert set(row.run_id for row in log.select("run_id").distinct().collect()) == {
        "run-a",
        "run-b",
    }

    with pytest.raises(ValueError, match="run_id already exists"):
        write_all(
            tables,
            {},
            cfg,
            run_id="run-a",
            base_path=base_path,
            fmt="parquet",
        )


def test_write_all_lakehouse_mode_signature():
    import inspect
    from retail_setup.generation.writer import write_all

    params = inspect.signature(write_all).parameters
    assert "lakehouse" in params  # catalog mode for notebooks
