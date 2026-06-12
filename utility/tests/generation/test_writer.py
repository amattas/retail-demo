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
    from datetime import date

    from retail_setup.config.generation import GenerationConfig
    from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
    from retail_setup.generation.engine import generate_all
    from retail_setup.generation.gold import generate_gold
    from retail_setup.generation.writer import write_all

    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 11, 3),
                           end_date=date(2025, 11, 4), store_count=2, dc_count=1,
                           customer_count=100, seed=3, transactions_per_store_day=15,
                           online_orders_per_day=8)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    result = generate_all(spark, dicts, cfg)
    gold = generate_gold(spark, result.tables)

    written = write_all(result.tables, gold, cfg, run_id="testrun",
                        base_path=str(tmp_path), fmt="parquet")
    # silver tables under <base>/ag/<table>, gold under <base>/au/<table>
    assert (tmp_path / "ag" / "fact_receipts").exists()
    assert (tmp_path / "au" / "tender_mix_daily").exists()
    assert (tmp_path / "ag" / "dim_date").exists()
    log = spark.read.parquet(str(tmp_path / "ag" / "setup_run_log"))
    assert log.filter("run_id = 'testrun'").count() == len(written)
    cols = set(log.columns)
    assert {"run_id", "store_type", "seed", "start_date", "end_date",
            "table_name", "row_count", "generated_at"} <= cols


def test_write_all_lakehouse_mode_signature():
    import inspect
    from retail_setup.generation.writer import write_all
    params = inspect.signature(write_all).parameters
    assert "lakehouse" in params  # catalog mode for notebooks
