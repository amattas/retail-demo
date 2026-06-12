from retail_setup.generation.writer import write_table


def test_write_table_parquet_roundtrip(spark, tmp_path):
    df = spark.range(5).withColumnRenamed("id", "ID")
    # format override lets unit tests avoid delta-spark; Fabric uses the default
    write_table(df, table="t_demo", location=str(tmp_path / "t_demo"), fmt="parquet")
    back = spark.read.parquet(str(tmp_path / "t_demo"))
    assert back.count() == 5


def test_write_table_to_catalog_signature():
    import inspect
    from retail_setup.generation.writer import write_to_lakehouse
    params = list(inspect.signature(write_to_lakehouse).parameters)
    assert params == ["df", "lakehouse", "schema", "table"]
