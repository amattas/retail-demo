def test_spark_session_works(spark):
    df = spark.range(3)
    assert df.count() == 3


def test_spark_session_is_session_scoped(spark):
    # same JVM across tests — cheap sanity check that the fixture is reused
    assert spark.sparkContext.appName == "retail-setup-tests"
