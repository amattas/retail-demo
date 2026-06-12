from datetime import date

from retail_setup.generation.runtime import derive_seed, store_day_grid


def test_derive_seed_deterministic_and_distinct():
    a = derive_seed(42, "receipts", 7, date(2025, 3, 1))
    b = derive_seed(42, "receipts", 7, date(2025, 3, 1))
    c = derive_seed(42, "receipts", 8, date(2025, 3, 1))
    d = derive_seed(43, "receipts", 7, date(2025, 3, 1))
    assert a == b
    assert len({a, c, d}) == 3
    assert 0 <= a < 2**31  # numpy-seedable


def test_store_day_grid(spark):
    grid = store_day_grid(
        spark,
        store_ids=[1, 2],
        start=date(2025, 1, 1),
        end=date(2025, 1, 3),
        global_seed=42,
        section="receipts",
    )
    rows = grid.collect()
    assert len(rows) == 6  # 2 stores x 3 days
    cols = set(grid.columns)
    assert {"store_id", "day", "partition_seed"} <= cols
    seeds = {(r.store_id, str(r.day)): r.partition_seed for r in rows}
    assert len(set(seeds.values())) == 6  # all distinct


def test_seeded_draws_uniform_properties(spark):
    from pyspark.sql import functions as F
    from retail_setup.generation.runtime import seeded_draws

    d = seeded_draws(seed=42)
    df = spark.range(2000).withColumn("u", d.u(["id"], "test"))
    row = df.agg(F.min("u"), F.max("u"), F.avg("u")).first()
    assert 0.0 <= row[0] and row[1] < 1.0
    assert 0.4 < row[2] < 0.6
    # different salt -> different values; same salt -> identical
    df2 = df.withColumn("u2", d.u(["id"], "other")).withColumn("u3", d.u(["id"], "test"))
    assert df2.filter("u = u3").count() == 2000
    assert df2.filter("u = u2").count() < 100


def test_seeded_draws_seed_sensitivity(spark):
    from retail_setup.generation.runtime import seeded_draws

    a = seeded_draws(seed=1)
    b = seeded_draws(seed=2)
    df = spark.range(100)
    da = [r[0] for r in df.select(a.u(["id"], "s")).collect()]
    db = [r[0] for r in df.select(b.u(["id"], "s")).collect()]
    assert da != db


def test_pick_by_weights(spark):
    from retail_setup.generation.runtime import seeded_draws

    d = seeded_draws(seed=42)
    df = spark.range(5000).withColumn(
        "pick", d.pick_by_weights(["id"], "p", [("A", 0.7), ("B", 0.2), ("C", 0.1)])
    )
    counts = {r["pick"]: r["count"] for r in df.groupBy("pick").count().collect()}
    assert 0.6 < counts["A"] / 5000 < 0.8
    assert set(counts) == {"A", "B", "C"}
