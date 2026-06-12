"""Deterministic seeding + partition grids for the generation engine."""

import hashlib
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def derive_seed(global_seed: int, section: str, key: int, day: date) -> int:
    """Stable 31-bit seed from (global_seed, section, key, day).

    Independent of Spark partitioning/execution order — safe for per-row
    or per-group RNG seeding.
    """
    payload = f"{global_seed}|{section}|{key}|{day.isoformat()}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:4], "big") % (2**31)


def store_day_grid(
    spark: SparkSession,
    store_ids: list[int],
    start: date,
    end: date,
    global_seed: int,
    section: str,
) -> DataFrame:
    """store_id x day cross grid with a precomputed per-partition seed column.

    Seeds are computed driver-side so the engine never depends on F.rand()'s
    partition-arrangement semantics for keys. Fine for realistic grids
    (hundreds of stores x a year ~ 10^5 rows collected on the driver); if a
    grid ever grows past ~10^6, switch the seed column to F.xxhash64 and keep
    derive_seed as the reference with a parity test.
    """
    days = spark.sql(
        f"SELECT explode(sequence(to_date('{start.isoformat()}'), "
        f"to_date('{end.isoformat()}'), interval 1 day)) AS day"
    )
    stores = spark.createDataFrame([(s,) for s in store_ids], "store_id long")
    grid = stores.crossJoin(days)
    seed_udf_rows = [
        (s, d, derive_seed(global_seed, section, s, d))
        for s in store_ids
        for d in [r.day for r in days.collect()]
    ]
    seeds = spark.createDataFrame(seed_udf_rows, "store_id long, day date, partition_seed long")
    return grid.join(seeds, ["store_id", "day"])
