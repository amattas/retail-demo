"""Deterministic seeding + partition grids for the generation engine."""

import hashlib
import math
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig

# Spark's default of 200 shuffle partitions leaves billion-row full-demo shuffles
# (receipt lines, inventory events, BLE pings) at millions of rows per task: long
# spilling sorts and too few tasks to occupy a large pool. Target roughly one
# million rows of the dominant stream per partition; adaptive query execution
# still coalesces the many small shuffles back to its advisory size.
SHUFFLE_ROWS_PER_PARTITION = 1_000_000
MIN_SHUFFLE_PARTITIONS = 200
MAX_SHUFFLE_PARTITIONS = 8192


def shuffle_partitions_for(cfg: GenerationConfig, basket_lambda: float = 12.0) -> int:
    """Shuffle partition count sized to the configured receipt-line volume.

    Rounded up to a multiple of 64 so tasks spread evenly across executors.
    Generation output is independent of the partition count.
    """
    days = (cfg.end_date - cfg.start_date).days + 1
    lines = cfg.store_count * days * cfg.transactions_per_store_day * basket_lambda
    wanted = math.ceil(lines / SHUFFLE_ROWS_PER_PARTITION / 64) * 64
    return max(MIN_SHUFFLE_PARTITIONS, min(MAX_SHUFFLE_PARTITIONS, wanted))


class seeded_draws:
    """Deterministic draw expressions bound to a seed.

    u(cols, salt)        -> uniform [0,1) double
    gauss(cols, salt)    -> N(0,1) (Box-Muller)
    negative_binomial(cols, salt, mean, shape) -> count, variance mean + mean²/shape
    h64(cols, salt)      -> non-negative long hash
    pick_by_weights(cols, salt, [(value, weight), ...]) -> weighted categorical
    """

    _U_MOD = 10**12

    def __init__(self, seed: int):
        self.seed = seed

    def h64(self, cols: list, salt: str):
        from pyspark.sql import functions as F

        return F.pmod(F.xxhash64(*cols, F.lit(f"{salt}|{self.seed}")), F.lit(2**62))

    def u(self, cols: list, salt: str):
        from pyspark.sql import functions as F

        return (self.h64(cols, salt) % F.lit(self._U_MOD)) / F.lit(float(self._U_MOD))

    def gauss(self, cols: list, salt: str):
        from pyspark.sql import functions as F

        radius = F.sqrt(-2.0 * F.log(F.lit(1.0) - self.u(cols, f"{salt}|g1")))
        return radius * F.cos(F.lit(6.283185307179586) * self.u(cols, f"{salt}|g2"))

    def negative_binomial(self, cols: list, salt: str, mean: Column, shape: int = 12) -> Column:
        """Exact sum of geometric draws; integer shape controls overdispersion."""
        if shape < 1:
            raise ValueError("negative-binomial shape must be positive")
        valid_mean = F.when((mean >= 0) & (mean < float("inf")), mean).otherwise(
            F.raise_error("negative-binomial mean must be finite and non-negative")
        )
        probability = valid_mean / (valid_mean + F.lit(float(shape)))
        draws = [
            F.when(valid_mean == 0, F.lit(0))
            .otherwise(
                F.floor(F.log(F.lit(1.0) - self.u(cols, f"{salt}|nb{i}")) / F.log(probability))
            )
            .cast("long")
            for i in range(shape)
        ]
        return sum(draws, F.lit(0).cast("long"))

    def pick_by_weights(self, cols: list, salt: str, weighted: list[tuple[str, float]]):
        from pyspark.sql import functions as F

        total = sum(w for _, w in weighted)
        uu = self.u(cols, salt)
        expr, acc = None, 0.0
        for value, w in weighted[:-1]:
            acc += w / total
            expr = expr.when(uu < acc, value) if expr is not None else F.when(uu < acc, value)
        return expr.otherwise(weighted[-1][0]) if expr is not None else F.lit(weighted[0][0])


def derive_seed(global_seed: int, section: str, key: int, day: date) -> int:
    """Stable 31-bit seed from (global_seed, section, key, day).

    Independent of Spark partitioning/execution order — safe for per-row
    or per-group RNG seeding.
    """
    payload = f"{global_seed}|{section}|{key}|{day.isoformat()}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:4], "big") % (2**31)


def legacy_index(*key_cols):
    """Deterministic long for the legacy __index_level_0__ pandas column.

    The semantic model binds the column by name only; nothing consumes its
    values, so a hash beats a dense row_number — which forced a
    single-partition global sort at full volume.
    """
    from pyspark.sql import functions as F

    return F.pmod(F.xxhash64(*key_cols, F.lit("__legacy_index__")), F.lit(2**62))


def fixed_width_decimal(value: Column, width: int, label: str) -> Column:
    """Zero-pad a non-negative decimal value and fail if it exceeds ``width``."""
    if width < 1:
        raise ValueError("width must be positive")
    text = value.cast("string")
    invalid = (value < F.lit(0)) | (F.length(text) > F.lit(width))
    error = F.raise_error(F.lit(f"{label} exceeds {width} digits"))
    return (
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(invalid, error)
        .otherwise(F.lpad(text, width, "0"))
    )


def minimum_width_decimal(value: Column, width: int) -> Column:
    """Zero-pad to at least ``width`` digits without truncating larger values."""
    return minimum_width_text(value.cast("string"), width)


def minimum_width_text(value: Column, width: int) -> Column:
    """Zero-pad text to at least ``width`` characters without truncation."""
    if width < 1:
        raise ValueError("width must be positive")
    text = value.cast("string")
    return F.when(F.length(text) >= width, text).otherwise(F.lpad(text, width, "0"))


def date_grid(
    spark: SparkSession,
    start: date,
    end: date,
    column: str = "day",
) -> DataFrame:
    """Inclusive Spark-native date range without a driver-materialized row list."""
    if end < start:
        raise ValueError("end must be on or after start")
    days = (end - start).days + 1
    return spark.range(days).select(F.date_add(F.lit(start), F.col("id").cast("int")).alias(column))


def store_day_grid(
    spark: SparkSession,
    store_ids: list[int],
    start: date,
    end: date,
    global_seed: int,
    section: str,
) -> DataFrame:
    """store_id x day cross grid.

    Only the small store-ID set is materialized on the driver. Dates and the
    Cartesian expansion remain Spark-native, so long histories do not create a
    large Python row list. Random draws derive from stable row keys rather than
    partition order.
    """
    del global_seed, section
    store_values = F.array(*[F.lit(int(store_id)).cast("long") for store_id in store_ids])
    stores = spark.range(len(store_ids)).select(
        F.element_at(store_values, F.col("id").cast("int") + 1).alias("store_id")
    )
    return stores.crossJoin(date_grid(spark, start, end))
