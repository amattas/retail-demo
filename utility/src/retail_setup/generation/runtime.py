"""Deterministic seeding + partition grids for the generation engine."""

import hashlib
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession


class seeded_draws:
    """Deterministic draw expressions bound to a seed.

    u(cols, salt)        -> uniform [0,1) double
    gauss(cols, salt)    -> ~N(0,1) (Irwin-Hall of 3 uniforms, scaled)
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

        s = self.u(cols, f"{salt}|g1") + self.u(cols, f"{salt}|g2") + self.u(cols, f"{salt}|g3")
        return (s - F.lit(1.5)) * F.lit(2.0)

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


def store_day_grid(
    spark: SparkSession,
    store_ids: list[int],
    start: date,
    end: date,
    global_seed: int,
    section: str,
) -> DataFrame:
    """store_id x day cross grid.

    Rows are computed driver-side so the engine never depends on F.rand()'s
    partition-arrangement semantics for keys. Fine for realistic grids
    (hundreds of stores x a year ~ 10^5 rows collected on the driver).

    Spark-native generators derive their own draws via seeded_draws/xxhash64
    and do not need a driver-side seed column.
    """
    day_list = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    rows = [
        (s, d)
        for s in store_ids
        for d in day_list
    ]
    # the rows ARE the full grid — no crossJoin/join round-trip needed
    return spark.createDataFrame(rows, "store_id long, day date")
