"""Spark helpers for the live stream: bounded catch-up and existing-ID sampling."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def cap_backlog(
    frame: DataFrame, max_lag_seconds: int, timestamp_col: str = "timestamp"
) -> DataFrame:
    """Drop source rows more than ``max_lag_seconds`` older than the micro-batch time.

    After a stop, Spark's ``rate`` source emits every row for the downtime in
    the next micro-batch. This filter keeps only the newest ``max_lag_seconds``
    of them, so a resumed stream continues from about now. In a streaming
    query ``current_timestamp()`` is the batch timestamp Spark stores in the
    offset log, so a replayed batch keeps exactly the same rows.
    """

    if int(max_lag_seconds) < 1:
        raise ValueError("max_lag_seconds must be at least 1")
    floor = F.current_timestamp() - F.expr(f"INTERVAL {int(max_lag_seconds)} SECONDS")
    return frame.where(F.col(timestamp_col) >= floor)


def dense_id_lookup(dimension: DataFrame, id_col: str = "ID") -> tuple[DataFrame, int]:
    """Number a dimension's distinct, non-null IDs 0..n-1 and return (lookup, n).

    The lookup has ``_idx`` and ``_id`` columns. It is persisted because the
    stream joins it on every micro-batch. Gaps in the IDs (a custom layout or a
    filtered publish) are fine: rows only ever receive an ID that exists.
    """

    lookup = (
        dimension.select(F.col(id_col).cast("long").alias("_id"))
        .where(F.col("_id").isNotNull())
        .distinct()
        .withColumn("_idx", (F.row_number().over(Window.orderBy("_id")) - 1).cast("long"))
        .persist()
    )
    return lookup, lookup.count()


def attach_sampled_id(
    frame: DataFrame,
    key: Column,
    salt: str,
    seed: int,
    lookup: DataFrame,
    count: int,
    output: str,
) -> DataFrame:
    """Add ``output``: an existing ID picked deterministically per row from ``lookup``.

    The pick is ``xxhash64(key, "salt|seed") mod count``, the same draw the
    stream used for contiguous ``1..count`` ranges, so a dimension with IDs
    1..n gets exactly the IDs it got before.
    """

    if int(count) < 1:
        raise ValueError(f"cannot sample {output}: the ID lookup is empty")
    index = f"_{output}_idx"
    choice = lookup.select(F.col("_idx").alias(index), F.col("_id").alias(output))
    return (
        frame.withColumn(index, F.pmod(F.xxhash64(key, F.lit(f"{salt}|{seed}")), F.lit(int(count))))
        .join(F.broadcast(choice), index, "inner")
        .drop(index)
    )
