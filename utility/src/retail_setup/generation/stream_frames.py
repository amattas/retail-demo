"""Spark helpers for the live stream that keep catch-up bounded."""

from pyspark.sql import DataFrame
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
