"""Balance + stockout helpers for the inventory chain (Plan 2b Task 9).

Split out of ``inventory.py`` per the plan's ~400-line guidance. Covers
stages 7-8: day-0 INITIAL seed txns plus the running-balance window, and the
balance-crossing stockout extraction. Shared draw/column primitives used by
both modules live here to keep the import direction one-way
(``inventory`` -> ``inventory_balances``).
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.runtime import seeded_draws

# Canonical column layout for raw (pre-balance) inventory txn streams.
TXN_COLS = ["node_id", "product_id", "quantity", "txn_type", "source",
            "event_ts", "event_date", "trace_id"]


def draw_int(u: Column, lo: int, hi: int) -> Column:
    """Uniform integer draw in [lo, hi] from a [0,1) uniform column."""
    return (F.lit(lo) + F.floor(u * F.lit(hi - lo + 1))).cast("long")


# ---------------------------------------------------------------------------
# Stage 7: initial-stock seeds + running balances
# ---------------------------------------------------------------------------

def with_balances(txns: DataFrame, lo: int, hi: int, tag: str,
                  d: seeded_draws, cfg: GenerationConfig) -> DataFrame:
    """Fold a day-0 INITIAL seed txn per (node, product) into the stream and
    compute the running balance ordered by (event_ts, trace_id). Negative
    balances are not clamped — they become stockout signals."""
    seeds = (txns.select("node_id", "product_id").distinct()
             .withColumn("quantity",
                         draw_int(d.u(["node_id", "product_id"],
                                      f"seed-stock-{tag}"), lo, hi))
             .withColumn("txn_type", F.lit("INITIAL"))
             .withColumn("source", F.lit("SEED"))
             # String-built timestamp (session-TZ semantics) to match every
             # other event_ts in the chain; a naive Python datetime literal
             # would shift with the driver's local timezone.
             .withColumn("event_ts", F.to_timestamp(
                 F.lit(f"{cfg.start_date.isoformat()} 00:00:00")))
             .withColumn("event_date", F.lit(cfg.start_date).cast("date"))
             .withColumn("trace_id", F.concat(
                 F.lit(f"TRC-INIT-{tag}-"), F.col("node_id").cast("string"),
                 F.lit("-"), F.col("product_id").cast("string")))
             .select(*TXN_COLS))
    run_w = (Window.partitionBy("node_id", "product_id")
             .orderBy("event_ts", "trace_id")
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    return (txns.unionByName(seeds)
            .withColumn("balance", F.sum("quantity").over(run_w).cast("long")))


# ---------------------------------------------------------------------------
# Stage 8: stockouts
# ---------------------------------------------------------------------------

def stockouts(balanced: DataFrame, tag: str, node_as: str) -> DataFrame:
    """Balance crossings to <=0 (previous balance > 0); deduped to one per
    (node, product, day). ``node_as`` is 'StoreID' or 'DCID' — the other
    contract column stays NULL (double, per the TMDL contract)."""
    order_w = Window.partitionBy("node_id", "product_id").orderBy(
        "event_ts", "trace_id")
    day_w = Window.partitionBy("node_id", "product_id", "event_date").orderBy(
        "event_ts", "trace_id")
    other = "DCID" if node_as == "StoreID" else "StoreID"
    return (balanced
            .withColumn("_prev", F.lag("balance").over(order_w))
            .filter((F.col("balance") <= 0) & (F.col("_prev") > 0))
            .withColumn("_dup", F.row_number().over(day_w))
            .filter(F.col("_dup") == 1)
            .select(
                "event_ts",
                F.concat(F.lit(f"TRC-SO-{tag}-"),
                         F.col("node_id").cast("string"), F.lit("-"),
                         F.col("product_id").cast("string"), F.lit("-"),
                         F.date_format("event_date", "yyyyMMdd"))
                .alias("trace_id"),
                F.col("node_id").cast("double").alias(node_as),
                F.lit(None).cast("double").alias(other),
                F.col("product_id").alias("ProductID"),
                F.abs("quantity").cast("long").alias("LastKnownQuantity"),
                "event_date",
            ))
