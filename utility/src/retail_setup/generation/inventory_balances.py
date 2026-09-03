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
    compute the running balance ordered by (event_ts, trace_id).

    The seed is **demand-aware**: rather than a flat opening draw (which made
    the running balance drift to roughly ``-(period demand)`` once cumulative
    SALE outflow dwarfed the tiny seed + sparse replenishment), the opening
    stock is sized so the *ending* on-hand lands at a realistic positive
    buffer. With ``net = Σ quantity`` (inbound − outbound) for the (node,
    product) and a per-key target ``buffer ∈ [lo, hi]``, we set
    ``seed = greatest(buffer - net, 0)`` so the final running balance equals
    ``greatest(buffer, net) ≥ 0``. Intermediate balances can still dip on a
    bursty interleave, so genuine stockout crossings remain possible, but the
    current inventory position is no longer a large negative number."""
    # Net flow per (node, product): SALE/OUTBOUND are negative, INBOUND/RETURN
    # positive. Used to back out the opening stock that yields a positive
    # ending balance.
    net = (txns.groupBy("node_id", "product_id")
           .agg(F.sum("quantity").alias("_net")))
    seeds = (net
             .withColumn("_buffer",
                         draw_int(d.u(["node_id", "product_id"],
                                      f"seed-stock-{tag}"), lo, hi))
             .withColumn("quantity",
                         F.greatest(F.col("_buffer") - F.col("_net"),
                                    F.lit(0)).cast("long"))
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
