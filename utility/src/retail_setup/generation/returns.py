"""Returns fact group, Spark-native (unioned into the receipts contract).

Semantics (datagen utils_mixin): sample ~``cfg.return_rate`` of SALE receipts
per day — Dec 26 spikes 6x, capped at 10% of the day's receipts. The return
header gets a new ``receipt_id_ext`` with the same 25-char layout as sales
(``RET`` + yyyyMMddHHmm + store4 + seq6), ``receipt_type='RETURN'``, noon
``event_ts`` on the same day, NULL ``customer_id``, CREDIT_CARD tender, and
negated cents. Lines mirror the original receipt's lines with negative
quantity / ext_cents (``promo_code='RETURN'``); each return gets one negative,
APPROVED payment — refunds don't decline.

All randomness comes from `runtime.seeded_draws` keyed on the original
receipt id, so output is deterministic for a (config, seed) pair.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.receipts import _fmt
from retail_setup.generation.runtime import seeded_draws
from retail_setup.generation.schemas import column_names

# CREDIT_CARD processing-time bounds (ms), matching the sales TENDERS table.
_PROC_MS_LO = 1500
_PROC_MS_HI = 4000


def generate_returns(
    spark: SparkSession,
    sales_group: dict[str, DataFrame],
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
) -> dict[str, DataFrame]:
    """Generate RETURN fact_receipts / fact_receipt_lines / fact_payments."""

    d = seeded_draws(cfg.seed)

    sales = sales_group["fact_receipts"].filter(F.col("receipt_type") == "SALE")

    # --- sample: per-day rate, 6x on Dec 26, hard-capped at 10%.
    # Deterministic rank-based sampling (lowest seeded u first, exactly
    # floor(day_rate * n_day) per day) rather than per-row Bernoulli
    # `u < day_rate`: with Bernoulli the Dec-26 expectation equals the cap,
    # which equals exactly 2x the nominal rate, so the spike contract is a
    # coin flip on hash noise. Ranking pins other days at/below nominal and
    # lands Dec 26 on the cap, keeping output deterministic per (config, seed).
    day_rate = F.least(
        F.lit(0.10),
        F.lit(cfg.return_rate)
        * F.when(
            (F.month("event_date") == 12) & (F.dayofmonth("event_date") == 26),
            F.lit(6.0),
        ).otherwise(F.lit(1.0)),
    )
    u_ret = d.u(["receipt_id_ext"], "return")
    day_w = Window.partitionBy("event_date")
    sampled = (
        sales
        .withColumn("_n_keep", F.floor(day_rate * F.count("*").over(day_w)))
        .withColumn("_ret_rank", F.row_number().over(
            day_w.orderBy(u_ret, "receipt_id_ext")))
        .filter(F.col("_ret_rank") <= F.col("_n_keep"))
        .drop("_n_keep", "_ret_rank")
    )

    # --- header: new RET id (RET(3)+yyyyMMddHHmm(12)+store4+seq6 = 25 chars,
    # unique because seq is a row_number per (store_id, event_date) at noon)
    seq_w = Window.partitionBy("store_id", "event_date").orderBy("orig_receipt_id_ext")
    header = (
        sampled
        .withColumnRenamed("receipt_id_ext", "orig_receipt_id_ext")
        .withColumn("event_ts", F.to_timestamp(
            F.concat(F.col("event_date").cast("string"), F.lit(" 12:00:00"))))
        .withColumn("seq", F.row_number().over(seq_w))
        .withColumn("receipt_id_ext", F.concat(
            F.lit("RET"), F.date_format("event_ts", "yyyyMMddHHmm"),
            F.lpad(F.col("store_id").cast("string"), 4, "0"),
            F.lpad(F.col("seq").cast("string"), 6, "0")))
        .withColumn("trace_id", F.concat(F.lit("TRC"), F.col("receipt_id_ext")))
        .withColumn("customer_id", F.lit(None).cast("long"))
        .withColumn("receipt_type", F.lit("RETURN"))
        .withColumn("tender_type", F.lit("CREDIT_CARD"))
        .withColumn("payment_method", F.lit("CREDIT_CARD"))
        .withColumn("subtotal_cents", -F.col("subtotal_cents"))
        .withColumn("tax_cents", -F.col("tax_cents"))
        .withColumn("total_cents", -F.col("total_cents"))
        # IMP-007: negate the original discount alongside subtotal/tax so
        # gross - discount = net continues to hold for the return row.
        # Returns are never attribution candidates (SALE-only per contract).
        .withColumn("discount_cents", -F.col("discount_cents"))
        .withColumn("gross_subtotal_cents", F.col("subtotal_cents") + F.col("discount_cents"))
        .withColumn("attribution_journey_id", F.lit(None).cast("string"))
        .withColumn("campaign_id", F.lit(None).cast("string"))
        .withColumn("impression_id_ext", F.lit(None).cast("string"))
    )

    fact_receipts = header.select(
        "receipt_id_ext", "trace_id", "event_ts", "event_date", "store_id",
        "customer_id", "receipt_type", "tender_type", "subtotal_cents",
        F.lit("0.00").alias("discount_amount"), "tax_cents", "total_cents",
        _fmt(F.col("subtotal_cents")).alias("subtotal_amount"),
        _fmt(F.col("tax_cents")).alias("tax_amount"),
        _fmt(F.col("total_cents")).alias("total_amount"), "payment_method",
        # Legacy semantic-model column mirrors subtotal_amount (TMDL contract).
        _fmt(F.col("subtotal_cents")).alias("Subtotal"),
        "gross_subtotal_cents", "discount_cents", "attribution_journey_id",
        "campaign_id", "impression_id_ext",
    ).select(*column_names("fact_receipts"))

    # --- lines: mirror the original receipt's lines, negated
    orig_lines = sales_group["fact_receipt_lines"].select(
        F.col("receipt_id_ext").alias("orig_receipt_id_ext"),
        "line_num", "product_id", "quantity", "unit_cents", "ext_cents",
    )
    fact_receipt_lines = (
        header.select("orig_receipt_id_ext", "receipt_id_ext",
                      "event_ts", "event_date")
        .join(orig_lines, "orig_receipt_id_ext")
        .withColumn("quantity", (-F.col("quantity")).cast("int"))
        .withColumn("ext_cents", -F.col("ext_cents"))
        .withColumn("promo_code", F.lit("RETURN"))
        .select(
            "receipt_id_ext", "event_ts", "event_date", "line_num", "product_id",
            "quantity", _fmt(F.col("unit_cents")).alias("unit_price"), "unit_cents",
            _fmt(F.col("ext_cents")).alias("ext_price"), "ext_cents", "promo_code",
        ).select(*column_names("fact_receipt_lines"))
    )

    # --- payments: one negative APPROVED CREDIT_CARD refund per return
    fact_payments = (
        fact_receipts
        .withColumn("order_id_ext", F.lit(None).cast("string"))
        .withColumn("amount_cents", F.col("total_cents"))
        .withColumn("amount", _fmt(F.col("amount_cents")))
        .withColumn("transaction_id", F.concat(
            F.lit("TXN_"), F.unix_timestamp("event_ts").cast("string"), F.lit("_"),
            F.lpad((d.h64(["receipt_id_ext"], "txn") % 1_000_000).cast("string"),
                   6, "0")))
        .withColumn("status", F.lit("APPROVED"))
        .withColumn("decline_reason", F.lit(None).cast("string"))
        .withColumn("processing_time_ms", (
            F.lit(_PROC_MS_LO) + d.u(["receipt_id_ext"], "proc")
            * F.lit(_PROC_MS_HI - _PROC_MS_LO)).cast("long"))
        .select(*column_names("fact_payments"))
    )

    return {
        "fact_receipts": fact_receipts,
        "fact_receipt_lines": fact_receipt_lines,
        "fact_payments": fact_payments,
    }
