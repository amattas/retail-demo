"""Returns fact group, Spark-native (unioned into the receipts contract).

Semantics: sample ~``cfg.return_rate`` of SALE receipts per day (Dec 26 spikes
6x, capped at 10% of the day's receipts), favouring baskets with categories
customers return more (``RETURN_CATEGORY_WEIGHTS``: apparel and electronics
far more than groceries). Each return posts 1..``RETURN_WINDOW_DAYS`` days
*after* its sale (IMP-010: no same-day returns), clamped to ``cfg.end_date``,
at a random time inside the store's hours (store-local).

A return is partial: it takes a subset of the receipt's lines, each with a
probability that follows its category (at least one line), and of a multi-unit
line some or all of its units (``returned_lines``). The refund keeps the
sale's customer and goes back to the tender the sale was paid with; its
header links the sale (``original_receipt_id_ext``). Lines keep their own
promo code (``receipt_type`` marks the return). Refund cents are the returned
share of each line's net, with integer basis-point tax on it. The return ID
has the sales layout (``RET`` + yyyyMMddHHmm + store4 + seq6); the refund
payment is APPROVED with a transaction ID derived from the return ID.

All randomness comes from `runtime.seeded_draws` keyed on the original
receipt and line, so output is deterministic for a (config, seed) pair.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.baskets import line_tax_cents
from retail_setup.generation.receipts import _fmt, _open_close_cols
from retail_setup.generation.runtime import case_when, fixed_width_decimal, seeded_draws
from retail_setup.generation.schemas import column_names
from retail_setup.generation.vocabulary import processing_ms_column, transaction_id_column

# Returns post 1..RETURN_WINDOW_DAYS days after the originating sale.
RETURN_WINDOW_DAYS = 14
# Relative return propensity by department token (lowercase substring match,
# first match wins); other departments use DEFAULT_RETURN_WEIGHT.
RETURN_CATEGORY_WEIGHTS: list[tuple[str, float]] = [
    ("clothing", 1.8),
    ("apparel", 1.8),
    ("shoe", 1.8),
    ("fitting", 1.6),
    ("electronics", 1.5),
    ("jewelry", 1.4),
    ("accessories", 1.4),
    ("toys", 1.2),
    ("home", 1.0),
    ("garden", 1.0),
    ("tools", 1.0),
    ("hardware", 1.0),
    ("sport", 1.0),
    ("beauty", 0.7),
    ("health", 0.5),
    ("pharmacy", 0.3),
    ("grocery", 0.25),
    ("food", 0.25),
    ("fresh", 0.15),
    ("produce", 0.15),
]
DEFAULT_RETURN_WEIGHT = 0.8
# A line of a returned receipt is returned with LINE_RETURN_SHARE x its weight
# (at most MAX_LINE_RETURN_SHARE); the receipt's most likely line always is.
LINE_RETURN_SHARE = 0.5
MAX_LINE_RETURN_SHARE = 0.95
_RETURNED_LINE = "array<struct<line_num:int,quantity:int>>"


def return_weight(department: Column) -> Column:
    """Return propensity of a department (``RETURN_CATEGORY_WEIGHTS``)."""
    lower = F.lower(F.coalesce(department, F.lit("")))
    return case_when(
        (lower.contains(token), F.lit(weight)) for token, weight in RETURN_CATEGORY_WEIGHTS
    ).otherwise(F.lit(DEFAULT_RETURN_WEIGHT))


def _receipt_lines(
    sales_group: dict[str, DataFrame], dims: dict[str, DataFrame] | None
) -> DataFrame | None:
    """Lines of each receipt with their return weight, as one sorted array."""
    lines = sales_group.get("fact_receipt_lines")
    if lines is None:
        return None
    lines = lines.select("receipt_id_ext", "line_num", "product_id", "quantity")
    if dims is not None and "dim_products" in dims:
        departments = dims["dim_products"].select(
            F.col("ID").alias("product_id"), F.col("Department").alias("_department")
        )
        lines = lines.join(departments, "product_id", "left")
    else:
        lines = lines.withColumn("_department", F.lit(None).cast("string"))
    return lines.groupBy("receipt_id_ext").agg(
        F.array_sort(
            F.collect_list(
                F.struct(
                    F.col("line_num").cast("int").alias("line_num"),
                    F.col("quantity").cast("int").alias("quantity"),
                    return_weight(F.col("_department")).alias("weight"),
                )
            )
        ).alias("_lines")
    )


def returned_lines_column(d: seeded_draws, key: Column, lines: Column) -> Column:
    """The lines (and units) a return takes from ``lines`` (line_num, quantity, weight).

    Each line is returned with ``LINE_RETURN_SHARE`` x its weight, and the
    line with the largest weighted draw always is, so a return is never
    empty. A multi-unit line returns 1..all of its units.
    """
    anchor = F.array_max(
        F.transform(
            lines,
            lambda line: F.struct(
                (line["weight"] * d.u([key, line["line_num"]], "return-anchor")).alias("score"),
                line["line_num"].alias("line_num"),
            ),
        )
    )["line_num"]
    chosen = F.filter(
        lines,
        lambda line: (
            (
                d.u([key, line["line_num"]], "return-line")
                < F.least(F.lit(MAX_LINE_RETURN_SHARE), line["weight"] * LINE_RETURN_SHARE)
            )
            | (line["line_num"] == anchor)
        ),
    )
    return F.transform(
        chosen,
        lambda line: F.struct(
            line["line_num"].alias("line_num"),
            F.when(
                line["quantity"] > 1,
                1 + F.floor(d.u([key, line["line_num"]], "return-units") * line["quantity"]),
            )
            .otherwise(F.lit(1))
            .cast("int")
            .alias("quantity"),
        ),
    )


def build_return_headers(
    sales_group: dict[str, DataFrame],
    cfg: GenerationConfig,
    dims: dict[str, DataFrame] | None = None,
) -> DataFrame:
    """Sample SALE receipts for return and date each strictly after its sale.

    Returns the RETURN header plan, which retains ``orig_receipt_id_ext``,
    ``orig_event_date`` (the sale day) and ``returned_lines`` (the lines and
    units returned; NULL means the whole receipt when the plan was built
    without lines) alongside the return ``event_date``/``event_ts``. With
    ``dims`` the sample favours returnable categories and the return time falls
    inside the store's hours; without them it is noon. Deterministic per
    (config, seed).
    """
    d = seeded_draws(cfg.seed)

    sales = sales_group["fact_receipts"].filter(
        (F.col("receipt_type") == "SALE")
        & (F.col("event_date") < F.lit(cfg.end_date))
        & ~((F.month("event_date") == 12) & (F.dayofmonth("event_date") == 25))
    )
    receipt_lines = _receipt_lines(sales_group, dims)
    if receipt_lines is not None:
        sales = sales.join(receipt_lines, "receipt_id_ext", "left")
        weight = F.coalesce(
            F.array_max(F.transform("_lines", lambda line: line["weight"])),
            F.lit(DEFAULT_RETURN_WEIGHT),
        )
    else:
        sales = sales.withColumn(
            "_lines", F.lit(None).cast("array<struct<line_num:int,quantity:int,weight:double>>")
        )
        weight = F.lit(DEFAULT_RETURN_WEIGHT)

    # --- sample: per-day rate, 6x on Dec 26, hard-capped at 10%.
    # Accumulate fractional daily quotas before rounding so low-volume days do
    # not each lose their fraction independently. Within a day, receipts race
    # on an exponential draw scaled by their basket's return weight, so baskets
    # of returnable categories are returned more often.
    day_rate = F.least(
        F.lit(0.10),
        F.lit(cfg.return_rate)
        * F.when(
            (F.month("event_date") == 12) & (F.dayofmonth("event_date") == 26),
            F.lit(6.0),
        ).otherwise(F.lit(1.0)),
    )
    daily = (
        sales.groupBy("event_date")
        .agg(F.count("*").alias("_day_count"))
        .withColumn("_expected", F.col("_day_count") * day_rate)
    )
    cumulative = Window.orderBy("event_date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    prior = Window.orderBy("event_date").rowsBetween(Window.unboundedPreceding, -1)
    quotas = (
        daily.withColumn("_cumulative_expected", F.sum("_expected").over(cumulative))
        .withColumn(
            "_prior_expected",
            F.coalesce(F.sum("_expected").over(prior), F.lit(0.0)),
        )
        .withColumn(
            "_n_keep",
            (
                F.floor(F.col("_cumulative_expected") + F.lit(1e-9))
                - F.floor(F.col("_prior_expected") + F.lit(1e-9))
            ).cast("long"),
        )
        .select("event_date", "_n_keep")
    )
    race = -F.log(F.lit(1.0) - d.u(["receipt_id_ext"], "return")) / weight
    day_w = Window.partitionBy("event_date")
    sampled = (
        sales.join(quotas, "event_date")
        .withColumn("_race", race)
        .withColumn("_ret_rank", F.row_number().over(day_w.orderBy("_race", "receipt_id_ext")))
        .filter(F.col("_ret_rank") <= F.col("_n_keep"))
        .drop("_n_keep", "_ret_rank", "_race")
        .withColumn(
            "returned_lines",
            F.when(
                F.col("_lines").isNotNull(),
                returned_lines_column(d, F.col("receipt_id_ext"), F.col("_lines")),
            ).cast(_RETURNED_LINE),
        )
        .drop("_lines")
    )

    sequenced = (
        sampled.withColumnRenamed("receipt_id_ext", "orig_receipt_id_ext")
        .withColumnRenamed("event_date", "orig_event_date")
        .withColumn(
            "_max_delay",
            F.least(
                F.lit(RETURN_WINDOW_DAYS),
                F.datediff(F.lit(cfg.end_date), F.col("orig_event_date")),
            ).cast("int"),
        )
        .filter(F.col("_max_delay") >= 1)
        .withColumn(
            "_return_delay",
            (
                F.lit(1)
                + F.floor(d.u(["orig_receipt_id_ext"], "return_delay") * F.col("_max_delay"))
            ).cast("int"),
        )
        .withColumn(
            "_candidate_date",
            F.date_add("orig_event_date", F.col("_return_delay")),
        )
        .withColumn(
            "_latest_return_date",
            F.date_add("orig_event_date", F.col("_max_delay")),
        )
        .withColumn(
            "event_date",
            F.when(
                (F.month("_candidate_date") == 12) & (F.dayofmonth("_candidate_date") == 25),
                F.when(
                    F.date_add("_candidate_date", 1) <= F.col("_latest_return_date"),
                    F.date_add("_candidate_date", 1),
                ).otherwise(F.date_add("_candidate_date", -1)),
            ).otherwise(F.col("_candidate_date")),
        )
        .filter(
            (F.col("event_date") > F.col("orig_event_date"))
            & (F.col("event_date") <= F.col("_latest_return_date"))
        )
    )
    # Return time: a random minute inside the store's (local) hours.
    key = ["orig_receipt_id_ext"]
    if dims is not None and "dim_stores" in dims:
        hours = dims["dim_stores"].select(F.col("ID").alias("store_id"), "operating_hours")
        sequenced = sequenced.join(hours, "store_id", "left")
        open_h, close_h = _open_close_cols(F.col("operating_hours"))
        open_h, close_h = F.coalesce(open_h, F.lit(12)), F.coalesce(close_h, F.lit(13))
    else:
        sequenced = sequenced.withColumn("operating_hours", F.lit(None).cast("string"))
        open_h, close_h = F.lit(12), F.lit(13)
    minute_of_day = open_h * 60 + F.floor(d.u(key, "return-time") * (close_h - open_h) * 60).cast(
        "int"
    )
    sequenced = sequenced.withColumn(
        "event_ts",
        F.make_timestamp(
            F.year("event_date"),
            F.month("event_date"),
            F.dayofmonth("event_date"),
            F.floor(minute_of_day / 60).cast("int"),
            (minute_of_day % 60).cast("int"),
            F.floor(d.u(key, "return-second") * 60).cast("int"),
        ),
    ).drop("operating_hours")
    seq_w = Window.partitionBy("store_id", "event_date").orderBy("orig_receipt_id_ext")
    return (
        sequenced.withColumn("seq", F.row_number().over(seq_w))
        .withColumn(
            "receipt_id_ext",
            F.concat(
                F.lit("RET"),
                F.date_format("event_ts", "yyyyMMddHHmm"),
                fixed_width_decimal(F.col("store_id"), 4, "return store ID"),
                fixed_width_decimal(F.col("seq"), 6, "daily return sequence"),
            ),
        )
        .withColumn("trace_id", F.concat(F.lit("TRC"), F.col("receipt_id_ext")))
        .withColumn("receipt_type", F.lit("RETURN"))
        # The refund goes back to the tender the sale was paid with.
        .withColumn(
            "tender_type",
            F.col("payment_method")
            if "payment_method" in sequenced.columns
            else F.lit(None).cast("string"),
        )
        .withColumn("payment_method", F.col("tender_type"))
        .withColumn("subtotal_cents", -F.col("subtotal_cents"))
        .withColumn("tax_cents", -F.col("tax_cents"))
        .withColumn("total_cents", -F.col("total_cents"))
        .withColumn("discount_cents", -F.col("discount_cents"))
        .withColumn("gross_subtotal_cents", F.col("subtotal_cents") + F.col("discount_cents"))
        # Returns are never attribution candidates (SALE-only per contract).
        .withColumn("attribution_journey_id", F.lit(None).cast("string"))
        .withColumn("campaign_id", F.lit(None).cast("string"))
        .withColumn("impression_id_ext", F.lit(None).cast("string"))
    )


def returned_quantity(returned_lines: Column, line_num: Column, quantity: Column) -> Column:
    """Units of ``line_num`` a return takes (the whole line when the plan has no lines)."""
    picked = F.aggregate(
        returned_lines,
        F.lit(0),
        lambda units, r: units + F.when(r["line_num"] == line_num, r["quantity"]).otherwise(0),
    )
    return F.when(returned_lines.isNull(), quantity).otherwise(F.least(picked, quantity))


def generate_returns(
    spark: SparkSession,
    sales_group: dict[str, DataFrame],
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
    *,
    return_headers: DataFrame | None = None,
) -> dict[str, DataFrame]:
    """Generate refunds, optionally retaining a pre-fulfillment sampling plan.

    Planned dates/IDs/lines survive fulfillment; refunds are priced from the
    accepted lines, so a returned line the store never filled is not refunded
    and a return with nothing left disappears.
    """

    d = seeded_draws(cfg.seed)

    header = (
        build_return_headers(sales_group, cfg, dims) if return_headers is None else return_headers
    )
    if "returned_lines" not in header.columns:
        header = header.withColumn("returned_lines", F.lit(None).cast(_RETURNED_LINE))
    sold = sales_group["fact_receipt_lines"].select(
        F.col("receipt_id_ext").alias("orig_receipt_id_ext"),
        "line_num",
        "product_id",
        F.col("quantity").alias("_sold"),
        "unit_cents",
        F.col("ext_cents").alias("_sold_ext"),
        "promo_code",
    )
    taxability = dims["dim_products"].select(F.col("ID").alias("product_id"), "taxability")
    store_bps = dims["dim_stores"].select(
        F.col("ID").alias("store_id"),
        F.round(F.col("tax_rate") * 10000).cast("long").alias("_bps"),
    )
    units = F.col("_units").cast("long")
    sold_units = F.col("_sold").cast("long")
    lines = (
        header.select(
            "orig_receipt_id_ext",
            "receipt_id_ext",
            "event_ts",
            "event_date",
            "store_id",
            "returned_lines",
        )
        .join(sold, "orig_receipt_id_ext")
        .withColumn(
            "_units", returned_quantity(F.col("returned_lines"), F.col("line_num"), F.col("_sold"))
        )
        .filter(F.col("_units") > 0)
        .join(taxability, "product_id", "left")
        .join(store_bps, "store_id", "left")
        # The returned share of the line's net, rounded half up, and its tax.
        .withColumn(
            "_net",
            F.floor((F.col("_sold_ext") * units * 2 + sold_units) / (sold_units * 2)).cast("long"),
        )
        .withColumn("_gross", F.col("unit_cents") * units)
        .withColumn("_tax", line_tax_cents(F.col("_net"), F.col("taxability"), F.col("_bps")))
    )
    totals = lines.groupBy("receipt_id_ext").agg(
        (-F.sum("_net")).cast("long").alias("subtotal_cents"),
        (-F.sum(F.col("_gross") - F.col("_net"))).cast("long").alias("discount_cents"),
        (-F.sum("_tax")).cast("long").alias("tax_cents"),
    )
    money = ["subtotal_cents", "discount_cents", "tax_cents", "total_cents", "gross_subtotal_cents"]
    header = (
        header.drop(*money)
        .join(totals, "receipt_id_ext")
        .withColumn("total_cents", F.col("subtotal_cents") + F.col("tax_cents"))
        .withColumn("gross_subtotal_cents", F.col("subtotal_cents") + F.col("discount_cents"))
    )

    fact_receipts = header.select(
        "receipt_id_ext",
        "trace_id",
        "event_ts",
        "event_date",
        "store_id",
        "customer_id",
        "receipt_type",
        "tender_type",
        "subtotal_cents",
        _fmt(F.col("discount_cents")).alias("discount_amount"),
        "tax_cents",
        "total_cents",
        _fmt(F.col("subtotal_cents")).alias("subtotal_amount"),
        _fmt(F.col("tax_cents")).alias("tax_amount"),
        _fmt(F.col("total_cents")).alias("total_amount"),
        F.col("tender_type").alias("payment_method"),
        # Legacy semantic-model column mirrors subtotal_amount (TMDL contract).
        _fmt(F.col("subtotal_cents")).alias("Subtotal"),
        "gross_subtotal_cents",
        "discount_cents",
        "attribution_journey_id",
        "campaign_id",
        "impression_id_ext",
        F.hour("event_ts").alias("local_hour"),
        F.col("orig_receipt_id_ext").alias("original_receipt_id_ext"),
    ).select(*column_names("fact_receipts"))

    # --- lines: the returned units of each line, negated; the line keeps its
    # own promo code (receipt_type marks the return)
    fact_receipt_lines = lines.select(
        "receipt_id_ext",
        "event_ts",
        "event_date",
        "line_num",
        "product_id",
        (-units).cast("int").alias("quantity"),
        _fmt(F.col("unit_cents")).alias("unit_price"),
        "unit_cents",
        _fmt(-F.col("_net")).alias("ext_price"),
        (-F.col("_net")).alias("ext_cents"),
        "promo_code",
    ).select(*column_names("fact_receipt_lines"))

    # --- payments: one negative APPROVED refund per return, to the sale's tender
    fact_payments = (
        fact_receipts.withColumn("order_id_ext", F.lit(None).cast("string"))
        .withColumn("amount_cents", F.col("total_cents"))
        .withColumn("amount", _fmt(F.col("amount_cents")))
        .withColumn("transaction_id", transaction_id_column("TXN", F.col("receipt_id_ext"), 1))
        .withColumn("status", F.lit("APPROVED"))
        .withColumn("decline_reason", F.lit(None).cast("string"))
        .withColumn(
            "processing_time_ms",
            processing_ms_column(d, ["receipt_id_ext"], F.col("payment_method")),
        )
        .withColumn("attribution_journey_id", F.lit(None).cast("string"))
        .select(*column_names("fact_payments"))
    )

    return {
        "fact_receipts": fact_receipts,
        "fact_receipt_lines": fact_receipt_lines,
        "fact_payments": fact_payments,
    }
