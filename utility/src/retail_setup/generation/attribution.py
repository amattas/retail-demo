"""Deterministic marketing-attribution fact (IMP-007), Spark-native.

Builds ``ag.fact_marketing_attribution``: one row per in-store SALE receipt or
online order, including attributed, unattributed, declined, and
reconciliation-failed outcomes, using deterministic last-touch attribution
within an inclusive 7-day window.

Conversion is driven by exposure. Every background impression
(``marketing.py``) reaches the customer whose ad ID it carries; an impression
converts with a small per-impression probability weighted by channel
(``vocabulary.CHANNEL_CONVERSION_WEIGHTS``) when that customer makes an
eligible purchase within the 7 days after it. The probability is calibrated
per run so the campaigns return about ``TARGET_ROAS`` times their spend
(realistic retail ROAS), which keeps conversions far below 1% of impressions
at real CPMs. Journeys are therefore made of real impressions: a journey's
touches are indistinguishable from other impressions except for their
``attribution_journey_id``, every campaign family can convert, and each touch
lies inside its own campaign window.

Pipeline:

1. Purchase candidates are built from SALE receipts and online order headers,
   each joined to its final payment (a declined first tender that was retried
   has an approved second payment). A purchase is "reconciled" only when that
   payment is APPROVED and equals the purchase's ``total_cents``; anything else
   is ``PAYMENT_DECLINED`` or ``RECONCILIATION_FAILED``.
2. Candidate impressions (a stable draw ``u`` below ``CANDIDATE_RATE`` x the
   channel weight) are joined to the reached customer's eligible purchases
   within the window; each reaches the customer's next purchase.
3. A purchase's conversion score is the smallest ``u / weight`` of the
   impressions reaching it. Purchases convert in score order until their net
   revenue reaches ``TARGET_ROAS`` x the spend of all impressions; the
   impressions at or under the cut are the journey's touches.
4. Last-touch attribution is DERIVED via ``_last_touch`` (window/rank join
   ordered by touch time then impression ID); ``attributed_revenue_cents`` is
   ``net_subtotal_cents`` for ATTRIBUTED rows and 0 otherwise.
5. Receipts/orders/payments/promotions are enriched with the derived
   ``attribution_journey_id``/``campaign_id``/``impression_id_ext``, and the
   journey's impressions in ``fact_marketing`` carry the journey ID.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from datetime import date

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.marketing import campaign_window_start
from retail_setup.generation.runtime import case_when, seeded_draws
from retail_setup.generation.schemas import column_names
from retail_setup.generation.vocabulary import (
    AD_ID_PREFIX,
    CAMPAIGN_ARCHETYPES,
    CHANNEL_CONVERSION_WEIGHTS,
    CHANNEL_COSTS,
    DEVICE_MULTIPLIERS,
    DEVICE_WEIGHTS,
    campaign_id_column,
    channel_conversion_weight,
)

ATTRIBUTION_MODEL = "LAST_TOUCH_7D"
ATTRIBUTION_WINDOW_DAYS = 7
# Campaigns return about this many dollars of net revenue per dollar spent.
TARGET_ROAS = 4.0
# Impressions that may convert at all: a stable draw below this rate times the
# channel's weight relative to the strongest channel. Bounds the conversion
# probability (and the join) well above what TARGET_ROAS needs.
CANDIDATE_RATE = 0.03
# Live stream journeys (with_journey_plan): 1-3 touches, 1 minute to 7 days
# before the purchase, each in the campaign window of its own time.
JOURNEY_MAX_TOUCHES = 3
JOURNEY_TOUCH_MIN_S = 60
FLASH_SALE_DAY_SHARE = 1.0 / 7.0


def _last_touch(touches: DataFrame, purchases: DataFrame) -> DataFrame:
    """Deterministic last-touch join: one output row per input purchase row.

    ``touches`` columns: match_key, touch_ts, impression_id_ext, campaign_id,
    creative_id, channel, customer_ad_id, customer_id.
    ``purchases`` columns: match_key, purchase_ts (any other columns are
    passed through unchanged on the output).

    For each purchase, selects the touch with
    ``touch_ts <= purchase_ts <= touch_ts + ATTRIBUTION_WINDOW_DAYS days``,
    ranked by the greatest ``touch_ts``, ties broken by ``impression_id_ext``
    descending — a real window/rank computation, not a lookup of a
    pre-known "right answer". Purchases with no qualifying touch keep all
    touch columns NULL (left join).
    """
    window_s = ATTRIBUTION_WINDOW_DAYS * 86400
    t = touches.select(
        F.col("match_key").alias("_t_match_key"),
        F.col("touch_ts"),
        F.col("impression_id_ext"),
        F.col("campaign_id"),
        F.col("creative_id"),
        F.col("channel"),
        F.col("customer_ad_id"),
        F.col("customer_id").alias("touch_customer_id"),
    )
    p_cols = purchases.columns
    cond = (
        (F.col("_t_match_key") == F.col("match_key"))
        & (F.col("touch_ts").cast("long") <= F.col("purchase_ts").cast("long"))
        & (F.col("touch_ts").cast("long") >= F.col("purchase_ts").cast("long") - F.lit(window_s))
    )
    joined = purchases.join(t, cond, "left")
    w = Window.partitionBy("match_key").orderBy(
        F.col("touch_ts").desc_nulls_last(),
        F.col("impression_id_ext").desc_nulls_last(),
    )
    ranked = (
        joined.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_t_match_key")
        .withColumn(
            "lag_seconds",
            F.when(
                F.col("touch_ts").isNotNull(),
                F.col("purchase_ts").cast("long") - F.col("touch_ts").cast("long"),
            ).cast("long"),
        )
    )
    return ranked.select(
        *p_cols,
        "touch_ts",
        "impression_id_ext",
        "campaign_id",
        "creative_id",
        "channel",
        "customer_ad_id",
        "touch_customer_id",
        "lag_seconds",
    )


def reached_customer(customer_ad_id: Column) -> Column:
    """The customer an impression reaches: the ID inside its ad ID (``AD`` + digits)."""
    return F.substring(customer_ad_id, len(AD_ID_PREFIX) + 1, 32).cast("long")


def conversion_score(d: seeded_draws, impression_id: Column, channel: Column) -> Column:
    """An impression's conversion score: a stable draw over its channel weight.

    Lower converts first; candidates are impressions scoring below
    ``CANDIDATE_RATE / max weight``.
    """
    return d.u([impression_id], "attr_conversion") / channel_conversion_weight(channel)


def candidate_score_limit() -> float:
    return CANDIDATE_RATE / max(CHANNEL_CONVERSION_WEIGHTS.values())


def archetype_weights() -> list[tuple[str, float]]:
    """Each campaign archetype's share of impressions (flash sales run one day in 7)."""
    return [
        (str(index + 1), base * (FLASH_SALE_DAY_SHARE if name == "flash_sale" else 1.0))
        for index, (name, _, base, _) in enumerate(CAMPAIGN_ARCHETYPES)
    ]


def archetype_share(name: str) -> float:
    """An archetype's share of the background impression picks (before flash gating)."""
    total = sum(base for _, _, base, _ in CAMPAIGN_ARCHETYPES)
    return sum(base for archetype, _, base, _ in CAMPAIGN_ARCHETYPES if archetype == name) / total


def mean_impression_cost() -> float:
    """Expected dollars per background impression across archetypes, channels and devices."""
    device = sum(weight * DEVICE_MULTIPLIERS[name] for name, weight in DEVICE_WEIGHTS)
    weights = dict(archetype_weights())
    total = sum(weights.values())
    cost = 0.0
    for index, (_, channels, _, _) in enumerate(CAMPAIGN_ARCHETYPES):
        mean = sum(sum(CHANNEL_COSTS[channel]) / 2 for channel in channels) / len(channels)
        cost += weights[str(index + 1)] / total * mean
    return cost * device


def live_journey_rate(
    impressions_per_day: float, eligible_purchases_per_day: float, net_ticket_dollars: float
) -> float:
    """Share of the live stream's eligible purchases it attributes.

    Sized like history: the attributed net revenue is ``TARGET_ROAS`` times the
    day's impression spend.
    """
    if eligible_purchases_per_day <= 0 or net_ticket_dollars <= 0:
        return 0.0
    spend = impressions_per_day * mean_impression_cost()
    return min(1.0, TARGET_ROAS * spend / (eligible_purchases_per_day * net_ticket_dollars))


def with_journey_plan(purchases: DataFrame, d: seeded_draws, history_start: date) -> DataFrame:
    """Add a live purchase's journey: its ID and 1-3 touches, oldest first.

    ``purchases`` needs ``match_key`` (the purchase's own ID) and
    ``purchase_ts``. Each touch (``journey_touches``: ``seq``, ``touch_ts``,
    ``archetype``, ``campaign_id``, ``channel``) is 1 minute to 7 days before
    the purchase, on an archetype drawn by impression volume (a flash sale only
    on its active days), in the campaign window of its own time, on one of the
    archetype's channels, like a background impression. ``campaign_id`` and
    ``touch_ts`` are the last touch's.
    """
    key = F.col("match_key")
    window_s = ATTRIBUTION_WINDOW_DAYS * 86400
    count = (1 + F.floor(d.u([key], "attr_touches") * JOURNEY_MAX_TOUCHES)).cast("int")
    offsets = F.slice(
        F.array(
            *[
                (
                    F.lit(JOURNEY_TOUCH_MIN_S)
                    + d.u([key], f"attr_touch{index}") * (window_s - JOURNEY_TOUCH_MIN_S)
                ).cast("long")
                for index in range(1, JOURNEY_MAX_TOUCHES + 1)
            ]
        ),
        1,
        count,
    )
    oldest_first = F.reverse(F.array_sort(offsets))
    flash = next(
        str(index + 1)
        for index, (name, *_) in enumerate(CAMPAIGN_ARCHETYPES)
        if name == "flash_sale"
    )
    durations = {str(index + 1): entry[3] for index, entry in enumerate(CAMPAIGN_ARCHETYPES)}
    channels = {str(index + 1): entry[1] for index, entry in enumerate(CAMPAIGN_ARCHETYPES)}

    def touch(offset: Column, index: Column) -> Column:
        ts = F.timestamp_seconds(F.col("purchase_ts").cast("long") - offset)
        day = F.to_date(ts)
        drawn = d.pick_by_weights([key, index], "attr_archetype", archetype_weights())
        archetype = F.when(
            (drawn == flash) & ~(d.u([day], "mk_flash_gate") < F.lit(FLASH_SALE_DAY_SHARE)),
            F.lit("2"),
        ).otherwise(drawn)
        duration = case_when((archetype == name, F.lit(days)) for name, days in durations.items())
        channel = case_when(
            (
                archetype == name,
                d.pick_by_weights(
                    [key, index], f"attr_channel_{name}", [(c, 1.0) for c in options]
                ),
            )
            for name, options in channels.items()
        )
        start = campaign_window_start(day, history_start, duration)
        return F.struct(
            (index + 1).alias("seq"),
            ts.alias("touch_ts"),
            archetype.cast("int").alias("archetype"),
            campaign_id_column(start, archetype.cast("int")).alias("campaign_id"),
            channel.alias("channel"),
        )

    last = F.element_at(F.col("journey_touches"), -1)
    return (
        purchases.withColumn(
            "attribution_journey_id",
            F.concat(F.lit("JRN"), F.hex(F.xxhash64(key, F.lit(f"attr_journey|{d.seed}")))),
        )
        .withColumn("journey_touches", F.transform(oldest_first, touch))
        .withColumn("campaign_id", last["campaign_id"])
        .withColumn("touch_ts", last["touch_ts"])
    )


def generate_attribution(
    spark: SparkSession,
    t: dict[str, DataFrame],
    cfg: GenerationConfig,
) -> dict[str, DataFrame]:
    """Derive fact_marketing_attribution and enrich receipts/orders/payments/
    promotions/marketing with the selected attribution mapping.

    Must run after fact_receipts, fact_receipt_lines, fact_online_order_headers,
    fact_payments, fact_promotions and the background fact_marketing all exist
    in ``t`` — wired in engine.py right after marketing.generate_marketing.
    """
    d = seeded_draws(cfg.seed)

    # --- 1. purchase candidates: SALE receipts + online orders, each
    # left-joined to its own payment.
    receipts = t["fact_receipts"].filter(F.col("receipt_type") == "SALE")
    # A purchase's payment is its final attempt: a declined first tender that
    # was retried has an approved second payment (vocabulary decline model).
    final = Window.partitionBy(F.coalesce(F.col("receipt_id_ext"), F.col("order_id_ext"))).orderBy(
        (F.col("status") == "APPROVED").desc(), F.col("transaction_id").desc()
    )
    payments = (
        t["fact_payments"]
        .withColumn("_attempt_rank", F.row_number().over(final))
        .filter(F.col("_attempt_rank") == 1)
        .drop("_attempt_rank")
    )
    store_pay = payments.filter(F.col("receipt_id_ext").isNotNull()).select(
        F.col("receipt_id_ext"),
        F.col("status").alias("payment_status"),
        F.col("amount_cents").alias("payment_cents"),
    )
    online_pay = payments.filter(F.col("order_id_ext").isNotNull()).select(
        F.col("order_id_ext"),
        F.col("status").alias("payment_status"),
        F.col("amount_cents").alias("payment_cents"),
    )

    store_purchases = receipts.join(store_pay, "receipt_id_ext", "left").select(
        F.col("receipt_id_ext").alias("match_key"),
        F.lit("STORE").alias("purchase_type"),
        F.col("receipt_id_ext"),
        F.lit(None).cast("string").alias("order_id_ext"),
        F.col("event_ts").alias("purchase_ts"),
        F.col("event_date"),
        F.col("store_id"),
        F.col("customer_id"),
        F.col("gross_subtotal_cents"),
        F.col("discount_cents"),
        F.col("subtotal_cents").alias("net_subtotal_cents"),
        F.col("tax_cents"),
        F.col("total_cents"),
        F.col("payment_status"),
        F.coalesce(F.col("payment_cents"), F.lit(0).cast("long")).alias("payment_cents"),
    )

    online_purchases = (
        t["fact_online_order_headers"]
        .join(online_pay, "order_id_ext", "left")
        .select(
            F.col("order_id_ext").alias("match_key"),
            F.lit("ONLINE").alias("purchase_type"),
            F.lit(None).cast("string").alias("receipt_id_ext"),
            F.col("order_id_ext"),
            F.col("event_ts").alias("purchase_ts"),
            F.col("event_date"),
            F.lit(None).cast("long").alias("store_id"),
            F.col("customer_id"),
            F.col("gross_subtotal_cents"),
            F.col("discount_cents"),
            F.col("subtotal_cents").alias("net_subtotal_cents"),
            F.col("tax_cents"),
            F.col("total_cents"),
            F.col("payment_status"),
            F.coalesce(F.col("payment_cents"), F.lit(0).cast("long")).alias("payment_cents"),
        )
    )

    purchases_all = store_purchases.unionByName(online_purchases)

    # --- 2. reconciliation status: an APPROVED payment whose amount exactly
    # equals the purchase total is the only path to attribution eligibility.
    reconciled = (F.col("payment_status") == "APPROVED") & (
        F.col("payment_cents") == F.col("total_cents")
    )
    purchases_all = purchases_all.withColumn(
        "base_status",
        F.when(F.col("payment_status").isNull(), F.lit("RECONCILIATION_FAILED"))
        .when(F.col("payment_status") == "DECLINED", F.lit("PAYMENT_DECLINED"))
        .when(~reconciled, F.lit("RECONCILIATION_FAILED"))
        .otherwise(F.lit("ELIGIBLE")),
    )

    # --- 3. exposure-driven conversion: candidate impressions reach their
    # customer's next eligible purchase within the window.
    earliest_purchase_ts = F.lit(cfg.start_date).cast("timestamp")
    eligible = purchases_all.filter(
        (F.col("base_status") == "ELIGIBLE")
        & F.col("customer_id").isNotNull()
        & (F.col("purchase_ts") >= earliest_purchase_ts)
    )
    impressions = t["fact_marketing"]
    spend_cents = impressions.agg(F.sum("cost_cents")).collect()[0][0] or 0
    candidates = (
        impressions.withColumn(
            "_score", conversion_score(d, F.col("impression_id_ext"), F.col("channel"))
        )
        .filter(F.col("_score") < F.lit(candidate_score_limit()))
        .select(
            "impression_id_ext",
            F.col("event_ts").alias("touch_ts"),
            "campaign_id",
            "creative_id",
            "channel",
            "customer_ad_id",
            reached_customer(F.col("customer_ad_id")).alias("_reached"),
            "_score",
        )
    )
    window_s = ATTRIBUTION_WINDOW_DAYS * 86400
    reach = candidates.join(
        eligible.select("match_key", "customer_id", "purchase_ts", "net_subtotal_cents"),
        (F.col("_reached") == F.col("customer_id"))
        & (F.col("purchase_ts").cast("long") > F.col("touch_ts").cast("long"))
        & (F.col("purchase_ts").cast("long") <= F.col("touch_ts").cast("long") + window_s),
    ).drop("_reached")
    next_purchase = Window.partitionBy("impression_id_ext").orderBy("purchase_ts", "match_key")
    reach = (
        reach.withColumn("_n", F.row_number().over(next_purchase))
        .filter(F.col("_n") == 1)
        .drop("_n")
    )
    # --- 4. convert purchases in score order until revenue reaches the target.
    scored = reach.groupBy("match_key").agg(
        F.min("_score").alias("_purchase_score"),
        F.first("net_subtotal_cents").alias("_revenue"),
    )
    by_score = Window.orderBy("_purchase_score", "match_key").rowsBetween(
        Window.unboundedPreceding, -1
    )
    target_cents = TARGET_ROAS * spend_cents
    cut = (
        scored.withColumn("_before", F.coalesce(F.sum("_revenue").over(by_score), F.lit(0)))
        .filter(F.col("_before") < F.lit(target_cents))
        .agg(F.max("_purchase_score"))
        .collect()[0][0]
    )
    # No purchase converts when nothing was reached (or nothing was spent).
    touches_all = reach.filter(F.col("_score") <= F.lit(-1.0 if cut is None else float(cut)))
    selected = eligible.join(touches_all.select("match_key").distinct(), "match_key").withColumn(
        "attribution_journey_id",
        F.concat(
            F.lit("JRN"),
            F.hex(F.xxhash64(F.col("match_key"), F.lit(f"attr_journey|{d.seed}"))),
        ),
    )
    touches = touches_all.join(selected.select("match_key", "attribution_journey_id"), "match_key")
    journey_of = touches.select("impression_id_ext", "attribution_journey_id")
    fact_marketing = (
        impressions.drop("attribution_journey_id")
        .join(journey_of, "impression_id_ext", "left")
        .select(*column_names("fact_marketing"))
    )

    # --- 5. derive last-touch attribution via a real window/rank join over
    # each journey's own touches.
    best_touch = _last_touch(
        touches.select(
            F.col("attribution_journey_id").alias("match_key"),
            "touch_ts",
            "impression_id_ext",
            "campaign_id",
            "creative_id",
            "channel",
            "customer_ad_id",
            "customer_id",
        ),
        selected.select(F.col("attribution_journey_id").alias("match_key"), "purchase_ts"),
    ).select(
        F.col("match_key").alias("attribution_journey_id"),
        "touch_ts",
        "impression_id_ext",
        "campaign_id",
        "creative_id",
        "channel",
        "customer_ad_id",
        "lag_seconds",
    )

    attributed = selected.join(best_touch, "attribution_journey_id").withColumn(
        "attribution_status", F.lit("ATTRIBUTED")
    )

    non_attributed = (
        purchases_all.join(selected.select("match_key"), "match_key", "left_anti")
        .withColumn(
            "attribution_status",
            F.when(F.col("base_status") == "ELIGIBLE", F.lit("UNATTRIBUTED_NO_JOURNEY")).otherwise(
                F.col("base_status")
            ),
        )
        .withColumn("attribution_journey_id", F.lit(None).cast("string"))
        .withColumn("touch_ts", F.lit(None).cast("timestamp"))
        .withColumn("impression_id_ext", F.lit(None).cast("string"))
        .withColumn("campaign_id", F.lit(None).cast("string"))
        .withColumn("creative_id", F.lit(None).cast("string"))
        .withColumn("channel", F.lit(None).cast("string"))
        .withColumn("customer_ad_id", F.lit(None).cast("string"))
        .withColumn("lag_seconds", F.lit(None).cast("long"))
    )

    combined = attributed.unionByName(non_attributed)
    fact_marketing_attribution = (
        combined.withColumn(
            "attribution_id",
            F.concat(
                F.lit("ATTR-"),
                F.col("purchase_type"),
                F.lit("-"),
                F.col("match_key"),
            ),
        )
        .withColumn("attribution_model", F.lit(ATTRIBUTION_MODEL))
        .withColumn("attribution_window_days", F.lit(ATTRIBUTION_WINDOW_DAYS).cast("int"))
        .withColumn(
            "attributed_revenue_cents",
            F.when(
                F.col("attribution_status") == "ATTRIBUTED", F.col("net_subtotal_cents")
            ).otherwise(F.lit(0).cast("long")),
        )
        .select(*column_names("fact_marketing_attribution"))
    )

    # --- 6. enrich receipts/orders/payments/promotions with the DERIVED
    # (not pre-assigned) journey/campaign/impression for attributed purchases.
    enrich_map = attributed.select(
        "match_key", "purchase_type", "attribution_journey_id", "campaign_id", "impression_id_ext"
    )
    store_enrich = enrich_map.filter(F.col("purchase_type") == "STORE").select(
        F.col("match_key").alias("receipt_id_ext"),
        "attribution_journey_id",
        "campaign_id",
        "impression_id_ext",
    )
    online_enrich = enrich_map.filter(F.col("purchase_type") == "ONLINE").select(
        F.col("match_key").alias("order_id_ext"),
        "attribution_journey_id",
        "campaign_id",
        "impression_id_ext",
    )
    pay_enrich = enrich_map.select(F.col("match_key").alias("pay_key"), "attribution_journey_id")

    fact_receipts = (
        t["fact_receipts"]
        .drop("attribution_journey_id", "campaign_id", "impression_id_ext")
        .join(store_enrich, "receipt_id_ext", "left")
        .select(*column_names("fact_receipts"))
    )
    fact_online_order_headers = (
        t["fact_online_order_headers"]
        .drop("attribution_journey_id", "campaign_id", "impression_id_ext")
        .join(online_enrich, "order_id_ext", "left")
        .select(*column_names("fact_online_order_headers"))
    )
    fact_payments = (
        t["fact_payments"]
        .drop("attribution_journey_id")
        .withColumn("pay_key", F.coalesce(F.col("receipt_id_ext"), F.col("order_id_ext")))
        .join(pay_enrich, "pay_key", "left")
        .drop("pay_key")
        .select(*column_names("fact_payments"))
    )
    fact_promotions = (
        t["fact_promotions"]
        .drop("attribution_journey_id")
        .join(
            store_enrich.select("receipt_id_ext", "attribution_journey_id"),
            "receipt_id_ext",
            "left",
        )
        .select(*column_names("fact_promotions"))
    )

    return {
        "fact_receipts": fact_receipts,
        "fact_online_order_headers": fact_online_order_headers,
        "fact_payments": fact_payments,
        "fact_promotions": fact_promotions,
        "fact_marketing": fact_marketing,
        "fact_marketing_attribution": fact_marketing_attribution,
    }
