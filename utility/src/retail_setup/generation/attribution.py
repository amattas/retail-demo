"""Deterministic marketing-attribution fact (IMP-007), Spark-native.

Builds ``ag.fact_marketing_attribution``: one row per in-store SALE receipt or
online order, including attributed, unattributed, declined, and
reconciliation-failed outcomes, using deterministic last-touch attribution
within an inclusive 7-day window.

Pipeline:

1. Purchase candidates are built from SALE receipts and online order headers,
   each left-joined to its own payment row. A purchase is "reconciled" only
   when it has an APPROVED payment whose ``amount_cents`` exactly equals the
   purchase's ``total_cents``; anything else is ``PAYMENT_DECLINED`` (a
   payment exists but is DECLINED) or ``RECONCILIATION_FAILED`` (no payment
   row at all — e.g. a cancelled online order — or an approved payment that
   doesn't reconcile to the total).
2. A deterministic, seed-bound ~5% share of the reconciled purchases is
   selected for attribution (``seeded_draws.u`` keyed on the purchase's own
   id, so selection is independent of cluster shape and stable for a given
   (config, seed)). Each selected purchase gets a unique
   ``attribution_journey_id`` and two synthetic ad-impression touches for the
   SAME customer/campaign/journey: an "older" touch (3-7 days before
   purchase) and a "newer" touch (1 minute-2 days before purchase). Both land
   inside the 7-day attribution window, and the two offset ranges never
   overlap, so the newer touch is always the more recent of the two.
3. The two touches are unioned into ``fact_marketing`` (customer_ad_id/
   customer_id populated, ``attribution_journey_id`` set). Last-touch
   attribution is then DERIVED — not copied — via ``_last_touch``: a real
   window/rank join keyed on ``touch_ts <= purchase_ts <= touch_ts + 7d``,
   ordered by ``touch_ts`` desc then ``impression_id_ext`` desc. Because the
   "newer" touch's timestamp is always later than the "older" touch's (by
   construction — the offset ranges never overlap), the newer touch always
   wins the rank; the winner is computed, not assumed, so a swapped
   offset/ordering bug would surface as a failing rank/tie-break test rather
   than silently passing.
4. All other reconciled purchases get ``UNATTRIBUTED_NO_JOURNEY`` (no journey
   was selected for them); ``attributed_revenue_cents`` is 0 for every
   non-ATTRIBUTED row and equals ``net_subtotal_cents`` (tax-exclusive) for
   ATTRIBUTED rows.
5. Selected receipts/orders/payments/promotions are enriched in place with
   the *derived* ``attribution_journey_id``/``campaign_id``/
   ``impression_id_ext`` — every other row keeps the NULL placeholders the
   base generators already emit.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.marketing import (
    ARCHETYPES,
    CHANNEL_COSTS,
    DEVICE_MULTIPLIERS,
    DEVICE_WEIGHTS,
)
from retail_setup.generation.runtime import legacy_index, seeded_draws
from retail_setup.generation.schemas import column_names

ATTRIBUTION_MODEL = "LAST_TOUCH_7D"
ATTRIBUTION_WINDOW_DAYS = 7
SELECTION_RATE = 0.05  # deterministic bounded share (~5%) of eligible purchases

# Linked journeys reuse the product-launch and loyalty campaign families from
# marketing.py. Their 14/30-day durations provide real background spend while
# still allowing a conversion up to seven days after the selected touch.
JOURNEY_CAMPAIGNS = [
    (2, ARCHETYPES[1][1], ARCHETYPES[1][3]),
    (3, ARCHETYPES[2][1], ARCHETYPES[2][3]),
]

# Touch offsets (seconds before purchase_ts). The ranges never overlap
# (older_min = 3d > newer_max = 2d), so the "newer" touch is always the more
# recent one of the pair — attribution is still DERIVED by real ranking (see
# _last_touch); this just keeps the pipeline's own construction sane.
OLDER_OFFSET_MIN_S = 3 * 86400
OLDER_OFFSET_MAX_S = ATTRIBUTION_WINDOW_DAYS * 86400
NEWER_OFFSET_MIN_S = 60
NEWER_OFFSET_MAX_S = 2 * 86400


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


def _journey_touch_rows(
    selected: DataFrame,
    d: seeded_draws,
    ts_col: str,
    suffix: str,
) -> DataFrame:
    """Build one fact_marketing row per selected journey for a single touch
    ("older" or "newer"). Device/cost draws mirror marketing.py's organic
    impressions, keyed on this touch's own impression id so the two touches
    of a journey draw independently of each other.
    """
    rows = selected.withColumn("touch_ts", F.col(ts_col)).withColumn(
        "impression_id_ext",
        F.concat(F.lit("IMPJRN"), F.col("_jhash"), F.lit(suffix[0].upper())),
    )
    key = [F.col("impression_id_ext")]
    rows = rows.withColumn(
        "device", d.pick_by_weights(key, f"attr_device_{suffix}", DEVICE_WEIGHTS)
    )
    mult = None
    for device, m in DEVICE_MULTIPLIERS.items():
        cond = F.col("device") == device
        mult = F.when(cond, m) if mult is None else mult.when(cond, m)
    rows = rows.withColumn("device_mult", mult)
    lo, hi = None, None
    for channel, (c_lo, c_hi) in CHANNEL_COSTS.items():
        cond = F.col("channel") == channel
        lo = F.when(cond, c_lo) if lo is None else lo.when(cond, c_lo)
        hi = F.when(cond, c_hi) if hi is None else hi.when(cond, c_hi)
    cost_dollars = (lo + d.u(key, f"attr_cost_{suffix}") * (hi - lo)) * F.col("device_mult")
    rows = (
        rows.withColumn("cost_cents", F.round(cost_dollars * 100).cast("long"))
        .withColumn("cost", F.format_string("%.2f", cost_dollars))
        .withColumn("trace_id", F.concat(F.lit("TRC-MKT-"), F.col("impression_id_ext")))
        .withColumn("creative_id", F.concat(F.col("creative_id"), F.lit(f"-{suffix.upper()}")))
        .withColumn("event_date", F.col("touch_ts").cast("date"))
    )
    out = rows.select(
        F.col("touch_ts").alias("event_ts"),
        "trace_id",
        "channel",
        "campaign_id",
        "creative_id",
        "customer_ad_id",
        F.col("customer_id").cast("double").alias("customer_id"),
        "cost_cents",
        F.col("customer_id").cast("double").alias("CustomerId"),
        F.col("cost_cents").alias("CostCents"),
        "impression_id_ext",
        "cost",
        "device",
        "event_date",
        "attribution_journey_id",
    ).withColumn("__index_level_0__", legacy_index("impression_id_ext"))
    return out.select(*column_names("fact_marketing"))


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
    payments = t["fact_payments"]
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

    # --- 3. deterministic ~5% selection of eligible (reconciled) purchases,
    # with a known customer to anchor the journey's touches to.
    earliest_purchase_ts = F.date_add(F.lit(cfg.start_date), ATTRIBUTION_WINDOW_DAYS).cast(
        "timestamp"
    )
    eligible = purchases_all.filter(
        (F.col("base_status") == "ELIGIBLE")
        & F.col("customer_id").isNotNull()
        # Keep generated touches inside the requested historical range.
        & (F.col("purchase_ts") >= earliest_purchase_ts)
    )
    selected = (
        eligible.withColumn("_sel_u", d.u(["match_key"], "attr_select"))
        .filter(F.col("_sel_u") < F.lit(SELECTION_RATE))
        .drop("_sel_u")
    )

    selected = (
        selected.withColumn(
            "_jhash", F.hex(F.xxhash64(F.col("match_key"), F.lit(f"attr_journey|{cfg.seed}")))
        )
        .withColumn("attribution_journey_id", F.concat(F.lit("JRN"), F.col("_jhash")))
        .withColumn("creative_id", F.concat(F.lit("CREATJRN"), F.substring(F.col("_jhash"), 1, 12)))
        .withColumn(
            "_campaign_choice",
            (d.h64(["match_key"], "attr_campaign") % len(JOURNEY_CAMPAIGNS)).cast("int"),
        )
        .withColumn(
            "_campaign_index",
            F.when(F.col("_campaign_choice") == 0, F.lit(JOURNEY_CAMPAIGNS[0][0])).otherwise(
                F.lit(JOURNEY_CAMPAIGNS[1][0])
            ),
        )
        .withColumn(
            "_campaign_duration",
            F.when(F.col("_campaign_choice") == 0, F.lit(JOURNEY_CAMPAIGNS[0][2])).otherwise(
                F.lit(JOURNEY_CAMPAIGNS[1][2])
            ),
        )
        .withColumn(
            "channel",
            F.when(
                F.col("_campaign_choice") == 0,
                d.pick_by_weights(
                    ["match_key"],
                    "attr_channel_product",
                    [(channel, 1.0) for channel in JOURNEY_CAMPAIGNS[0][1]],
                ),
            ).otherwise(
                d.pick_by_weights(
                    ["match_key"],
                    "attr_channel_loyalty",
                    [(channel, 1.0) for channel in JOURNEY_CAMPAIGNS[1][1]],
                )
            ),
        )
        .withColumn(
            "older_offset",
            (
                F.lit(OLDER_OFFSET_MIN_S)
                + d.u(["match_key"], "attr_older") * F.lit(OLDER_OFFSET_MAX_S - OLDER_OFFSET_MIN_S)
            ).cast("long"),
        )
        .withColumn(
            "newer_offset",
            (
                F.lit(NEWER_OFFSET_MIN_S)
                + d.u(["match_key"], "attr_newer") * F.lit(NEWER_OFFSET_MAX_S - NEWER_OFFSET_MIN_S)
            ).cast("long"),
        )
        .withColumn(
            "touch_ts_older",
            F.timestamp_seconds(F.col("purchase_ts").cast("long") - F.col("older_offset")),
        )
        .withColumn(
            "touch_ts_newer",
            F.timestamp_seconds(F.col("purchase_ts").cast("long") - F.col("newer_offset")),
        )
        .withColumn("_campaign_touch_day", F.to_date("touch_ts_newer"))
        .withColumn(
            "_campaign_start",
            F.date_add(
                F.lit(cfg.start_date),
                (
                    F.datediff(F.col("_campaign_touch_day"), F.lit(cfg.start_date))
                    / F.col("_campaign_duration")
                ).cast("int")
                * F.col("_campaign_duration"),
            ),
        )
        .withColumn(
            "campaign_id",
            F.concat(
                F.lit("CAMP"),
                F.date_format("_campaign_start", "yyyyMMdd"),
                F.lpad(F.col("_campaign_index").cast("string"), 2, "0"),
            ),
        )
    )

    cust_ad = t["dim_customers"].select(
        F.col("ID").alias("customer_id"), F.col("AdId").alias("customer_ad_id")
    )
    selected = selected.join(cust_ad, "customer_id", "left")

    # --- 4. the two linked touches per journey, unioned into fact_marketing.
    older_rows = _journey_touch_rows(selected, d, "touch_ts_older", "older")
    newer_rows = _journey_touch_rows(selected, d, "touch_ts_newer", "newer")
    fact_marketing = t["fact_marketing"].unionByName(older_rows).unionByName(newer_rows)

    # --- 5. derive last-touch attribution via a real window/rank join,
    # scoped to each journey's own two touches (never mixed with organic
    # background impressions or other journeys — deterministic regardless of
    # catalog size/config) but still a genuine ranking computation.
    touches = older_rows.select(
        F.col("attribution_journey_id").alias("match_key"),
        F.col("event_ts").alias("touch_ts"),
        "impression_id_ext",
        "campaign_id",
        "creative_id",
        "channel",
        "customer_ad_id",
        "customer_id",
    ).unionByName(
        newer_rows.select(
            F.col("attribution_journey_id").alias("match_key"),
            F.col("event_ts").alias("touch_ts"),
            "impression_id_ext",
            "campaign_id",
            "creative_id",
            "channel",
            "customer_ad_id",
            "customer_id",
        )
    )
    purchase_for_rank = selected.select(
        F.col("attribution_journey_id").alias("match_key"), "purchase_ts"
    )
    best_touch = _last_touch(touches, purchase_for_rank).select(
        F.col("match_key").alias("attribution_journey_id"),
        "touch_ts",
        "impression_id_ext",
        "campaign_id",
        "creative_id",
        "channel",
        "customer_ad_id",
        "lag_seconds",
    )

    attributed = (
        selected.drop(
            "channel",
            "campaign_id",
            "creative_id",
            "customer_ad_id",
            "_jhash",
            "older_offset",
            "newer_offset",
            "touch_ts_older",
            "touch_ts_newer",
            "_campaign_choice",
            "_campaign_index",
            "_campaign_duration",
            "_campaign_touch_day",
            "_campaign_start",
        )
        .join(best_touch, "attribution_journey_id")
        .withColumn("attribution_status", F.lit("ATTRIBUTED"))
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
