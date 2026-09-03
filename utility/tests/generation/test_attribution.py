"""Tests for IMP-007 deterministic marketing attribution (attribution.py)."""

from datetime import date, datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation import attribution, marketing, online_orders, promotions
from retail_setup.generation import receipts as receipts_mod
from retail_setup.generation import returns as returns_mod
from retail_setup.generation.attribution import (
    ATTRIBUTION_WINDOW_DAYS,
    _last_touch,
)
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.schemas import column_names


def _build_pre_attribution(spark, cfg, dicts):
    """Mirror engine.generate_all up to (but excluding) the attribution step,
    so tests can call attribution.generate_attribution directly/repeatedly
    against an identical, already-materialized upstream snapshot.
    """
    t = dict(generate_dimensions(spark, dicts, cfg))
    sales = receipts_mod.generate_receipts_group(spark, t, dicts.profile, cfg)
    rets = returns_mod.generate_returns(spark, sales, t, cfg)
    t["fact_receipts"] = sales["fact_receipts"].unionByName(rets["fact_receipts"])
    t["fact_receipt_lines"] = sales["fact_receipt_lines"].unionByName(rets["fact_receipt_lines"])
    online = online_orders.generate_online_orders(spark, t, dicts.profile, cfg)
    t["fact_online_order_headers"] = online["fact_online_order_headers"]
    t["fact_online_order_lines"] = online["fact_online_order_lines"]
    t["fact_payments"] = (
        sales["fact_payments"].unionByName(rets["fact_payments"]).unionByName(online["payments"])
    )
    promos, promo_lines = promotions.generate_promotions(spark, sales)
    t["fact_promotions"], t["fact_promo_lines"] = promos, promo_lines
    t["fact_marketing"] = marketing.generate_marketing(spark, t, cfg)
    return t


@pytest.fixture(scope="module")
def cfg():
    return GenerationConfig(
        store_type="grocery",
        start_date=date(2025, 6, 1),
        end_date=date(2025, 6, 10),
        store_count=3,
        dc_count=1,
        customer_count=250,
        seed=77,
        transactions_per_store_day=40,
        online_orders_per_day=25,
        return_rate=0.05,
    )


@pytest.fixture(scope="module")
def dicts():
    return load_dictionaries(default_dictionary_root(), "grocery")


@pytest.fixture(scope="module")
def pre(spark, cfg, dicts):
    # Attribution joins several deeply derived frames. Local Spark can choose
    # an auto-broadcast plan whose serialized lineage exceeds the test JVM
    # heap even though the fixture data is small; use deterministic shuffle
    # joins here instead. Explicit production broadcast hints remain intact.
    setting = "spark.sql.autoBroadcastJoinThreshold"
    previous = spark.conf.get(setting)
    spark.conf.set(setting, "-1")
    try:
        t = _build_pre_attribution(spark, cfg, dicts)
        for name in t:
            t[name] = t[name].cache()
        yield t
    finally:
        spark.conf.set(setting, previous)


@pytest.fixture(scope="module")
def attr(spark, cfg, pre):
    return attribution.generate_attribution(spark, pre, cfg)


@pytest.fixture(scope="module")
def fma(attr):
    return attr["fact_marketing_attribution"]


# --------------------------------------------------------------------------
# Schema / contract
# --------------------------------------------------------------------------


def test_contract_columns(fma):
    assert fma.columns == column_names("fact_marketing_attribution")


def test_contract_columns_on_enriched_tables(attr):
    assert attr["fact_receipts"].columns == column_names("fact_receipts")
    assert attr["fact_online_order_headers"].columns == column_names("fact_online_order_headers")
    assert attr["fact_payments"].columns == column_names("fact_payments")
    assert attr["fact_promotions"].columns == column_names("fact_promotions")
    assert attr["fact_marketing"].columns == column_names("fact_marketing")


def test_attribution_model_and_window_constant(fma):
    assert fma.filter(F.col("attribution_model") != "LAST_TOUCH_7D").count() == 0
    assert fma.filter(F.col("attribution_window_days") != ATTRIBUTION_WINDOW_DAYS).count() == 0


# --------------------------------------------------------------------------
# One row per purchase; XOR purchase keys
# --------------------------------------------------------------------------


def test_one_row_per_sale_or_order(pre, fma):
    sale_count = pre["fact_receipts"].filter(F.col("receipt_type") == "SALE").count()
    order_count = pre["fact_online_order_headers"].count()
    assert fma.count() == sale_count + order_count


def test_purchase_key_xor(fma):
    store = fma.filter(F.col("purchase_type") == "STORE")
    online = fma.filter(F.col("purchase_type") == "ONLINE")
    assert (
        store.filter(F.col("receipt_id_ext").isNull() | F.col("order_id_ext").isNotNull()).count()
        == 0
    )
    assert (
        online.filter(F.col("order_id_ext").isNull() | F.col("receipt_id_ext").isNotNull()).count()
        == 0
    )
    assert fma.filter(~F.col("purchase_type").isin("STORE", "ONLINE")).count() == 0


def test_attribution_id_unique(fma):
    assert fma.select("attribution_id").distinct().count() == fma.count()


# --------------------------------------------------------------------------
# Status distribution
# --------------------------------------------------------------------------


def test_status_values_and_presence(fma):
    valid = {"ATTRIBUTED", "UNATTRIBUTED_NO_JOURNEY", "PAYMENT_DECLINED", "RECONCILIATION_FAILED"}
    statuses = {r.attribution_status for r in fma.select("attribution_status").distinct().collect()}
    assert statuses <= valid
    assert fma.filter(F.col("attribution_status") == "ATTRIBUTED").count() > 0
    assert fma.filter(F.col("attribution_status") == "UNATTRIBUTED_NO_JOURNEY").count() > 0


def test_journey_id_set_iff_attributed(fma):
    mismatched = fma.filter(
        (F.col("attribution_status") == "ATTRIBUTED") != F.col("attribution_journey_id").isNotNull()
    )
    assert mismatched.count() == 0


def test_selection_rate_bounded(fma):
    reconciled = fma.filter(
        F.col("attribution_status").isin("ATTRIBUTED", "UNATTRIBUTED_NO_JOURNEY")
    )
    attributed = fma.filter(F.col("attribution_status") == "ATTRIBUTED")
    share = attributed.count() / reconciled.count()
    # deterministic ~5% draw; allow generous slack for small-sample noise
    assert 0.01 < share < 0.12, share


# --------------------------------------------------------------------------
# One-to-one journey <-> purchase cardinality
# --------------------------------------------------------------------------


def test_one_purchase_per_journey_and_vice_versa(fma):
    attributed = fma.filter(F.col("attribution_status") == "ATTRIBUTED")
    n = attributed.count()
    assert attributed.select("attribution_journey_id").distinct().count() == n
    assert n == fma.filter(F.col("attribution_journey_id").isNotNull()).count()


def test_fact_marketing_two_touches_per_journey(attr):
    mk = attr["fact_marketing"]
    journeys = mk.filter(F.col("attribution_journey_id").isNotNull())
    counts = journeys.groupBy("attribution_journey_id").agg(F.count("*").alias("n"))
    assert counts.filter(F.col("n") != 2).count() == 0
    # every synthetic touch has a real customer/ad linkage
    assert (
        journeys.filter(F.col("customer_ad_id").isNull() | F.col("customer_id").isNull()).count()
        == 0
    )


def test_fact_marketing_journeys_match_attributed_purchases(attr, fma):
    mk_journeys = (
        attr["fact_marketing"]
        .filter(F.col("attribution_journey_id").isNotNull())
        .select("attribution_journey_id")
        .distinct()
    )
    attributed_journeys = (
        fma.filter(F.col("attribution_status") == "ATTRIBUTED")
        .select("attribution_journey_id")
        .distinct()
    )
    assert mk_journeys.exceptAll(attributed_journeys).count() == 0
    assert attributed_journeys.exceptAll(mk_journeys).count() == 0


def test_attributed_journeys_reuse_background_campaign_catalog(pre, fma):
    background_campaigns = pre["fact_marketing"].select("campaign_id").distinct()
    attributed_campaigns = (
        fma.filter(F.col("attribution_status") == "ATTRIBUTED").select("campaign_id").distinct()
    )

    assert attributed_campaigns.join(background_campaigns, "campaign_id", "left_anti").count() == 0


# --------------------------------------------------------------------------
# Window / lag correctness on the derived (not copied) last touch
# --------------------------------------------------------------------------


def test_attributed_touch_within_window(fma):
    attributed = fma.filter(F.col("attribution_status") == "ATTRIBUTED")
    assert attributed.filter(F.col("touch_ts").isNull()).count() == 0
    bad = attributed.filter(
        (F.col("lag_seconds") < 0) | (F.col("lag_seconds") > ATTRIBUTION_WINDOW_DAYS * 86400)
    )
    assert bad.count() == 0
    assert (
        attributed.filter(
            F.col("purchase_ts").cast("long") - F.col("touch_ts").cast("long")
            != F.col("lag_seconds")
        ).count()
        == 0
    )


def test_attributed_touches_stay_inside_requested_history(fma, cfg):
    attributed = fma.filter(F.col("attribution_status") == "ATTRIBUTED")
    assert attributed.filter(F.to_date("touch_ts") < F.lit(cfg.start_date)).count() == 0
    assert attributed.filter(F.to_date("purchase_ts") > F.lit(cfg.end_date)).count() == 0


def test_non_attributed_touch_fields_null(fma):
    non_attr = fma.filter(F.col("attribution_status") != "ATTRIBUTED")
    for c in [
        "touch_ts",
        "impression_id_ext",
        "campaign_id",
        "creative_id",
        "channel",
        "customer_ad_id",
        "lag_seconds",
    ]:
        assert non_attr.filter(F.col(c).isNotNull()).count() == 0, c


# --------------------------------------------------------------------------
# Enrichment: derived mapping matches what lands on receipts/orders/payments/promotions
# --------------------------------------------------------------------------


def test_receipt_enrichment_matches_attribution(attr, fma):
    attributed_store = fma.filter(
        (F.col("attribution_status") == "ATTRIBUTED") & (F.col("purchase_type") == "STORE")
    ).select("receipt_id_ext", "attribution_journey_id", "campaign_id", "impression_id_ext")
    j = attr["fact_receipts"].join(attributed_store, "receipt_id_ext")
    assert j.count() == attributed_store.count()
    mismatched = (
        attr["fact_receipts"]
        .alias("r")
        .join(attributed_store.alias("a"), "receipt_id_ext")
        .filter(
            (F.col("r.attribution_journey_id") != F.col("a.attribution_journey_id"))
            | (F.col("r.campaign_id") != F.col("a.campaign_id"))
            | (F.col("r.impression_id_ext") != F.col("a.impression_id_ext"))
        )
    )
    assert mismatched.count() == 0
    # every other SALE receipt keeps NULL placeholders
    other_sales = (
        attr["fact_receipts"]
        .filter(F.col("receipt_type") == "SALE")
        .join(attributed_store.select("receipt_id_ext"), "receipt_id_ext", "left_anti")
    )
    assert other_sales.filter(F.col("attribution_journey_id").isNotNull()).count() == 0


def test_online_enrichment_matches_attribution(attr, fma):
    attributed_online = fma.filter(
        (F.col("attribution_status") == "ATTRIBUTED") & (F.col("purchase_type") == "ONLINE")
    ).select("order_id_ext", "attribution_journey_id", "campaign_id", "impression_id_ext")
    mismatched = (
        attr["fact_online_order_headers"]
        .alias("o")
        .join(attributed_online.alias("a"), "order_id_ext")
        .filter(
            (F.col("o.attribution_journey_id") != F.col("a.attribution_journey_id"))
            | (F.col("o.campaign_id") != F.col("a.campaign_id"))
            | (F.col("o.impression_id_ext") != F.col("a.impression_id_ext"))
        )
    )
    assert mismatched.count() == 0
    j = attr["fact_online_order_headers"].join(attributed_online, "order_id_ext")
    assert j.count() == attributed_online.count()


def test_payment_enrichment_matches_attribution(attr, fma):
    attributed = fma.filter(F.col("attribution_status") == "ATTRIBUTED")
    pay_key = F.coalesce(F.col("receipt_id_ext"), F.col("order_id_ext"))
    enrich = attributed.withColumn("pay_key", pay_key).select("pay_key", "attribution_journey_id")
    payments = attr["fact_payments"].withColumn(
        "pay_key", F.coalesce(F.col("receipt_id_ext"), F.col("order_id_ext"))
    )
    mismatched = (
        payments.alias("p")
        .join(enrich.alias("e"), "pay_key")
        .filter(F.col("p.attribution_journey_id") != F.col("e.attribution_journey_id"))
    )
    assert mismatched.count() == 0
    assert payments.join(enrich, "pay_key").count() == enrich.count()


def test_promotion_enrichment_matches_attribution(attr, fma):
    attributed_store = fma.filter(
        (F.col("attribution_status") == "ATTRIBUTED") & (F.col("purchase_type") == "STORE")
    ).select("receipt_id_ext", "attribution_journey_id")
    promos_with_journey = attr["fact_promotions"].filter(
        F.col("attribution_journey_id").isNotNull()
    )
    # every promotion row carrying a journey must belong to an attributed receipt
    assert promos_with_journey.join(attributed_store, "receipt_id_ext", "left_anti").count() == 0
    mismatched = (
        promos_with_journey.alias("p")
        .join(attributed_store.alias("a"), "receipt_id_ext")
        .filter(F.col("p.attribution_journey_id") != F.col("a.attribution_journey_id"))
    )
    assert mismatched.count() == 0


# --------------------------------------------------------------------------
# Cent-equation reconciliation
# --------------------------------------------------------------------------


def test_gross_discount_net_universal(fma):
    assert (
        fma.filter(
            F.col("gross_subtotal_cents") - F.col("discount_cents") != F.col("net_subtotal_cents")
        ).count()
        == 0
    )


def test_net_tax_total_universal(fma):
    assert (
        fma.filter(F.col("net_subtotal_cents") + F.col("tax_cents") != F.col("total_cents")).count()
        == 0
    )


def test_payment_equals_total_for_reconciled_statuses(fma):
    reconciled = fma.filter(
        F.col("attribution_status").isin("ATTRIBUTED", "UNATTRIBUTED_NO_JOURNEY")
    )
    assert reconciled.filter(F.col("payment_cents") != F.col("total_cents")).count() == 0


def test_attributed_revenue_cents(fma):
    attributed = fma.filter(F.col("attribution_status") == "ATTRIBUTED")
    assert (
        attributed.filter(F.col("attributed_revenue_cents") != F.col("net_subtotal_cents")).count()
        == 0
    )
    non_attr = fma.filter(F.col("attribution_status") != "ATTRIBUTED")
    assert non_attr.filter(F.col("attributed_revenue_cents") != 0).count() == 0


def test_receipts_and_orders_gross_discount_net(attr):
    for tbl in ["fact_receipts", "fact_online_order_headers"]:
        df = attr[tbl]
        bad = df.filter(
            F.col("gross_subtotal_cents") - F.col("discount_cents") != F.col("subtotal_cents")
        )
        assert bad.count() == 0, tbl


# --------------------------------------------------------------------------
# Determinism
# --------------------------------------------------------------------------


def test_generate_attribution_is_deterministic(spark, cfg, pre):
    run1 = attribution.generate_attribution(spark, pre, cfg)
    run2 = attribution.generate_attribution(spark, pre, cfg)
    for key in [
        "fact_marketing_attribution",
        "fact_receipts",
        "fact_online_order_headers",
        "fact_payments",
        "fact_promotions",
        "fact_marketing",
    ]:
        a, b = run1[key], run2[key]
        assert a.exceptAll(b).count() == 0, key
        assert b.exceptAll(a).count() == 0, key


# --------------------------------------------------------------------------
# _last_touch: direct, isolated unit tests of the ranking/window join itself
# --------------------------------------------------------------------------

_TOUCH_SCHEMA = StructType(
    [
        StructField("match_key", StringType()),
        StructField("touch_ts", TimestampType()),
        StructField("impression_id_ext", StringType()),
        StructField("campaign_id", StringType()),
        StructField("creative_id", StringType()),
        StructField("channel", StringType()),
        StructField("customer_ad_id", StringType()),
        StructField("customer_id", LongType()),
    ]
)

_PURCHASE_SCHEMA = StructType(
    [
        StructField("match_key", StringType()),
        StructField("purchase_ts", TimestampType()),
    ]
)


def _ts(offset_seconds: float, base: datetime) -> datetime:
    return base + timedelta(seconds=offset_seconds)


@pytest.fixture(scope="module")
def last_touch_case(spark):
    base = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    window_s = ATTRIBUTION_WINDOW_DAYS * 86400

    touches = [
        # P1: two touches within window, same timestamp -> tie broken by
        # impression_id_ext desc (IMP2 > IMP1).
        ("P1", _ts(-3600, base), "IMP1", "CAMP1", "CREA1", "EMAIL", "AD1", 1),
        ("P1", _ts(-3600, base), "IMP2", "CAMP1", "CREA1", "EMAIL", "AD1", 1),
        # P2: an older in-window touch and a newer in-window touch -> newer wins.
        ("P2", _ts(-5 * 86400, base), "IMPOLD", "CAMP2", "CREA2", "SOCIAL", "AD2", 2),
        ("P2", _ts(-2 * 86400, base), "IMPNEW", "CAMP2", "CREA2", "SOCIAL", "AD2", 2),
        # P3: only a touch strictly AFTER the purchase -> must be excluded
        # (touch_ts <= purchase_ts is a join condition, not just ORDER BY).
        ("P3", _ts(3600, base), "IMPFUTURE", "CAMP3", "CREA3", "SEARCH", "AD3", 3),
        # P4: only a touch more than 7 days before purchase -> excluded (out of window).
        ("P4", _ts(-(window_s + 3600), base), "IMPTOOOLD", "CAMP4", "CREA4", "DISPLAY", "AD4", 4),
        # P5: touch exactly at the 7-day boundary -> inclusive, must be selected.
        ("P5", _ts(-window_s, base), "IMPBOUND", "CAMP5", "CREA5", "GOOGLE", "AD5", 5),
    ]
    purchases = [
        ("P1", base),
        ("P2", base),
        ("P3", base),
        ("P4", base),
        ("P5", base),
        # P6: no touches at all -> left join keeps purchase with NULL touch fields.
        ("P6", base),
    ]
    return spark.createDataFrame(touches, _TOUCH_SCHEMA), spark.createDataFrame(
        purchases, _PURCHASE_SCHEMA
    )


def test_last_touch_tie_break_by_impression_id_desc(last_touch_case):
    touches, purchases = last_touch_case
    result = _last_touch(touches, purchases).filter(F.col("match_key") == "P1").collect()
    assert len(result) == 1
    assert result[0].impression_id_ext == "IMP2"


def test_last_touch_prefers_newer_touch(last_touch_case):
    touches, purchases = last_touch_case
    result = _last_touch(touches, purchases).filter(F.col("match_key") == "P2").collect()
    assert len(result) == 1
    assert result[0].impression_id_ext == "IMPNEW"
    assert result[0].lag_seconds == 2 * 86400


def test_last_touch_excludes_touch_after_purchase(last_touch_case):
    touches, purchases = last_touch_case
    result = _last_touch(touches, purchases).filter(F.col("match_key") == "P3").collect()
    assert len(result) == 1
    assert result[0].impression_id_ext is None
    assert result[0].lag_seconds is None


def test_last_touch_excludes_touch_outside_window(last_touch_case):
    touches, purchases = last_touch_case
    result = _last_touch(touches, purchases).filter(F.col("match_key") == "P4").collect()
    assert len(result) == 1
    assert result[0].impression_id_ext is None


def test_last_touch_boundary_is_inclusive(last_touch_case):
    touches, purchases = last_touch_case
    result = _last_touch(touches, purchases).filter(F.col("match_key") == "P5").collect()
    assert len(result) == 1
    assert result[0].impression_id_ext == "IMPBOUND"
    assert result[0].lag_seconds == ATTRIBUTION_WINDOW_DAYS * 86400


def test_last_touch_no_touch_yields_null(last_touch_case):
    touches, purchases = last_touch_case
    result = _last_touch(touches, purchases).filter(F.col("match_key") == "P6").collect()
    assert len(result) == 1
    assert result[0].impression_id_ext is None
    assert result[0].lag_seconds is None


def test_last_touch_one_row_per_purchase(last_touch_case):
    touches, purchases = last_touch_case
    result = _last_touch(touches, purchases)
    assert result.count() == purchases.count()
