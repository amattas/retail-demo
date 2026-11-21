"""Customer lifecycle: hazard churn with a taper, acquisition, home stores, anonymity."""

from __future__ import annotations

from datetime import date, timedelta
from math import exp

import pytest

from retail_setup.config.generation import GenerationConfig, default_customer_count
from retail_setup.generation import customers as cust


def _lifecycle(**overrides) -> cust.CustomerLifecycle:
    values = dict(
        seed=5,
        history_start=date(2024, 1, 1),
        history_end=date(2025, 6, 30),
        annual_churn_rate=0.20,
        anonymous_share=0.45,
        visits_per_year=9.0,
    )
    values.update(overrides)
    return cust.CustomerLifecycle(**values)


def test_activity_tapers_before_churn_and_is_zero_outside_the_lifetime() -> None:
    # Signed up on day 0, churns on day 100, tapering over the last 60 days.
    assert cust.activity(-1, 0, 100, True) == 0.0
    assert cust.activity(0, 0, 100, True) == 1.0
    assert cust.activity(39, 0, 100, True) == 1.0
    assert cust.activity(40, 0, 100, True) == 1.0
    assert cust.activity(70, 0, 100, True) == pytest.approx(1 - 0.85 * 30 / 60)
    assert cust.activity(99, 0, 100, True) == pytest.approx(1 - 0.85 * 59 / 60)
    assert cust.activity(100, 0, 100, True) == 0.0
    # Abrupt churners keep shopping at their full rate until they stop.
    assert cust.activity(99, 0, 100, False) == 1.0


def test_base_hazard_churns_the_configured_share_of_active_customers_a_year() -> None:
    for rate in (0.15, 0.20, 0.25):
        base = cust.calibrate_base_hazard(rate)
        churned = sum(w * (1 - exp(-base * m)) for w, _, m in cust._customer_types())
        assert churned == pytest.approx(rate, abs=1e-9)
    with pytest.raises(ValueError):
        cust.calibrate_base_hazard(1.0)


def test_hazard_rises_for_budget_infrequent_and_small_spenders() -> None:
    loyal = cust.hazard_multiplier("BRAND_LOYAL", 1.0, 0.0)
    assert cust.hazard_multiplier("BUDGET", 1.0, 0.0) > loyal
    assert cust.hazard_multiplier("BRAND_LOYAL", 0.2, 0.0) > loyal
    assert cust.hazard_multiplier("BRAND_LOYAL", 1.0, -1.0) > loyal


def test_acquisition_replaces_churn_and_identification_hits_the_anonymous_share() -> None:
    lifecycle = _lifecycle()
    mean_hazard = lifecycle.base_hazard * sum(w * m for w, _, m in cust._customer_types())
    years = lifecycle.history_days / 365.25
    assert lifecycle.pre_history_share == pytest.approx(1 / (1 + mean_hazard * years))
    assert 0.6 < lifecycle.pre_history_share < 0.85
    active = lifecycle.expected_active_share()
    assert 0.6 < active < 0.9
    assert lifecycle.identify_probability == pytest.approx(0.55 / active)
    # A very long history cannot reach the identified share; it caps at 1.
    assert _lifecycle(history_end=date(2033, 12, 31)).identify_probability == 1.0


def test_default_customer_count_scales_with_identified_traffic() -> None:
    assert default_customer_count(10, 400, 0.45, 8.0) == round(
        10 * 400 * 365 * 1.0525 * 0.55 / (0.75 * 8.0)
    )
    cfg = GenerationConfig(
        store_type="supercenter",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        store_count=10,
    )
    # About 14k customers per supercenter at 400 receipts a day.
    assert 12_000 < cfg.customer_count / cfg.store_count < 16_000
    luxury = GenerationConfig(
        store_type="luxury",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        store_count=10,
    )
    assert luxury.customer_count < cfg.customer_count
    more_anonymous = GenerationConfig(
        store_type="supercenter",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        store_count=10,
        anonymous_share=0.7,
    )
    assert more_anonymous.customer_count < cfg.customer_count


# ---------------------------------------------------------------------------
# Spark: the lifecycle over a generated year of store visits
# ---------------------------------------------------------------------------

START = date(2024, 1, 1)
END = date(2024, 12, 31)
RECEIPTS_PER_STORE_DAY = 200


@pytest.fixture(scope="module")
def year(spark):
    from pyspark.sql import functions as F

    from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
    from retail_setup.generation.dims import generate_dimensions
    from retail_setup.generation.receipts import _assign_customers

    cfg = GenerationConfig(
        store_type="grocery",
        start_date=START,
        end_date=END,
        store_count=2,
        dc_count=1,
        seed=5,
        transactions_per_store_day=RECEIPTS_PER_STORE_DAY,
    )
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = {name: frame.cache() for name, frame in generate_dimensions(spark, dicts, cfg).items()}
    lifecycle = cust.lifecycle_for(cfg, dicts.profile.resolved_customer_visits_per_year())
    days = (END - START).days + 1
    receipts = spark.range(2 * days * RECEIPTS_PER_STORE_DAY).select(
        (F.col("id") % 2 + 1).alias("store_id"),
        F.date_add(
            F.lit(START), (F.floor(F.col("id") / (2 * RECEIPTS_PER_STORE_DAY))).cast("int")
        ).alias("event_date"),
        F.concat(F.lit("R"), F.col("id").cast("string")).alias("receipt_id_ext"),
    )
    rows = (
        _assign_customers(receipts, dims, lifecycle)
        .select("store_id", "event_date", "customer_id")
        .collect()
    )
    customers = (
        dims["dim_customers"]
        .select(
            F.col("ID").alias("customer_id"),
            "HomeStoreID",
            cust.lifecycle_struct(lifecycle, F.col("ID")).alias("life"),
            cust.visit_tickets(lifecycle, F.col("ID")).alias("tickets"),
        )
        .collect()
    )
    return {
        "cfg": cfg,
        "lifecycle": lifecycle,
        "receipts": rows,
        "customers": {row["customer_id"]: row for row in customers},
        "days": days,
    }


def _visits(year) -> dict[int, list[int]]:
    visits: dict[int, list[int]] = {}
    for row in year["receipts"]:
        if row["customer_id"] is not None:
            visits.setdefault(row["customer_id"], []).append((row["event_date"] - START).days)
    return {customer: sorted(days) for customer, days in visits.items()}


@pytest.mark.spark
def test_anonymous_share_and_home_store_loyalty(year) -> None:
    receipts = year["receipts"]
    identified = [row for row in receipts if row["customer_id"] is not None]
    anonymous = 1 - len(identified) / len(receipts)
    print(f"STAT anonymous share={anonymous:.3f}")
    assert 0.40 <= anonymous <= 0.50, anonymous
    home = sum(
        row["store_id"] == year["customers"][row["customer_id"]]["HomeStoreID"]
        for row in identified
    ) / len(identified)
    print(f"STAT home-store share={home:.3f}")
    assert home >= 0.85, home


@pytest.mark.spark
def test_customers_shop_only_between_signup_and_churn_and_join_over_time(year) -> None:
    visits = _visits(year)
    for customer, days in visits.items():
        life = year["customers"][customer]["life"]
        assert life["signup_day"] <= days[0] and days[-1] < life["churn_day"], customer
    # Acquisition: a share of the base signs up during the year, so first
    # purchases keep arriving instead of all landing in the first weeks.
    joined = [c for c in year["customers"].values() if 0 <= c["life"]["signup_day"] < year["days"]]
    share = len(joined) / len(year["customers"])
    assert 1 - year["lifecycle"].pre_history_share - 0.03 < share
    late_first = sum(days[0] >= 180 for days in visits.values()) / len(visits)
    print(f"STAT joined in history={share:.3f} first purchase in H2={late_first:.3f}")
    assert late_first > 0.08


@pytest.mark.spark
def test_annual_churn_rate_is_within_the_band(year) -> None:
    active = [c["life"] for c in year["customers"].values() if c["life"]["signup_day"] <= 0]
    active = [life for life in active if life["churn_day"] > 0]
    churned = sum(life["churn_day"] < 365 for life in active) / len(active)
    print(f"STAT annual churn={churned:.3f}")
    assert 0.15 <= churned <= 0.25


@pytest.mark.spark
def test_tapering_churners_visit_less_before_they_churn(year) -> None:
    visits = _visits(year)
    before = taper = 0
    before_days = taper_days = 0
    for customer, days in visits.items():
        life = year["customers"][customer]["life"]
        churn = life["churn_day"]
        if not life["tapered"] or not 300 <= churn < year["days"] or life["signup_day"] > 0:
            continue
        before += sum(churn - 240 <= day < churn - 60 for day in days)
        taper += sum(churn - 60 <= day < churn for day in days)
        before_days += 180
        taper_days += 60
    ratio = (taper / taper_days) / (before / before_days)
    print(f"STAT pre-churn visit rate ratio={ratio:.3f}")
    # Linear decline to 15%: about (1 + 0.15) / 2 of the earlier rate.
    assert 0.4 < ratio < 0.75, ratio


@pytest.mark.spark
def test_recency_does_not_perfectly_separate_forward_churn(year) -> None:
    visits = _visits(year)
    snapshot, horizon = 270, 90
    scored = []
    for days in visits.values():
        past = [day for day in days if snapshot - 180 <= day < snapshot]
        if not past:
            continue
        churned = not any(snapshot <= day < snapshot + horizon for day in days)
        scored.append((snapshot - past[-1], churned))
    positives = [score for score, label in scored if label]
    negatives = [score for score, label in scored if not label]
    assert positives and negatives
    wins = sum((p > n) + 0.5 * (p == n) for p in positives for n in negatives)
    auc = wins / (len(positives) * len(negatives))
    print(f"STAT recency AUC={auc:.3f} churn label rate={len(positives) / len(scored):.3f}")
    assert 0.6 < auc < 0.95, auc


@pytest.mark.spark
def test_heavy_customers_shop_at_most_a_few_times_a_week(year) -> None:
    visits = _visits(year)
    rates = []
    for customer, days in visits.items():
        life = year["customers"][customer]["life"]
        start = max(0, life["signup_day"])
        end = min(year["days"], life["churn_day"])
        if end - start >= 120:
            rates.append(len(days) / (end - start) * 365)
    rates.sort()
    p999 = rates[int(len(rates) * 0.999)]
    print(
        f"STAT trips/year median={rates[len(rates) // 2]:.1f} p99.9={p999:.1f} max={rates[-1]:.1f}"
    )
    assert rates[len(rates) // 2] < 12
    assert p999 <= 200 and rates[-1] <= 260
    assert max(c["tickets"] for c in year["customers"].values()) <= round(
        cust.MAX_VISITS_PER_YEAR / year["lifecycle"].ticket_unit
    )


@pytest.mark.spark
def test_activity_column_matches_the_python_lifecycle(spark, year) -> None:
    from pyspark.sql import functions as F

    lifecycle = year["lifecycle"]
    ids = list(year["customers"])[:200]
    days = [START + timedelta(days=offset) for offset in (0, 45, 200, 364)]
    frame = spark.createDataFrame([(i, day) for i in ids for day in days], "id long, day date")
    rows = frame.select(
        "id", "day", cust.activity_column(lifecycle, F.col("id"), F.col("day")).alias("a")
    ).collect()
    for row in rows:
        life = year["customers"][row["id"]]["life"]
        expected = cust.activity(
            (row["day"] - START).days, life["signup_day"], life["churn_day"], life["tapered"]
        )
        assert row["a"] == pytest.approx(expected), row
