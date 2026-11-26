"""Logistics incident plan and delivery timing (pure Python, no Spark)."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone

import pytest

from retail_setup.generation import calibration as cal
from retail_setup.generation.incidents import (
    DELAY_REASONS,
    INCIDENT_TYPES,
    Incident,
    IncidentCalendar,
    IncidentNetwork,
    StoreIncidentView,
    end_of_history_incident,
    local_instant,
    plan_delivery,
    plan_incidents,
    supplier_group,
    to_local,
    traffic_delay_minutes,
)
from retail_setup.generation.network import estimated_unload_minutes, unload_minutes
from retail_setup.generation.stream_events import IncidentPlanner
from retail_setup.generation.stream_supply import incident_transitions

UTC = timezone.utc
NETWORK = IncidentNetwork(
    store_dc={1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 3},
    store_zones={s: "America/Chicago" for s in range(1, 7)},
    store_geo={1: 10, 2: 10, 3: 11, 4: 10, 5: 12, 6: 13},
    geo_state={10: "MN", 11: "TX", 12: "CA", 13: "NY"},
)


def _years(start: int, years: int) -> list[Incident]:
    return plan_incidents(5, NETWORK, date(start, 1, 1), date(start + years - 1, 12, 31))


def test_the_plan_is_deterministic_and_windows_agree() -> None:
    whole = plan_incidents(5, NETWORK, date(2024, 1, 1), date(2025, 12, 31))
    assert whole == plan_incidents(5, NETWORK, date(2024, 1, 1), date(2025, 12, 31))
    first = plan_incidents(5, NETWORK, date(2024, 1, 1), date(2024, 12, 31))
    second = plan_incidents(5, NETWORK, date(2025, 1, 1), date(2025, 12, 31))
    assert sorted(first + second, key=lambda i: (i.started_at, i.incident_id)) == whole
    assert whole != plan_incidents(6, NETWORK, date(2024, 1, 1), date(2025, 12, 31))
    assert len({incident.incident_id for incident in whole}) == len(whole)
    assert {incident.incident_type for incident in whole} <= set(INCIDENT_TYPES)


def test_random_incidents_occur_a_few_times_per_dc_and_year() -> None:
    years = 20
    incidents = _years(2010, years)
    per_dc_year = {
        kind: count / (years * len(NETWORK.dc_ids()))
        for kind, count in Counter(incident.incident_type for incident in incidents).items()
    }
    print(f"STAT incidents per DC-year {per_dc_year}")
    assert per_dc_year["DC_OUTAGE"] == pytest.approx(cal.DC_OUTAGES_PER_DC_YEAR, rel=0.35)
    assert per_dc_year["LANE_DELAY"] == pytest.approx(cal.LANE_DELAYS_PER_DC_YEAR, rel=0.25)
    assert per_dc_year["SUPPLIER_SHORT_SHIP"] == pytest.approx(
        cal.SUPPLIER_SHORT_SHIPS_PER_DC_YEAR, rel=0.3
    )
    # Two strained holiday weekends a year, each with HOLIDAY_CAPACITY_SHARE.
    assert per_dc_year["HOLIDAY_CAPACITY"] == pytest.approx(2 * cal.HOLIDAY_CAPACITY_SHARE, rel=0.2)


def test_each_incident_affects_the_stores_its_source_serves() -> None:
    for incident in _years(2020, 3):
        assert incident.resolved_at > incident.started_at
        assert incident.severity in ("LOW", "MEDIUM", "HIGH")
        if incident.incident_type == "LANE_DELAY":
            assert incident.affected_stores == (incident.store_id,)
            assert NETWORK.store_dc[incident.store_id] == incident.dc_id
            assert incident.delay_minutes > 0
        elif incident.incident_type != "WEATHER":
            assert incident.store_id is None
            assert incident.affected_stores == NETWORK.stores_of(incident.dc_id)
    short = next(i for i in _years(2020, 3) if i.incident_type == "SUPPLIER_SHORT_SHIP")
    low, high = cal.SUPPLIER_FILL_RATE
    assert low <= short.fill_rate <= high and short.supplier_group is not None


def test_weather_incidents_follow_severe_days_and_the_season() -> None:
    # Every third day is severe everywhere: each is a one-day run.
    days = [date(2020, 1, 1) + timedelta(days=n) for n in range(0, 366 * 4, 3)]
    severe = {(geo, day) for geo in (10, 11, 12, 13) for day in days}
    weather = [
        i
        for i in plan_incidents(5, NETWORK, date(2020, 1, 1), date(2023, 12, 31), severe)
        if i.incident_type == "WEATHER"
    ]
    assert weather
    for incident in weather:
        stores = tuple(
            s
            for s, geo in sorted(NETWORK.store_geo.items())
            if geo == incident.geography_id and NETWORK.store_dc[s] == incident.dc_id
        )
        assert incident.affected_stores == stores
        local = [
            to_local(t, "America/Chicago") for t in (incident.started_at, incident.resolved_at)
        ]
        assert local[1] - local[0] == timedelta(days=1)
    # Geography 10 has stores of two DCs: one incident per DC on the same day.
    by_day = Counter((i.started_at.date(), i.geography_id) for i in weather if i.geography_id == 10)
    assert set(by_day.values()) == {2}
    # New York (cold) disruptions concentrate in winter.
    months = Counter(i.started_at.month for i in weather if i.geography_id == 13)
    winter, summer = sum(months[m] for m in (12, 1, 2)), sum(months[m] for m in (6, 7, 8))
    print(f"STAT cold-climate weather incidents winter={winter} summer={summer}")
    assert winter > 1.8 * summer
    # A run of severe days is one incident spanning the run.
    run = {(13, date(2021, 1, 10) + timedelta(days=n)) for n in range(3)}
    found = [
        i
        for i in plan_incidents(1, NETWORK, date(2021, 1, 9), date(2021, 1, 14), run)
        if i.incident_type == "WEATHER"
    ]
    assert len(found) <= 1
    if found:
        local = [
            to_local(t, "America/Chicago") for t in (found[0].started_at, found[0].resolved_at)
        ]
        assert local[1] - local[0] == timedelta(days=3)


HISTORY_END = date(2025, 8, 17)
CHICAGO = "America/Chicago"


def test_every_history_ends_inside_a_prolonged_outage_at_the_busiest_dc() -> None:
    outage = end_of_history_incident(5, NETWORK, HISTORY_END)
    assert outage is not None
    # DC 1 serves the most stores; the outage stops all of its dispatches.
    assert outage.incident_id == "INC-OUT-001-20250815-END"
    assert (outage.incident_type, outage.dc_id, outage.store_id) == ("DC_OUTAGE", 1, None)
    assert outage.affected_stores == NETWORK.stores_of(1) == (1, 2, 3)
    assert outage.severity == "HIGH"
    cause = outage.description.split("(")[1].split(")")[0]
    assert cause in ("warehouse system outage", "labor shortage")
    # It starts early (DC local) two days before the last day and is still
    # open when the last local day closes; it resolves in the live stream.
    start = to_local(outage.started_at, CHICAGO)
    assert start.date() == HISTORY_END - timedelta(days=2)
    low, high = cal.END_OF_HISTORY_OUTAGE_START_MINUTES
    assert low <= start.hour * 60 + start.minute <= high
    hours = (outage.resolved_at - outage.started_at) / timedelta(hours=1)
    assert cal.END_OF_HISTORY_OUTAGE_HOURS[0] <= hours <= cal.END_OF_HISTORY_OUTAGE_HOURS[1]
    assert f"no dispatches for {hours:.0f}h" in outage.description
    closes = local_instant(HISTORY_END + timedelta(days=1), 0, CHICAGO)
    assert outage.active_at(closes - timedelta(seconds=1)) and outage.active_at(closes)
    # Deterministic in the seed and the last day only.
    assert end_of_history_incident(5, NETWORK, HISTORY_END) == outage
    later = end_of_history_incident(5, NETWORK, HISTORY_END + timedelta(days=1))
    assert later is not None and later.started_at > outage.started_at
    # Equally busy DCs: a stable draw picks one, and both get picked over time.
    tied = IncidentNetwork(store_dc={1: 1, 2: 2}, store_zones={1: CHICAGO, 2: CHICAGO})
    picked = {
        end_of_history_incident(7, tied, HISTORY_END + timedelta(days=n)).dc_id  # type: ignore[union-attr]
        for n in range(20)
    }
    assert picked == {1, 2}
    assert end_of_history_incident(5, IncidentNetwork(store_dc={}), HISTORY_END) is None


def test_the_end_of_history_outage_leaves_every_other_incident_unchanged() -> None:
    first = HISTORY_END - timedelta(days=400)
    base = plan_incidents(5, NETWORK, first, HISTORY_END)
    anchored = plan_incidents(5, NETWORK, first, HISTORY_END, history_end=HISTORY_END)
    outage = end_of_history_incident(5, NETWORK, HISTORY_END)
    assert [i for i in anchored if i != outage] == base
    assert anchored.count(outage) == 1  # type: ignore[arg-type]
    assert len({i.incident_id for i in anchored}) == len(anchored)
    # Only a window holding its start day plans it.
    day = HISTORY_END - timedelta(days=2)
    assert outage in plan_incidents(5, NETWORK, day, day, history_end=HISTORY_END)
    assert outage not in plan_incidents(
        5, NETWORK, day + timedelta(days=1), HISTORY_END, history_end=HISTORY_END
    )


def test_the_stream_plans_the_same_outage_and_resolves_it_at_its_end() -> None:
    outage = end_of_history_incident(5, NETWORK, HISTORY_END)
    assert outage is not None
    planner = IncidentPlanner(5, NETWORK, lambda first, last: set(), history_end=HISTORY_END)
    # The stream starts a few hours after history ends.
    start = local_instant(HISTORY_END + timedelta(days=1), 3 * 60, CHICAGO)
    start_ms = int(start.timestamp() * 1000)
    window = planner.window(None, start_ms)
    assert outage in window
    started, resolved = incident_transitions(window, None, start_ms)
    assert outage in started and outage not in resolved
    # It resolves in the batch whose window holds its end, and only there.
    end_ms = int(outage.resolved_at.timestamp() * 1000)
    later = planner.window(end_ms - 60_000, end_ms + 60_000)
    started, resolved = incident_transitions(later, end_ms - 60_000, end_ms + 60_000)
    assert outage in resolved and outage not in started
    # Without the history end the stream would never see it.
    blind = IncidentPlanner(5, NETWORK, lambda first, last: set())
    assert outage not in blind.window(None, start_ms)


def test_calendar_queries() -> None:
    t0 = datetime(2025, 3, 3, 12, tzinfo=UTC)

    def incident(kind: str, start_h: int, end_h: int, **extra) -> Incident:
        return Incident(
            f"I-{kind}-{start_h}",
            kind,
            extra.pop("dc_id", 1),
            extra.pop("store_id", None),
            t0 + timedelta(hours=start_h),
            t0 + timedelta(hours=end_h),
            "LOW",
            (1, 2, 3),
            "",
            **extra,
        )

    calendar = IncidentCalendar(
        [
            incident("DC_OUTAGE", 0, 5),
            incident("DC_OUTAGE", 4, 9),
            incident("SUPPLIER_SHORT_SHIP", 0, 48, fill_rate=0.3, supplier_group=2),
        ]
    )
    # Chained outages hold the DC until the last one ends.
    assert calendar.outage_end(1, t0 + timedelta(hours=1)) == t0 + timedelta(hours=9)
    assert calendar.outage_end(1, t0 + timedelta(hours=10)) is None
    assert calendar.outage_end(2, t0) is None
    assert calendar.held_dcs(t0 + timedelta(hours=6)) == frozenset({1})
    assert calendar.held_dcs(t0 + timedelta(hours=9)) == frozenset()
    assert calendar.supplier_fill(1, 2, t0 + timedelta(hours=1)) == 0.3
    assert calendar.supplier_fill(1, 3, t0 + timedelta(hours=1)) == 1.0
    assert calendar.supplier_fill(1, 2, t0 + timedelta(hours=48)) == 1.0
    view = calendar.dc_view(1, "UTC")
    assert view.fill_rate(2, datetime(2025, 3, 3, 13)) == 0.3
    assert 0 <= supplier_group(5, 42) < cal.SUPPLIER_GROUPS


SLOT = 10 * 60  # 10:00 local
TRANSIT = 120
READY = datetime(2025, 3, 4, 23, 59, 59)  # a Tuesday's close


def test_an_undisturbed_load_arrives_as_planned() -> None:
    timing = None
    for store in range(1, 50):
        timing = plan_delivery(
            9, store_id=store, dc_id=1, ready=READY, slot_minute=SLOT, transit_minutes=TRANSIT
        )
        if timing.delay_minutes == 0:
            break
    assert timing is not None and timing.delay_minutes == 0 and timing.delay_reason is None
    assert timing.planned_loading == timing.loading == datetime(2025, 3, 5, 10)
    assert timing.departure == timing.loading + timedelta(minutes=cal.TRUCK_LOAD_MINUTES)
    assert timing.planned_arrival == timing.arrival == timing.departure + timedelta(minutes=TRANSIT)


def _no_traffic_store(ready: datetime) -> int:
    for store in range(1, 200):
        if traffic_delay_minutes(9, store, 1, TRANSIT, ready.replace(hour=10, minute=0)) == 0:
            return store
    raise AssertionError("no store without traffic")


def test_no_load_arrives_on_a_closure_day() -> None:
    eve = datetime(2025, 12, 24, 23, 59, 59)
    store = _no_traffic_store(datetime(2025, 12, 26, 10))
    timing = plan_delivery(
        9,
        store_id=store,
        dc_id=1,
        ready=eve,
        slot_minute=SLOT,
        transit_minutes=TRANSIT,
        closed=lambda day: (day.month, day.day) == (12, 25),
    )
    # Christmas is planned around: the load ships the 26th, on plan.
    assert timing.planned_loading == datetime(2025, 12, 26, 10)
    assert timing.arrival.date() == date(2025, 12, 26) and timing.delay_minutes == 0


def test_incidents_delay_loads_with_a_reason() -> None:
    store = _no_traffic_store(datetime(2025, 3, 5, 10))
    base = {
        "store_id": store,
        "dc_id": 1,
        "ready": READY,
        "slot_minute": SLOT,
        "transit_minutes": TRANSIT,
    }
    day = datetime(2025, 3, 5)
    # An outage over the slot holds the load to the next day's slot.
    outage = StoreIncidentView(outages=((day + timedelta(hours=8), day + timedelta(hours=20), 0),))
    held = plan_delivery(9, view=outage, **base)
    assert held.loading == datetime(2025, 3, 6, 10)
    assert (held.delay_minutes, held.delay_reason) == (24 * 60, "DC_OUTAGE")
    # Holiday capacity delays loading; a lane incident at departure and weather
    # at arrival delay the run. The largest cause names the delay.
    window = (day, day + timedelta(days=1))
    mixed = plan_delivery(
        9,
        view=StoreIncidentView(
            capacity=((*window, 30),), lanes=((*window, 90),), weather=((*window, 60),)
        ),
        **base,
    )
    assert mixed.loading == datetime(2025, 3, 5, 10, 30)
    assert (mixed.delay_minutes, mixed.delay_reason) == (180, "LANE_DELAY")
    assert mixed.arrival - mixed.planned_arrival == timedelta(minutes=180)
    # An incident elsewhere in time changes nothing.
    later = (day + timedelta(days=3), day + timedelta(days=4), 90)
    assert plan_delivery(9, view=StoreIncidentView(lanes=(later,)), **base).delay_minutes == 0


def test_traffic_delays_grow_with_lane_length_and_always_carry_a_reason() -> None:
    def late_share(transit: int) -> float:
        late = 0
        runs = 0
        for store in range(1, 40):
            for days in range(60):
                timing = plan_delivery(
                    3,
                    store_id=store,
                    dc_id=1,
                    ready=READY + timedelta(days=days),
                    slot_minute=SLOT,
                    transit_minutes=transit,
                )
                runs += 1
                if timing.delay_minutes:
                    late += 1
                    assert timing.delay_reason == "TRAFFIC"
                    assert timing.delay_reason in DELAY_REASONS
                else:
                    assert timing.delay_reason is None
        return late / runs

    short, long = (
        late_share(cal.TRUCK_TRANSIT_MINUTES_MIN),
        late_share(cal.TRUCK_TRANSIT_MINUTES_MAX),
    )
    print(f"STAT traffic late share short={short:.3f} long={long:.3f}")
    assert 0.02 < short < long < 0.3


def test_unload_follows_load_size_hour_and_lateness() -> None:
    assert estimated_unload_minutes(1, 1, 2400, hour=14) > estimated_unload_minutes(
        1, 1, 2400, hour=9
    )
    assert estimated_unload_minutes(1, 1, 2400, hour=2) < estimated_unload_minutes(
        1, 1, 2400, hour=9
    )
    assert estimated_unload_minutes(1, 1, 4000) > estimated_unload_minutes(1, 1, 400)
    on_time = unload_minutes(1, 1, 2400, "SHP-1", hour=9, delay_minutes=0)
    late = unload_minutes(1, 1, 2400, "SHP-1", hour=9, delay_minutes=cal.TRUCK_LATE_MINUTES)
    assert late - on_time == cal.TRUCK_LATE_DOCK_WAIT_MINUTES
