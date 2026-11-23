"""Logistics rules (fabric/rules/definitions.kql) on simulated live stream output.

Each rule is evaluated in Python with the semantics of its KQL query over the
records the live supply state machine emits. Every rule fires on an injected
incident and stays quiet (the late-truck rule: within a small alert-rate band)
on normal data.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from retail_setup.generation import calibration as cal
from retail_setup.generation.incidents import Incident, supplier_group
from retail_setup.generation.stream_state import MemoryStateStore
from retail_setup.generation.stream_supply import (
    StockDemand,
    SupplyBatch,
    SupplyNetwork,
    TruckDeparture,
    default_dc_entry,
    incident_transitions,
    inventory_entry,
    run_supply_batch,
)

MINUTE = 60_000
HOUR = 60 * MINUTE
DAY = 24 * HOUR
START = datetime(2026, 3, 2, tzinfo=timezone.utc)
T0 = int(START.timestamp() * 1000)
DAYS = 8
# Products of one supplier group, so a short-ship of that group hits them all.
GROUP = supplier_group(7, 1)
PRODUCTS = [p for p in range(1, 200) if supplier_group(7, p) == GROUP][:4]
# Two DCs with six stores each; DC 1 serves stores 1-6, DC 2 stores 7-12.
STORES = range(1, 13)
DC1_STORES, DC2_STORES = tuple(range(1, 7)), tuple(range(7, 13))
NETWORK = SupplyNetwork(
    seed=7,
    namespace="ns",
    store_dc={store: 1 if store in DC1_STORES else 2 for store in STORES},
    fleets={1: (10, 11, 12), 2: (20, 21, 22)},
    daily_prior=30.0,
    slots={store: 360 + 60 * ((store - 1) % 6) for store in STORES},
    zones={store: "UTC" for store in STORES},
)


def _demand() -> list[StockDemand]:
    demand = []
    for day in range(DAYS):
        for minute in range(9 * 60, 21 * 60, 20):
            at = T0 + day * DAY + minute * MINUTE
            for store in NETWORK.store_dc:
                for product in PRODUCTS:
                    demand.append(
                        StockDemand(
                            at,
                            store,
                            product,
                            1 + (minute + product) % 2,
                            (START + timedelta(days=day)).date(),
                            f"R-{store}-{product}-{at}#1",
                        )
                    )
    return demand


def _seeds(keys: set[str]) -> dict:
    seeded = {}
    for key in keys:
        kind, node, product = key.split(":")
        if kind == "inv":
            # About five days of cover at the simulated demand.
            seeded[key] = inventory_entry(300, 54.0)
        else:
            # DC 1 runs lean (about a day of stock); DC 2 is well stocked.
            prior = 10.0 if int(node) == 1 else 400.0
            seeded[key] = default_dc_entry(7, int(node), int(product), prior, 6)
    return seeded


def _simulate(
    incidents: tuple[Incident, ...], step: int = 30 * MINUTE, extra: tuple[StockDemand, ...] = ()
):
    """Batches of ``step`` over the run: ``[(previous, clock, output, started, resolved)]``."""
    demand = _demand() + list(extra)
    store = MemoryStateStore("rules")
    batches = []
    previous = None
    clock = T0 + step
    while clock <= T0 + DAYS * DAY:
        lines = tuple(
            d for d in demand if (previous is None or d.at_ms > previous) and d.at_ms <= clock
        )
        batch = SupplyBatch(NETWORK, clock, previous, lines, incidents=incidents)
        output, changes = run_supply_batch(store, len(batches), batch, _seeds)
        for name, change in changes.items():
            store.write(name, len(batches), change)
        started, resolved = incident_transitions(incidents, previous, clock)
        batches.append((previous, clock, output, started, resolved))
        previous, clock = clock, clock + step
    return batches


# --- Rules (Python mirrors of fabric/rules/definitions.kql) -----------------


def _windows(batches, minutes: int):
    """Rule evaluations every ``minutes`` over the run and an hour past its end."""
    window = minutes * MINUTE
    return range(T0 + window, batches[-1][1] + HOUR + 1, window)


def per_store_alerts(records, batches, minutes: int) -> list[tuple[int, int | None, int, int]]:
    """``(evaluated_at, store_id, dc_id, products)``: one alert per store (or DC) and window.

    Mirrors the stockout and URGENT reorder rules: rows of the window
    deduplicated on their event key (``trace_id``), then counted per entity.
    """
    window = minutes * MINUTE
    alerts = []
    for at in _windows(batches, minutes):
        rows = {r.key: r for r in records if at - window < r.at_ms <= at}
        products: dict[tuple[int | None, int], set[int]] = defaultdict(set)
        for row in rows.values():
            products[(row.store_id, row.dc_id)].add(row.product_id)
        alerts += [
            (at, store, dc, len(ids)) for (store, dc), ids in sorted(products.items(), key=str)
        ]
    return alerts


def stockout_alerts(batches, replayed: int = 0) -> list[tuple[int, int | None, int, int]]:
    stockouts = [s for _, _, output, _, _ in batches for s in output.stockouts]
    # A replayed micro-batch writes the same events (same trace_id) again.
    return per_store_alerts(
        stockouts + stockouts[:replayed], batches, cal.STOCKOUT_RULE_WINDOW_MINUTES
    )


def urgent_alerts(batches) -> list[tuple[int, int | None, int, int]]:
    urgent = [
        r for _, _, output, _, _ in batches for r in output.reorders if r.priority == "URGENT"
    ]
    return per_store_alerts(urgent, batches, cal.URGENT_REORDER_RULE_WINDOW_MINUTES)


def dwell_breaches(batches) -> list[tuple[int, str]]:
    """``(evaluated_at, shipment_id)`` of the dwell-breach rule.

    A departure is ingested with the batch that emits it (``ingestion_time()``),
    whatever its own departure time; the rule looks back one window.
    """
    window = cal.TRUCK_DWELL_RULE_WINDOW_MINUTES * MINUTE
    arrived = {a.shipment_id: a.at_ms for _, _, output, _, _ in batches for a in output.arrivals}
    ingested = [(clock, d) for _, clock, output, _, _ in batches for d in output.departures]
    return [
        (at, departure.shipment_id)
        for at in _windows(batches, cal.TRUCK_DWELL_RULE_WINDOW_MINUTES)
        for clock, departure in ingested
        if at - window < clock <= at
        and (departure.at_ms - arrived[departure.shipment_id]) / MINUTE
        > cal.TRUCK_DWELL_SLA_MINUTES
    ]


def late_trucks(batches) -> list:
    return [
        arrival
        for _, _, output, _, _ in batches
        for arrival in output.arrivals
        if arrival.delay_minutes >= cal.TRUCK_LATE_MINUTES
    ]


def lane_delays(batches) -> set[tuple[int, int]]:
    """Lanes whose incident- or weather-delayed trucks averaged the threshold in a window."""
    window = cal.LANE_DELAY_RULE_WINDOW_MINUTES * MINUTE
    arrivals = [a for _, _, output, _, _ in batches for a in output.arrivals]
    fired = set()
    for _, clock, _, _, _ in batches:
        lanes: dict[tuple[int, int], list[int]] = defaultdict(list)
        for arrival in arrivals:
            if clock - window < arrival.at_ms <= clock and arrival.delay_reason in (
                "LANE_DELAY",
                "WEATHER",
            ):
                lanes[(arrival.dc_id, arrival.store_id)].append(arrival.delay_minutes)
        fired.update(
            lane
            for lane, delays in lanes.items()
            if sum(delays) / len(delays) >= cal.LANE_DELAY_RULE_MINUTES
        )
    return fired


def dc_outages(batches) -> list[str]:
    return [
        incident.incident_id
        for _, _, _, started, _ in batches
        for incident in started
        if incident.incident_type == "DC_OUTAGE"
    ]


def incident_stockouts(batches) -> set[str]:
    """Open incidents whose affected stores reported enough stockouts in a window."""
    window = cal.INCIDENT_STOCKOUT_RULE_WINDOW_MINUTES * MINUTE
    stockouts = [s for _, _, output, _, _ in batches for s in output.stockouts if s.store_id]
    opened: dict[str, Incident] = {}
    fired = set()
    for _, clock, _, started, resolved in batches:
        opened.update({incident.incident_id: incident for incident in started})
        for incident in resolved:
            opened.pop(incident.incident_id, None)
        for incident in opened.values():
            start_ms = int(incident.started_at.timestamp() * 1000)
            hits = {
                s.key
                for s in stockouts
                if clock - window < s.at_ms <= clock
                and s.at_ms >= start_ms
                and s.store_id in incident.affected_stores
            }
            if len(hits) >= cal.INCIDENT_STOCKOUT_RULE_MIN_STOCKOUTS:
                fired.add(incident.incident_id)
    return fired


def _incident(kind: str, dc_id: int, start: datetime, hours: int, stores, **extra) -> Incident:
    return Incident(
        f"INC-{kind}-{dc_id}-{start:%Y%m%d%H}",
        kind,
        dc_id,
        extra.pop("store_id", None),
        start,
        start + timedelta(hours=hours),
        "HIGH",
        tuple(stores),
        kind,
        **extra,
    )


OUTAGE = _incident("DC_OUTAGE", 2, START + timedelta(days=5, hours=4), 30, DC2_STORES)
SHORT = _incident(
    "SUPPLIER_SHORT_SHIP",
    1,
    START + timedelta(hours=6),
    24 * DAYS,
    DC1_STORES,
    fill_rate=0.0,
    supplier_group=GROUP,
)
LANE = _incident(
    "LANE_DELAY", 2, START + timedelta(hours=4), 96, (7,), store_id=7, delay_minutes=120
)
# A storm over DC 2's last stores.
WEATHER = _incident(
    "WEATHER", 2, START + timedelta(days=4), 24, (11, 12), delay_minutes=90, geography_id=9
)


def test_logistics_rules_stay_quiet_on_normal_data() -> None:
    batches = _simulate(())
    arrivals = [a for _, _, output, _, _ in batches for a in output.arrivals]
    assert len(arrivals) >= 2 * DAYS
    assert dc_outages(batches) == []
    assert lane_delays(batches) == set()
    assert incident_stockouts(batches) == set()
    late_share = len(late_trucks(batches)) / len(arrivals)
    print(f"STAT normal late-truck alert share={late_share:.3f} of {len(arrivals)}")
    assert late_share <= 0.25
    # Every delay has a reason, and on-time trucks have none.
    assert all((a.delay_minutes > 0) == (a.delay_reason is not None) for a in arrivals)


def test_logistics_rules_fire_on_injected_incidents() -> None:
    batches = _simulate((OUTAGE, LANE, WEATHER, SHORT))
    arrivals = [a for _, _, output, _, _ in batches for a in output.arrivals]
    # The outage starts once and alerts once.
    assert dc_outages(batches) == [OUTAGE.incident_id]
    # Held loads arrive late with the outage as their cause.
    held = [a for a in arrivals if a.delay_reason == "DC_OUTAGE"]
    assert held and {a.dc_id for a in held} == {2}
    assert all(a.delay_minutes >= cal.TRUCK_LATE_MINUTES for a in held)
    # The lane incident and the storm raise lane alerts on their own lanes only.
    assert (2, 7) in lane_delays(batches)
    assert lane_delays(batches) <= {(2, 7), (2, 11), (2, 12)}
    # The short-shipping supplier empties DC 1 and then its stores' shelves:
    # stockouts are attributed to that incident only. DC 2's stores keep
    # their cover through the outage, so it raises no stockout alert.
    assert incident_stockouts(batches) == {SHORT.incident_id}
    dc_stockouts = [s for _, _, output, _, _ in batches for s in output.stockouts if not s.store_id]
    assert {s.dc_id for s in dc_stockouts} == {1}
    stores_out = {s.store_id for _, _, out, _, _ in batches for s in out.stockouts if s.store_id}
    assert stores_out and stores_out <= set(DC1_STORES)


def test_rule_alerts_are_deduped_per_incident_across_replays() -> None:
    first = _simulate((OUTAGE,))
    again = _simulate((OUTAGE,), step=45 * MINUTE)
    assert dc_outages(first) == dc_outages(again) == [OUTAGE.incident_id]
    resolved = [i.incident_id for _, _, _, _, ended in first for i in ended]
    assert resolved == [OUTAGE.incident_id]


# A shopping rush at store 7 (served by the well-stocked DC 2) empties every
# shelf of the supplier group in one minute.
RUSH_AT = T0 + 3 * DAY + 14 * HOUR + 2 * MINUTE
RUSH = tuple(
    StockDemand(RUSH_AT, 7, product, 1, (START + timedelta(days=3)).date(), f"RUSH-{product}-{n}")
    for product in PRODUCTS
    for n in range(400)
)


def test_inventory_rules_alert_once_per_store_and_window_within_a_band() -> None:
    batches = _simulate(())
    alerts = stockout_alerts(batches)
    # Only the lean DC 1 and its stores run short; DC 2's stores keep their cover.
    assert alerts and {store for _, store, _, _ in alerts if store} <= set(DC1_STORES)
    assert len({(at, store, dc) for at, store, dc, _ in alerts}) == len(alerts)
    per_store_day = sum(1 for _, store, _, _ in alerts if store) / (len(STORES) * DAYS)
    print(f"STAT normal stockout alerts per store-day={per_store_day:.3f}")
    assert per_store_day <= 0.25
    # Replayed events (same trace_id) add no alert.
    stockouts = sum(len(output.stockouts) for _, _, output, _, _ in batches)
    assert stockout_alerts(batches, replayed=stockouts) == alerts
    urgent = urgent_alerts(batches)
    # Empty shelves at the lean DC's stores reorder URGENT; DC 2's never do.
    assert urgent and {store for _, store, _, _ in urgent} <= set(DC1_STORES)
    assert len({(at, store, dc) for at, store, dc, _ in urgent}) == len(urgent)
    urgent_per_store_day = len(urgent) / (len(STORES) * DAYS)
    print(f"STAT normal URGENT reorder alerts per store-day={urgent_per_store_day:.3f}")
    assert urgent_per_store_day <= 0.25


def test_stockout_rule_fires_once_per_store_for_a_rush() -> None:
    batches = _simulate((), extra=RUSH)
    window = cal.STOCKOUT_RULE_WINDOW_MINUTES * MINUTE
    rush_window = T0 + ((RUSH_AT - T0) // window + 1) * window
    # Every product runs out at once: one alert for the store, not one per product.
    assert [a for a in stockout_alerts(batches) if a[1] == 7][0] == (
        rush_window,
        7,
        2,
        len(PRODUCTS),
    )


def test_dwell_breach_alerts_once_when_the_departure_is_ingested() -> None:
    batches = _simulate((OUTAGE,))
    # Normal unloads stay under the SLA.
    assert dwell_breaches(batches) == []
    # Inject a breach that reaches the Eventhouse three hours after the truck
    # left (a stream restarted after a gap): the payload time is long past,
    # but the rule alerts once, in the window after ingestion.
    late_batch = 4 * 48
    clock = batches[late_batch][1]
    arrival = next(
        a
        for _, _, output, _, _ in batches
        for a in output.arrivals
        if a.at_ms + 3 * HOUR + 2 * HOUR <= clock
    )
    batches[late_batch][2].departures.append(
        TruckDeparture(
            arrival.at_ms + 2 * HOUR,
            arrival.truck_id,
            arrival.dc_id,
            arrival.store_id,
            arrival.shipment_id,
            120,
            1,
        )
    )
    window = cal.TRUCK_DWELL_RULE_WINDOW_MINUTES * MINUTE
    assert dwell_breaches(batches) == [
        (T0 + ((clock - T0 - 1) // window + 1) * window, arrival.shipment_id)
    ]
