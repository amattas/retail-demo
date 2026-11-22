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


def _simulate(incidents: tuple[Incident, ...], step: int = 30 * MINUTE):
    """Batches of ``step`` over the run: ``[(previous, clock, output, started, resolved)]``."""
    demand = _demand()
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
