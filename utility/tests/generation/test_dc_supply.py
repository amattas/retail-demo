"""DC-limited, incident-aware SKU supply (pure Python, no Spark)."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from retail_setup.generation.calibration import TRUCK_LOAD_MINUTES
from retail_setup.generation.dc_supply import (
    DcDemand,
    StoreLane,
    delivery_timing,
    simulate_dc_sku,
)
from retail_setup.generation.incidents import DcIncidentView, StoreIncidentView, supplier_group
from retail_setup.generation.inventory_policy import Demand, dc_lead_days

START = date(2025, 3, 3)
SEED = 11
PRODUCT = 7
STORES = (1, 2, 3)


def _lanes(view: StoreIncidentView | None = None, prior: float = 20.0) -> dict[int, StoreLane]:
    return {store: StoreLane(600 + 30 * store, 90 + store, prior, view) for store in STORES}


def _rows(days: int, units: int = 20, online: int = 0):
    rows = []
    for day in range(days):
        for store in STORES:
            for line in range(units // 2):
                ts = datetime.combine(START + timedelta(days=day), datetime.min.time()) + timedelta(
                    hours=9, minutes=line * 5 + store
                )
                rows.append((store, Demand(ts, f"R{store}-{day}-{line}", 1, 2)))
        for line in range(online):
            ts = datetime.combine(START + timedelta(days=day), datetime.min.time()) + timedelta(
                hours=1 + line
            )
            rows.append((None, DcDemand(ts, f"ONL-{day}-{line}", 1, 4)))
    return sorted(rows, key=lambda row: (row[1].event_ts, row[1].trace_id))


def _run(days: int, **kwargs):
    rows = kwargs.pop("rows", None) or _rows(days, online=kwargs.pop("online", 0))
    return list(
        simulate_dc_sku(
            rows,
            dc_id=1,
            product_id=PRODUCT,
            lanes=kwargs.pop("lanes", None) or _lanes(),
            start_date=START,
            end_date=START + timedelta(days=days - 1),
            seed=SEED,
            **kwargs,
        )
    )


def _dc_physical(events, end: date) -> list[tuple[datetime, int]]:
    """DC stock moves in time: its own rows plus loads leaving at loading time."""
    moves = [
        (event.event_ts, event.quantity) for event in events if event.kind.startswith("DC_")
    ] + [
        (event.loading_ts, -event.quantity)
        for event in events
        if event.kind == "SHIPMENT" and event.loading_ts.date() <= end
    ]
    return sorted(moves)


def _lost(events) -> int:
    return sum(1 for event in events if event.kind == "LOST_SALE")


def test_dc_stock_is_conserved_and_never_negative() -> None:
    days = 90
    events = _run(days, online=3)
    running = 0
    for _, quantity in _dc_physical(events, START + timedelta(days=days - 1)):
        running += quantity
        assert running >= 0
    kinds = {event.kind for event in events}
    assert {"DC_INITIAL", "DC_RECEIPT", "DC_ONLINE", "SHIPMENT", "REORDER"} <= kinds
    # Supplier receipts arrive at the DC's receipt hour, a lead time after ordering.
    receipts = [event for event in events if event.kind == "DC_RECEIPT"]
    assert all(event.event_ts.hour == 4 for event in receipts)
    lead = dc_lead_days(SEED, 1, PRODUCT)
    ordered = {event.source: event.event_ts.date() for event in receipts}
    for source, arrived in ordered.items():
        placed = datetime.strptime(source.rsplit("-", 1)[1], "%Y%m%d").date()
        assert arrived - placed == timedelta(days=lead)
    # Every store reorder ships in full while the DC has stock.
    shipped = sum(event.quantity for event in events if event.kind == "SHIPMENT")
    requested = sum(event.quantity for event in events if event.kind == "REORDER")
    assert shipped == requested
    assert _lost(events) == 0
    # Unique traces; online lines draw on the DC.
    assert len({event.trace_id for event in events}) == len(events)
    assert sum(-event.quantity for event in events if event.kind == "DC_ONLINE") == 90 * 3 * 4


def test_a_supplier_short_ship_empties_the_dc_and_reaches_store_shelves() -> None:
    days = 80
    group = supplier_group(SEED, PRODUCT)
    window = (datetime(2025, 3, 10), datetime(2025, 4, 20))
    short = DcIncidentView(short_ships=((*window, group, 0.0),))
    baseline, disrupted = _run(days), _run(days, dc_view=short)
    assert _lost(baseline) == 0
    lost = [event for event in disrupted if event.kind == "LOST_SALE"]
    assert lost
    assert all(window[0] < event.event_ts < window[1] + timedelta(days=14) for event in lost)
    # The DC ran out: reorders were short-shipped while the supplier failed.
    requested = sum(event.quantity for event in disrupted if event.kind == "REORDER")
    shipped = sum(event.quantity for event in disrupted if event.kind == "SHIPMENT")
    assert shipped < requested
    # An unrelated supplier group changes nothing.
    other = DcIncidentView(short_ships=((*window, (group + 1) % 6, 0.0),))
    assert _run(days, dc_view=other) == baseline


def test_online_lines_the_dc_cannot_fill_are_expedited() -> None:
    lanes = _lanes(prior=0.5)
    rows = [(None, DcDemand(datetime(2025, 3, 3, 8), "ONL-BIG", 1, 5000))]
    events = _run(3, rows=rows, lanes=lanes)
    expedite = [event for event in events if event.kind == "DC_EXPEDITE"]
    opening = next(event for event in events if event.kind == "DC_INITIAL").quantity
    assert [(event.quantity, event.event_ts) for event in expedite] == [
        (5000 - opening, datetime(2025, 3, 3, 7, 59, 59))
    ]


def test_an_outage_holds_loads_and_merges_them_with_the_next_days_reorders() -> None:
    days = 30
    outage = StoreIncidentView(outages=((datetime(2025, 3, 10), datetime(2025, 3, 12, 23), 0),))
    events = _run(days, lanes=_lanes(outage))
    shipments = [event for event in events if event.kind == "SHIPMENT"]
    held = [event for event in shipments if event.delay_reason == "DC_OUTAGE"]
    assert held
    for event in held:
        assert event.loading_ts >= datetime(2025, 3, 12, 23)
        assert event.delay_minutes == round((event.eta - event.planned_eta).total_seconds() / 60)
        assert event.delay_minutes >= 24 * 60
    # No load leaves the DC during the outage.
    assert not [
        event
        for event in shipments
        if datetime(2025, 3, 10) <= event.loading_ts < datetime(2025, 3, 12, 23)
    ]
    # One shipment per store and slot: held reorders ride with later ones.
    keys = [(event.store_id, event.shipment_id) for event in shipments]
    assert len(keys) == len(set(keys))
    assert sum(event.quantity for event in shipments) == sum(
        event.quantity for event in events if event.kind == "REORDER"
    )


def test_delivery_timing_is_shared_by_every_sku_of_a_load() -> None:
    view = StoreIncidentView(lanes=((datetime(2025, 3, 5), datetime(2025, 3, 6), 75),))
    first = delivery_timing(SEED, 1, 1, date(2025, 3, 4), 630, 91, view, "general")
    again = delivery_timing(SEED, 1, 1, date(2025, 3, 4), 630, 91, view, "general")
    assert first is again
    assert first.departure == first.loading + timedelta(minutes=TRUCK_LOAD_MINUTES)
    assert first.delay_reason == "LANE_DELAY" and first.delay_minutes >= 75


def test_the_dc_simulation_is_deterministic() -> None:
    assert _run(40, online=2) == _run(40, online=2)
