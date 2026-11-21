"""Live inventory and truck logistics state machine (pure Python, no Spark)."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest

from retail_setup.generation.calibration import (
    TRUCK_LOAD_MINUTES,
    TRUCK_NORMAL_DWELL_MINUTES_MIN,
    TRUCK_TRANSIT_MINUTES_MAX,
    TRUCK_TRANSIT_MINUTES_MIN,
)
from retail_setup.generation.inventory_policy import CASE_PACK, opening_stock
from retail_setup.generation.stream_state import MemoryStateStore
from retail_setup.generation.stream_supply import (
    INVENTORY_STATE,
    LOGISTICS_STATE,
    StockDemand,
    SupplyBatch,
    SupplyNetwork,
    apply_supply_batch,
    default_inventory_entry,
    inventory_entry,
    inventory_key,
    lost_lines,
    required_inventory_keys,
    run_supply_batch,
)

MINUTE = 60_000
T0 = int(datetime(2026, 3, 2, 15, tzinfo=timezone.utc).timestamp() * 1000)
DAY = date(2026, 3, 2)
# Every store's daily delivery slot is 16:00 UTC, an hour after T0.
SLOT_MINUTE = 16 * 60


class _FixedSlotNetwork(SupplyNetwork):
    def delivery_slot_minute(self, store_id: int) -> int:
        return SLOT_MINUTE


NETWORK = _FixedSlotNetwork(
    seed=7,
    namespace="ns",
    store_dc={1: 1, 2: 1, 3: 2},
    fleets={1: (10,), 2: (20, 21)},
    daily_prior=2.0,
)
SLOT = T0 + MINUTE * 60


def _demand(at_ms, store, product, quantity, key, day=DAY):
    return StockDemand(at_ms, store, product, quantity, day, key)


def _batch(clock_ms, demand=(), previous_ms=None, **kwargs):
    return SupplyBatch(NETWORK, clock_ms, previous_ms, tuple(demand), **kwargs)


def test_sales_never_drive_stock_negative_and_lost_sales_keep_the_balance() -> None:
    state = {inventory_key(1, 5): inventory_entry(5, 1.0)}
    demand = [
        _demand(T0, 1, 5, 3, "R1#1"),
        _demand(T0 + MINUTE, 1, 5, 4, "R2#1"),
        _demand(T0 + 2 * MINUTE, 1, 5, 2, "R3#2"),
    ]
    new, out = apply_supply_batch(state, _batch(T0 + 3 * MINUTE, demand))

    assert [(m.reason, m.quantity_delta, m.requested_quantity, m.balance) for m in out.moves] == [
        ("SALE", -3, 3, 2),
        ("LOST_SALE", 0, 4, 2),
        ("SALE", -2, 2, 0),
    ]
    assert {m.dc_id for m in out.moves} == {1}
    assert lost_lines(out) == {"R2": [1]}
    # Conservation: opening stock minus filled demand.
    assert new[inventory_key(1, 5)]["on_hand"] == 5 - 3 - 2
    # The sale that empties the shelf reports one stockout with the stock it took.
    assert [(s.at_ms, s.last_known_quantity, s.dc_id) for s in out.stockouts] == [
        (T0 + 2 * MINUTE, 2, 1)
    ]
    # rate 1/day -> reorder point 12 and target 24; position 2 after the first sale.
    assert [
        (r.current_quantity, r.reorder_quantity, r.reorder_point, r.priority) for r in out.reorders
    ] == [(2, 24, 12, "NORMAL")]
    assert out.reorders[0].reorder_quantity % CASE_PACK == 0
    assert new[inventory_key(1, 5)]["inbound"] == 24
    assert new["pending:1"] == {"dc_id": 1, "since_ms": T0, "lines": {"5": 24}}


def test_stockouts_fire_once_per_day_and_never_on_lost_sales() -> None:
    state = {inventory_key(1, 5): {**inventory_entry(1, 0.1), "inbound": 100}}
    demand = [
        _demand(T0, 1, 5, 1, "A#1"),
        _demand(T0 + MINUTE, 1, 5, 1, "B#1"),
    ]
    new, out = apply_supply_batch(state, _batch(T0 + 2 * MINUTE, demand))
    assert len(out.stockouts) == 1 and out.lost == ["B#1"]
    # Restocked and emptied again the same UTC day: still one stockout that day.
    new[inventory_key(1, 5)]["on_hand"] = 1
    _, again = apply_supply_batch(
        new, _batch(T0 + 4 * MINUTE, [_demand(T0 + 3 * MINUTE, 1, 5, 1, "C#1")], T0 + 2 * MINUTE)
    )
    assert again.stockouts == []


def test_urgent_reorder_when_the_shelf_is_empty_and_demand_rate_rolls_by_local_day() -> None:
    entry = {**inventory_entry(0, 2.0), "day": "2026-03-01", "attempted": 4}
    later = DAY + timedelta(days=1)
    new, out = apply_supply_batch(
        {inventory_key(2, 9): entry},
        _batch(T0 + MINUTE, [_demand(T0, 2, 9, 1, "R#1", later)]),
    )
    rolled = new[inventory_key(2, 9)]
    # 0.75*2 + 0.25*4 = 2.5 at the close of Mar 1; Mar 2 had no demand: 2.5*0.75.
    assert rolled["rate"] == pytest.approx(1.875)
    assert rolled["day"] == later.isoformat() and rolled["attempted"] == 1
    assert out.lost == ["R#1"]
    assert [(r.priority, r.current_quantity) for r in out.reorders] == [("URGENT", 0)]


def test_reorders_ship_on_the_dc_fleet_and_unloads_restock_the_store() -> None:
    state = {inventory_key(1, 5): inventory_entry(5, 1.0)}
    _, first = apply_supply_batch(state, _batch(T0, [_demand(T0, 1, 5, 3, "R1#1")]))
    assert first.reorders and first.arrivals == []

    state, _ = apply_supply_batch(state, _batch(T0, [_demand(T0, 1, 5, 3, "R1#1")]))
    clock = T0 + 2 * 24 * 60 * MINUTE
    new, out = apply_supply_batch(state, _batch(clock, previous_ms=T0))

    (arrival,) = out.arrivals
    (departure,) = out.departures
    transit = NETWORK.transit_minutes(1)
    assert TRUCK_TRANSIT_MINUTES_MIN <= transit <= TRUCK_TRANSIT_MINUTES_MAX
    dispatched = SLOT
    assert arrival.truck_id in NETWORK.fleets[1] and arrival.dc_id == 1
    assert arrival.planned_ms == dispatched + (TRUCK_LOAD_MINUTES + transit) * MINUTE
    assert (arrival.at_ms, arrival.delay_minutes, arrival.delay_reason) == (
        arrival.planned_ms,
        0,
        None,
    )
    assert arrival.shipment_id == f"SHP-ns-1-{dispatched}"
    assert departure.shipment_id == arrival.shipment_id
    assert departure.at_ms == arrival.at_ms + departure.unload_minutes * MINUTE
    assert departure.unload_minutes >= TRUCK_NORMAL_DWELL_MINUTES_MIN
    assert departure.at_ms <= clock
    (received,) = [m for m in out.moves if m.reason == "INBOUND_SHIPMENT"]
    assert (received.at_ms, received.quantity_delta, received.balance, received.source) == (
        departure.at_ms,
        24,
        2 + 24,
        arrival.shipment_id,
    )
    assert new[inventory_key(1, 5)]["inbound"] == 0
    assert new["truck:10"] == {
        "dc_id": 1,
        "free_ms": departure.at_ms + transit * MINUTE,
        "shipment": None,
    }
    assert not any(key.startswith(("pending:", "shipment:")) for key in new)


def test_departures_are_emitted_only_once_their_time_has_passed() -> None:
    state, _ = apply_supply_batch(
        {inventory_key(1, 5): inventory_entry(5, 1.0)}, _batch(T0, [_demand(T0, 1, 5, 3, "R#1")])
    )
    probe, out = apply_supply_batch(state, _batch(T0 + 2 * 24 * 60 * MINUTE, previous_ms=T0))
    arrival_ms, departure_ms = out.arrivals[0].at_ms, out.departures[0].at_ms

    mid, before = apply_supply_batch(state, _batch(departure_ms - 1, previous_ms=T0))
    assert [a.at_ms for a in before.arrivals] == [arrival_ms] and before.departures == []
    assert [m for m in before.moves if m.reason == "INBOUND_SHIPMENT"] == []
    final, after = apply_supply_batch(
        mid, _batch(T0 + 2 * 24 * 60 * MINUTE, previous_ms=departure_ms - 1)
    )
    assert after.arrivals == [] and [d.at_ms for d in after.departures] == [departure_ms]
    assert final == probe


def test_a_truck_carries_one_shipment_at_a_time() -> None:
    state = {
        inventory_key(1, 5): inventory_entry(3, 1.0),
        inventory_key(2, 6): inventory_entry(3, 1.0),
    }
    demand = [_demand(T0, 1, 5, 1, "A#1"), _demand(T0, 2, 6, 1, "B#1")]
    _, out = apply_supply_batch(state, _batch(T0 + 3 * 24 * 60 * MINUTE, demand))

    assert len(out.arrivals) == 2 and {a.truck_id for a in out.arrivals} == {10}
    first, second = sorted(out.arrivals, key=lambda a: a.at_ms)
    first_done = next(d for d in out.departures if d.shipment_id == first.shipment_id)
    back = first_done.at_ms + NETWORK.transit_minutes(first.store_id) * MINUTE
    assert back > SLOT
    # Both loads were ready at the slot; the second waits for the truck to come
    # back, load and drive out.
    assert second.planned_ms == (
        back + (TRUCK_LOAD_MINUTES + NETWORK.transit_minutes(second.store_id)) * MINUTE
    )


def test_a_held_dc_dispatches_nothing() -> None:
    state, _ = apply_supply_batch(
        {inventory_key(3, 5): inventory_entry(5, 1.0)}, _batch(T0, [_demand(T0, 3, 5, 3, "R#1")])
    )
    clock = T0 + 24 * 60 * MINUTE
    held, out = apply_supply_batch(state, _batch(clock, previous_ms=T0, held_dcs=frozenset({2})))
    assert out.arrivals == [] and "pending:3" in held
    _, released = apply_supply_batch(held, _batch(clock + 24 * 60 * MINUTE, previous_ms=clock))
    assert [a.truck_id for a in released.arrivals] == [20]
    assert released.arrivals[0].planned_ms > clock


def test_required_keys_cover_only_loads_that_can_unload_by_the_clock() -> None:
    state, _ = apply_supply_batch(
        {inventory_key(1, 5): inventory_entry(5, 1.0)}, _batch(T0, [_demand(T0, 1, 5, 3, "R#1")])
    )
    logistics = {key: value for key, value in state.items() if not key.startswith("inv:")}
    soon = _batch(T0 + MINUTE, [_demand(T0 + MINUTE, 2, 6, 1, "X#1")], previous_ms=T0)
    assert required_inventory_keys(logistics, soon) == {inventory_key(2, 6)}
    later = _batch(T0 + 2 * 24 * 60 * MINUTE, previous_ms=T0)
    assert required_inventory_keys(logistics, later) == {inventory_key(1, 5)}


def _day_of_demand() -> list[StockDemand]:
    demand = []
    for minute in range(0, 24 * 60, 7):
        for store, product in ((1, 5), (2, 6), (3, 7)):
            demand.append(
                _demand(T0 + minute * MINUTE, store, product, 1 + minute % 3, f"{store}-{minute}#1")
            )
    return demand


def _seeds(keys: set[str]) -> dict:
    return {
        key: default_inventory_entry(7, int(key.split(":")[1]), int(key.split(":")[2]), 20.0)
        for key in keys
    }


def _run(store: MemoryStateStore, windows: list[tuple[int | None, int]], demand) -> list:
    emitted = []
    for batch_id, (previous, clock) in enumerate(windows):
        lines = tuple(
            d for d in demand if (previous is None or d.at_ms > previous) and d.at_ms <= clock
        )
        batch = _batch(clock, lines, previous)
        output, changes = run_supply_batch(store, batch_id, batch, _seeds)
        replay, replay_changes = run_supply_batch(store, batch_id, batch, _seeds)
        assert (replay, replay_changes) == (output, changes)
        for name, change in changes.items():
            store.write(name, batch_id, change)
        emitted.append(output)
    return emitted


def test_replay_and_batch_size_do_not_change_the_outcome() -> None:
    demand = _day_of_demand()
    end = T0 + 3 * 24 * 60 * MINUTE
    first = demand[0].at_ms
    one = MemoryStateStore("inc")
    whole = _run(one, [(None, end)], demand)
    many = MemoryStateStore("inc")
    windows, previous = [], None
    for clock in range(first, end + 1, 37 * MINUTE):
        windows.append((previous, clock))
        previous = clock
    windows.append((previous, end))
    parts = _run(many, windows, demand)

    def flatten(outputs, attribute):
        return [item for output in outputs for item in getattr(output, attribute)]

    for attribute in ("moves", "stockouts", "reorders", "arrivals", "departures", "lost"):
        assert flatten(parts, attribute) == flatten(whole, attribute), attribute
    for name in (INVENTORY_STATE, LOGISTICS_STATE):
        assert many.read(name, len(windows)) == one.read(name, 1)

    moves = flatten(whole, "moves")
    assert all(m.balance >= 0 for m in moves)
    inventory = one.read(INVENTORY_STATE, 1)
    for key, entry in inventory.items():
        store_id, product_id = (int(part) for part in key.split(":")[1:])
        delta = sum(
            m.quantity_delta for m in moves if (m.store_id, m.product_id) == (store_id, product_id)
        )
        assert entry["on_hand"] == opening_stock(7, store_id, product_id, 20.0) + delta
    received = {m.source: m.quantity_delta for m in moves if m.reason == "INBOUND_SHIPMENT"}
    assert set(received) == {d.shipment_id for d in flatten(whole, "departures")}
    reordered = sum(r.reorder_quantity for r in flatten(whole, "reorders"))
    in_flight = sum(
        quantity
        for key, payload in one.read(LOGISTICS_STATE, 1).items()
        if key.startswith(("pending:", "shipment:"))
        for quantity in payload["lines"].values()
    )
    assert reordered == sum(received.values()) + in_flight


def test_missing_inventory_seed_fails_loudly() -> None:
    store = MemoryStateStore("inc")
    batch = _batch(T0, [_demand(T0, 1, 5, 1, "R#1")])
    with pytest.raises(KeyError, match="no inventory seed"):
        run_supply_batch(store, 0, batch, lambda keys: {})
    output, changes = run_supply_batch(
        store, 0, replace(batch), lambda keys: {key: inventory_entry(9, 1.0) for key in keys}
    )
    assert output.moves[0].balance == 8
    assert changes[INVENTORY_STATE][inventory_key(1, 5)]["on_hand"] == 8


def test_each_store_ships_at_its_daily_delivery_slot() -> None:
    real = SupplyNetwork(7, "ns", {1: 1}, {1: (10,)}, 2.0)
    slot = real.delivery_slot_minute(1)
    assert 0 <= slot < 1440 and slot == real.delivery_slot_minute(1)
    day_start = T0 - T0 % (24 * 60 * MINUTE)
    reorder_at = day_start + (slot + 1) * MINUTE  # just after today's slot
    state, _ = apply_supply_batch(
        {inventory_key(1, 5): inventory_entry(5, 1.0)},
        SupplyBatch(real, reorder_at, None, (_demand(reorder_at, 1, 5, 3, "R#1"),)),
    )
    clock = reorder_at + 3 * 24 * 60 * MINUTE
    _, out = apply_supply_batch(state, SupplyBatch(real, clock, reorder_at))
    (arrival,) = out.arrivals
    # Missed today's slot, so the load ships at tomorrow's.
    tomorrow = day_start + 24 * 60 * MINUTE + slot * MINUTE
    assert arrival.shipment_id == f"SHP-ns-1-{tomorrow}"
    assert arrival.planned_ms == tomorrow + (TRUCK_LOAD_MINUTES + real.transit_minutes(1)) * MINUTE
