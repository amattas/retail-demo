"""Live store inventory and truck logistics as one pure state machine.

Each micro-batch replays, in time order, everything that happens to store
stock and the DC fleets between the previous batch clock and this one:

- **Demand.** Every emitted in-store receipt line and every store-fulfilled
  online order line (ship-from-store, BOPIS) takes stock from its store. A line
  the store cannot fill becomes a lost sale instead, as in history: the stock
  never goes negative and the line is dropped from the receipt or order.
- **Stockouts and reorders.** A sale that takes the balance to zero reports a
  stockout (once per store, product and UTC day, like ``fact_stockouts``).
  After each sale or lost sale the shared replenishment policy
  (``inventory_policy``: EWMA demand, reorder point, order-up-to target, case
  pack, priority) checks the inventory position, on hand plus inbound.
- **Shipments.** A reorder joins its store's pending load at the store's DC.
  Each store takes one consolidated delivery a day: at the store's daily
  delivery slot the next free truck of the DC's fleet loads,
  drives the lane, arrives (planned arrival plus delay, zero until the
  incident model adds delays), unloads for a load- and site-driven dwell,
  departs and drives back. A truck carries one shipment at a time, so it is
  never in two places at once. Arrivals and departures are emitted only when
  their time has passed, and the unload adds the units to store stock.

State lives in two tables. ``inventory`` holds one entry per store and
product that the stream has touched (``inv:<store>:<product>``); entries the
stream has not touched yet are seeded from the Silver ledger as of the
incarnation start. ``logistics`` holds pending loads (``pending:<store>``),
shipments on the road (``shipment:<id>``) and truck positions
(``truck:<id>``); a truck with no entry is idle at its DC.

Everything here is pure Python: :func:`apply_supply_batch` maps a state and a
:class:`SupplyBatch` to the next state and the emitted records, so a replayed
batch emits exactly the same events. ``build_notebooks.py`` inlines this
module into the stream notebook.
"""

import heapq
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from retail_setup.generation.calibration import (
    STOCKOUT_QUANTITY_THRESHOLD,
    TRUCK_DOCK_FACTOR_RANGE,
    TRUCK_LOAD_MINUTES,
    TRUCK_NORMAL_DWELL_MINUTES_MIN,
    TRUCK_TRANSIT_MINUTES_MAX,
    TRUCK_TRANSIT_MINUTES_MIN,
    TRUCK_UNLOAD_BASE_MINUTES,
    TRUCK_UNLOAD_NOISE_MINUTES,
    TRUCK_UNLOAD_UNITS_PER_MINUTE,
)
from retail_setup.generation.inventory_policy import (
    decay_demand,
    opening_stock,
    reorder_levels,
    reorder_priority,
    reorder_quantity,
    smooth_demand,
    stable_int,
)
from retail_setup.generation.stream_state import Changes, StateStore, apply_batch, state_changes

INVENTORY_STATE = "inventory"
LOGISTICS_STATE = "logistics"
STORE_SALE_SOURCE = "CUSTOMER_PURCHASE"
ONLINE_SALE_SOURCE = "ONLINE_ORDER"
_MINUTE_MS = 60_000
_DAY_MS = 86_400_000
# Same-instant events: arrivals, then unloads, then demand, then dispatching.
_ARRIVE, _UNLOAD, _DEMAND, _DISPATCH = range(4)

SupplyState = dict[str, dict[str, Any]]


def inventory_key(store_id: int, product_id: int) -> str:
    return f"inv:{int(store_id)}:{int(product_id)}"


def parse_inventory_key(key: str) -> tuple[int, int]:
    _, store, product = key.split(":")
    return int(store), int(product)


def inventory_entry(on_hand: int, rate: float) -> dict[str, Any]:
    """A store-product entry with no live history: balance and smoothed daily demand."""
    return {
        "on_hand": max(0, int(on_hand)),
        "inbound": 0,
        "rate": float(rate),
        "day": None,
        "attempted": 0,
        "stockout_day": None,
    }


def default_inventory_entry(
    seed: int, store_id: int, product_id: int, daily_prior: float
) -> dict[str, Any]:
    """Entry for a store-product the Silver ledger has never seen (history's opening stock)."""
    return inventory_entry(opening_stock(seed, store_id, product_id), daily_prior)


def split_supply_state(state: SupplyState) -> tuple[SupplyState, SupplyState]:
    """(inventory, logistics) entries of a combined supply state."""
    inventory = {key: value for key, value in state.items() if key.startswith("inv:")}
    logistics = {key: value for key, value in state.items() if not key.startswith("inv:")}
    return inventory, logistics


@dataclass(frozen=True)
class SupplyNetwork:
    """Static supply inputs: store routing, DC fleets and the demand prior."""

    seed: int
    namespace: str
    store_dc: dict[int, int]
    fleets: dict[int, tuple[int, ...]]
    daily_prior: float

    def dc_of(self, store_id: int) -> int:
        return self.store_dc[int(store_id)]

    def transit_minutes(self, store_id: int) -> int:
        """One-way drive between a store and its DC (a stable draw per lane)."""
        span = TRUCK_TRANSIT_MINUTES_MAX - TRUCK_TRANSIT_MINUTES_MIN + 1
        draw = stable_int(self.seed, self.dc_of(store_id), int(store_id), "lane")
        return TRUCK_TRANSIT_MINUTES_MIN + draw % span

    def delivery_slot_minute(self, store_id: int) -> int:
        """Minute of the UTC day at which a store's pending load ships (a stable draw)."""
        return stable_int(self.seed, int(store_id), "slot") % 1440

    def dock_factor(self, store_id: int) -> float:
        """How slowly a store's dock unloads relative to the calibrated rate."""
        low, high = TRUCK_DOCK_FACTOR_RANGE
        return low + (high - low) * (stable_int(self.seed, int(store_id), "dock") % 1001) / 1000

    def estimated_unload_minutes(self, store_id: int, units: int) -> int:
        """Planned dwell of a load: base time plus the units at the site's rate."""
        handling = units / TRUCK_UNLOAD_UNITS_PER_MINUTE * self.dock_factor(store_id)
        return max(TRUCK_NORMAL_DWELL_MINUTES_MIN, round(TRUCK_UNLOAD_BASE_MINUTES + handling))

    def unload_minutes(self, store_id: int, units: int, shipment_id: str) -> int:
        """Actual dwell: the estimate plus a little noise per shipment."""
        span = 2 * TRUCK_UNLOAD_NOISE_MINUTES + 1
        noise = stable_int(self.seed, shipment_id, "unload") % span - TRUCK_UNLOAD_NOISE_MINUTES
        return max(
            TRUCK_NORMAL_DWELL_MINUTES_MIN,
            self.estimated_unload_minutes(store_id, units) + noise,
        )


@dataclass(frozen=True)
class StockDemand:
    """One demand line: a receipt line or a store-fulfilled online order line."""

    at_ms: int
    store_id: int
    product_id: int
    quantity: int
    local_day: date
    key: str
    source: str = STORE_SALE_SOURCE


@dataclass(frozen=True)
class SupplyBatch:
    """Inputs of one micro-batch.

    ``held_dcs`` is the hook for logistics incidents: a held DC dispatches no
    trucks, so its stores' loads wait (shipments already on the road continue).
    """

    network: SupplyNetwork
    clock_ms: int
    previous_ms: int | None
    demand: tuple[StockDemand, ...] = ()
    held_dcs: frozenset[int] = frozenset()


@dataclass(frozen=True)
class StockMove:
    at_ms: int
    store_id: int
    dc_id: int
    product_id: int
    quantity_delta: int
    reason: str
    source: str
    requested_quantity: int
    balance: int
    key: str


@dataclass(frozen=True)
class StockoutRecord:
    at_ms: int
    store_id: int
    dc_id: int
    product_id: int
    last_known_quantity: int
    key: str


@dataclass(frozen=True)
class ReorderRecord:
    at_ms: int
    store_id: int
    dc_id: int
    product_id: int
    current_quantity: int
    reorder_quantity: int
    reorder_point: int
    priority: str
    key: str


@dataclass(frozen=True)
class TruckArrival:
    at_ms: int
    truck_id: int
    dc_id: int
    store_id: int
    shipment_id: str
    planned_ms: int
    delay_minutes: int
    delay_reason: str | None
    estimated_unload_minutes: int


@dataclass(frozen=True)
class TruckDeparture:
    at_ms: int
    truck_id: int
    dc_id: int
    store_id: int
    shipment_id: str
    unload_minutes: int
    units: int


@dataclass
class SupplyOutput:
    moves: list[StockMove] = field(default_factory=list)
    stockouts: list[StockoutRecord] = field(default_factory=list)
    reorders: list[ReorderRecord] = field(default_factory=list)
    arrivals: list[TruckArrival] = field(default_factory=list)
    departures: list[TruckDeparture] = field(default_factory=list)
    lost: list[str] = field(default_factory=list)


def _utc_day(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).date().isoformat()


def _lines(payload: dict[str, Any]) -> list[tuple[int, int]]:
    return sorted((int(product), int(quantity)) for product, quantity in payload["lines"].items())


def _earliest_dispatch(
    network: SupplyNetwork, store_id: int, pending: dict[str, Any], floor_ms: int
) -> int:
    """The store's first delivery slot at or after its oldest pending reorder."""
    since = int(pending["since_ms"])
    slot = since - since % _DAY_MS + network.delivery_slot_minute(store_id) * _MINUTE_MS
    if slot < since:
        slot += _DAY_MS
    return max(slot, floor_ms)


def batch_floor(batch: SupplyBatch) -> int:
    """Earliest instant this batch may act at: just after the previous batch clock."""
    if batch.previous_ms is not None:
        return int(batch.previous_ms) + 1
    return min([demand.at_ms for demand in batch.demand] + [batch.clock_ms])


def required_inventory_keys(logistics: SupplyState, batch: SupplyBatch) -> set[str]:
    """Inventory entries a batch can touch: its demand and every load that may unload by its clock."""
    keys = {inventory_key(d.store_id, d.product_id) for d in batch.demand}
    floor_ms = batch_floor(batch)
    network = batch.network
    for key, payload in logistics.items():
        if key.startswith("shipment:"):
            completes = int(payload["done_ms"])
        elif key.startswith("pending:"):
            store_id = int(key.split(":")[1])
            completes = (
                _earliest_dispatch(network, store_id, payload, floor_ms)
                + (TRUCK_LOAD_MINUTES + network.transit_minutes(store_id)) * _MINUTE_MS
                + TRUCK_NORMAL_DWELL_MINUTES_MIN * _MINUTE_MS
            )
        else:
            continue
        if completes <= batch.clock_ms:
            store_id = int(payload["store_id"]) if "store_id" in payload else int(key.split(":")[1])
            keys.update(inventory_key(store_id, product) for product, _ in _lines(payload))
    return keys


def apply_supply_batch(state: SupplyState, batch: SupplyBatch) -> tuple[SupplyState, SupplyOutput]:
    """Advance store stock and the fleets through one batch window, in time order."""
    st = deepcopy(state)
    out = SupplyOutput()
    network = batch.network
    floor_ms = batch_floor(batch)
    queue: list[tuple[int, int, str, Any]] = []

    def push(at_ms: int, kind: int, tie: str, ref: Any) -> None:
        heapq.heappush(queue, (int(at_ms), kind, tie, ref))

    def stock(store_id: int, product_id: int) -> dict[str, Any]:
        key = inventory_key(store_id, product_id)
        if key not in st:
            raise KeyError(f"inventory entry {key} was not loaded for this batch")
        return st[key]

    for key in sorted(st):
        payload = st[key]
        if key.startswith("shipment:"):
            if payload["arrived"]:
                push(payload["done_ms"], _UNLOAD, key, key)
            else:
                push(payload["arrival_ms"], _ARRIVE, key, key)
        elif key.startswith("pending:"):
            dc_id, store_id = int(payload["dc_id"]), int(key.split(":")[1])
            ready = _earliest_dispatch(network, store_id, payload, floor_ms)
            push(ready, _DISPATCH, f"dc:{dc_id:09d}", dc_id)
        elif key.startswith("truck:") and payload.get("free_ms") is not None:
            dc_id = int(payload["dc_id"])
            push(max(int(payload["free_ms"]), floor_ms), _DISPATCH, f"dc:{dc_id:09d}", dc_id)
    for dc_id in sorted(network.fleets):
        push(floor_ms, _DISPATCH, f"dc:{dc_id:09d}", dc_id)
    for demand in sorted(batch.demand, key=lambda d: (d.at_ms, d.key)):
        push(demand.at_ms, _DEMAND, demand.key, demand)

    def arrive(key: str, at_ms: int) -> None:
        shipment = st[key]
        shipment["arrived"] = True
        out.arrivals.append(
            TruckArrival(
                at_ms,
                int(shipment["truck_id"]),
                int(shipment["dc_id"]),
                int(shipment["store_id"]),
                key.split(":", 1)[1],
                int(shipment["planned_ms"]),
                int(shipment["delay_minutes"]),
                shipment["delay_reason"],
                int(shipment["estimated_unload"]),
            )
        )
        push(shipment["done_ms"], _UNLOAD, key, key)

    def unload(key: str, at_ms: int) -> None:
        shipment = st.pop(key)
        shipment_id = key.split(":", 1)[1]
        store_id, dc_id = int(shipment["store_id"]), int(shipment["dc_id"])
        for product_id, quantity in _lines(shipment):
            entry = stock(store_id, product_id)
            entry["on_hand"] += quantity
            entry["inbound"] = max(0, entry["inbound"] - quantity)
            out.moves.append(
                StockMove(
                    at_ms,
                    store_id,
                    dc_id,
                    product_id,
                    quantity,
                    "INBOUND_SHIPMENT",
                    shipment_id,
                    0,
                    entry["on_hand"],
                    f"{shipment_id}#{product_id}",
                )
            )
        out.departures.append(
            TruckDeparture(
                at_ms,
                int(shipment["truck_id"]),
                dc_id,
                store_id,
                shipment_id,
                int(shipment["unload_minutes"]),
                int(shipment["units"]),
            )
        )
        free_ms = at_ms + network.transit_minutes(store_id) * _MINUTE_MS
        st[f"truck:{int(shipment['truck_id'])}"] = {
            "dc_id": dc_id,
            "free_ms": free_ms,
            "shipment": None,
        }
        push(free_ms, _DISPATCH, f"dc:{dc_id:09d}", dc_id)

    def sell(demand: StockDemand) -> None:
        store_id, product_id = demand.store_id, demand.product_id
        dc_id = network.dc_of(store_id)
        entry = stock(store_id, product_id)
        day = demand.local_day.isoformat()
        if entry["day"] is None:
            entry["day"] = day
        elif entry["day"] != day:
            closed = date.fromisoformat(entry["day"])
            rate = smooth_demand(float(entry["rate"]), int(entry["attempted"]))
            entry["rate"] = decay_demand(rate, (demand.local_day - closed).days - 1)
            entry["attempted"] = 0
            entry["day"] = day
        entry["attempted"] += demand.quantity
        before = int(entry["on_hand"])
        if demand.quantity <= before:
            entry["on_hand"] = before - demand.quantity
            out.moves.append(
                StockMove(
                    demand.at_ms,
                    store_id,
                    dc_id,
                    product_id,
                    -demand.quantity,
                    "SALE",
                    demand.source,
                    demand.quantity,
                    entry["on_hand"],
                    demand.key,
                )
            )
            utc_day = _utc_day(demand.at_ms)
            crossed = before >= STOCKOUT_QUANTITY_THRESHOLD > entry["on_hand"]
            if crossed and entry["stockout_day"] != utc_day:
                entry["stockout_day"] = utc_day
                out.stockouts.append(
                    StockoutRecord(demand.at_ms, store_id, dc_id, product_id, before, demand.key)
                )
        else:
            out.lost.append(demand.key)
            out.moves.append(
                StockMove(
                    demand.at_ms,
                    store_id,
                    dc_id,
                    product_id,
                    0,
                    "LOST_SALE",
                    demand.source,
                    demand.quantity,
                    before,
                    demand.key,
                )
            )
        rate = float(entry["rate"])
        point, target = reorder_levels(rate)
        position = int(entry["on_hand"]) + int(entry["inbound"])
        if position > point:
            return
        quantity = reorder_quantity(target, position)
        entry["inbound"] += quantity
        out.reorders.append(
            ReorderRecord(
                demand.at_ms,
                store_id,
                dc_id,
                product_id,
                int(entry["on_hand"]),
                quantity,
                point,
                reorder_priority(int(entry["on_hand"]), rate),
                demand.key,
            )
        )
        pending_key = f"pending:{store_id}"
        pending = st.get(pending_key)
        if pending is None:
            pending = {"dc_id": dc_id, "since_ms": demand.at_ms, "lines": {}}
            st[pending_key] = pending
            ready = _earliest_dispatch(network, store_id, pending, floor_ms)
            push(ready, _DISPATCH, f"dc:{dc_id:09d}", dc_id)
        lines = pending["lines"]
        lines[str(product_id)] = int(lines.get(str(product_id), 0)) + quantity

    def dispatch(dc_id: int, at_ms: int) -> None:
        if dc_id in batch.held_dcs:
            return
        free = []
        for truck_id in network.fleets.get(dc_id, ()):
            truck = st.get(f"truck:{truck_id}")
            if truck is None:
                free.append((0, truck_id))
            elif truck["shipment"] is None and int(truck["free_ms"]) <= at_ms:
                free.append((int(truck["free_ms"]), truck_id))
        ready = sorted(
            (int(payload["since_ms"]), int(key.split(":")[1]))
            for key, payload in st.items()
            if key.startswith("pending:")
            and int(payload["dc_id"]) == dc_id
            and _earliest_dispatch(network, int(key.split(":")[1]), payload, floor_ms) <= at_ms
        )
        for (_, truck_id), (_, store_id) in zip(sorted(free), ready):
            pending = st.pop(f"pending:{store_id}")
            units = sum(quantity for _, quantity in _lines(pending))
            shipment_id = f"SHP-{network.namespace}-{store_id}-{at_ms}"
            depart_ms = at_ms + TRUCK_LOAD_MINUTES * _MINUTE_MS
            planned_ms = depart_ms + network.transit_minutes(store_id) * _MINUTE_MS
            delay_minutes, delay_reason = 0, None
            arrival_ms = planned_ms + delay_minutes * _MINUTE_MS
            unload = network.unload_minutes(store_id, units, shipment_id)
            st[f"shipment:{shipment_id}"] = {
                "truck_id": truck_id,
                "dc_id": dc_id,
                "store_id": store_id,
                "lines": pending["lines"],
                "units": units,
                "dispatch_ms": at_ms,
                "depart_ms": depart_ms,
                "planned_ms": planned_ms,
                "arrival_ms": arrival_ms,
                "delay_minutes": delay_minutes,
                "delay_reason": delay_reason,
                "estimated_unload": network.estimated_unload_minutes(store_id, units),
                "unload_minutes": unload,
                "done_ms": arrival_ms + unload * _MINUTE_MS,
                "arrived": False,
            }
            st[f"truck:{truck_id}"] = {"dc_id": dc_id, "free_ms": None, "shipment": shipment_id}
            push(arrival_ms, _ARRIVE, f"shipment:{shipment_id}", f"shipment:{shipment_id}")

    while queue and queue[0][0] <= batch.clock_ms:
        at_ms, kind, _tie, ref = heapq.heappop(queue)
        if kind == _ARRIVE:
            if ref in st and not st[ref]["arrived"]:
                arrive(ref, at_ms)
        elif kind == _UNLOAD:
            if ref in st and st[ref]["arrived"]:
                unload(ref, at_ms)
        elif kind == _DEMAND:
            sell(ref)
        else:
            dispatch(int(ref), at_ms)
    return st, out


def lost_lines(output: SupplyOutput) -> dict[str, list[int]]:
    """Lost-sale line numbers by receipt or order ID (demand keys are ``<id>#<line>``)."""
    lost: dict[str, list[int]] = {}
    for key in output.lost:
        owner, line = key.rsplit("#", 1)
        lost.setdefault(owner, []).append(int(line))
    return {owner: sorted(lines) for owner, lines in sorted(lost.items())}


def run_supply_batch(
    store: StateStore,
    batch_id: int,
    batch: SupplyBatch,
    seed_entries: Callable[[set[str]], SupplyState],
) -> tuple[SupplyOutput, dict[str, Changes]]:
    """Read the supply state a batch needs, run it, and return its output and state changes.

    Inventory entries the stream has not touched yet come from
    ``seed_entries(keys)``, which must return an entry for every key it is
    given (the Silver ledger as of the incarnation start, else history's
    opening stock). Reads are as of the previous batch, so a replay of
    ``batch_id`` returns the same output and changes.
    """
    logistics = store.read(LOGISTICS_STATE, batch_id)
    keys = required_inventory_keys(logistics, batch)
    inventory = store.read_keys(INVENTORY_STATE, batch_id, keys)
    missing = keys - inventory.keys()
    if missing:
        seeded = seed_entries(missing)
        absent = missing - seeded.keys()
        if absent:
            raise KeyError(f"no inventory seed for {sorted(absent)[:5]}")
        inventory.update({key: seeded[key] for key in sorted(missing)})
    new_state, output = apply_batch({**inventory, **logistics}, batch, apply_supply_batch)
    new_inventory, new_logistics = split_supply_state(new_state)
    return output, {
        INVENTORY_STATE: state_changes(inventory, new_inventory),
        LOGISTICS_STATE: state_changes(logistics, new_logistics),
    }
