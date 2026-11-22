"""Live store and DC inventory, trucks and incidents as one pure state machine.

Each micro-batch replays, in time order, everything that happens to store
stock, DC stock and the DC fleets between the previous batch clock and this
one:

- **Demand.** Every emitted in-store receipt line and every store-fulfilled
  online order line (ship-from-store, BOPIS) takes stock from its store. A line
  the store cannot fill becomes a lost sale instead, as in history: the stock
  never goes negative and the line is dropped from the receipt or order.
  Ship-from-DC online lines take stock from the DC; units the DC lacks are
  expedited, as in history.
- **Stockouts and reorders.** A sale that takes the balance to zero reports a
  stockout (once per store, product and UTC day, like ``fact_stockouts``).
  After each sale or lost sale the shared replenishment policy
  (``inventory_policy``: EWMA demand, reorder point, order-up-to target, case
  pack, priority) checks the inventory position, on hand plus inbound.
- **Shipments.** A reorder joins its store's pending load at the store's DC.
  The load's schedule comes from ``incidents.plan_delivery``, as in history:
  the store's next open delivery slot, held through DC outages, loaded late
  under holiday capacity limits, and late from traffic, lane incidents and
  weather. When it is due the next free truck of the DC's fleet loads what the
  DC has in stock (a short-ship leaves the rest of the reorder unfilled, so the
  store reorders it), drives the lane, arrives at the planned arrival plus the
  delay, unloads for a load-, site-, hour- and lateness-driven dwell, departs
  and drives back. A truck carries one shipment at a time, so it is never in
  two places at once. Arrivals and departures are emitted only when their time
  has passed.
- **DC stock.** Each DC product keeps stock, smoothed daily demand and open
  supplier orders (the ``inventory_policy`` DC policy, as in history).
  Supplier orders arrive after the product's lead time, short during a
  ``SUPPLIER_SHORT_SHIP`` incident on its supplier group. A DC stockout is
  reported like a store's, with no store.

State lives in two tables. ``inventory`` holds one entry per store and
product (``inv:<store>:<product>``) and per DC and product
(``dc:<dc>:<product>``) that the stream has touched; entries the stream has
not touched yet are seeded from the Silver ledgers as of the incarnation
start. ``logistics`` holds pending loads (``pending:<store>``), shipments on
the road (``shipment:<id>``), truck positions (``truck:<id>``) and each DC's
open supplier orders (``supply:<dc>``); a truck with no entry is idle at its
DC.

Incidents are not state: they are planned from the seed and the dates
(``incidents.plan_incidents``), and :func:`incident_transitions` emits each
start and resolution in the batch whose window holds it.

Everything here is pure Python: :func:`apply_supply_batch` maps a state and a
:class:`SupplyBatch` to the next state and the emitted records, so a replayed
batch emits exactly the same events. ``build_notebooks.py`` inlines this
module into the stream notebook.
"""

import heapq
from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from retail_setup.generation.calibration import (
    DC_ONLINE_DEMAND_SHARE,
    DC_SUPPLIER_RECEIPT_HOUR,
    STOCKOUT_QUANTITY_THRESHOLD,
    TRUCK_LOAD_MINUTES,
    TRUCK_NORMAL_DWELL_MINUTES_MIN,
)
from retail_setup.generation.incidents import (
    DeliveryTiming,
    Incident,
    IncidentCalendar,
    StoreIncidentView,
    plan_delivery,
    supplier_group,
    to_local,
    to_utc,
)
from retail_setup.generation.inventory_policy import (
    dc_case_floor,
    dc_lead_days,
    dc_opening_stock,
    dc_reorder_levels,
    decay_demand,
    delivery_slot_minute,
    lane_transit_minutes,
    opening_stock,
    reorder_levels,
    reorder_priority,
    reorder_quantity,
    smooth_dc_demand,
    smooth_demand,
    supplier_receipt,
)
from retail_setup.generation.network import (
    dock_factor,
    estimated_unload_minutes,
    next_slot_ms,
    unload_minutes,
)
from retail_setup.generation.stream_calendar import is_closed_date
from retail_setup.generation.stream_state import Changes, StateStore, apply_batch, state_changes

INVENTORY_STATE = "inventory"
LOGISTICS_STATE = "logistics"
STORE_SALE_SOURCE = "CUSTOMER_PURCHASE"
ONLINE_SALE_SOURCE = "ONLINE_ORDER"
_MINUTE_MS = 60_000
_DAY_MS = 86_400_000
# Same-instant events: arrivals, unloads, supplier receipts, demand, dispatching.
_ARRIVE, _UNLOAD, _RECEIPT, _DEMAND, _DISPATCH = range(5)
# delay_reason of a load that waited for a free truck beyond its schedule.
TRUCK_WAIT_REASON = "TRUCK_AVAILABILITY"

SupplyState = dict[str, dict[str, Any]]


def inventory_key(store_id: int, product_id: int) -> str:
    return f"inv:{int(store_id)}:{int(product_id)}"


def dc_key(dc_id: int, product_id: int) -> str:
    return f"dc:{int(dc_id)}:{int(product_id)}"


def parse_inventory_key(key: str) -> tuple[int, int]:
    """(store or DC, product) of an ``inv:`` or ``dc:`` key."""
    _, node, product = key.split(":")
    return int(node), int(product)


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


def dc_entry(on_hand: int, rate: float) -> dict[str, Any]:
    """A DC-product entry with no live history: balance and smoothed daily demand."""
    return {
        "on_hand": max(0, int(on_hand)),
        "rate": float(rate),
        "day": None,
        "requested": 0,
        "on_order": 0,
        "stockout_day": None,
    }


def default_inventory_entry(
    seed: int, store_id: int, product_id: int, daily_prior: float
) -> dict[str, Any]:
    """Entry for a store-product the Silver ledger has never seen (history's opening stock)."""
    return inventory_entry(opening_stock(seed, store_id, product_id, daily_prior), daily_prior)


def default_dc_entry(
    seed: int, dc_id: int, product_id: int, prior: float, stores: int = 1
) -> dict[str, Any]:
    """Entry for a DC product the Silver ledger has never seen (history's opening stock)."""
    return dc_entry(dc_opening_stock(seed, dc_id, product_id, prior, dc_case_floor(stores)), prior)


def split_supply_state(state: SupplyState) -> tuple[SupplyState, SupplyState]:
    """(inventory, logistics) entries of a combined supply state."""
    stock = ("inv:", "dc:")
    inventory = {key: value for key, value in state.items() if key.startswith(stock)}
    logistics = {key: value for key, value in state.items() if not key.startswith(stock)}
    return inventory, logistics


@dataclass(frozen=True)
class SupplyNetwork:
    """Static supply inputs: store routing, DC fleets, delivery slots and the demand prior.

    ``slots`` and ``zones`` hold each store's delivery slot (minute of the local
    day, from ``network.plan_routes`` as in history) and IANA time zone; a store
    without them ships at a stable UTC minute. ``closed_class`` is the store
    type's holiday class: no delivery arrives on its closure days.
    """

    seed: int
    namespace: str
    store_dc: dict[int, int]
    fleets: dict[int, tuple[int, ...]]
    daily_prior: float
    slots: dict[int, int] = field(default_factory=dict)
    zones: dict[int, str] = field(default_factory=dict)
    closed_class: str | None = None

    def dc_of(self, store_id: int) -> int:
        return self.store_dc[int(store_id)]

    def zone(self, store_id: int) -> str:
        return self.zones.get(int(store_id)) or "UTC"

    def dc_zone(self, dc_id: int) -> str:
        """A DC's zone: that of the first store it serves, as in history."""
        stores = sorted(s for s, dc in self.store_dc.items() if int(dc) == int(dc_id))
        return self.zone(stores[0]) if stores else "UTC"

    def dc_stores(self, dc_id: int) -> int:
        """Number of stores a DC serves."""
        return sum(1 for dc in self.store_dc.values() if int(dc) == int(dc_id))

    def dc_prior(self, dc_id: int) -> float:
        """A DC product's expected daily demand: its stores' prior plus online orders."""
        return self.daily_prior * max(1, self.dc_stores(dc_id)) * (1.0 + DC_ONLINE_DEMAND_SHARE)

    def transit_minutes(self, store_id: int) -> int:
        """One-way drive between a store and its DC (a stable draw per lane)."""
        return lane_transit_minutes(self.seed, self.dc_of(store_id), int(store_id))

    def delivery_slot_minute(self, store_id: int) -> int:
        """Minute of the store's day at which its pending load ships."""
        slot = self.slots.get(int(store_id))
        return delivery_slot_minute(self.seed, store_id) if slot is None else int(slot)

    def next_slot_ms(self, store_id: int, since_ms: int) -> int:
        """The store's first delivery slot at or after ``since_ms``."""
        return next_slot_ms(
            since_ms, self.delivery_slot_minute(store_id), self.zones.get(int(store_id))
        )

    def timing(
        self, store_id: int, since_ms: int, view: StoreIncidentView | None = None
    ) -> DeliveryTiming:
        """Schedule (store-local) of a load whose oldest reorder came at ``since_ms``."""
        klass = self.closed_class
        return plan_delivery(
            self.seed,
            store_id=int(store_id),
            dc_id=self.dc_of(store_id),
            ready=to_local(_utc(since_ms), self.zone(store_id)),
            slot_minute=self.delivery_slot_minute(store_id),
            transit_minutes=self.transit_minutes(store_id),
            view=view,
            closed=None if klass is None else lambda day: is_closed_date(day, klass),
        )

    def dock_factor(self, store_id: int) -> float:
        """How slowly a store's dock unloads relative to the calibrated rate."""
        return dock_factor(self.seed, store_id)

    def estimated_unload_minutes(self, store_id: int, units: int, hour: int | None = None) -> int:
        """Planned dwell of a load: base time plus the units at the site's rate and hour."""
        return estimated_unload_minutes(self.seed, store_id, units, hour)

    def unload_minutes(
        self,
        store_id: int,
        units: int,
        shipment_id: str,
        hour: int | None = None,
        delay_minutes: int = 0,
    ) -> int:
        """Actual dwell: the estimate plus a missed-appointment wait and a little noise."""
        return unload_minutes(self.seed, store_id, units, shipment_id, hour, delay_minutes)


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
class DcStockDemand:
    """A ship-from-DC online order line (the DC ships it when the order is placed)."""

    at_ms: int
    dc_id: int
    product_id: int
    quantity: int
    key: str
    source: str = ONLINE_SALE_SOURCE


@dataclass(frozen=True)
class SupplyBatch:
    """Inputs of one micro-batch.

    ``incidents`` are the planned incidents that can be open in the window
    (``incidents.plan_incidents``): DC outages hold dispatches, holiday
    capacity, lane incidents and weather delay runs, supplier short-ships cut
    supplier receipts. ``held_dcs`` additionally holds every dispatch of the
    listed DCs for this batch (shipments already on the road continue).
    """

    network: SupplyNetwork
    clock_ms: int
    previous_ms: int | None
    demand: tuple[StockDemand, ...] = ()
    held_dcs: frozenset[int] = frozenset()
    incidents: tuple[Incident, ...] = ()
    dc_demand: tuple[DcStockDemand, ...] = ()


@dataclass(frozen=True)
class StockMove:
    """A stock change; ``store_id`` is None for a DC's own rows."""

    at_ms: int
    store_id: int | None
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
    """A balance reaching zero; ``store_id`` is None for a DC stockout."""

    at_ms: int
    store_id: int | None
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
    units: int = 0


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


def _utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def _ms(at: datetime) -> int:
    return round(at.timestamp() * 1000)


def _utc_day(ms: int) -> str:
    return _utc(ms).date().isoformat()


def _lines(payload: dict[str, Any]) -> list[tuple[int, int]]:
    return sorted((int(product), int(quantity)) for product, quantity in payload["lines"].items())


class _BatchIncidents:
    """A batch's incident calendar with per-store views and cached load schedules."""

    def __init__(self, network: SupplyNetwork, incidents: Iterable[Incident]) -> None:
        self.network = network
        self.calendar = IncidentCalendar(incidents)
        self._views: dict[int, StoreIncidentView] = {}
        self._timings: dict[tuple[int, int], DeliveryTiming] = {}

    def view(self, store_id: int) -> StoreIncidentView:
        store_id = int(store_id)
        if store_id not in self._views:
            self._views[store_id] = self.calendar.store_view(
                store_id, self.network.dc_of(store_id), self.network.zone(store_id)
            )
        return self._views[store_id]

    def timing(self, store_id: int, since_ms: int) -> DeliveryTiming:
        key = (int(store_id), int(since_ms))
        if key not in self._timings:
            self._timings[key] = self.network.timing(store_id, since_ms, self.view(store_id))
        return self._timings[key]

    def loading_ms(self, store_id: int, since_ms: int) -> int:
        timing = self.timing(store_id, since_ms)
        return _ms(to_utc(timing.loading, self.network.zone(store_id)))


def _earliest_dispatch(
    incidents: _BatchIncidents, store_id: int, pending: dict[str, Any], floor_ms: int
) -> int:
    """When the store's pending load is due: its scheduled loading, not before ``floor_ms``."""
    return max(incidents.loading_ms(store_id, int(pending["since_ms"])), floor_ms)


def batch_floor(batch: SupplyBatch) -> int:
    """Earliest instant this batch may act at: just after the previous batch clock."""
    if batch.previous_ms is not None:
        return int(batch.previous_ms) + 1
    return min(
        [demand.at_ms for demand in batch.demand]
        + [line.at_ms for line in batch.dc_demand]
        + [batch.clock_ms]
    )


def required_inventory_keys(logistics: SupplyState, batch: SupplyBatch) -> set[str]:
    """Inventory entries a batch can touch.

    Its store and DC demand (with the DC entries a sale's reorder may ship
    from), the store and DC entries of every load that may dispatch or unload
    by its clock, and the DC entries of supplier orders due.
    """
    network = batch.network
    keys = {inventory_key(d.store_id, d.product_id) for d in batch.demand}
    # A sale can reorder, and the reorder can ship from the DC within the batch.
    keys.update(dc_key(network.dc_of(d.store_id), d.product_id) for d in batch.demand)
    keys.update(dc_key(line.dc_id, line.product_id) for line in batch.dc_demand)
    floor_ms = batch_floor(batch)
    incidents = _BatchIncidents(network, batch.incidents)
    for key, payload in logistics.items():
        if key.startswith("supply:"):
            dc_id = int(key.split(":")[1])
            keys.update(
                dc_key(dc_id, product)
                for at_ms, product, _, _ in payload["orders"]
                if at_ms <= batch.clock_ms
            )
            continue
        if key.startswith("shipment:"):
            completes = int(payload["done_ms"])
        elif key.startswith("pending:"):
            store_id = int(key.split(":")[1])
            dispatch_ms = _earliest_dispatch(incidents, store_id, payload, floor_ms)
            if dispatch_ms <= batch.clock_ms:
                for product, _ in _lines(payload):
                    keys.add(dc_key(int(payload["dc_id"]), product))
                    keys.add(inventory_key(store_id, product))
            completes = (
                dispatch_ms
                + (TRUCK_LOAD_MINUTES + network.transit_minutes(store_id)) * _MINUTE_MS
                + TRUCK_NORMAL_DWELL_MINUTES_MIN * _MINUTE_MS
            )
        else:
            continue
        if completes <= batch.clock_ms:
            store_id = int(payload["store_id"]) if "store_id" in payload else int(key.split(":")[1])
            keys.update(inventory_key(store_id, product) for product, _ in _lines(payload))
    return keys


def incident_transitions(
    incidents: Iterable[Incident], previous_ms: int | None, clock_ms: int
) -> tuple[list[Incident], list[Incident]]:
    """``(started, resolved)`` incidents of a batch window.

    An incident starts in the batch whose window ``(previous, clock]`` holds
    its start and is resolved in the one that holds its end. The first batch
    of an incarnation (no previous clock) starts every incident open at its
    clock. Each transition therefore falls in exactly one of a series of
    contiguous windows, and a replayed batch emits the same ones.
    """
    started, resolved = [], []
    for incident in sorted(incidents, key=lambda i: (i.started_at, i.incident_id)):
        start, end = _ms(incident.started_at), _ms(incident.resolved_at)
        if previous_ms is None:
            if start <= clock_ms < end:
                started.append(incident)
            continue
        if previous_ms < start <= clock_ms:
            started.append(incident)
        if previous_ms < end <= clock_ms:
            resolved.append(incident)
    return started, resolved


def apply_supply_batch(state: SupplyState, batch: SupplyBatch) -> tuple[SupplyState, SupplyOutput]:
    """Advance store and DC stock and the fleets through one batch window, in time order."""
    st = deepcopy(state)
    out = SupplyOutput()
    network = batch.network
    floor_ms = batch_floor(batch)
    incidents = _BatchIncidents(network, batch.incidents)
    queue: list[tuple[int, int, str, Any]] = []

    def push(at_ms: int, kind: int, tie: str, ref: Any) -> None:
        heapq.heappush(queue, (int(at_ms), kind, tie, ref))

    def stock(store_id: int, product_id: int) -> dict[str, Any]:
        key = inventory_key(store_id, product_id)
        if key not in st:
            raise KeyError(f"inventory entry {key} was not loaded for this batch")
        return st[key]

    def dc_stock(dc_id: int, product_id: int, at_ms: int) -> dict[str, Any]:
        """The DC entry, its demand estimate rolled to the DC-local day of ``at_ms``."""
        key = dc_key(dc_id, product_id)
        if key not in st:
            raise KeyError(f"inventory entry {key} was not loaded for this batch")
        entry = st[key]
        today = to_local(_utc(at_ms), network.dc_zone(dc_id)).date()
        if entry["day"] is None:
            entry["day"] = today.isoformat()
        elif entry["day"] != today.isoformat():
            closed = date.fromisoformat(entry["day"])
            rate = smooth_dc_demand(float(entry["rate"]), int(entry["requested"]))
            for _ in range(min(60, max(0, (today - closed).days - 1))):
                rate = smooth_dc_demand(rate, 0)
            entry.update(rate=rate, requested=0, day=today.isoformat())
        return entry

    def dc_move(
        at_ms: int,
        dc_id: int,
        product_id: int,
        entry: dict[str, Any],
        delta: int,
        reason: str,
        source: str,
        requested: int,
        key: str,
    ) -> None:
        before = int(entry["on_hand"])
        entry["on_hand"] = before + delta
        out.moves.append(
            StockMove(
                at_ms,
                None,
                dc_id,
                product_id,
                delta,
                reason,
                source,
                requested,
                entry["on_hand"],
                key,
            )
        )
        day = _utc_day(at_ms)
        crossed = before >= STOCKOUT_QUANTITY_THRESHOLD > entry["on_hand"]
        if crossed and entry["stockout_day"] != day:
            entry["stockout_day"] = day
            out.stockouts.append(StockoutRecord(at_ms, None, dc_id, product_id, before, key))

    def replenish(dc_id: int, product_id: int, entry: dict[str, Any], at_ms: int) -> None:
        """Order from the supplier when the DC position reaches its reorder point."""
        lead = dc_lead_days(network.seed, dc_id, product_id)
        point, target = dc_reorder_levels(
            float(entry["rate"]), lead, dc_case_floor(network.dc_stores(dc_id))
        )
        position = int(entry["on_hand"]) + int(entry["on_order"])
        if position > point:
            return
        ordered = reorder_quantity(target, position)
        zone = network.dc_zone(dc_id)
        today = to_local(_utc(at_ms), zone).date()
        arrives = datetime.combine(today + timedelta(days=lead), time(DC_SUPPLIER_RECEIPT_HOUR))
        receipt_ms = max(_ms(to_utc(arrives, zone)), at_ms + 1)
        source = f"SUPPLIER-{dc_id}-{product_id}-{at_ms}"
        supply = st.setdefault(f"supply:{dc_id}", {"orders": []})
        supply["orders"] = sorted(supply["orders"] + [[receipt_ms, product_id, ordered, source]])
        entry["on_order"] = int(entry["on_order"]) + ordered
        push(receipt_ms, _RECEIPT, source, (dc_id, source))

    for key in sorted(st):
        payload = st[key]
        if key.startswith("shipment:"):
            if payload["arrived"]:
                push(payload["done_ms"], _UNLOAD, key, key)
            else:
                push(payload["arrival_ms"], _ARRIVE, key, key)
        elif key.startswith("pending:"):
            dc_id, store_id = int(payload["dc_id"]), int(key.split(":")[1])
            ready = _earliest_dispatch(incidents, store_id, payload, floor_ms)
            push(ready, _DISPATCH, f"dc:{dc_id:09d}", dc_id)
        elif key.startswith("truck:") and payload.get("free_ms") is not None:
            dc_id = int(payload["dc_id"])
            push(max(int(payload["free_ms"]), floor_ms), _DISPATCH, f"dc:{dc_id:09d}", dc_id)
        elif key.startswith("supply:"):
            dc_id = int(key.split(":")[1])
            for at_ms, _, _, source in payload["orders"]:
                push(max(int(at_ms), floor_ms), _RECEIPT, source, (dc_id, source))
    for dc_id in sorted(network.fleets):
        push(floor_ms, _DISPATCH, f"dc:{dc_id:09d}", dc_id)
    for demand in sorted(batch.demand, key=lambda d: (d.at_ms, d.key)):
        push(demand.at_ms, _DEMAND, demand.key, demand)
    for line in sorted(batch.dc_demand, key=lambda d: (d.at_ms, d.key)):
        push(line.at_ms, _DEMAND, line.key, line)

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
                int(shipment["units"]),
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

    def receive(dc_id: int, source: str, at_ms: int) -> None:
        supply = st.get(f"supply:{dc_id}")
        due = [order for order in (supply or {"orders": []})["orders"] if order[3] == source]
        if supply is None or not due:
            return
        supply["orders"] = [order for order in supply["orders"] if order[3] != source]
        if not supply["orders"]:
            st.pop(f"supply:{dc_id}")
        _, product_id, ordered, _ = due[0]
        entry = dc_stock(dc_id, int(product_id), at_ms)
        entry["on_order"] = max(0, int(entry["on_order"]) - int(ordered))
        fill = incidents.calendar.supplier_fill(
            dc_id, supplier_group(network.seed, int(product_id)), _utc(at_ms)
        )
        received = supplier_receipt(int(ordered), fill)
        dc_move(
            at_ms,
            dc_id,
            int(product_id),
            entry,
            received,
            "INBOUND_SHIPMENT",
            source,
            int(ordered),
            source,
        )

    def ship_online(line: DcStockDemand) -> None:
        entry = dc_stock(line.dc_id, line.product_id, line.at_ms)
        short = line.quantity - int(entry["on_hand"])
        if short > 0:
            dc_move(
                line.at_ms,
                line.dc_id,
                line.product_id,
                entry,
                short,
                "EXPEDITED_RECEIPT",
                f"EXPEDITE-{line.key.rsplit('#', 1)[0]}",
                short,
                f"{line.key}#expedite",
            )
        entry["requested"] = int(entry["requested"]) + line.quantity
        dc_move(
            line.at_ms,
            line.dc_id,
            line.product_id,
            entry,
            -line.quantity,
            "ONLINE_SHIPMENT",
            line.source,
            line.quantity,
            line.key,
        )
        replenish(line.dc_id, line.product_id, entry, line.at_ms)

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
            ready = _earliest_dispatch(incidents, store_id, pending, floor_ms)
            push(ready, _DISPATCH, f"dc:{dc_id:09d}", dc_id)
        lines = pending["lines"]
        lines[str(product_id)] = int(lines.get(str(product_id), 0)) + quantity

    def load(
        store_id: int, dc_id: int, pending: dict[str, Any], at_ms: int, shipment_id: str
    ) -> dict[str, int]:
        """Take the pending load's units from the DC: the lines that ship (short-ships drop)."""
        shipped: dict[str, int] = {}
        for product_id, quantity in _lines(pending):
            entry = dc_stock(dc_id, product_id, at_ms)
            units = min(quantity, int(entry["on_hand"]))
            entry["requested"] = int(entry["requested"]) + quantity
            dc_move(
                at_ms,
                dc_id,
                product_id,
                entry,
                -units,
                "OUTBOUND_SHIPMENT",
                shipment_id,
                quantity,
                f"{shipment_id}#{product_id}",
            )
            if units < quantity:
                # A short-ship: the unfilled units are no longer inbound.
                store_entry = stock(store_id, product_id)
                store_entry["inbound"] = max(0, store_entry["inbound"] - (quantity - units))
            if units > 0:
                shipped[str(product_id)] = units
            replenish(dc_id, product_id, entry, at_ms)
        return shipped

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
            and _earliest_dispatch(incidents, int(key.split(":")[1]), payload, floor_ms) <= at_ms
        )
        trucks = iter(sorted(free))
        free_truck = next(trucks, None)
        for _, store_id in ready:
            if free_truck is None:
                break
            pending = st.pop(f"pending:{store_id}")
            shipment_id = f"SHP-{network.namespace}-{store_id}-{at_ms}"
            lines = load(store_id, dc_id, pending, at_ms, shipment_id)
            if not lines:
                continue
            truck_id = free_truck[1]
            free_truck = next(trucks, None)
            units = sum(lines.values())
            timing = incidents.timing(store_id, int(pending["since_ms"]))
            zone = network.zone(store_id)
            wait = max(0, at_ms - _ms(to_utc(timing.loading, zone)))
            planned_ms = _ms(to_utc(timing.planned_arrival, zone))
            arrival_ms = _ms(to_utc(timing.arrival, zone)) + wait
            delay_minutes = max(0, round((arrival_ms - planned_ms) / _MINUTE_MS))
            delay_reason = timing.delay_reason if delay_minutes else None
            if delay_minutes and wait > timing.delay_minutes * _MINUTE_MS:
                delay_reason = TRUCK_WAIT_REASON
            unload = network.unload_minutes(
                store_id, units, shipment_id, to_local(_utc(arrival_ms), zone).hour, delay_minutes
            )
            st[f"shipment:{shipment_id}"] = {
                "truck_id": truck_id,
                "dc_id": dc_id,
                "store_id": store_id,
                "lines": lines,
                "units": units,
                "dispatch_ms": at_ms,
                "depart_ms": at_ms + TRUCK_LOAD_MINUTES * _MINUTE_MS,
                "planned_ms": planned_ms,
                "arrival_ms": arrival_ms,
                "delay_minutes": delay_minutes,
                "delay_reason": delay_reason,
                "estimated_unload": network.estimated_unload_minutes(
                    store_id, units, timing.planned_arrival.hour
                ),
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
        elif kind == _RECEIPT:
            receive(int(ref[0]), str(ref[1]), at_ms)
        elif kind == _DEMAND:
            if isinstance(ref, DcStockDemand):
                ship_online(ref)
            else:
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
