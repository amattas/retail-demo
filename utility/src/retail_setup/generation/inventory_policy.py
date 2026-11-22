"""Causal, streaming inventory policy, independent of Spark.

Each SKU learns an EWMA of attempted daily demand (including lost sales).
There are no backorders: rejected whole lines have no stock commitment.
Inventory position includes outstanding deliveries and accepted restock returns.

Deliveries follow the store's delivery slot, as in the live stream: every SKU a
store reorders when a day closes ships on that store's next consolidated load,
which leaves the DC at the store's daily slot (``network.plan_routes``), drives
the lane and is unloaded on arrival. ``StoreSku`` holds one store-SKU's state;
``dc_supply.simulate_dc_sku`` runs every store of a DC for one SKU against the
DC's own stock (supplier lead times, short-ships, incidents), and
:func:`simulate_sku` runs one store against an unlimited DC with loads on plan.
``inventory.py`` consolidates the shipments into capacity-bounded legs and puts
them on trucks. The DC replenishment policy (lead time, reorder levels,
allocation) is shared with the live stream. This is a stock/service
simulation, not a vehicle-routing model.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as inventory_time
from hashlib import sha256
from heapq import heappop, heappush
from math import ceil
from typing import Iterable, Iterator

from retail_setup.generation.calibration import (
    DC_DEMAND_SMOOTHING,
    DC_OPENING_DAYS,
    DC_ORDER_UP_TO_DAYS,
    DC_SAFETY_DAYS,
    DC_SUPPLIER_LEAD_DAYS,
    TRUCK_CHECKIN_MINUTES,
    TRUCK_LOAD_MINUTES,
    TRUCK_TRANSIT_MINUTES_MAX,
    TRUCK_TRANSIT_MINUTES_MIN,
)

CASE_PACK = 12


def stable_int(seed: int, *keys: object) -> int:
    """Stable across processes, Python hash seeds, and Spark partition counts."""
    value = "|".join(str(key) for key in (seed, *keys))
    return int.from_bytes(sha256(value.encode("utf-8")).digest()[:8], "big")


def opening_stock(seed: int, store_id: int, product_id: int, rate: float = 1.0) -> int:
    """Opening shelf stock for a SKU expected to sell ``rate`` units a day.

    A stable draw between the reorder point and the order-up-to target, so a
    store opens with 4-8 days of cover and its SKUs do not all reorder on the
    first day. ``rate`` is a prior from the demand model, never generated demand.
    """
    point, target = reorder_levels(rate)
    return point + 1 + stable_int(seed, store_id, product_id, "opening") % (target - point)


def delivery_slot_minute(seed: int, store_id: int) -> int:
    """Fallback daily delivery slot (minute of the day) of a store without a route."""
    return stable_int(seed, int(store_id), "slot") % 1440


def lane_transit_minutes(seed: int, dc_id: int, store_id: int) -> int:
    """One-way drive between a store and its DC (a stable draw per lane)."""
    span = TRUCK_TRANSIT_MINUTES_MAX - TRUCK_TRANSIT_MINUTES_MIN + 1
    return TRUCK_TRANSIT_MINUTES_MIN + stable_int(seed, int(dc_id), int(store_id), "lane") % span


# The replenishment policy, shared by history (``simulate_sku``) and the live
# stream (``stream_supply``).


def smooth_demand(rate: float, attempted: int) -> float:
    """Close a day: fold its attempted demand into the EWMA daily demand rate."""
    return 0.75 * rate + 0.25 * attempted


def decay_demand(rate: float, quiet_days: int) -> float:
    """EWMA rate after ``quiet_days`` closed days without any attempted demand."""
    return rate * 0.75 ** max(0, quiet_days)


def reorder_levels(rate: float) -> tuple[int, int]:
    """(reorder point, order-up-to target) for a smoothed daily demand ``rate``."""
    point = max(CASE_PACK, ceil(rate * 4))
    target = max(point + CASE_PACK, ceil(rate * 8))
    return point, target


def reorder_quantity(target: int, position: int) -> int:
    """Whole cases that lift the inventory position to the target."""
    return ceil((target - position) / CASE_PACK) * CASE_PACK


def reorder_priority(on_hand: int, rate: float) -> str:
    return "URGENT" if on_hand == 0 else ("HIGH" if on_hand < rate * 2 else "NORMAL")


@dataclass(frozen=True)
class Demand:
    event_ts: datetime
    receipt_id: str
    line_num: int
    quantity: int
    return_ts: datetime | None = None
    return_id: str | None = None
    # Units the return brings back (a partial return); None means the whole line.
    return_quantity: int | None = None

    @property
    def trace_id(self) -> str:
        return f"TRC-INV-{self.receipt_id}-{self.line_num}"


@dataclass
class InventoryEvent:
    # NULL for the DC's own rows (``kind`` DC_*), which carry ``dc_id``.
    store_id: int | None
    product_id: int
    event_ts: datetime
    trace_id: str
    kind: str
    quantity: int
    source: str
    balance: int = 0
    receipt_id: str | None = None
    line_num: int | None = None
    requested_quantity: int = 0
    unfulfilled_quantity: int = 0
    reorder_point: int | None = None
    priority: str | None = None
    shipment_id: str | None = None
    leg: int | None = None
    scheduled_ts: datetime | None = None
    loading_ts: datetime | None = None
    depart_ts: datetime | None = None
    eta: datetime | None = None
    unloading_ts: datetime | None = None
    etd: datetime | None = None
    dc_id: int | None = None
    # Planned arrival and the delay (minutes, reason) of a shipment's load.
    planned_eta: datetime | None = None
    delay_minutes: int = 0
    delay_reason: str | None = None


def shipment_base_id(dispatch: datetime, store_id: int) -> str:
    """A store's consolidated load of one dispatch day (``inventory.py`` adds the leg)."""
    return f"SHP-{dispatch:%Y%m%d}-{int(store_id):04d}"


def shipment_event(
    store_id: int,
    product_id: int,
    day: date,
    quantity: int,
    *,
    slot_minute: int,
    transit_minutes: int,
) -> InventoryEvent:
    """The SKU's line on its store's next load: the day after the reorder, at the slot.

    Timing depends only on the store's slot and lane, so every SKU the store
    reorders on ``day`` shares one dispatch and one arrival. Stock is received
    when unloading starts; ``inventory.py`` adds the unload duration (``etd``).
    """
    if not 0 <= slot_minute < 1440 or transit_minutes < 0:
        raise ValueError("invalid delivery slot or lane time")
    loading = datetime.combine(day + timedelta(days=1), inventory_time()) + timedelta(
        minutes=slot_minute
    )
    departure = loading + timedelta(minutes=TRUCK_LOAD_MINUTES)
    arrival = departure + timedelta(minutes=transit_minutes)
    shipment_id = shipment_base_id(loading, store_id)
    return InventoryEvent(
        store_id,
        product_id,
        datetime.combine(day, inventory_time(23, 59, 59)),
        f"TRC-PLAN-{shipment_id}-{product_id}",
        "SHIPMENT",
        quantity,
        shipment_id,
        shipment_id=shipment_id,
        leg=0,
        scheduled_ts=datetime.combine(day + timedelta(days=1), inventory_time()),
        loading_ts=loading,
        depart_ts=departure,
        eta=arrival,
        unloading_ts=arrival + timedelta(minutes=TRUCK_CHECKIN_MINUTES),
        planned_eta=arrival,
    )


class StoreSku:
    """One store-SKU's stock, demand estimate and pending receipts (the store policy).

    Shared by the single-store simulation (:func:`simulate_sku`) and the DC
    simulation (``dc_supply``), which decides how much of each reorder ships.
    """

    def __init__(
        self, *, store_id: int, product_id: int, seed: int, start_date: date, prior: float
    ) -> None:
        self.store_id = int(store_id)
        self.product_id = int(product_id)
        self.on_hand = opening_stock(seed, store_id, product_id, prior)
        self.rate = prior
        self.inbound = 0
        self.expected_returns = 0
        self.attempted = 0
        self.start_date = start_date
        self.pending: list[tuple[datetime, str, InventoryEvent]] = []
        self.previous: tuple[datetime, str] | None = None

    def opening(self) -> InventoryEvent:
        return InventoryEvent(
            self.store_id,
            self.product_id,
            datetime.combine(self.start_date, inventory_time()),
            f"TRC-INIT-ST-{self.store_id}-{self.product_id}",
            "INITIAL",
            self.on_hand,
            "SEED",
            balance=self.on_hand,
        )

    def _receive(self) -> InventoryEvent:
        _, _, event = heappop(self.pending)
        self.on_hand += event.quantity
        if event.kind == "RETURN":
            self.expected_returns -= event.quantity
        else:
            self.inbound -= event.quantity
        event.balance = self.on_hand
        return event

    def consume(self, candidate: Demand) -> Iterator[InventoryEvent]:
        """Receipts due before the demand line, then the sale or lost sale."""
        key = (candidate.event_ts, candidate.trace_id)
        if candidate.event_ts.date() < self.start_date or (self.previous and key <= self.previous):
            raise ValueError("demand must have unique, chronological line keys")
        if candidate.quantity <= 0:
            raise ValueError("candidate demand quantity must be positive")
        self.previous = key
        while self.pending and self.pending[0][:2] <= key:
            yield self._receive()
        self.attempted += candidate.quantity
        fulfilled = candidate.quantity <= self.on_hand
        if fulfilled:
            self.on_hand -= candidate.quantity
        yield InventoryEvent(
            self.store_id,
            self.product_id,
            candidate.event_ts,
            candidate.trace_id,
            "SALE" if fulfilled else "LOST_SALE",
            -candidate.quantity if fulfilled else 0,
            "CUSTOMER_PURCHASE",
            balance=self.on_hand,
            receipt_id=candidate.receipt_id,
            line_num=candidate.line_num,
            requested_quantity=candidate.quantity,
            unfulfilled_quantity=0 if fulfilled else candidate.quantity,
        )
        if fulfilled and candidate.return_ts is not None:
            if candidate.return_ts <= candidate.event_ts or not candidate.return_id:
                raise ValueError("restock returns must identify a later return")
            returned = InventoryEvent(
                self.store_id,
                self.product_id,
                candidate.return_ts,
                f"TRC-INV-{candidate.return_id}-{candidate.line_num}",
                "RETURN",
                min(candidate.quantity, candidate.return_quantity or candidate.quantity),
                candidate.return_id,
            )
            heappush(self.pending, (returned.event_ts, returned.trace_id, returned))
            self.expected_returns += returned.quantity

    def release(self, closing: datetime) -> Iterator[InventoryEvent]:
        """Receipts and returns due by ``closing``."""
        while self.pending and self.pending[0][0] <= closing:
            yield self._receive()

    def close_day(self, closing: datetime) -> tuple[int, int, str] | None:
        """Fold the day's demand into the rate; ``(quantity, point, priority)`` to reorder."""
        self.rate = smooth_demand(self.rate, self.attempted)
        self.attempted = 0
        point, target = reorder_levels(self.rate)
        # Only near-term returns reduce the order. A distant return cannot
        # cover demand before the service lead time has elapsed.
        near_returns = (
            sum(
                event.quantity
                for ts, _, event in self.pending
                if event.kind == "RETURN" and ts <= closing + timedelta(days=4)
            )
            if self.expected_returns
            else 0
        )
        position = self.on_hand + self.inbound + near_returns
        if position > point:
            return None
        return (
            reorder_quantity(target, position),
            point,
            reorder_priority(self.on_hand, self.rate),
        )

    def reorder_event(self, day: date, quantity: int, point: int, priority: str) -> InventoryEvent:
        return InventoryEvent(
            self.store_id,
            self.product_id,
            datetime.combine(day, inventory_time(23, 59, 59)),
            f"TRC-RO-{self.store_id}-{day}-{self.product_id}",
            "REORDER",
            quantity,
            "POLICY",
            balance=self.on_hand,
            reorder_point=point,
            priority=priority,
        )

    def expect(self, shipment: InventoryEvent) -> InventoryEvent:
        """Book a shipment's receipt (when its unloading starts); returns the receipt."""
        if shipment.unloading_ts is None:
            raise RuntimeError(f"shipment {shipment.source} has no unloading time")
        delivery = InventoryEvent(
            self.store_id,
            self.product_id,
            shipment.unloading_ts,
            f"TRC-SI-{shipment.source}-{self.product_id}",
            "INBOUND_SHIPMENT",
            shipment.quantity,
            shipment.source,
        )
        heappush(self.pending, (delivery.event_ts, delivery.trace_id, delivery))
        self.inbound += delivery.quantity
        return delivery

    def add_to(self, delivery: InventoryEvent, quantity: int) -> None:
        """Add units to a receipt already booked (a load that took a later reorder)."""
        delivery.quantity += quantity
        self.inbound += quantity


def simulate_sku(
    demand: Iterable[Demand],
    *,
    store_id: int,
    product_id: int,
    start_date: date,
    end_date: date,
    seed: int,
    daily_prior: float = 1.0,
    slot_minute: int | None = None,
    transit_minutes: int | None = None,
) -> Iterator[InventoryEvent]:
    """One store-SKU supplied by an unlimited DC, with loads always on plan.

    Consumes sorted demand with O(one SKU + pending deliveries/returns) state.
    ``daily_prior`` is the SKU's expected daily demand: it sizes the opening
    stock and starts the demand estimate, so history has no start-up stockout
    wave. ``slot_minute`` and ``transit_minutes`` are the store's delivery slot
    and lane (``network.plan_routes``); they default to the stable fallbacks.
    History runs the DC-limited, incident-aware ``dc_supply.simulate_dc_sku``.
    """
    if daily_prior < 0 or end_date < start_date:
        raise ValueError("invalid inventory simulation bounds")
    if slot_minute is None:
        slot_minute = delivery_slot_minute(seed, store_id)
    if transit_minutes is None:
        transit_minutes = lane_transit_minutes(seed, 0, store_id)
    sku = StoreSku(
        store_id=store_id,
        product_id=product_id,
        seed=seed,
        start_date=start_date,
        prior=daily_prior,
    )
    yield sku.opening()
    iterator = iter(demand)
    candidate = next(iterator, None)
    day = start_date
    while day <= end_date:
        closing = datetime.combine(day, inventory_time(23, 59, 59))
        while candidate is not None and candidate.event_ts <= closing:
            yield from sku.consume(candidate)
            candidate = next(iterator, None)
        yield from sku.release(closing)
        order = sku.close_day(closing)
        if order is not None:
            quantity, point, priority = order
            yield sku.reorder_event(day, quantity, point, priority)
            shipment = shipment_event(
                store_id,
                product_id,
                day,
                quantity,
                slot_minute=slot_minute,
                transit_minutes=transit_minutes,
            )
            yield shipment
            sku.expect(shipment)
        day += timedelta(days=1)
    if candidate is not None:
        raise ValueError("candidate demand falls outside the simulation history")


# ---------------------------------------------------------------------------
# DC replenishment (shared by dc_supply and stream_supply)
# ---------------------------------------------------------------------------


def dc_lead_days(seed: int, dc_id: int, product_id: int) -> int:
    """Supplier lead time (days) of a DC's product (a stable draw)."""
    low, high = DC_SUPPLIER_LEAD_DAYS
    return low + stable_int(seed, int(dc_id), int(product_id), "lead") % (high - low + 1)


def dc_case_floor(stores: int) -> int:
    """Smallest useful DC stock: a store reorders whole cases, so a case per store."""
    return max(1, int(stores)) * CASE_PACK


def dc_opening_stock(
    seed: int, dc_id: int, product_id: int, rate: float, floor: int = CASE_PACK
) -> int:
    """A DC's opening stock: a stable 10-16 days of its expected daily demand, in cases.

    At least two of ``floor`` (``dc_case_floor``): a slow mover still covers a
    first case to each store and the next.
    """
    low, high = DC_OPENING_DAYS
    days = low + stable_int(seed, int(dc_id), int(product_id), "dc-opening") % (high - low + 1)
    return max(2 * floor, ceil(rate * days / CASE_PACK) * CASE_PACK)


def smooth_dc_demand(rate: float, outbound: int) -> float:
    """Close a DC day: fold its requested units into the smoothed daily demand."""
    return (1.0 - DC_DEMAND_SMOOTHING) * rate + DC_DEMAND_SMOOTHING * outbound


def dc_reorder_levels(rate: float, lead_days: int, floor: int = CASE_PACK) -> tuple[int, int]:
    """(reorder point, order-up-to target) of a DC product with ``lead_days`` supply.

    ``floor`` (``dc_case_floor``) keeps a case per store in reserve for slow movers.
    """
    point = max(floor, ceil(rate * (lead_days + 1 + DC_SAFETY_DAYS)))
    target = max(point + floor, ceil(rate * (lead_days + DC_ORDER_UP_TO_DAYS)))
    return point, target


def supplier_receipt(ordered: int, fill_rate: float) -> int:
    """Units of a supplier order that arrive (whole cases) at a fill rate."""
    if fill_rate >= 1.0:
        return ordered
    return int(ordered * fill_rate) // CASE_PACK * CASE_PACK


def allocate(available: int, requests: Iterable[tuple[object, int]]) -> list[tuple[object, int]]:
    """Ship requests in order from ``available`` units: ``[(key, shipped)]``."""
    shipped = []
    for key, quantity in requests:
        ship = max(0, min(int(quantity), available))
        available -= ship
        shipped.append((key, ship))
    return shipped


PRIORITY_ORDER = {"URGENT": 0, "HIGH": 1, "NORMAL": 2}
