"""Causal, streaming inventory policy, independent of Spark.

Each SKU learns an EWMA of attempted daily demand (including lost sales).
There are no backorders: rejected whole lines have no stock commitment.
Inventory position includes outstanding deliveries and accepted restock returns.

Deliveries follow the store's delivery slot, as in the live stream: every SKU a
store reorders when a day closes ships on that store's next consolidated load,
which leaves the DC at the store's daily slot (``network.plan_routes``), drives
the lane and is unloaded on arrival. The SKU simulation only needs the slot and
the lane time, so it stays independent of other SKUs; ``inventory.py``
consolidates the reorders into capacity-bounded shipments and puts them on
trucks. The DC is an unlimited just-in-time supplier. This is a stock/service
simulation, not a vehicle-routing or supplier-capacity model.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as inventory_time
from hashlib import sha256
from heapq import heappop, heappush
from math import ceil
from typing import Iterable, Iterator

from retail_setup.generation.calibration import (
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

    @property
    def trace_id(self) -> str:
        return f"TRC-INV-{self.receipt_id}-{self.line_num}"


@dataclass
class InventoryEvent:
    store_id: int
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
    )


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
    """Consume sorted demand with O(one SKU + pending deliveries/returns) state.

    ``daily_prior`` is the SKU's expected daily demand: it sizes the opening
    stock and starts the demand estimate, so history has no start-up stockout
    wave. ``slot_minute`` and ``transit_minutes`` are the store's delivery slot
    and lane (``network.plan_routes``); they default to the stable fallbacks.
    """
    if daily_prior < 0 or end_date < start_date:
        raise ValueError("invalid inventory simulation bounds")
    if slot_minute is None:
        slot_minute = delivery_slot_minute(seed, store_id)
    if transit_minutes is None:
        transit_minutes = lane_transit_minutes(seed, 0, store_id)
    on_hand = opening_stock(seed, store_id, product_id, daily_prior)
    yield InventoryEvent(
        store_id,
        product_id,
        datetime.combine(start_date, inventory_time()),
        f"TRC-INIT-ST-{store_id}-{product_id}",
        "INITIAL",
        on_hand,
        "SEED",
        balance=on_hand,
    )
    pending: list[tuple[datetime, str, InventoryEvent]] = []
    inbound = 0
    expected_returns = 0
    rate = daily_prior
    iterator = iter(demand)
    candidate = next(iterator, None)
    previous: tuple[datetime, str] | None = None
    day = start_date
    while day <= end_date:
        closing = datetime.combine(day, inventory_time(23, 59, 59))
        attempted = 0
        while candidate is not None and candidate.event_ts <= closing:
            key = (candidate.event_ts, candidate.trace_id)
            if candidate.event_ts.date() < start_date or (previous and key <= previous):
                raise ValueError("demand must have unique, chronological line keys")
            if candidate.quantity <= 0:
                raise ValueError("candidate demand quantity must be positive")
            previous = key
            while pending and pending[0][:2] <= key:
                _, _, event = heappop(pending)
                on_hand += event.quantity
                if event.kind == "RETURN":
                    expected_returns -= event.quantity
                else:
                    inbound -= event.quantity
                event.balance = on_hand
                yield event
            attempted += candidate.quantity
            fulfilled = candidate.quantity <= on_hand
            if fulfilled:
                on_hand -= candidate.quantity
            yield InventoryEvent(
                store_id,
                product_id,
                candidate.event_ts,
                candidate.trace_id,
                "SALE" if fulfilled else "LOST_SALE",
                -candidate.quantity if fulfilled else 0,
                "CUSTOMER_PURCHASE",
                balance=on_hand,
                receipt_id=candidate.receipt_id,
                line_num=candidate.line_num,
                requested_quantity=candidate.quantity,
                unfulfilled_quantity=0 if fulfilled else candidate.quantity,
            )
            if fulfilled and candidate.return_ts is not None:
                if candidate.return_ts <= candidate.event_ts or not candidate.return_id:
                    raise ValueError("restock returns must identify a later return")
                returned = InventoryEvent(
                    store_id,
                    product_id,
                    candidate.return_ts,
                    f"TRC-INV-{candidate.return_id}-{candidate.line_num}",
                    "RETURN",
                    candidate.quantity,
                    candidate.return_id,
                )
                heappush(pending, (returned.event_ts, returned.trace_id, returned))
                expected_returns += returned.quantity
            candidate = next(iterator, None)
        while pending and pending[0][0] <= closing:
            _, _, event = heappop(pending)
            on_hand += event.quantity
            if event.kind == "RETURN":
                expected_returns -= event.quantity
            else:
                inbound -= event.quantity
            event.balance = on_hand
            yield event
        rate = smooth_demand(rate, attempted)
        point, target = reorder_levels(rate)
        # Only near-term returns reduce the order. A distant return cannot
        # cover demand before the service lead time has elapsed.
        near_returns = (
            sum(
                event.quantity
                for ts, _, event in pending
                if event.kind == "RETURN" and ts <= closing + timedelta(days=4)
            )
            if expected_returns
            else 0
        )
        position = on_hand + inbound + near_returns
        if position <= point:
            quantity = reorder_quantity(target, position)
            priority = reorder_priority(on_hand, rate)
            yield InventoryEvent(
                store_id,
                product_id,
                closing,
                f"TRC-RO-{store_id}-{day}-{product_id}",
                "REORDER",
                quantity,
                "POLICY",
                balance=on_hand,
                reorder_point=point,
                priority=priority,
            )
            shipment = shipment_event(
                store_id,
                product_id,
                day,
                quantity,
                slot_minute=slot_minute,
                transit_minutes=transit_minutes,
            )
            yield shipment
            if shipment.unloading_ts is None:
                raise RuntimeError(f"shipment {shipment.source} has no unloading time")
            delivery = InventoryEvent(
                store_id,
                product_id,
                shipment.unloading_ts,
                f"TRC-SI-{shipment.source}-{product_id}",
                "INBOUND_SHIPMENT",
                shipment.quantity,
                shipment.source,
            )
            heappush(pending, (delivery.event_ts, delivery.trace_id, delivery))
            inbound += delivery.quantity
        day += timedelta(days=1)
    if candidate is not None:
        raise ValueError("candidate demand falls outside the simulation history")
