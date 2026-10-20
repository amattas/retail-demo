"""Causal, streaming inventory policy, independent of Spark.

Each SKU learns an EWMA of attempted daily demand (including lost sales).
There are no backorders: rejected whole lines have no stock commitment.
Inventory position includes outstanding deliveries and accepted restock returns.
SKU-specific shipments use deterministic 1-3 day service, capacity-bounded
legs, and unlimited just-in-time supplier replenishment at the DC. This is a
stock/service simulation, not a vehicle-routing or supplier-capacity model.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as inventory_time
from hashlib import sha256
from heapq import heappop, heappush
from math import ceil
from typing import Iterable, Iterator

CASE_PACK = 12


def stable_int(seed: int, *keys: object) -> int:
    """Stable across processes, Python hash seeds, and Spark partition counts."""
    value = "|".join(str(key) for key in (seed, *keys))
    return int.from_bytes(sha256(value.encode("utf-8")).digest()[:8], "big")


def opening_stock(seed: int, store_id: int, product_id: int) -> int:
    """History-independent opening state; never inspect future demand."""
    return 40 + stable_int(seed, store_id, product_id, "opening") % 81


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


def shipment_events(
    seed: int, store_id: int, product_id: int, day: date, quantity: int, capacity: int,
) -> Iterator[InventoryEvent]:
    """One SKU per shipment leg; the ledger consumes these exact timestamps."""
    draw = stable_int(seed, store_id, product_id, day, "service")
    service_day = day + timedelta(days=1 + draw % 3)
    for leg, offset in enumerate(range(0, quantity, capacity)):
        stagger = timedelta(minutes=30 * leg)
        loading = datetime.combine(service_day, inventory_time(4)) + stagger
        departure = loading + timedelta(hours=2)
        arrival = departure + timedelta(minutes=120 + (draw // 3) % 601)
        unloading = arrival + timedelta(minutes=15)
        completed = arrival + timedelta(minutes=30 + (draw // 1803) % 91)
        shipment_id = f"SHIP-{day:%Y%m%d}-{store_id}-{product_id}-{leg}"
        yield InventoryEvent(
            store_id, product_id, datetime.combine(day, inventory_time(23, 59, 59)),
            f"TRC-PLAN-{shipment_id}", "SHIPMENT", min(capacity, quantity - offset),
            shipment_id, shipment_id=shipment_id, leg=leg,
            scheduled_ts=datetime.combine(day + timedelta(days=1), inventory_time()),
            loading_ts=loading, depart_ts=departure, eta=arrival,
            unloading_ts=unloading, etd=completed,
        )


def simulate_sku(
    demand: Iterable[Demand], *, store_id: int, product_id: int,
    start_date: date, end_date: date, seed: int, capacity: int,
    daily_prior: float = 1.0,
) -> Iterator[InventoryEvent]:
    """Consume sorted demand with O(one SKU + pending deliveries/returns) state."""
    if capacity < 1 or daily_prior < 0 or end_date < start_date:
        raise ValueError("invalid inventory simulation bounds")
    on_hand = opening_stock(seed, store_id, product_id)
    yield InventoryEvent(
        store_id, product_id, datetime.combine(start_date, inventory_time()),
        f"TRC-INIT-ST-{store_id}-{product_id}", "INITIAL", on_hand, "SEED",
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
                store_id, product_id, candidate.event_ts, candidate.trace_id,
                "SALE" if fulfilled else "LOST_SALE",
                -candidate.quantity if fulfilled else 0, "CUSTOMER_PURCHASE",
                balance=on_hand, receipt_id=candidate.receipt_id,
                line_num=candidate.line_num, requested_quantity=candidate.quantity,
                unfulfilled_quantity=0 if fulfilled else candidate.quantity,
            )
            if fulfilled and candidate.return_ts is not None:
                if candidate.return_ts <= candidate.event_ts or not candidate.return_id:
                    raise ValueError("restock returns must identify a later return")
                returned = InventoryEvent(
                    store_id, product_id, candidate.return_ts,
                    f"TRC-INV-{candidate.return_id}-{candidate.line_num}", "RETURN",
                    candidate.quantity, candidate.return_id,
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
        rate = 0.75 * rate + 0.25 * attempted
        point = max(CASE_PACK, ceil(rate * 4))
        target = max(point + CASE_PACK, ceil(rate * 8))
        # Only near-term returns reduce the order. A distant return cannot
        # cover demand before the service lead time has elapsed.
        near_returns = sum(
            event.quantity for ts, _, event in pending
            if event.kind == "RETURN" and ts <= closing + timedelta(days=4)
        ) if expected_returns else 0
        position = on_hand + inbound + near_returns
        if position <= point:
            quantity = ceil((target - position) / CASE_PACK) * CASE_PACK
            priority = "URGENT" if on_hand == 0 else (
                "HIGH" if on_hand < rate * 2 else "NORMAL"
            )
            yield InventoryEvent(
                store_id, product_id, closing,
                f"TRC-RO-{store_id}-{day}-{product_id}", "REORDER", quantity,
                "POLICY", balance=on_hand, reorder_point=point, priority=priority,
            )
            for shipment in shipment_events(seed, store_id, product_id, day, quantity, capacity):
                yield shipment
                delivery = InventoryEvent(
                    store_id, product_id, shipment.unloading_ts,
                    f"TRC-SI-{shipment.shipment_id}-{product_id}", "INBOUND_SHIPMENT",
                    shipment.quantity, shipment.shipment_id,
                )
                heappush(pending, (delivery.event_ts, delivery.trace_id, delivery))
                inbound += delivery.quantity
        day += timedelta(days=1)
    if candidate is not None:
        raise ValueError("candidate demand falls outside the simulation history")
