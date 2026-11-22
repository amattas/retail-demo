"""DC-limited, incident-aware supply of one SKU across a DC's stores (history).

:func:`simulate_dc_sku` runs every store a DC serves for one product, day by
day, against the DC's own stock:

- **Stores.** Each store-SKU follows the store policy (``inventory_policy``):
  sales and lost sales, restocking returns, the smoothed daily demand, and a
  reorder when a day closes with the inventory position at the reorder point.
- **Allocation.** When a day closes the DC ships the day's reorders from the
  stock it has, most urgent first (URGENT, HIGH, NORMAL, then store ID). A
  reorder the DC cannot cover is short-shipped: only the shipped units are
  inbound to the store, so the store reorders again the next day and runs out
  if the DC stays empty. DC stockouts therefore reach store shelves.
- **Supplier.** The DC orders from its supplier when its position (on hand plus
  open orders) falls to the reorder point (``inventory_policy.dc_reorder_levels``);
  an order arrives after the product's stable lead time at
  ``DC_SUPPLIER_RECEIPT_HOUR``. During a ``SUPPLIER_SHORT_SHIP`` incident on
  the product's supplier group only part of it arrives.
- **Online.** Ship-from-DC online lines take stock when they ship; a line the
  DC cannot fill is covered by an expedited receipt just before it.
- **Delivery.** Each shipment follows ``incidents.plan_delivery``: the store's
  next open delivery slot, held through DC outages, delayed by holiday
  capacity, traffic, lane incidents and weather. Reorders whose loads leave at
  the same slot (a load held by an outage) travel as one load.

All times are local: store rows in the store's zone, DC rows (receipts, online
lines, expedites, the opening stock) in the DC's zone. Supplier receipts
arrive early in the DC's day and allocations only use stock received by the
previous close, so the DC's physical balance never goes negative in true time.
"""

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from retail_setup.generation.calibration import (
    DC_ONLINE_DEMAND_SHARE,
    DC_SUPPLIER_RECEIPT_HOUR,
    TRUCK_CHECKIN_MINUTES,
)
from retail_setup.generation.incidents import (
    DcIncidentView,
    DeliveryTiming,
    StoreIncidentView,
    plan_delivery,
    supplier_group,
)
from retail_setup.generation.inventory_policy import (
    PRIORITY_ORDER,
    Demand,
    InventoryEvent,
    StoreSku,
    dc_case_floor,
    dc_lead_days,
    dc_opening_stock,
    dc_reorder_levels,
    delivery_slot_minute,
    lane_transit_minutes,
    reorder_quantity,
    shipment_base_id,
    smooth_dc_demand,
    supplier_receipt,
)
from retail_setup.generation.stream_calendar import is_closed_date

DC_KINDS = ("DC_INITIAL", "DC_RECEIPT", "DC_ONLINE", "DC_EXPEDITE")


@dataclass(frozen=True)
class StoreLane:
    """A store's place in a DC simulation: slot, lane, demand prior and incidents."""

    slot_minute: int
    transit_minutes: int
    prior: float = 1.0
    view: StoreIncidentView | None = None


@dataclass(frozen=True)
class DcDemand:
    """A ship-from-DC online order line (DC-local ship time)."""

    event_ts: datetime
    order_id: str
    line_num: int
    quantity: int

    @property
    def trace_id(self) -> str:
        return f"TRC-DCO-{self.order_id}-{self.line_num}"


# A plain dict, not functools.lru_cache: the setup notebooks inline this module
# into __main__, and cloudpickle ships an lru_cache wrapper to Spark workers by
# reference, where __main__ has no such name.
_TIMINGS: dict[tuple[object, ...], DeliveryTiming] = {}
_TIMINGS_MAX = 1 << 17


def delivery_timing(
    seed: int,
    store_id: int,
    dc_id: int,
    day: date,
    slot_minute: int,
    transit_minutes: int,
    view: StoreIncidentView | None,
    closed_class: str | None,
) -> DeliveryTiming:
    """Timing of the load that carries a store's reorders of ``day`` (cached per process)."""
    key = (seed, store_id, dc_id, day, slot_minute, transit_minutes, view, closed_class)
    found = _TIMINGS.get(key)
    if found is None:
        if len(_TIMINGS) >= _TIMINGS_MAX:
            _TIMINGS.clear()
        found = _TIMINGS[key] = plan_delivery(
            seed,
            store_id=store_id,
            dc_id=dc_id,
            ready=datetime.combine(day, time(23, 59, 59)),
            slot_minute=slot_minute,
            transit_minutes=transit_minutes,
            view=view,
            closed=None if closed_class is None else lambda d: is_closed_date(d, closed_class),
        )
    return found


class DcSku:
    """One DC product's stock, smoothed demand and open supplier orders."""

    def __init__(
        self,
        *,
        dc_id: int,
        product_id: int,
        seed: int,
        start_date: date,
        prior: float,
        view: DcIncidentView | None = None,
        stores: int = 1,
    ) -> None:
        self.dc_id = int(dc_id)
        self.product_id = int(product_id)
        self.lead = dc_lead_days(seed, dc_id, product_id)
        self.group = supplier_group(seed, product_id)
        self.floor = dc_case_floor(stores)
        self.rate = prior
        self.on_hand = dc_opening_stock(seed, dc_id, product_id, prior, self.floor)
        self.start_date = start_date
        self.view = view
        self.orders: list[tuple[datetime, int, str]] = []
        self.on_order = 0

    def _event(
        self, at: datetime, trace: str, kind: str, quantity: int, source: str, **extra: object
    ) -> InventoryEvent:
        return InventoryEvent(
            None,
            self.product_id,
            at,
            trace,
            kind,
            quantity,
            source,
            balance=self.on_hand,
            dc_id=self.dc_id,
            **extra,  # type: ignore[arg-type]
        )

    def opening(self) -> InventoryEvent:
        return self._event(
            datetime.combine(self.start_date, time()),
            f"TRC-INIT-DC-{self.dc_id}-{self.product_id}",
            "DC_INITIAL",
            self.on_hand,
            "SEED",
        )

    def receive(self, until: datetime) -> Iterator[InventoryEvent]:
        """Supplier orders that arrive by ``until`` (short while the supplier short-ships)."""
        while self.orders and self.orders[0][0] <= until:
            at, ordered, source = self.orders.pop(0)
            self.on_order -= ordered
            fill = 1.0 if self.view is None else self.view.fill_rate(self.group, at)
            received = supplier_receipt(ordered, fill)
            if received <= 0:
                continue
            self.on_hand += received
            yield self._event(
                at,
                f"TRC-SUP-{source}",
                "DC_RECEIPT",
                received,
                source,
                requested_quantity=ordered,
                unfulfilled_quantity=ordered - received,
            )

    def fill_online(self, line: DcDemand) -> Iterator[InventoryEvent]:
        """Ship an online line, expediting any units the DC lacks just before it."""
        short = line.quantity - self.on_hand
        if short > 0:
            self.on_hand += short
            yield self._event(
                line.event_ts - timedelta(seconds=1),
                f"TRC-EXP-{line.order_id}-{line.line_num}",
                "DC_EXPEDITE",
                short,
                f"EXPEDITE-{line.order_id}",
            )
        self.on_hand -= line.quantity
        yield self._event(
            line.event_ts,
            line.trace_id,
            "DC_ONLINE",
            -line.quantity,
            line.order_id,
            requested_quantity=line.quantity,
        )

    def ship(self, requested: int) -> int:
        shipped = max(0, min(int(requested), self.on_hand))
        self.on_hand -= shipped
        return shipped

    def close_day(self, day: date, outbound: int) -> None:
        """Fold the day's requests into the demand estimate and order from the supplier."""
        self.rate = smooth_dc_demand(self.rate, outbound)
        point, target = dc_reorder_levels(self.rate, self.lead, self.floor)
        position = self.on_hand + self.on_order
        if position > point:
            return
        ordered = reorder_quantity(target, position)
        arrival = datetime.combine(day + timedelta(days=self.lead), time(DC_SUPPLIER_RECEIPT_HOUR))
        source = f"SUPPLIER-{self.dc_id}-{self.product_id}-{day:%Y%m%d}"
        self.orders.append((arrival, ordered, source))
        self.orders.sort()
        self.on_order += ordered


def load_event(
    store_id: int, product_id: int, dc_id: int, timing: DeliveryTiming, quantity: int
) -> InventoryEvent:
    """The SKU's line on the store's load that leaves at ``timing``'s slot."""
    shipment_id = shipment_base_id(timing.slot, store_id)
    scheduled = datetime.combine(timing.planned_loading.date(), time())
    return InventoryEvent(
        store_id,
        product_id,
        scheduled - timedelta(seconds=1),
        f"TRC-PLAN-{shipment_id}-{product_id}",
        "SHIPMENT",
        quantity,
        shipment_id,
        shipment_id=shipment_id,
        leg=0,
        scheduled_ts=scheduled,
        loading_ts=timing.loading,
        depart_ts=timing.departure,
        eta=timing.arrival,
        unloading_ts=timing.arrival + timedelta(minutes=TRUCK_CHECKIN_MINUTES),
        dc_id=dc_id,
        planned_eta=timing.planned_arrival,
        delay_minutes=timing.delay_minutes,
        delay_reason=timing.delay_reason,
    )


def simulate_dc_sku(
    rows: Iterable[tuple[int | None, Demand | DcDemand]],
    *,
    dc_id: int,
    product_id: int,
    lanes: Mapping[int, StoreLane],
    start_date: date,
    end_date: date,
    seed: int,
    default_prior: float = 1.0,
    dc_prior: float | None = None,
    dc_view: DcIncidentView | None = None,
    closed_class: str | None = None,
) -> Iterator[InventoryEvent]:
    """Every store of a DC for one product, against the DC's stock.

    ``rows`` are ``(store_id, Demand)`` store lines and ``(None, DcDemand)``
    online lines in local time order. A store with demand but no lane uses
    ``default_prior`` and the stable fallback slot and lane.
    """
    if end_date < start_date:
        raise ValueError("invalid inventory simulation bounds")
    lanes = dict(lanes)
    prior = (
        dc_prior
        if dc_prior is not None
        else sum(lane.prior for lane in lanes.values()) * (1.0 + DC_ONLINE_DEMAND_SHARE)
    )
    dc = DcSku(
        dc_id=dc_id,
        product_id=product_id,
        seed=seed,
        start_date=start_date,
        prior=max(prior, default_prior),
        view=dc_view,
        stores=len(lanes),
    )
    yield dc.opening()
    skus: dict[int, StoreSku] = {}

    def store(store_id: int) -> StoreSku:
        if store_id not in skus:
            lane = lanes.setdefault(store_id, _fallback_lane(seed, dc_id, store_id, default_prior))
            skus[store_id] = StoreSku(
                store_id=store_id,
                product_id=product_id,
                seed=seed,
                start_date=start_date,
                prior=lane.prior,
            )
        return skus[store_id]

    for store_id in sorted(lanes):
        yield store(store_id).opening()

    def timing(store_id: int, day: date) -> DeliveryTiming:
        lane = lanes[store_id]
        return delivery_timing(
            seed,
            store_id,
            dc_id,
            day,
            lane.slot_minute,
            lane.transit_minutes,
            lane.view,
            closed_class,
        )

    def load_timing(store_id: int, day: date) -> DeliveryTiming:
        """The load's plan is that of the earliest reorder day that ships in it."""
        current = timing(store_id, day)
        earlier = day - timedelta(days=1)
        while earlier >= start_date:
            candidate = timing(store_id, earlier)
            if candidate.slot != current.slot:
                break
            current, earlier = candidate, earlier - timedelta(days=1)
        return current

    open_loads: dict[tuple[int, datetime], tuple[InventoryEvent, InventoryEvent]] = {}
    iterator = iter(rows)
    candidate = next(iterator, None)
    day = start_date
    while day <= end_date:
        closing = datetime.combine(day, time(23, 59, 59))
        online = 0
        while candidate is not None and candidate[1].event_ts <= closing:
            row_store, line = candidate
            if row_store is None:
                if not isinstance(line, DcDemand):
                    raise ValueError("DC rows must be online lines")
                if line.event_ts.date() < start_date or line.quantity <= 0:
                    raise ValueError("online DC lines must be positive and inside history")
                yield from dc.receive(line.event_ts)
                yield from dc.fill_online(line)
                online += line.quantity
            else:
                if not isinstance(line, Demand):
                    raise ValueError("store rows must be demand lines")
                if int(row_store) not in skus:
                    yield store(int(row_store)).opening()
                yield from skus[int(row_store)].consume(line)
            candidate = next(iterator, None)
        yield from dc.receive(closing)
        requests = []
        for store_id in sorted(skus):
            sku = skus[store_id]
            yield from sku.release(closing)
            order = sku.close_day(closing)
            if order is not None:
                requests.append(((PRIORITY_ORDER.get(order[2], 3), store_id), order))
        requested = 0
        for (_, store_id), (quantity, point, priority) in sorted(requests):
            sku = skus[store_id]
            yield sku.reorder_event(day, quantity, point, priority)
            requested += quantity
            shipped = dc.ship(quantity)
            if shipped <= 0:
                continue
            plan = load_timing(store_id, day)
            key = (store_id, plan.slot)
            if key in open_loads:
                shipment, delivery = open_loads[key]
                shipment.quantity += shipped
                sku.add_to(delivery, shipped)
            else:
                shipment = load_event(store_id, product_id, dc_id, plan, shipped)
                open_loads[key] = (shipment, sku.expect(shipment))
        dc.close_day(day, requested + online)
        for key in sorted(key for key in open_loads if key[1].date() <= day):
            yield open_loads.pop(key)[0]
        day += timedelta(days=1)
    for key in sorted(open_loads):
        yield open_loads.pop(key)[0]
    if candidate is not None:
        raise ValueError("candidate demand falls outside the simulation history")


def _fallback_lane(seed: int, dc_id: int, store_id: int, prior: float) -> StoreLane:
    return StoreLane(
        delivery_slot_minute(seed, store_id), lane_transit_minutes(seed, dc_id, store_id), prior
    )
