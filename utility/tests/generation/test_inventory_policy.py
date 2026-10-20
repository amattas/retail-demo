from datetime import date, datetime, timedelta

import pytest

from retail_setup.generation.inventory_policy import (
    CASE_PACK,
    Demand,
    opening_stock,
    simulate_sku,
)

START = date(2024, 1, 1)
STOCK_KINDS = {"INITIAL", "SALE", "LOST_SALE", "RETURN", "INBOUND_SHIPMENT"}


def _demand(days, daily_units, *, returns=False):
    for day in range(days):
        units = daily_units(day) if callable(daily_units) else daily_units
        for line in range(units // 3):
            ts = datetime(2024, 1, 1, 9) + timedelta(days=day, minutes=line)
            receipt_id = f"R{day:04d}-{line:04d}"
            returned = returns and day % 10 == 0 and line == 0
            yield Demand(
                ts, receipt_id, 1, 3,
                ts + timedelta(days=2) if returned else None,
                f"RET-{receipt_id}" if returned else None,
            )


def _run(demand, days, product=1, **kwargs):
    return list(simulate_sku(
        demand, store_id=1, product_id=product, start_date=START,
        end_date=START + timedelta(days=days - 1), seed=17, capacity=100, **kwargs,
    ))


def _assert_conservation(events):
    stock = sorted(
        (event for event in events if event.kind in STOCK_KINDS),
        key=lambda event: (event.event_ts, event.trace_id),
    )
    running = 0
    for event in stock:
        before = running
        running += event.quantity
        assert event.balance == running >= 0
        if event.kind in {"SALE", "LOST_SALE"}:
            assert event.requested_quantity == -event.quantity + event.unfulfilled_quantity
            if event.kind == "SALE":
                assert event.requested_quantity <= before
                assert event.unfulfilled_quantity == 0
            else:
                assert event.quantity == 0
                assert event.requested_quantity > before
    assert len({event.trace_id for event in events}) == len(events)
    return stock


@pytest.mark.parametrize("days", [90, 731])
def test_all_ten_skus_replenish_and_recover_without_deficits(days):
    for product, units in enumerate([60, 45, 33, 24, 21, 18, 15, 12, 9, 6], 1):
        events = _run(_demand(days, units, returns=True), days, product)
        stock = _assert_conservation(events)
        orders = [event for event in events if event.kind == "REORDER"]
        assert len(orders) >= 5, product
        assert all(order.quantity > 0 and order.quantity % CASE_PACK == 0 for order in orders)
        assert any(order.balance > 0 for order in orders)
        assert any(order.priority != "URGENT" for order in orders)
        assert max(event.balance for event in stock) < units * 12 + 120
        recovered = [event for event in stock if (event.event_ts.date() - START).days >= 20]
        assert not [event for event in recovered if event.kind == "LOST_SALE"]
        accepted = {event.receipt_id for event in stock if event.kind == "SALE"}
        for event in stock:
            if event.kind == "RETURN":
                assert event.source.removeprefix("RET-") in accepted
        shipments = [event for event in events if event.kind == "SHIPMENT"]
        assert all(0 < event.quantity <= 100 for event in shipments)
        delivered = sum(
            event.quantity for event in shipments
            if event.unloading_ts.date() <= START + timedelta(days=days - 1)
        )
        assert delivered == sum(event.quantity for event in stock if event.kind == "INBOUND_SHIPMENT")


def test_large_demand_shock_is_explicit_and_recovers_within_two_weeks():
    events = _run(_demand(220, lambda day: 12 if day < 45 or day >= 100 else 120), 220)
    stock = _assert_conservation(events)
    lost = [event for event in stock if event.kind == "LOST_SALE"]
    assert lost
    assert all(45 <= (event.event_ts.date() - START).days < 59 for event in lost)
    assert max(event.balance for event in stock) < 1500
    assert stock[-1].balance < 120


def test_depletion_lost_line_and_only_fulfilled_returns_are_restocked():
    opening = opening_stock(17, 1, 1)
    demand = [
        Demand(datetime(2024, 1, 1, 9), "R1", 1, opening,
               datetime(2024, 1, 2, 12), "RET1"),
        Demand(datetime(2024, 1, 1, 10), "R2", 1, 1000,
               datetime(2024, 1, 2, 13), "RET2"),
    ]
    events = _run(demand, 5)
    stock = _assert_conservation(events)
    assert next(event for event in stock if event.kind == "SALE").balance == 0
    assert next(event for event in stock if event.kind == "LOST_SALE").unfulfilled_quantity == 1000
    returned = [event for event in stock if event.kind == "RETURN"]
    assert [(event.source, event.quantity) for event in returned] == [("RET1", opening)]


def test_near_term_accepted_returns_reduce_inventory_position_orders():
    opening = opening_stock(17, 1, 1)
    sale = Demand(datetime(2024, 1, 1, 9), "R", 1, opening)
    without = _run([sale], 1)
    with_return = _run([
        Demand(sale.event_ts, "R", 1, opening, datetime(2024, 1, 2, 12), "RET"),
    ], 1)
    def ordered(rows):
        return sum(event.quantity for event in rows if event.kind == "REORDER")

    assert ordered(with_return) < ordered(without)


def test_history_prefix_is_identical_and_future_deliveries_never_change_balance():
    short = _run(_demand(30, 60, returns=True), 30)
    long = _run(_demand(365, 60, returns=True), 365)
    assert short == [event for event in long if event.event_ts.date() < START + timedelta(days=30)]
    assert short[0] == long[0]
    one_day = _run(_demand(1, 60), 1)
    future = [event for event in one_day if event.kind == "SHIPMENT"
              and event.unloading_ts.date() > START]
    assert future
    assert all(event.event_ts.date() < START + timedelta(days=30)
               for event in short if event.kind in STOCK_KINDS)


def test_seed_and_key_reproducibility_independent_of_processing_order():
    first = {key: _run(_demand(15, 30), 15, key) for key in range(1, 11)}
    reversed_keys = {key: _run(_demand(15, 30), 15, key) for key in reversed(range(1, 11))}
    assert first == reversed_keys
    assert len({opening_stock(seed, 1, 1) for seed in range(10)}) > 1


@pytest.mark.parametrize("demand", [
    [Demand(datetime(2024, 1, 1, 9), "R", 1, 0)],
    [Demand(datetime(2023, 12, 31, 9), "R", 1, 1)],
    [Demand(datetime(2024, 1, 2, 9), "R", 1, 1)],
    [Demand(datetime(2024, 1, 1, 9), "R", 1, 1)] * 2,
])
def test_invalid_or_unsorted_demand_fails_explicitly(demand):
    with pytest.raises(ValueError):
        _run(demand, 1)


def test_partition_adapter_uses_utc_and_isolates_sku_state():
    from datetime import timezone

    import pyarrow as pa

    from retail_setup.generation.fulfillment import _simulate_arrow_partition

    utc = pa.timestamp("us", tz="UTC")
    products = list(range(1, 4))
    batch = pa.RecordBatch.from_pydict({
        "store_id": pa.array([1] * 3, pa.int64()),
        "product_id": pa.array(products, pa.int64()),
        "event_ts": pa.array([datetime(2024, 1, 1)] * 3, utc),
        "receipt_id_ext": [f"R-{product}" for product in products],
        "line_num": pa.array([1] * 3, pa.int64()),
        "quantity": pa.array([opening_stock(17, 1, p) for p in products], pa.int64()),
        "return_ts": pa.array([datetime(2024, 1, 2, 12)] * 3, utc),
        "return_id": [f"RET-{product}" for product in products],
    })
    # Split the SKU stream across Arrow batches: state must still follow the SKU.
    batches = [batch.slice(0, 2), batch.slice(2)]
    result = pa.Table.from_batches(list(_simulate_arrow_partition(
        iter(batches), start_date=START, end_date=START + timedelta(days=2),
        seed=17, capacity=100, daily_prior=1.0, time_zone="UTC",
    ))).to_pylist()
    for product in products:
        events = [event for event in result if event["product_id"] == product]
        sale = next(event for event in events if event["kind"] == "SALE")
        assert sale["balance"] == 0
        assert sale["event_ts"] == datetime(2024, 1, 1, tzinfo=timezone.utc)
        returned = next(event for event in events if event["kind"] == "RETURN")
        assert returned["quantity"] == opening_stock(17, 1, product)
        assert returned["event_ts"] == datetime(2024, 1, 2, 12, tzinfo=timezone.utc)
