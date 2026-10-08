from datetime import UTC, datetime, timedelta

import pytest

from deploy.scripts import _twin_cohort
from deploy.scripts._twin_cohort import select_cohort, validate_product_ids

AS_OF = datetime(2026, 9, 8, 10, tzinfo=UTC)


def route(truck, store, *, dc=1, shipment=None, hours=1):
    return {
        "dc_id": dc, "truck_id": truck, "store_id": store,
        "shipment_id": shipment or f"shipment-{dc}-{truck}-{store}",
        "event_ts": AS_OF - timedelta(hours=hours),
    }


def test_cohort_is_connected_bounded_and_deterministic():
    rows = [route(10, store, hours=store) for store in range(1, 8)]
    rows += [route(20, 8), route(20, 9, hours=2), route(30, 10)]
    result = select_cohort(rows, as_of=AS_OF)
    assert result["dc_id"] == 1
    assert result["truck_ids"] == [10, 20]
    assert len(result["store_ids"]) == 5
    assert {1, 8} <= set(result["store_ids"])
    assert select_cohort(list(reversed(rows)), as_of=AS_OF) == result
    for edge in result["routes"]:
        assert edge["truck_id"] in result["truck_ids"]
        assert edge["store_id"] in result["store_ids"]
        assert edge["shipment_id"] in result["shipment_ids"]
        assert edge["dc_id"] == result["dc_id"]


def test_cohort_uses_recorded_ids_not_assumed_first_entities():
    result = select_cohort([route(101, 701, dc=99), route(202, 702, dc=99)], as_of=AS_OF)
    assert result["dc_id"] == 99
    assert result["truck_ids"] == [101, 202]
    assert result["store_ids"] == [701, 702]


@pytest.mark.parametrize("rows", [
    [], [route(1, 1)], [route(1, 1), route(2, 2, dc=2)],
    [route(1, 1), route(2, 2, shipment="shipment-1-1-1")],
    [route(1, 1), route(2, 2, hours=-1)],
    [route(1, 1), route(2, 2, hours=400)],
    [route(True, 1), route(2, 2)],
])
def test_invalid_or_disconnected_evidence_fails_closed(rows):
    with pytest.raises(ValueError):
        select_cohort(rows, as_of=AS_OF)


def test_product_selection_rejects_unknown_duplicate_or_oversized_inputs():
    assert validate_product_ids([5, 2, 20]) == [2, 5, 20]
    for values in ([], [1, 1], [True], [0], list(range(1, 22))):
        with pytest.raises(ValueError):
            validate_product_ids(values)


def _busy_rows(count, *, trucks=(10, 20), stores=(1, 2, 3)):
    """One shipment per minute, newest first, alternating trucks and stores."""
    return [
        {
            "dc_id": 1,
            "truck_id": trucks[index % len(trucks)],
            "store_id": stores[index % len(stores)],
            "shipment_id": f"s-{index}",
            "event_ts": AS_OF - timedelta(minutes=index),
        }
        for index in range(count)
    ]


def test_busy_network_keeps_the_newest_shipments_and_narrows_the_window(monkeypatch):
    monkeypatch.setattr(_twin_cohort, "MAX_SHIPMENTS", 50)
    result = select_cohort(_busy_rows(120), as_of=AS_OF)
    assert len(result["shipment_ids"]) == 49  # the boundary timestamp is dropped whole
    start = datetime.fromisoformat(result["window_start"])
    assert start == AS_OF - timedelta(minutes=49) + timedelta(microseconds=1)
    assert all(datetime.fromisoformat(r["event_ts"]) >= start for r in result["routes"])
    assert set(result["store_ids"]) == {r["store_id"] for r in result["routes"]}


def test_route_discovery_beyond_the_row_bound_narrows_instead_of_failing(monkeypatch):
    monkeypatch.setattr(_twin_cohort, "MAX_ROUTE_ROWS", 30)
    result = select_cohort(_busy_rows(31), as_of=AS_OF)
    assert "s-30" not in result["shipment_ids"] and "s-29" not in result["shipment_ids"]
    assert datetime.fromisoformat(result["window_start"]) > AS_OF - timedelta(minutes=29)


def test_boundary_ties_are_dropped_together(monkeypatch):
    monkeypatch.setattr(_twin_cohort, "MAX_SHIPMENTS", 3)
    rows = _busy_rows(2) + [
        {"dc_id": 1, "truck_id": 10 + (i % 2) * 10, "store_id": 1,
         "shipment_id": f"tie-{i}", "event_ts": AS_OF - timedelta(minutes=5)}
        for i in range(4)
    ]
    result = select_cohort(rows, as_of=AS_OF)
    assert sorted(result["shipment_ids"]) == ["s-0", "s-1"]
