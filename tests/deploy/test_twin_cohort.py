from datetime import UTC, datetime, timedelta

import pytest

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
