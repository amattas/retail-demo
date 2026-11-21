"""Live-stream state store: versioned rows, replay idempotency and transitions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from retail_setup.generation.stream_calendar import StoreHours
from retail_setup.generation.stream_state import (
    SEED_BATCH_ID,
    DeltaStateStore,
    MemoryStateStore,
    StateRow,
    StoreBatch,
    VisitBatch,
    apply_batch,
    apply_store_batch,
    apply_visit_batch,
    replace_condition,
    seed_store_state,
    state_as_of,
    state_changes,
    superseded_rows,
)

UTC = timezone.utc
NEW_YORK = StoreHours(1, "6-22", "America/New_York")
PHOENIX_24H = StoreHours(2, "24h", "America/Phoenix")


def _utc(*parts: int) -> datetime:
    return datetime(*parts, tzinfo=UTC)


def _run_store_batch(
    store: MemoryStateStore, batch_id: int, stores: tuple[StoreHours, ...], clock: datetime
) -> list:
    """One micro-batch of the store-hours feature against a state store."""
    state = store.read("stores", batch_id)
    meta = store.read("meta", batch_id).get("clock")
    previous = None if meta is None else datetime.fromisoformat(meta["clock"])
    new_state, emitted = apply_batch(state, StoreBatch(stores, clock, previous), apply_store_batch)
    store.write("stores", batch_id, state_changes(state, new_state))
    store.write("meta", batch_id, {"clock": {"clock": clock.isoformat()}})
    return emitted


def test_state_as_of_reads_the_latest_version_before_the_batch() -> None:
    rows = [
        StateRow("a", -1, "s1", '{"v":0}'),
        StateRow("a", 0, "s1", '{"v":1}'),
        StateRow("a", 2, "s1", '{"v":2}'),
        StateRow("a", 1, "s2", '{"v":9}'),
        StateRow("a", 3, "s2", None, True),
        StateRow("b", 0, "s3", '{"v":7}'),
    ]
    assert state_as_of(rows, "a", 0) == {"s1": {"v": 0}}
    assert state_as_of(rows, "a", 2) == {"s1": {"v": 1}, "s2": {"v": 9}}
    assert state_as_of(rows, "a", 4) == {"s1": {"v": 2}}
    assert state_as_of(rows, "b", 1) == {"s3": {"v": 7}}
    assert state_as_of(rows, "c", 9) == {}


def test_replayed_batch_reads_previous_state_and_never_double_applies() -> None:
    store = MemoryStateStore("stream-0001")
    store.write("stores", SEED_BATCH_ID, seed_store_state((NEW_YORK,), {}, _utc(2025, 6, 2, 4)))
    clocks = [_utc(2025, 6, 2, 9), _utc(2025, 6, 2, 12), _utc(2025, 6, 3, 3)]
    first = [_run_store_batch(store, n, (NEW_YORK,), clock) for n, clock in enumerate(clocks)]
    rows_after_first_run = list(store.rows["stores"]) + list(store.rows["meta"])

    # Spark replays the last batch when its commit was not recorded.
    replay = _run_store_batch(store, 2, (NEW_YORK,), clocks[2])
    assert replay == first[2]
    assert list(store.rows["stores"]) + list(store.rows["meta"]) == rows_after_first_run

    # A failed later attempt that wrote state but never committed is replaced.
    _run_store_batch(store, 3, (NEW_YORK,), _utc(2025, 6, 4, 3))
    retried = _run_store_batch(store, 3, (NEW_YORK,), _utc(2025, 6, 4, 3))
    assert [op.operation_type for op in retried] == ["opened", "closed"]
    assert sum(1 for row in store.rows["stores"] if row.batch_id == 3) == 1

    emitted = [op for ops in first for op in ops]
    assert [(op.operation_type, op.event_time) for op in emitted] == [
        ("opened", _utc(2025, 6, 2, 10)),  # 06:00 EDT
        ("closed", _utc(2025, 6, 3, 2)),  # 22:00 EDT
    ]


def test_new_incarnation_starts_from_a_fresh_seed() -> None:
    old = MemoryStateStore("stream-0001")
    old.write("stores", 0, {"1": {"is_open": True, "changed_at": "2025-06-02T10:00:00+00:00"}})
    fresh = MemoryStateStore("stream-0002")
    fresh.rows = old.rows
    assert fresh.read("stores", 5) == {}
    assert not fresh.has_rows("stores")

    seeded = seed_store_state(
        (NEW_YORK, PHOENIX_24H),
        {1: (datetime(2025, 6, 2, 2, 0), "CLOSED")},
        _utc(2025, 6, 2, 12),
    )
    assert seeded == {
        "1": {"is_open": False, "changed_at": "2025-06-02T02:00:00+00:00"},
        "2": {"is_open": True, "changed_at": "2025-06-02T12:00:00+00:00"},
    }


def _hourly_ops(stores, state, start: datetime, hours: int) -> list:
    clock, previous, events = start, start, []
    for _ in range(hours):
        clock += timedelta(hours=1)
        state, ops = apply_store_batch(state, StoreBatch(stores, clock, previous))
        events.extend(ops)
        previous = clock
    return events


def test_store_hours_follow_local_time_across_daylight_saving() -> None:
    state = {"1": {"is_open": False, "changed_at": "2025-03-08T03:00:00+00:00"}}
    ops = _hourly_ops((NEW_YORK,), state, _utc(2025, 3, 8, 4), 72)
    # 06:00 and 22:00 New York time; daylight saving starts on Mar 9 at 02:00.
    assert [(op.operation_type, op.event_time) for op in ops] == [
        ("opened", _utc(2025, 3, 8, 11)),
        ("closed", _utc(2025, 3, 9, 3)),
        ("opened", _utc(2025, 3, 9, 10)),
        ("closed", _utc(2025, 3, 10, 2)),
        ("opened", _utc(2025, 3, 10, 10)),
        ("closed", _utc(2025, 3, 11, 2)),
    ]


def test_catch_up_after_a_stop_is_capped() -> None:
    state = {"1": {"is_open": False, "changed_at": "2025-06-01T02:00:00+00:00"}}
    new_state, ops = apply_store_batch(state, StoreBatch((NEW_YORK,), _utc(2025, 6, 9, 12)))
    assert [(op.operation_type, op.event_time) for op in ops] == [
        ("opened", _utc(2025, 6, 9, 10)),
    ]
    assert new_state["1"] == {"is_open": True, "changed_at": "2025-06-09T10:00:00+00:00"}


def test_open_and_close_alternate_and_pair_per_store_day() -> None:
    stores = (NEW_YORK, PHOENIX_24H)
    start = _utc(2025, 12, 23, 8)
    events = _hourly_ops(stores, seed_store_state(stores, {}, start), start, 4 * 24)
    for store in stores:
        zone = ZoneInfo(store.time_zone)
        mine = [op for op in events if op.store_id == store.store_id]
        sequence = [op.operation_type for op in mine]
        assert all(a != b for a, b in zip(sequence, sequence[1:])), sequence
        # Closed on Christmas Day: no operation on that local date.
        local_days = [op.event_time.astimezone(zone).date() for op in mine]
        assert datetime(2025, 12, 25).date() not in local_days
        # Every other full local day has exactly one opened and one closed.
        for day in (24, 26):
            ops = [
                op.operation_type
                for op, local in zip(mine, local_days)
                if local == datetime(2025, 12, day).date()
            ]
            assert sorted(ops) == ["closed", "opened"], (store, day, ops)
    phoenix = [(op.operation_type, op.event_time) for op in events if op.store_id == 2]
    assert ("closed", _utc(2025, 12, 25, 6, 59)) in phoenix  # 23:59 MST on the 24th
    assert ("opened", _utc(2025, 12, 26, 7)) in phoenix  # midnight MST on the 26th


def test_visits_stay_in_state_until_their_last_event() -> None:
    state = {"old-a": {"v": 1}, "old-b": {"v": 2}}
    batch = VisitBatch(
        clock_ms=1_000,
        visits={
            "old-a": ({"v": 1}, 900),
            "old-b": ({"v": 2}, 1_500),
            "new-c": ({"v": 3}, 2_000),
            "new-d": ({"v": 4}, 1_000),
        },
    )
    new_state, finished = apply_visit_batch(state, batch)
    assert new_state == {"new-c": {"v": 3}, "old-b": {"v": 2}}
    assert finished == ["new-d", "old-a"]
    assert state_changes(state, new_state) == {"new-c": {"v": 3}, "old-a": None}


def test_compaction_keeps_every_later_read_identical() -> None:
    store = MemoryStateStore("stream-0001")
    store.write("visits", 0, {"a": {"v": 1}, "b": {"v": 2}})
    store.write("visits", 1, {"a": {"v": 3}, "b": None})
    store.write("visits", 2, {"c": {"v": 4}})
    before = {batch: store.read("visits", batch) for batch in (3, 4)}
    stale = superseded_rows(store.rows["visits"], "stream-0001", 3)
    assert {(row.batch_id, row.entity_key) for row in stale} == {(0, "a"), (0, "b"), (1, "b")}
    store.compact("visits", 3)
    assert {batch: store.read("visits", batch) for batch in (3, 4)} == before
    assert len(store.rows["visits"]) == 2


def test_replace_condition_and_delta_write_replace_the_batch_atomically() -> None:
    assert replace_condition("s1-0a1b2c3d", 7) == "incarnation = 's1-0a1b2c3d' AND batch_id >= 7"
    with pytest.raises(ValueError):
        replace_condition("x' OR '1'='1", 0)

    spark = MagicMock()
    store = DeltaStateStore(spark, "lh.ag", "s1-0a1b2c3d")
    store.write("stores", 4, {"1": {"is_open": True}, "2": None})
    rows, schema = spark.createDataFrame.call_args.args
    assert rows == [
        ("s1-0a1b2c3d", 4, "1", '{"is_open":true}', False),
        ("s1-0a1b2c3d", 4, "2", None, True),
    ]
    assert schema.startswith("incarnation string, batch_id long")
    writer = spark.createDataFrame.return_value.write.format.return_value.mode.return_value
    writer.option.assert_called_once_with(
        "replaceWhere", "incarnation = 's1-0a1b2c3d' AND batch_id >= 4"
    )
    writer.option.return_value.saveAsTable.assert_called_once_with("lh.ag._stream_state_stores")
    assert "USING DELTA" in spark.sql.call_args.args[0]


def test_delta_read_matches_the_pure_as_of_view(spark) -> None:
    rows = [
        StateRow("s1-0a1b2c3d", -1, "1", '{"is_open":false}'),
        StateRow("s1-0a1b2c3d", 3, "1", '{"is_open":true}'),
        StateRow("s1-0a1b2c3d", 2, "2", '{"is_open":true}'),
        StateRow("s1-0a1b2c3d", 4, "2", None, True),
        StateRow("other", 0, "3", '{"is_open":true}'),
    ]
    spark.createDataFrame(
        [(r.incarnation, r.batch_id, r.entity_key, r.payload, r.deleted) for r in rows],
        "incarnation string, batch_id long, entity_key string, payload string, deleted boolean",
    ).createOrReplaceGlobalTempView("_stream_state_stores")
    store = DeltaStateStore(spark, "global_temp", "s1-0a1b2c3d")
    store._ready.add("stores")
    for batch_id in (0, 3, 4, 5):
        assert store.read("stores", batch_id) == state_as_of(rows, "s1-0a1b2c3d", batch_id)
