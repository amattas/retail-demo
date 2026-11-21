"""Live-stream run control: retries, restarts, backlog cap and checkpoint incarnations."""

import json
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from retail_setup.generation.stream_control import (
    SMOKE_TRIGGER_SECONDS,
    CheckpointStateError,
    call_with_retry,
    ingest_tag,
    is_transient_error,
    parse_ingest_tag,
    persist_incarnation,
    plan_incarnation,
    run_supervised,
    stream_checkpoint_name,
    trigger_interval_seconds,
)


class _Flaky:
    """Raise the queued errors in order, then return ``"ok"``."""

    def __init__(self, *errors: Exception) -> None:
        self.errors = list(errors)
        self.attempts: list[int] = []

    def __call__(self, attempt: int) -> str:
        self.attempts.append(attempt)
        if self.errors:
            raise self.errors.pop(0)
        return "ok"


def _retry(operation, **kwargs):
    sleeps: list[float] = []
    kwargs.setdefault("log", lambda _message: None)
    try:
        return call_with_retry(operation, label="t", sleep=sleeps.append, **kwargs), sleeps
    except Exception as exc:
        exc.sleeps = sleeps  # type: ignore[attr-defined]
        raise


def test_transient_write_errors_retry_with_exponential_backoff_then_succeed() -> None:
    operation = _Flaky(
        RuntimeError("Kusto request was throttled"),
        RuntimeError("HTTP 503 ServiceUnavailable"),
    )

    result, sleeps = _retry(operation)

    assert result == "ok"
    assert operation.attempts == [1, 2, 3]
    assert sleeps == [2.0, 4.0]


def test_permanent_write_errors_propagate_without_retry() -> None:
    operation = _Flaky(ValueError("column store_id has the wrong type"))

    with pytest.raises(ValueError, match="wrong type") as caught:
        _retry(operation)

    assert operation.attempts == [1]
    assert caught.value.sleeps == []  # type: ignore[attr-defined]


def test_transient_errors_fail_the_batch_once_attempts_are_exhausted() -> None:
    operation = _Flaky(*(RuntimeError("operation timed out") for _ in range(4)))

    with pytest.raises(RuntimeError, match="timed out") as caught:
        _retry(operation, attempts=4, base_seconds=10, max_seconds=25)

    assert operation.attempts == [1, 2, 3, 4]
    assert caught.value.sleeps == [10.0, 20.0, 25.0]  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("message", "transient"),
    [
        ("Request is throttled, retry later", True),
        ("HTTP 429 Too Many Requests", True),
        ("status code: 502 Bad Gateway", True),
        ("Read timed out", True),
        ("Connection reset by peer", True),
        ("The access token expired at 10:00", True),
        ("401 Unauthorized", True),
        ("Table 'receipt_created' does not exist", False),
        ("Forbidden (403): principal lacks ingestor role", False),
        ("batch 429 has 500 rows", False),
    ],
)
def test_transient_classification(message, transient) -> None:
    assert is_transient_error(RuntimeError(message)) is transient


def test_transient_classification_follows_the_exception_chain() -> None:
    try:
        try:
            raise ConnectionError("connection reset")
        except ConnectionError as inner:
            raise RuntimeError("batch 7 failed required Eventhouse writes") from inner
    except RuntimeError as outer:
        assert is_transient_error(outer)


class _Query:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.waits: list[int | None] = []
        self.stopped = False

    def awaitTermination(self, timeout: int | None = None) -> bool:  # noqa: N802
        self.waits.append(timeout)
        if self.error is not None:
            raise self.error
        return True

    def stop(self) -> None:
        self.stopped = True


def _supervise(queries, *, run_seconds=0, clock_times=None, **kwargs):
    started: list[_Query] = []
    sleeps: list[float] = []
    pending = list(queries)
    times = iter(clock_times or [0.0] * (2 * len(queries)))

    def start():
        started.append(pending.pop(0))
        return started[-1]

    try:
        restarts = run_supervised(
            start,
            run_seconds=run_seconds,
            sleep=sleeps.append,
            clock=lambda: next(times),
            log=lambda _message: None,
            **kwargs,
        )
    except Exception as exc:
        exc.started, exc.sleeps = started, sleeps  # type: ignore[attr-defined]
        raise
    return restarts, started, sleeps


def test_bounded_run_waits_once_then_stops_the_query() -> None:
    query = _Query()

    restarts, started, sleeps = _supervise([query], run_seconds=120)

    assert (restarts, started, sleeps) == (0, [query], [])
    assert query.waits == [120]
    assert query.stopped


def test_live_run_restarts_after_a_transient_failure() -> None:
    failing = _Query(RuntimeError("batch 4 failed: HTTP 503 ServiceUnavailable"))
    healthy = _Query()

    restarts, started, sleeps = _supervise([failing, healthy], base_seconds=30)

    assert restarts == 1
    assert started == [failing, healthy]
    assert sleeps == [30.0]


def test_live_run_does_not_restart_a_permanent_failure() -> None:
    with pytest.raises(ValueError) as caught:
        _supervise([_Query(ValueError("unmapped event_types in batch 2"))])

    assert len(caught.value.started) == 1  # type: ignore[attr-defined]
    assert caught.value.sleeps == []  # type: ignore[attr-defined]


def test_live_run_gives_up_after_the_restart_budget() -> None:
    queries = [_Query(RuntimeError("throttled")) for _ in range(3)]

    with pytest.raises(RuntimeError, match="throttled") as caught:
        _supervise(queries, max_restarts=2, base_seconds=30, max_seconds=45)

    assert len(caught.value.started) == 3  # type: ignore[attr-defined]
    assert caught.value.sleeps == [30.0, 45.0]  # type: ignore[attr-defined]


def test_restart_budget_resets_after_a_healthy_run() -> None:
    queries = [_Query(RuntimeError("throttled")), _Query(RuntimeError("throttled")), _Query()]
    # The second query ran for two hours before failing, so with a budget of
    # one restart it is still restarted.
    clock = [0.0, 10.0, 20.0, 7220.0, 7230.0]

    restarts, started, _sleeps = _supervise(
        queries, max_restarts=1, healthy_after_seconds=3600, clock_times=clock
    )

    assert restarts == 1
    assert started == queries


def test_bounded_and_live_runs_use_separate_checkpoints_and_triggers() -> None:
    assert stream_checkpoint_name("eventhouse", 0) == "eventhouse-live"
    assert stream_checkpoint_name("eventhouse", 300) == "eventhouse-smoke"
    assert trigger_interval_seconds(0, 60) == 60
    assert trigger_interval_seconds(300, 60) == SMOKE_TRIGGER_SECONDS
    with pytest.raises(ValueError):
        trigger_interval_seconds(0, 0)


@pytest.mark.spark_no_filesystem
def test_backlog_cap_keeps_only_rows_inside_the_max_lag(spark) -> None:
    from retail_setup.generation.stream_frames import cap_backlog

    now = datetime.now(UTC)
    ages = {
        1: timedelta(hours=12),
        2: timedelta(minutes=11),
        3: timedelta(minutes=8),
        4: timedelta(seconds=5),
    }
    frame = spark.createDataFrame(
        [(now - age, value) for value, age in ages.items()], "timestamp timestamp, value long"
    )

    kept = sorted(row.value for row in cap_backlog(frame, 600).collect())

    assert kept == [3, 4]
    with pytest.raises(ValueError):
        cap_backlog(frame, 0)


class _LocalFs:
    """``mssparkutils.fs`` over a temporary directory."""

    def __init__(self, base: Path) -> None:
        self.base = base

    def _path(self, path: str) -> Path:
        return self.base / path

    def exists(self, path: str) -> bool:
        return self._path(path).exists()

    def head(self, path: str, max_bytes: int) -> str:
        return self._path(path).read_text(encoding="utf-8")[:max_bytes]

    def put(self, path: str, content: str, overwrite: bool) -> bool:
        target = self._path(path)
        if target.exists() and not overwrite:
            raise FileExistsError(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return True

    def mkdirs(self, path: str) -> bool:
        self._path(path).mkdir(parents=True, exist_ok=True)
        return True

    def ls(self, path: str) -> list[SimpleNamespace]:
        return [
            SimpleNamespace(name=entry.name + ("/" if entry.is_dir() else ""))
            for entry in self._path(path).iterdir()
        ]

    def spark_log(self, location: str, *, offsets=(), commits=()) -> None:
        for log, batches in (("offsets", offsets), ("commits", commits)):
            folder = self._path(f"{location}/{log}")
            folder.mkdir(parents=True, exist_ok=True)
            for batch in batches:
                (folder / str(batch)).write_text("v1", encoding="utf-8")
                (folder / f".{batch}.crc").write_text("", encoding="utf-8")


ROOT_DIR = "Files/stream/checkpoint"
DAY_1 = date(2026, 11, 1)
DAY_2 = date(2026, 11, 20)


def _nonces(*values: str):
    pending = list(values)
    return lambda: pending.pop(0)


def _plan(fs, today=DAY_1, nonce="0a1b2c3d", stream="stream-a"):
    return plan_incarnation(
        fs, ROOT_DIR, "eventhouse-live", stream, today, new_nonce=_nonces(nonce)
    )


def test_new_checkpoint_gets_a_recorded_incarnation(tmp_path) -> None:
    fs = _LocalFs(tmp_path)

    plan = _plan(fs)
    assert (plan.incarnation, plan.catalog_date, plan.resumed) == ("0a1b2c3d", DAY_1, False)
    assert plan.needs_write and not fs.exists(plan.record_path)
    assert plan.namespace == "stream-a-0a1b2c3d"

    persist_incarnation(fs, plan)
    record = json.loads(fs.head(plan.record_path, 4096))
    assert record["incarnation"] == "0a1b2c3d"
    assert record["catalog_date"] == "2026-11-01"
    assert plan.record_path == f"{ROOT_DIR}/_metadata/eventhouse-live.json"


def test_replayed_batch_keeps_its_incarnation_and_catalog_date(tmp_path) -> None:
    fs = _LocalFs(tmp_path)
    persist_incarnation(fs, _plan(fs))
    fs.spark_log(f"{ROOT_DIR}/eventhouse-live", offsets=[0, 1, 2], commits=[0, 1])

    plan = _plan(fs, today=DAY_2, nonce="ffffffff")

    assert plan.incarnation == "0a1b2c3d"
    assert plan.pending_batch == 2
    assert plan.catalog_date == DAY_1
    assert not plan.needs_write
    # Same batch, same incarnation: the same tags as the first attempt.
    assert ingest_tag("stream-a", plan.incarnation, "receipt_created", 2) == (
        "retail-demo:stream-a:0a1b2c3d:receipt_created:2"
    )


def test_restart_without_a_pending_batch_moves_the_catalog_date(tmp_path) -> None:
    fs = _LocalFs(tmp_path)
    persist_incarnation(fs, _plan(fs))
    fs.spark_log(f"{ROOT_DIR}/eventhouse-live", offsets=[0, 1], commits=[0, 1])

    plan = _plan(fs, today=DAY_2)
    assert (plan.incarnation, plan.catalog_date, plan.pending_batch) == ("0a1b2c3d", DAY_2, None)
    assert plan.needs_write

    persist_incarnation(fs, plan)
    assert _plan(fs, today=DAY_2).catalog_date == DAY_2
    assert not _plan(fs, today=DAY_2).needs_write


def test_resetting_only_the_spark_checkpoint_is_refused(tmp_path) -> None:
    fs = _LocalFs(tmp_path)
    first = persist_incarnation(fs, _plan(fs))
    fs.spark_log(f"{ROOT_DIR}/eventhouse-live", offsets=[0, 1], commits=[0])
    shutil.rmtree(tmp_path / ROOT_DIR / "eventhouse-live")

    with pytest.raises(CheckpointStateError, match="no offsets log") as caught:
        _plan(fs)
    assert first.record_path in str(caught.value)

    # Deleting the record as instructed starts a new incarnation with new tags.
    (tmp_path / first.record_path).unlink()
    second = _plan(fs, nonce="99aa88bb")
    assert second.incarnation != first.incarnation
    assert ingest_tag("stream-a", second.incarnation, "receipt_created", 0) != ingest_tag(
        "stream-a", first.incarnation, "receipt_created", 0
    )


@pytest.mark.parametrize(
    ("offsets", "commits", "write_record", "stream", "message"),
    [
        ([0, 1], [0], False, "stream-a", "no incarnation record"),
        ([0], [0, 1], True, "stream-a", "commits past its offsets"),
        ([], [0], True, "stream-a", "commits past its offsets"),
        ([0], [0], True, "stream-b", "belongs to stream"),
    ],
)
def test_inconsistent_checkpoint_folders_are_refused(
    tmp_path, offsets, commits, write_record, stream, message
) -> None:
    fs = _LocalFs(tmp_path)
    if write_record:
        persist_incarnation(fs, _plan(fs))
    fs.spark_log(f"{ROOT_DIR}/eventhouse-live", offsets=offsets, commits=commits)

    with pytest.raises(CheckpointStateError, match=message):
        _plan(fs, stream=stream)


def test_concurrent_first_start_is_refused(tmp_path) -> None:
    fs = _LocalFs(tmp_path)
    winner, loser = _plan(fs, nonce="11111111"), _plan(fs, nonce="22222222")
    persist_incarnation(fs, winner)

    with pytest.raises(CheckpointStateError, match="Another run"):
        persist_incarnation(fs, loser)
    assert json.loads(fs.head(winner.record_path, 4096))["incarnation"] == "11111111"


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        (
            "ingest-by:retail-demo:stream-a:0a1b2c3d:receipt_created:17",
            ("stream-a", "0a1b2c3d", "receipt_created", 17),
        ),
        ("retail-demo:stream-a:receipt_created:3", ("stream-a", None, "receipt_created", 3)),
        ("retail-demo:stream-a:0a1b2c3d:receipt_created:x", None),
        ("drop-by:other", None),
    ],
)
def test_ingest_tags_round_trip_and_accept_legacy_tags(tag, expected) -> None:
    parsed = parse_ingest_tag(tag)
    if expected is None:
        assert parsed is None
    else:
        assert parsed is not None
        assert (
            parsed["stream_id"],
            parsed["incarnation"],
            parsed["event_type"],
            parsed["batch_id"],
        ) == expected
