"""Run control for the live stream: checkpoints, Kusto write retries and restarts.

Pure Python with no Spark or Fabric imports. ``build_notebooks.py`` inlines it
into ``stream-events``, and the tests call it directly with fakes, so the
notebook's operational behavior is checked offline.
"""

import json
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from typing import Any, Protocol

# Live runs trigger a micro-batch every minute. Each batch makes one Kusto
# transaction per event table, so a longer trigger means fewer, larger extents.
DEFAULT_TRIGGER_SECONDS = 60
# Bounded (smoke) runs are short, so they trigger more often to show data quickly.
SMOKE_TRIGGER_SECONDS = 10
# After a stop, the rate source replays the whole downtime in one batch. Rows
# older than this many seconds behind the batch time are dropped instead.
DEFAULT_MAX_LAG_SECONDS = 600

# Kusto writes: attempts per table write, with exponential backoff between them.
WRITE_RETRY_ATTEMPTS = 5
WRITE_RETRY_BASE_SECONDS = 2.0
WRITE_RETRY_MAX_SECONDS = 60.0

# Supervised restarts of a failed live query. The counter resets once a
# restarted query has stayed healthy for RESTART_RESET_AFTER_SECONDS.
MAX_QUERY_RESTARTS = 5
RESTART_BASE_SECONDS = 30.0
RESTART_MAX_SECONDS = 600.0
RESTART_RESET_AFTER_SECONDS = 3600.0

_TRANSIENT_ERROR = re.compile(
    r"throttl"
    r"|too\s*many\s*requests"
    r"|\b(?:status|http|code)\b\D{0,8}(?:401|408|429|500|502|503|504)\b"
    r"|service\s*unavailable|server\s*busy|bad\s*gateway|gateway\s*time-?out"
    r"|internal\s*server\s*error"
    r"|timed?\s*out|timeout"
    r"|temporar(?:il)?y"
    r"|connection\s+(?:reset|refused|aborted|closed)"
    r"|token.{0,40}expir|expir.{0,40}token"
    r"|unauthori[sz]ed",
    re.IGNORECASE,
)


def stream_checkpoint_name(sink: str, run_seconds: int) -> str:
    """Return the sink checkpoint folder; bounded runs never share live offsets."""

    return f"{sink}-smoke" if int(run_seconds) > 0 else f"{sink}-live"


def trigger_interval_seconds(run_seconds: int, trigger_seconds: int) -> int:
    """Return the processing-time trigger for a live or bounded run."""

    if int(trigger_seconds) < 1:
        raise ValueError("trigger_seconds must be at least 1")
    if int(run_seconds) > 0:
        return min(int(trigger_seconds), SMOKE_TRIGGER_SECONDS)
    return int(trigger_seconds)


def _stream_error_text(exc: BaseException) -> str:
    parts = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return " | ".join(parts)


def is_transient_error(exc: BaseException) -> bool:
    """True for throttling, 5xx, timeouts, dropped connections and expired tokens."""

    return bool(_TRANSIENT_ERROR.search(_stream_error_text(exc)))


def backoff_seconds(attempt: int, base: float, ceiling: float) -> float:
    """Exponential backoff for the given 1-based retry number, capped at ``ceiling``."""

    return float(min(ceiling, base * 2 ** max(0, attempt - 1)))


def _stream_error_summary(exc: BaseException, limit: int = 300) -> str:
    text = " ".join(str(exc).split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def call_with_retry(
    operation: Callable[[int], Any],
    *,
    label: str,
    attempts: int = WRITE_RETRY_ATTEMPTS,
    base_seconds: float = WRITE_RETRY_BASE_SECONDS,
    max_seconds: float = WRITE_RETRY_MAX_SECONDS,
    is_transient: Callable[[BaseException], bool] = is_transient_error,
    sleep: Callable[[float], None] = time.sleep,
    log: Callable[[str], None] = print,
) -> Any:
    """Call ``operation(attempt)``, retrying transient failures with backoff.

    A permanent error, or a transient one on the last attempt, propagates
    unchanged so the caller fails the micro-batch and Spark does not commit it.
    """

    for attempt in range(1, attempts + 1):
        try:
            return operation(attempt)
        except Exception as exc:
            if attempt >= attempts or not is_transient(exc):
                raise
            delay = backoff_seconds(attempt, base_seconds, max_seconds)
            log(
                f"{label}: transient failure on attempt {attempt}/{attempts} "
                f"({type(exc).__name__}: {_stream_error_summary(exc)}); retrying in {delay:.0f}s"
            )
            sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


def run_supervised(
    start_query: Callable[[], Any],
    *,
    run_seconds: int,
    max_restarts: int = MAX_QUERY_RESTARTS,
    base_seconds: float = RESTART_BASE_SECONDS,
    max_seconds: float = RESTART_MAX_SECONDS,
    healthy_after_seconds: float = RESTART_RESET_AFTER_SECONDS,
    is_restartable: Callable[[BaseException], bool] = is_transient_error,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    log: Callable[[str], None] = print,
) -> int:
    """Run the streaming query; restart a failed live query a bounded number of times.

    A bounded run (``run_seconds > 0``) runs once and is stopped after its
    duration. A live run is restarted after a transient failure, with backoff,
    from the same checkpoint, so the failed micro-batch replays with the same
    tags and IDs. Permanent failures, and failures beyond ``max_restarts``,
    propagate. Returns the number of restarts used.
    """

    if int(run_seconds) > 0:
        query = start_query()
        try:
            query.awaitTermination(int(run_seconds))
        finally:
            query.stop()
        return 0

    restarts = 0
    while True:
        started = clock()
        query = start_query()
        try:
            query.awaitTermination()
            log(f"stream query stopped after {restarts} restart(s)")
            return restarts
        except Exception as exc:
            if clock() - started >= healthy_after_seconds:
                restarts = 0
            if restarts >= max_restarts or not is_restartable(exc):
                log(
                    f"stream query failed and will not be restarted "
                    f"(restarts used: {restarts}/{max_restarts}): {_stream_error_summary(exc)}"
                )
                raise
            restarts += 1
            delay = backoff_seconds(restarts, base_seconds, max_seconds)
            log(
                f"stream query failed ({type(exc).__name__}: {_stream_error_summary(exc)}); "
                f"restart {restarts}/{max_restarts} in {delay:.0f}s"
            )
            sleep(delay)


# ---------------------------------------------------------------------------
# Checkpoint incarnations
# ---------------------------------------------------------------------------
#
# The checkpoint root holds ``_metadata/stream_id`` and one Spark checkpoint per
# sink and run kind (``eventhouse-live``, ``eventhouse-smoke``, ...). Each Spark
# checkpoint has an incarnation record, ``_metadata/<checkpoint>.json``, with a
# random nonce that goes into every Kusto ingest tag and generated business ID,
# and the catalog date used to build live baskets. Spark restarts batch IDs at
# 0 when its checkpoint is deleted; the record makes that state visible, so a
# reset can never reuse the tags or IDs of an earlier incarnation.

_INCARNATION_PATTERN = re.compile(r"^[0-9a-f]{8}$")
INGEST_TAG_PREFIX = "retail-demo"


class CheckpointFs(Protocol):
    """The subset of ``mssparkutils.fs`` the checkpoint helpers use."""

    def exists(self, path: str) -> bool: ...

    def head(self, path: str, max_bytes: int) -> str: ...

    def put(self, path: str, content: str, overwrite: bool) -> bool: ...

    def mkdirs(self, path: str) -> bool: ...

    def ls(self, path: str) -> list[Any]: ...


class CheckpointStateError(RuntimeError):
    """The checkpoint folders disagree; starting would reuse or lose event identities."""


@dataclass(frozen=True)
class StreamIncarnation:
    """The identity and catalog snapshot a stream checkpoint runs with."""

    stream_id: str
    checkpoint: str
    incarnation: str
    catalog_date: date
    record_path: str
    # The batch Spark will replay first (offsets written, commit missing), if any.
    pending_batch: int | None
    # True when the record must be written (new incarnation or new catalog date).
    needs_write: bool
    resumed: bool

    @property
    def namespace(self) -> str:
        """Prefix for generated business IDs and trace IDs."""

        return f"{self.stream_id}-{self.incarnation}"

    def record(self) -> dict[str, str]:
        return {
            "stream_id": self.stream_id,
            "checkpoint": self.checkpoint,
            "incarnation": self.incarnation,
            "catalog_date": self.catalog_date.isoformat(),
        }


def ingest_tag(stream_id: str, incarnation: str, event_type: str, batch_id: int) -> str:
    """Kusto ingest-by tag for one table write of one micro-batch."""

    return f"{INGEST_TAG_PREFIX}:{stream_id}:{incarnation}:{event_type}:{int(batch_id)}"


def parse_ingest_tag(tag: str) -> dict[str, Any] | None:
    """Parse a stream ingest tag; Kusto may prefix it with ``ingest-by:``.

    Returns ``stream_id``, ``incarnation`` (None for tags written before
    incarnations existed), ``event_type`` and ``batch_id``, or None.
    """

    marker = tag.find(f"{INGEST_TAG_PREFIX}:")
    if marker < 0:
        return None
    parts = tag[marker:].split(":")
    if len(parts) == 5:
        _prefix, stream_id, incarnation, event_type, batch = parts
    elif len(parts) == 4:
        _prefix, stream_id, event_type, batch = parts
        incarnation = None
    else:
        return None
    if not batch.isdigit() or not stream_id or not event_type:
        return None
    return {
        "stream_id": stream_id,
        "incarnation": incarnation,
        "event_type": event_type,
        "batch_id": int(batch),
    }


def spark_log_batches(fs: CheckpointFs, directory: str) -> list[int]:
    """Batch IDs in a Spark ``offsets`` or ``commits`` log (ignores .crc/tmp files)."""

    if not fs.exists(directory):
        return []
    names = (str(getattr(entry, "name", entry)).rstrip("/") for entry in fs.ls(directory))
    return sorted(int(name) for name in names if name.isdigit())


def _new_incarnation() -> str:
    return uuid.uuid4().hex[:8]


def plan_incarnation(
    fs: CheckpointFs,
    root: str,
    checkpoint: str,
    stream_id: str,
    today: date,
    *,
    new_nonce: Callable[[], str] = _new_incarnation,
) -> StreamIncarnation:
    """Decide the incarnation a stream checkpoint starts with, without writing it.

    - No record and no Spark log: a new incarnation with today's catalog date.
    - Record and offsets: resume. A pending batch (offsets without a commit)
      replays with the recorded catalog date, so it regenerates the same
      baskets. With no pending batch the catalog date moves to today, so new
      launches enter the live catalog on restart.
    - Anything else (a record without offsets, offsets without a record,
      commits without offsets, or a record for another stream) is refused.
    """

    root = root.rstrip("/")
    record_path = f"{root}/_metadata/{checkpoint}.json"
    location = f"{root}/{checkpoint}"
    offsets = spark_log_batches(fs, f"{location}/offsets")
    commits = spark_log_batches(fs, f"{location}/commits")
    has_record = fs.exists(record_path)

    if commits and (not offsets or commits[-1] > offsets[-1]):
        raise CheckpointStateError(
            f"Spark checkpoint {location!r} has commits past its offsets log; it is "
            f"inconsistent. Delete {location!r} and {record_path!r} to start a new "
            "incarnation."
        )
    if not has_record:
        if offsets or commits:
            raise CheckpointStateError(
                f"Spark checkpoint {location!r} has offsets but no incarnation record "
                f"{record_path!r}, so the tags and IDs it already used are unknown. "
                f"Delete {location!r} to start a new incarnation."
            )
        incarnation = new_nonce()
        if not _INCARNATION_PATTERN.fullmatch(incarnation):
            raise ValueError(f"invalid incarnation nonce {incarnation!r}")
        return StreamIncarnation(
            stream_id, checkpoint, incarnation, today, record_path, None, True, False
        )

    record = json.loads(fs.head(record_path, 65536))
    if record.get("stream_id") != stream_id or record.get("checkpoint") != checkpoint:
        raise CheckpointStateError(
            f"Incarnation record {record_path!r} belongs to stream "
            f"{record.get('stream_id')!r} / {record.get('checkpoint')!r}, not "
            f"{stream_id!r} / {checkpoint!r}. Delete the whole checkpoint root to reset."
        )
    incarnation = str(record.get("incarnation", ""))
    if not _INCARNATION_PATTERN.fullmatch(incarnation):
        raise CheckpointStateError(f"Incarnation record {record_path!r} has no valid nonce")
    if not offsets:
        raise CheckpointStateError(
            f"Spark checkpoint {location!r} has no offsets log, but its incarnation "
            f"record {record_path!r} exists: the checkpoint was reset or never ran a "
            f"batch. Delete {record_path!r} as well to start a new incarnation with new "
            "tags and IDs, or restore the checkpoint to resume."
        )
    recorded_date = date.fromisoformat(str(record["catalog_date"]))
    pending = offsets[-1] if not commits or offsets[-1] > commits[-1] else None
    catalog_date = recorded_date if pending is not None else today
    return StreamIncarnation(
        stream_id,
        checkpoint,
        incarnation,
        catalog_date,
        record_path,
        pending,
        catalog_date != recorded_date,
        True,
    )


def persist_incarnation(fs: CheckpointFs, plan: StreamIncarnation) -> StreamIncarnation:
    """Write the incarnation record when the plan needs it; call just before starting."""

    if not plan.needs_write:
        return plan
    payload = {**plan.record(), "written_at": datetime.now(UTC).isoformat()}
    fs.mkdirs(plan.record_path.rsplit("/", 1)[0])
    content = json.dumps(payload, sort_keys=True)
    if plan.resumed:
        fs.put(plan.record_path, content, True)
        return replace(plan, needs_write=False)
    try:
        created = fs.put(plan.record_path, content, False)
    except Exception:
        if not fs.exists(plan.record_path):
            raise
        created = False
    if not created:
        raise CheckpointStateError(
            f"Another run created {plan.record_path!r} while this one was starting; "
            "run only one stream per checkpoint."
        )
    return replace(plan, needs_write=False)
