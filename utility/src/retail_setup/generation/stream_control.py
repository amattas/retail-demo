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
from datetime import date, datetime, timedelta, timezone
from typing import Any, Protocol

# Event types declared end to end (KQL, mappings, EVENT_PAYLOADS, Silver) for
# the incident and online-order lifecycle models, which the live stream does
# not emit yet. Readiness doesn't expect fresh rows for them. Shrink this set
# as the models land; tests/test_notebook_build.py checks it against the slots.
PENDING_EVENT_TYPES = frozenset(
    {
        "logistics_incident_started",
        "logistics_incident_resolved",
        "online_order_line_added",
        "online_order_delivered",
    }
)

# Live tables that are quiet by design. Live volume follows the store hours and
# the historical configuration, so a healthy stream can leave these tables
# without new rows for a while: store operations happen only at opening and
# closing, store visits stop overnight while every store is closed, and a small
# fleet has few online orders, impressions or promotions per hour. Readiness
# adds this quiet time to its freshness window for the table.
EVENT_TABLE_QUIET = {
    "store_opened": timedelta(hours=26),
    "store_closed": timedelta(hours=26),
    "customer_entered": timedelta(hours=12),
    "ble_ping_detected": timedelta(hours=12),
    "customer_zone_changed": timedelta(hours=12),
    "receipt_created": timedelta(hours=12),
    "receipt_line_added": timedelta(hours=12),
    "promotion_applied": timedelta(hours=12),
    "payment_processed": timedelta(hours=12),
    "online_order_created": timedelta(hours=12),
    "online_order_picked": timedelta(hours=12),
    "online_order_shipped": timedelta(hours=12),
    "ad_impression": timedelta(hours=6),
}


def event_table_max_age(event_type: str, base: timedelta) -> timedelta:
    """Freshness window of a live event table: ``base`` plus its quiet time."""

    return base + EVENT_TABLE_QUIET.get(event_type, timedelta(0))


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
    payload = {**plan.record(), "written_at": datetime.now(timezone.utc).isoformat()}
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


# ---------------------------------------------------------------------------
# History-to-stream handoff
# ---------------------------------------------------------------------------

# History ends yesterday (store-local), so the newest historical receipt is
# normally less than a day before the stream starts.
HANDOFF_MAX_GAP = timedelta(days=1)
# Local "yesterday" can end up to 14 hours after UTC midnight, so the newest
# historical receipt may legitimately be slightly after the stream start.
HANDOFF_OVERLAP_TOLERANCE = timedelta(hours=14)


class HandoffError(RuntimeError):
    """History and the live stream would leave a gap or overlap in the facts."""


@dataclass(frozen=True)
class HandoffCheck:
    ok: bool
    message: str


def check_history_handoff(
    history_end: datetime | None,
    stream_start: datetime,
    *,
    max_gap: timedelta = HANDOFF_MAX_GAP,
    overlap_tolerance: timedelta = HANDOFF_OVERLAP_TOLERANCE,
) -> HandoffCheck:
    """Compare the newest historical receipt time with the stream start (both UTC)."""

    if history_end is None:
        return HandoffCheck(
            False, "Silver fact_receipts has no rows, so there is no history to continue."
        )
    if history_end.tzinfo is None or stream_start.tzinfo is None:
        raise ValueError("history_end and stream_start must be timezone-aware UTC times")
    start = stream_start.astimezone(timezone.utc).isoformat(timespec="seconds")
    end = history_end.astimezone(timezone.utc).isoformat(timespec="seconds")
    if history_end > stream_start + overlap_tolerance:
        return HandoffCheck(
            False,
            f"History runs until {end}, after the stream start {start}: live events "
            "would duplicate the historical volume for the overlap.",
        )
    if history_end > stream_start:
        return HandoffCheck(
            True,
            f"History ends at {end}, slightly after the stream start {start} "
            "(store time zones); the overlap is within tolerance.",
        )
    gap = stream_start - history_end
    if gap > max_gap:
        return HandoffCheck(
            False,
            f"History ends at {end}, {gap.total_seconds() / 86400:.1f} days before the "
            f"stream start {start}: Silver and Gold facts will have a gap. Re-render "
            "and rerun setup so history ends yesterday.",
        )
    return HandoffCheck(True, "")


def enforce_history_handoff(
    check: HandoffCheck,
    *,
    resumed: bool,
    allow_gap: bool,
    log: Callable[[str], None] = print,
) -> None:
    """Fail a new stream on a bad handoff unless overridden; only warn on a resume.

    A resumed checkpoint continues its own events, so a gap after a stop is
    expected (the backlog cap skips the downtime) and is only reported.
    """

    if check.ok:
        if check.message:
            log(f"history handoff: {check.message}")
        return
    if resumed or allow_gap:
        reason = "resumed checkpoint" if resumed else "allow_history_gap=True"
        log(f"WARNING history handoff ({reason}): {check.message}")
        return
    raise HandoffError(f"{check.message} Set allow_history_gap=True to start anyway.")
