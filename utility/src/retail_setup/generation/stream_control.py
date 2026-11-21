"""Run control for the live stream: checkpoints, Kusto write retries and restarts.

Pure Python with no Spark or Fabric imports. ``build_notebooks.py`` inlines it
into ``stream-events``, and the tests call it directly with fakes, so the
notebook's operational behavior is checked offline.
"""

import re
import time
from collections.abc import Callable
from typing import Any

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


def _error_text(exc: BaseException) -> str:
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

    return bool(_TRANSIENT_ERROR.search(_error_text(exc)))


def backoff_seconds(attempt: int, base: float, ceiling: float) -> float:
    """Exponential backoff for the given 1-based retry number, capped at ``ceiling``."""

    return float(min(ceiling, base * 2 ** max(0, attempt - 1)))


def _summary(exc: BaseException, limit: int = 300) -> str:
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
                f"({type(exc).__name__}: {_summary(exc)}); retrying in {delay:.0f}s"
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
                    f"(restarts used: {restarts}/{max_restarts}): {_summary(exc)}"
                )
                raise
            restarts += 1
            delay = backoff_seconds(restarts, base_seconds, max_seconds)
            log(
                f"stream query failed ({type(exc).__name__}: {_summary(exc)}); "
                f"restart {restarts}/{max_restarts} in {delay:.0f}s"
            )
            sleep(delay)
