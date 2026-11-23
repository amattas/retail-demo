"""Lease locks that keep Lakehouse writers from overlapping.

``streaming-data-load`` (03, 04), ``daily-maintenance`` (05 and the ML
refresh), ``ml-required`` (setup-11 and the ML producers) and notebook 90 run
on independent schedules and write the same Delta tables. Fabric has no
cross-pipeline mutex, so each writer claims a lease row in
``<silver>._job_locks`` before it writes:

- ``silver``: notebook 03 and notebook 90 (all Silver tables and ``_watermarks``);
- ``gold``: notebook 04 and setup-11 (the ten Gold aggregates, ``setup_run_log``);
- ``ml:<table>``: the producer of that ML output table.

Notebook 05 claims the same lock for each table it maintains, without
waiting, and skips the table when another job holds it.

A claim is a Delta ``MERGE`` that succeeds only when the row is missing,
expired, or already ours; concurrent claims conflict in Delta and one of them
loses. The holder renews its lease from a background heartbeat, so a crashed
notebook releases its locks once the lease expires (``JOB_LOCK_TTL_SECONDS``).
All times come from Spark's ``current_timestamp()``, never the driver clock.

Keep this module free of package imports: Lakehouse notebooks run it as a
cell (``scripts/build_notebooks.py`` copies it in).
"""

import threading
import time as _time
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone

JOB_LOCK_TABLE = "_job_locks"
JOB_LOCK_TTL_SECONDS = 900
JOB_LOCK_HEARTBEAT_SECONDS = 120
JOB_LOCK_POLL_SECONDS = 30
SILVER_LOCK = "silver"
GOLD_LOCK = "gold"


def ml_output_lock(table: str) -> str:
    """The lock that guards one ML output table."""
    return f"ml:{table}"


def is_concurrent_write_error(exc: BaseException) -> bool:
    """True for Delta optimistic-concurrency conflicts (another writer won)."""
    text = f"{type(exc).__name__}: {exc}"
    return "Concurrent" in text or "DELTA_CONCURRENT" in text


class DeltaJobLockStore:
    """Lease rows in a Delta table: (lock_name, owner, acquired_at, expires_at)."""

    def __init__(self, spark, table_name: str, ttl_seconds: int = JOB_LOCK_TTL_SECONDS):
        self.spark = spark
        self.table_name = table_name
        self.ttl_seconds = int(ttl_seconds)
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            "(lock_name STRING, owner STRING, acquired_at TIMESTAMP, expires_at TIMESTAMP) "
            "USING DELTA"
        )

    @staticmethod
    def _quote(value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def holder(self, name: str) -> str | None:
        """The owner of a live lease on ``name``, or None."""
        rows = self.spark.sql(
            f"SELECT owner FROM {self.table_name} "
            f"WHERE lock_name = {self._quote(name)} AND expires_at > current_timestamp()"
        ).collect()
        return rows[0]["owner"] if rows else None

    def try_claim(self, name: str, owner: str) -> bool:
        try:
            self.spark.sql(
                f"""
                MERGE INTO {self.table_name} AS t
                USING (
                    SELECT {self._quote(name)} AS lock_name, {self._quote(owner)} AS owner,
                           current_timestamp() AS acquired_at,
                           current_timestamp() + INTERVAL {self.ttl_seconds} SECONDS AS expires_at
                ) AS s
                ON t.lock_name = s.lock_name
                WHEN MATCHED AND (t.expires_at <= s.acquired_at OR t.owner = s.owner)
                    THEN UPDATE SET t.owner = s.owner, t.acquired_at = s.acquired_at,
                                    t.expires_at = s.expires_at
                WHEN NOT MATCHED THEN INSERT (lock_name, owner, acquired_at, expires_at)
                    VALUES (s.lock_name, s.owner, s.acquired_at, s.expires_at)
                """
            )
        except Exception as exc:
            if is_concurrent_write_error(exc):
                return False
            raise
        return self.holder(name) == owner

    def renew(self, name: str, owner: str) -> bool:
        try:
            self.spark.sql(
                f"UPDATE {self.table_name} "
                f"SET expires_at = current_timestamp() + INTERVAL {self.ttl_seconds} SECONDS "
                f"WHERE lock_name = {self._quote(name)} AND owner = {self._quote(owner)}"
            )
        except Exception as exc:
            if not is_concurrent_write_error(exc):
                raise
        return self.holder(name) == owner

    def release(self, name: str, owner: str) -> None:
        for _attempt in range(5):
            try:
                self.spark.sql(
                    f"DELETE FROM {self.table_name} "
                    f"WHERE lock_name = {self._quote(name)} AND owner = {self._quote(owner)}"
                )
                return
            except Exception as exc:
                if not is_concurrent_write_error(exc):
                    raise
                _time.sleep(1)


class MemoryJobLockStore:
    """The same lease semantics in memory, with an injectable clock (tests)."""

    def __init__(
        self,
        ttl_seconds: int = JOB_LOCK_TTL_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ):
        self.ttl = timedelta(seconds=ttl_seconds)
        self.clock = clock or (lambda: datetime.now(timezone.utc).replace(tzinfo=None))
        self.rows: dict[str, tuple[str, datetime]] = {}

    def holder(self, name: str) -> str | None:
        row = self.rows.get(name)
        return row[0] if row and row[1] > self.clock() else None

    def try_claim(self, name: str, owner: str) -> bool:
        now = self.clock()
        row = self.rows.get(name)
        if row is None or row[1] <= now or row[0] == owner:
            self.rows[name] = (owner, now + self.ttl)
        return self.holder(name) == owner

    def renew(self, name: str, owner: str) -> bool:
        row = self.rows.get(name)
        if row is not None and row[0] == owner:
            self.rows[name] = (owner, self.clock() + self.ttl)
        return self.holder(name) == owner

    def release(self, name: str, owner: str) -> None:
        if self.rows.get(name, ("", None))[0] == owner:
            del self.rows[name]


def new_lock_owner(holder: str) -> str:
    """A unique owner for one notebook run, e.g. ``03-streaming-to-silver:1a2b...``."""
    return f"{holder}:{uuid.uuid4().hex[:12]}"


def acquire_job_locks(
    store,
    names: Iterable[str],
    owner: str,
    *,
    wait_seconds: float,
    poll_seconds: float = JOB_LOCK_POLL_SECONDS,
    sleep: Callable[[float], None] = _time.sleep,
    monotonic: Callable[[], float] = _time.monotonic,
) -> list[str] | None:
    """Claim every lock in ``names`` or none of them.

    Locks are claimed in sorted order, so two jobs that need several locks
    can't deadlock. Retries until ``wait_seconds`` elapse (0 = one attempt)
    and returns the claimed names, or None when another job still holds one.
    """
    ordered = sorted(set(names))
    deadline = monotonic() + max(wait_seconds, 0)
    while True:
        claimed = []
        for name in ordered:
            if not store.try_claim(name, owner):
                break
            claimed.append(name)
        if len(claimed) == len(ordered):
            return claimed
        for name in reversed(claimed):
            store.release(name, owner)
        remaining = deadline - monotonic()
        if remaining <= 0:
            return None
        sleep(min(poll_seconds, remaining))


class JobLocks:
    """A notebook's set of leases, renewed by a heartbeat thread until released."""

    def __init__(
        self,
        store,
        holder: str,
        names: Iterable[str],
        heartbeat_seconds: float = JOB_LOCK_HEARTBEAT_SECONDS,
    ):
        self.store = store
        self.owner = new_lock_owner(holder)
        self.names = sorted(set(names))
        self.heartbeat_seconds = heartbeat_seconds
        self.held: list[str] = []
        self.lost: list[str] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def blockers(self) -> dict[str, str | None]:
        """Current holders of the locks this job needs (for messages)."""
        return {name: self.store.holder(name) for name in self.names}

    def acquire(self, wait_seconds: float, **kwargs) -> bool:
        claimed = acquire_job_locks(
            self.store, self.names, self.owner, wait_seconds=wait_seconds, **kwargs
        )
        if claimed is None:
            return False
        self.held = claimed
        if self.heartbeat_seconds > 0:
            self._thread = threading.Thread(target=self._heartbeat, daemon=True)
            self._thread.start()
        return True

    def _heartbeat(self) -> None:
        while not self._stop.wait(self.heartbeat_seconds):
            self.renew()

    def renew(self) -> None:
        for name in self.held:
            try:
                renewed = self.store.renew(name, self.owner)
            except Exception as exc:
                print(f"Lock {name}: renewal failed ({exc}); retrying at the next heartbeat")
                continue
            if not renewed and name not in self.lost:
                self.lost.append(name)

    def assert_held(self) -> None:
        """Fail before a commit point if a lease expired and another job took it."""
        if self.lost:
            raise RuntimeError(f"Job locks lost before completion: {self.lost}")

    def release(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.heartbeat_seconds + 5)
        for name in reversed(self.held):
            self.store.release(name, self.owner)
        self.held = []
