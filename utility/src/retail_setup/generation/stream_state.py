"""Versioned state for the live stream, kept in Lakehouse Delta tables.

Stateful stream features (store open/closed, in-progress customer visits)
persist their state between micro-batches in ``<silver>._stream_state_<name>``.
Each table holds one row per entity version::

    (incarnation, batch_id, entity_key, payload, deleted)

Micro-batch N reads, for every entity, its latest version written before N,
and writes the versions it changes with ``batch_id = N``. The write first
replaces every row of the incarnation with ``batch_id >= N``, so when Spark
replays batch N (its commit was not recorded) the replay reads exactly the
state the first attempt read, regenerates identical events, and overwrites the
first attempt's versions instead of applying them twice.

State belongs to a checkpoint incarnation: a new incarnation finds no rows
and seeds its state once (``batch_id = -1``) from current Silver values.

The transitions are pure Python over plain dicts (``apply_*_batch``), so they
are unit-tested without Spark. :class:`MemoryStateStore` keeps the rows in
memory with the same semantics as :class:`DeltaStateStore`, which is the
Fabric adapter. ``build_notebooks.py`` inlines this module into the stream
notebook.
"""

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retail_setup.generation.stream_calendar import StoreHours, is_open_at, store_boundaries

STATE_TABLE_PREFIX = "_stream_state_"
SEED_BATCH_ID = -1
STATE_SCHEMA = (
    "incarnation string, batch_id long, entity_key string, payload string, deleted boolean"
)
# Superseded versions are deleted every this many batches.
COMPACT_EVERY_BATCHES = 60

StateMap = dict[str, dict[str, Any]]
Changes = dict[str, dict[str, Any] | None]


@dataclass(frozen=True)
class StateRow:
    incarnation: str
    batch_id: int
    entity_key: str
    payload: str | None
    deleted: bool = False


def encode_payload(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def decode_payload(text: str) -> dict[str, Any]:
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("state payload must be a JSON object")
    return value


def state_as_of(rows: Iterable[StateRow], incarnation: str, batch_id: int) -> StateMap:
    """State a batch sees: each entity's latest version written before ``batch_id``."""
    latest: dict[str, StateRow] = {}
    for row in rows:
        if row.incarnation != incarnation or row.batch_id >= batch_id:
            continue
        current = latest.get(row.entity_key)
        if current is None or row.batch_id > current.batch_id:
            latest[row.entity_key] = row
    return {
        key: decode_payload(row.payload)
        for key, row in sorted(latest.items())
        if not row.deleted and row.payload is not None
    }


def state_changes(before: StateMap, after: StateMap) -> Changes:
    """Entities whose value changed (``None`` marks a deletion)."""
    changes: Changes = {key: None for key in before if key not in after}
    changes.update({key: value for key, value in after.items() if before.get(key) != value})
    return dict(sorted(changes.items()))


def version_rows(incarnation: str, batch_id: int, changes: Changes) -> list[StateRow]:
    return [
        StateRow(
            incarnation,
            int(batch_id),
            key,
            None if value is None else encode_payload(value),
            value is None,
        )
        for key, value in sorted(changes.items())
    ]


def replace_batch(
    rows: Iterable[StateRow], incarnation: str, batch_id: int, new_rows: Iterable[StateRow]
) -> list[StateRow]:
    """Rows after writing batch ``batch_id``: drop the incarnation's ``>= batch_id`` rows first."""
    kept = [
        row for row in rows if not (row.incarnation == incarnation and row.batch_id >= batch_id)
    ]
    return kept + list(new_rows)


def superseded_rows(rows: Iterable[StateRow], incarnation: str, batch_id: int) -> list[StateRow]:
    """Versions before ``batch_id`` that no read at or after ``batch_id`` can see.

    An older version is hidden by a newer one written before ``batch_id``, and
    a deletion marker that is the latest version hides nothing.
    """
    latest: dict[str, StateRow] = {}
    candidates = [row for row in rows if row.incarnation == incarnation and row.batch_id < batch_id]
    for row in candidates:
        current = latest.get(row.entity_key)
        if current is None or row.batch_id > current.batch_id:
            latest[row.entity_key] = row
    return [row for row in candidates if latest[row.entity_key] is not row or row.deleted]


def replace_condition(incarnation: str, batch_id: int) -> str:
    """Delta ``replaceWhere`` predicate of a batch write."""
    if not incarnation.replace("-", "").replace("_", "").replace(".", "").isalnum():
        raise ValueError(f"invalid state incarnation {incarnation!r}")
    return f"incarnation = '{incarnation}' AND batch_id >= {int(batch_id)}"


def apply_batch(
    state: StateMap,
    inputs: Any,
    transition: Callable[[StateMap, Any], tuple[StateMap, list[Any]]],
) -> tuple[StateMap, list[Any]]:
    """Run one micro-batch transition on a copy of ``state``: ``(new_state, emitted)``."""
    return transition({key: dict(value) for key, value in state.items()}, inputs)


# ---------------------------------------------------------------------------
# Store open/closed state
# ---------------------------------------------------------------------------

# A store's open/close events are replayed at most this far back, for example
# after a long stop or when history ended days before the stream started.
MAX_STORE_CATCHUP = timedelta(days=1)
_OPERATIONS = ("opened", "closed")


@dataclass(frozen=True)
class StoreOperation:
    store_id: int
    event_time: datetime
    operation_type: str


@dataclass(frozen=True)
class StoreBatch:
    stores: tuple[StoreHours, ...]
    clock: datetime
    previous_clock: datetime | None = None


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def seed_store_state(
    stores: Iterable[StoreHours],
    last_operations: dict[int, tuple[datetime, str]],
    now: datetime,
) -> StateMap:
    """Initial store state of a new incarnation.

    A store's last historical operation (``fact_store_ops``) decides whether it
    is open; a store without one starts from its operating hours at ``now``.
    """
    seeded: StateMap = {}
    for store in stores:
        last = last_operations.get(store.store_id)
        operation = None if last is None else str(last[1]).strip().lower()
        if last is not None and operation in _OPERATIONS:
            event_time = last[0] if last[0].tzinfo else last[0].replace(tzinfo=timezone.utc)
            seeded[str(store.store_id)] = {
                "is_open": operation == "opened",
                "changed_at": _iso(event_time),
            }
        else:
            seeded[str(store.store_id)] = {
                "is_open": is_open_at(store, now),
                "changed_at": _iso(now),
            }
    return seeded


def apply_store_batch(state: StateMap, batch: StoreBatch) -> tuple[StateMap, list[StoreOperation]]:
    """Emit each store's open/close boundaries up to the batch clock.

    Boundaries come from the store's local operating hours. An operation that
    would not change the store's state is skipped, so every store alternates
    opened, closed, opened, ... and each open day yields one paired
    opened/closed. Only the window since the last batch (and at most
    :data:`MAX_STORE_CATCHUP`) is scanned.
    """
    new_state = dict(state)
    emitted: list[StoreOperation] = []
    floor = batch.clock - MAX_STORE_CATCHUP
    for store in sorted(batch.stores, key=lambda item: item.store_id):
        key = str(store.store_id)
        current = state.get(key)
        if current is None:
            current = {"is_open": is_open_at(store, floor), "changed_at": _iso(floor)}
            new_state[key] = current
        is_open = bool(current["is_open"])
        lower = max(_parse(current["changed_at"]), floor)
        if batch.previous_clock is not None:
            lower = max(lower, batch.previous_clock)
        changed = None
        for instant, operation in store_boundaries(store, lower, batch.clock):
            if (operation == "opened") == is_open:
                continue
            is_open = operation == "opened"
            changed = instant
            emitted.append(StoreOperation(store.store_id, instant, operation))
        if changed is not None:
            new_state[key] = {"is_open": is_open, "changed_at": _iso(changed)}
    return new_state, sorted(emitted, key=lambda op: (op.event_time, op.store_id))


# ---------------------------------------------------------------------------
# Customer visit state
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VisitBatch:
    """Visits known to a batch: ``visits[key] = (state payload, visit end in ms)``."""

    clock_ms: int
    visits: dict[str, tuple[dict[str, Any], int]]


def apply_visit_batch(state: StateMap, batch: VisitBatch) -> tuple[StateMap, list[str]]:
    """Keep visits that end after the batch clock; return the keys that finished.

    A visit's events are emitted as their time passes, so a visit stays in
    state until its last event (its end) is at or before the batch clock.
    """
    new_state = {
        key: payload
        for key, (payload, end_ms) in sorted(batch.visits.items())
        if end_ms > batch.clock_ms
    }
    finished = sorted(key for key in batch.visits if key not in new_state)
    for key in state:
        if key not in batch.visits:
            new_state[key] = state[key]
    return dict(sorted(new_state.items())), finished


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------


class StateStore(Protocol):
    incarnation: str

    def read(self, name: str, batch_id: int) -> StateMap: ...

    def write(self, name: str, batch_id: int, changes: Changes) -> None: ...

    def has_rows(self, name: str) -> bool: ...

    def compact(self, name: str, batch_id: int) -> None: ...


class MemoryStateStore:
    """In-memory state store with the Delta store's semantics (tests, offline runs)."""

    def __init__(self, incarnation: str) -> None:
        self.incarnation = incarnation
        self.rows: dict[str, list[StateRow]] = {}

    def read(self, name: str, batch_id: int) -> StateMap:
        return state_as_of(self.rows.get(name, []), self.incarnation, batch_id)

    def write(self, name: str, batch_id: int, changes: Changes) -> None:
        self.rows[name] = replace_batch(
            self.rows.get(name, []),
            self.incarnation,
            batch_id,
            version_rows(self.incarnation, batch_id, changes),
        )

    def has_rows(self, name: str) -> bool:
        return any(row.incarnation == self.incarnation for row in self.rows.get(name, []))

    def compact(self, name: str, batch_id: int) -> None:
        stale = set(superseded_rows(self.rows.get(name, []), self.incarnation, batch_id))
        self.rows[name] = [row for row in self.rows.get(name, []) if row not in stale]


class DeltaStateStore:
    """State tables ``<database>._stream_state_<name>`` in the Lakehouse (Delta)."""

    def __init__(self, spark: SparkSession, database: str, incarnation: str) -> None:
        replace_condition(incarnation, 0)
        self.spark = spark
        self.database = database
        self.incarnation = incarnation
        self._ready: set[str] = set()

    def table(self, name: str) -> str:
        return f"{self.database}.{STATE_TABLE_PREFIX}{name}"

    def _ensure(self, name: str) -> str:
        table = self.table(name)
        if name not in self._ready:
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table} (incarnation STRING, batch_id BIGINT, "
                "entity_key STRING, payload STRING, deleted BOOLEAN) USING DELTA"
            )
            self._ready.add(name)
        return table

    def _versions(self, name: str, batch_id: int) -> DataFrame:
        rows = self.spark.table(self._ensure(name)).where(
            (F.col("incarnation") == self.incarnation) & (F.col("batch_id") < int(batch_id))
        )
        newest = Window.partitionBy("entity_key").orderBy(F.col("batch_id").desc())
        return rows.withColumn("_version", F.row_number().over(newest))

    def read(self, name: str, batch_id: int) -> StateMap:
        latest = self._versions(name, batch_id).where((F.col("_version") == 1) & ~F.col("deleted"))
        return {
            row["entity_key"]: decode_payload(row["payload"])
            for row in latest.select("entity_key", "payload").collect()
        }

    def write(self, name: str, batch_id: int, changes: Changes) -> None:
        rows = version_rows(self.incarnation, batch_id, changes)
        frame = self.spark.createDataFrame(
            [(r.incarnation, r.batch_id, r.entity_key, r.payload, r.deleted) for r in rows],
            STATE_SCHEMA,
        )
        (
            frame.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_condition(self.incarnation, batch_id))
            .saveAsTable(self._ensure(name))
        )

    def has_rows(self, name: str) -> bool:
        table = self.spark.table(self._ensure(name))
        return not table.where(F.col("incarnation") == self.incarnation).limit(1).isEmpty()

    def compact(self, name: str, batch_id: int) -> None:
        if int(batch_id) <= 0 or int(batch_id) % COMPACT_EVERY_BATCHES:
            return
        stale = (
            self._versions(name, batch_id)
            .where((F.col("_version") > 1) | F.col("deleted"))
            .select("incarnation", "batch_id", "entity_key")
        )
        view = f"_stale_{STATE_TABLE_PREFIX}{name}"
        stale.createOrReplaceTempView(view)
        try:
            self.spark.sql(
                f"MERGE INTO {self.table(name)} AS t USING {view} AS s "
                "ON t.incarnation = s.incarnation AND t.batch_id = s.batch_id "
                "AND t.entity_key = s.entity_key WHEN MATCHED THEN DELETE"
            )
        finally:
            self.spark.catalog.dropTempView(view)
