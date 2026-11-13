"""Thread-safe task and log state rendered by the guided-setup TUI."""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field

TERMINAL_STATUSES = frozenset({"SUCCEEDED", "DEGRADED", "SKIPPED", "FAILED"})
MAX_LOG_ENTRIES = 20_000


@dataclass
class Task:
    key: str
    label: str
    parent: str | None = None
    status: str = "PENDING"
    required: bool = True
    detail: str | None = None
    started: float | None = None
    ended: float | None = None
    order: float = 0.0

    def duration(self, now: float) -> float | None:
        if self.started is None:
            return None
        return (self.ended if self.ended is not None else now) - self.started


@dataclass(frozen=True)
class LogEntry:
    task: str
    text: str
    timestamp: float


@dataclass(frozen=True)
class PromptRequest:
    kind: str  # confirm | text | int
    message: str
    default: str | None = None
    detail: str | None = None
    title: str = "Input required"


def matches_task(entry_task: str, key: str) -> bool:
    return entry_task == key or entry_task.startswith(key + "/")


@dataclass
class SetupState:
    """Phases, nested deploy steps, and the captured log, guarded by one lock."""

    phases: list[tuple[str, str]]
    lock: threading.RLock = field(default_factory=threading.RLock)
    tasks: dict[str, Task] = field(default_factory=dict)
    log: deque[LogEntry] = field(default_factory=lambda: deque(maxlen=MAX_LOG_ENTRIES))
    pending_log: deque[LogEntry] = field(default_factory=deque)
    started: float = field(default_factory=time.monotonic)
    finished: bool = False
    exit_code: int | None = None

    def __post_init__(self) -> None:
        for key, label in self.phases:
            self.tasks[key] = Task(key=key, label=label)

    @staticmethod
    def child_key(parent: str, task_id: str) -> str:
        return f"{parent}/{task_id}"

    def set_status(
        self,
        key: str,
        status: str,
        *,
        detail: str | None = None,
        duration: float | None = None,
    ) -> Task:
        with self.lock:
            task = self.tasks[key]
            now = time.monotonic()
            if status == "RUNNING" and task.status != "RUNNING":
                task.started = now
                task.ended = None
            elif status in TERMINAL_STATUSES and task.status not in TERMINAL_STATUSES:
                if duration is not None:
                    task.started, task.ended = now - duration, now
                else:
                    task.started = task.started if task.started is not None else now
                    task.ended = now
            task.status = status
            if detail is not None:
                task.detail = detail
            return task

    def upsert_child(
        self,
        parent: str,
        task_id: str,
        *,
        label: str,
        status: str,
        required: bool = True,
        detail: str | None = None,
    ) -> tuple[Task, str | None]:
        """Create or update a nested task; return it with its previous status."""

        key = self.child_key(parent, task_id)
        with self.lock:
            previous = self.tasks[key].status if key in self.tasks else None
            if previous is None:
                orders = [child.order for child in self.children(parent)]
                self.tasks[key] = Task(
                    key=key,
                    label=label,
                    parent=parent,
                    order=max(orders, default=0.0) + 1,
                )
            task = self.tasks[key]
            task.label = label
            task.required = required
            if previous != status:
                self.set_status(key, status)
            task.detail = detail
            return task, previous

    def ordered(self) -> list[Task]:
        """Top-level phases in order, each followed by its nested tasks."""

        with self.lock:
            rows: list[Task] = []
            for key, _ in self.phases:
                rows.append(self.tasks[key])
                rows.extend(self.children(key))
            return rows

    def children(self, parent: str) -> list[Task]:
        with self.lock:
            return sorted(
                (task for task in self.tasks.values() if task.parent == parent),
                key=lambda task: task.order,
            )

    def sync_children(self, parent: str, task_ids: list[str]) -> None:
        """Order a parent's children like a complete task list.

        Pending children missing from the list (a preview step the real plan
        dropped) are removed. Started children missing from it, such as the
        deploy sign-in, stay first.
        """

        with self.lock:
            listed = {
                self.child_key(parent, task_id): index for index, task_id in enumerate(task_ids)
            }
            kept = 0
            for child in self.children(parent):
                if child.key in listed:
                    child.order = float(listed[child.key])
                elif child.status == "PENDING":
                    del self.tasks[child.key]
                else:
                    child.order = -1000.0 + kept
                    kept += 1

    def skip_pending_children(self, parent: str) -> None:
        with self.lock:
            for child in self.children(parent):
                if child.status == "PENDING":
                    self.set_status(child.key, "SKIPPED")

    def phase_fraction(self, key: str) -> float:
        with self.lock:
            if self.tasks[key].status in TERMINAL_STATUSES:
                return 1.0
            children = self.children(key)
            if not children:
                return 0.0
            done = sum(child.status in TERMINAL_STATUSES for child in children)
            return done / len(children)

    def progress(self) -> float:
        """Overall completion in [0, 1]; nested steps advance their phase."""

        with self.lock:
            return sum(self.phase_fraction(key) for key, _ in self.phases) / len(self.phases)

    def active_task(self) -> Task | None:
        """The most recently started running task, preferring nested steps."""

        with self.lock:
            running = [task for task in self.tasks.values() if task.status == "RUNNING"]
            if not running:
                return None
            return max(running, key=lambda task: (task.parent is not None, task.started or 0.0))

    def add_log(self, task: str, text: str) -> LogEntry:
        entry = LogEntry(task=task, text=text, timestamp=time.time())
        with self.lock:
            self.log.append(entry)
            self.pending_log.append(entry)
        return entry

    def drain_pending_log(self) -> list[LogEntry]:
        with self.lock:
            entries = list(self.pending_log)
            self.pending_log.clear()
            return entries

    def log_for(self, key: str | None) -> list[LogEntry]:
        """All captured lines, or the lines for one task and its nested steps."""

        with self.lock:
            if key is None:
                return list(self.log)
            return [entry for entry in self.log if matches_task(entry.task, key)]
