"""Stage -> validate -> promote -> (rollback) coordinator for historical
setup publication (IMP-002).

Fabric Lakehouse targets are plain Delta tables; there is no cross-table
transaction spanning several ``saveAsTable``/``CREATE OR REPLACE TABLE``
calls. This module implements the staged-promotion pattern used by
setup-02/03/04 instead:

1. Every candidate table is first written to a run-scoped **staging**
   table/location — the final target is never touched during staging.
2. Each staged table is validated (schema + row count) against its source
   before any promotion happens. All staging and validation must succeed
   before a single final target changes.
3. Promotion swaps each staged table into its fixed final name. If the Nth
   promotion fails, every already-promoted target *and the Nth target
   itself* is rolled back: targets that existed before this run are
   restored to their pre-run Delta version (``RESTORE TABLE ... TO VERSION
   AS OF``); targets that did not exist before this run are dropped. The
   failed target is included because a Delta command can commit before a
   client/network error is observed by the caller — restoring/dropping it
   anyway is safe and idempotent (a no-op if it never actually committed)
   and keeps rollback fail-closed instead of trusting the exception.
   Rollback keeps trying every changed target even if one restore/drop
   fails, and the run is marked ROLLED_BACK (all recovered) or
   ROLLBACK_FAILED (some did not recover — this is never reported as
   success).
4. Staging is cleaned up once the run reaches a terminal state that no
   longer needs it (COMPLETED; FAILED before any promotion; ROLLED_BACK with
   a clean rollback). ROLLBACK_FAILED preserves staging for manual recovery
   and logs the preserved names. Cleanup itself never raises: it is
   attempted for every staged table and any failures are collected. A
   cleanup failure after an otherwise-successful promotion or rollback is
   never reported as plain COMPLETED/ROLLED_BACK (that would hide leftover
   staging) but is also never conflated with a data failure — it is
   reported as COMPLETED_CLEANUP_FAILED / ROLLED_BACK_CLEANUP_FAILED, with
   the message explicitly stating the data operation succeeded and only
   staging cleanup needs manual attention.

The coordinator itself has no Spark/Delta dependency: every side effect
(stage, validate, capture target state, promote, restore, drop, cleanup,
log) is injected through the ``PublicationBackend`` protocol, so the state
machine can be exercised deterministically in tests without a Spark session
or a Fabric workspace. ``writer.py`` supplies the concrete Spark/Delta
catalog backend for notebooks and a filesystem backend for local
path/parquet tests.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, Sequence

# Bound error text kept in setup_run_log so one runaway exception can't blow
# past the log column budget.
ERROR_TEXT_LIMIT = 1000

# Terminal coordinator states. STAGED/VALIDATED/PROMOTING/ROLLING_BACK are
# intermediate/log-only states; COMPLETED is success, the rest are failure.
COMPLETED = "COMPLETED"
FAILED = "FAILED"
ROLLED_BACK = "ROLLED_BACK"
ROLLBACK_FAILED = "ROLLBACK_FAILED"
# The data operation (promote or rollback) succeeded, but cleaning up staging
# afterward failed. Deliberately distinct from COMPLETED/ROLLED_BACK — never
# silently reported as success — and distinct from FAILED/ROLLBACK_FAILED —
# never conflated with a data problem.
COMPLETED_CLEANUP_FAILED = "COMPLETED_CLEANUP_FAILED"
ROLLED_BACK_CLEANUP_FAILED = "ROLLED_BACK_CLEANUP_FAILED"


def bound_error(exc: BaseException) -> str:
    """Render an exception as ``Type: message``, truncated for log storage."""
    return f"{type(exc).__name__}: {exc}"[:ERROR_TEXT_LIMIT]


@dataclass(frozen=True)
class TableTarget:
    """One candidate table to publish.

    ``name`` is the bare logical table name (e.g. ``fact_receipts``) used as
    the ``setup_run_log.table_name`` value. ``db`` is the schema/database it
    belongs to (silver or gold). ``staging_name`` is the run-scoped staging
    identifier the backend actually writes to — it is opaque to the
    coordinator (a qualified catalog table name, a filesystem path, ...).
    """

    name: str
    db: str
    staging_name: str


@dataclass(frozen=True)
class TargetState:
    """Pre-promotion snapshot of one target, captured before any promotion.

    ``existed`` distinguishes a target that must be *restored* on rollback
    from one that must be *dropped*. ``restore_token`` is backend-defined (a
    Delta version int for catalog targets, a backup path for filesystem
    targets) and is handed back to ``backend.restore()`` unchanged.
    """

    existed: bool
    restore_token: Any = None


@dataclass
class PublicationOutcome:
    """Terminal result of one ``PublicationCoordinator.publish()`` call."""

    # COMPLETED | FAILED | ROLLED_BACK | ROLLBACK_FAILED |
    # COMPLETED_CLEANUP_FAILED | ROLLED_BACK_CLEANUP_FAILED
    state: str
    promoted: list[str] = field(default_factory=list)
    error: str | None = None
    rollback_failures: list[tuple[str, str]] = field(default_factory=list)
    # Staging identifiers left behind for manual recovery: every staged
    # target on ROLLBACK_FAILED, or just the ones whose cleanup() call
    # itself failed on *_CLEANUP_FAILED.
    preserved_staging: list[str] = field(default_factory=list)
    cleanup_failures: list[tuple[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.state == COMPLETED


class PublicationBackend(Protocol):
    """Side effects the coordinator needs; implemented per storage mode."""

    def stage(self, target: TableTarget) -> int:
        """Write the candidate to ``target.staging_name``; return its row count."""
        ...

    def validate(self, target: TableTarget, staged_row_count: int) -> None:
        """Raise if the staged table's schema/row count disagree with the source."""
        ...

    def target_state(self, target: TableTarget) -> TargetState:
        """Snapshot whether the final target exists and its restore token."""
        ...

    def promote(self, target: TableTarget) -> int:
        """Swap staged data into the final target; return the promoted row count."""
        ...

    def restore(self, target: TableTarget, state: TargetState) -> None:
        """Undo a promotion of a pre-existing target using its captured state."""
        ...

    def drop(self, target: TableTarget) -> None:
        """Undo a promotion of a target that did not exist before this run."""
        ...

    def cleanup(self, target: TableTarget) -> None:
        """Remove the run-scoped staging artifact for ``target``."""
        ...


# (table_name, status, row_count, error) — mirrors setup_run_log's columns.
LogFn = Callable[[str, str, "int | None", "str | None"], None]


class PublicationCoordinator:
    """Runs the stage -> validate -> promote (-> rollback) state machine.

    ``targets`` are processed in the given order for staging, validation and
    promotion; rollback undoes them in reverse (LIFO) order. All logging
    goes through the injected ``log`` callback so callers can persist an
    append-only ``setup_run_log``-shaped history.
    """

    def __init__(self, backend: PublicationBackend, log: LogFn) -> None:
        self.backend = backend
        self.log = log

    def publish(self, targets: Sequence[TableTarget]) -> PublicationOutcome:
        self.log("__run__", "STARTED", None, None)

        staged: list[TableTarget] = []
        # Keyed by the full TableTarget (name+db+staging_name), not the bare
        # name: a silver and a gold table can legitimately share a name, and
        # keying by name alone would let one target's staged count/state
        # silently shadow the other's.
        staged_counts: dict[TableTarget, int] = {}

        # Phase 1 — stage every candidate. Final targets are untouched here.
        for target in targets:
            try:
                count = self.backend.stage(target)
            except Exception as exc:  # noqa: BLE001 — recorded, not swallowed
                return self._fail(target, exc, staged)
            staged.append(target)
            staged_counts[target] = count
            self.log(target.name, "STAGED", count, None)

        # Phase 2 — validate every staged table before any promotion.
        for target in targets:
            try:
                self.backend.validate(target, staged_counts[target])
            except Exception as exc:  # noqa: BLE001
                return self._fail(target, exc, staged)
        self.log("__run__", "VALIDATED", None, None)

        # Phase 3 — snapshot pre-promotion state (existed + restore token) for
        # every target before touching any of them. Keyed by TableTarget for
        # the same same-name-different-db reason as staged_counts above.
        target_states = {t: self.backend.target_state(t) for t in targets}

        # Phase 4 — promote in order; roll back everything already promoted
        # (plus the target whose promotion just failed) if any promotion
        # fails.
        self.log("__run__", "PROMOTING", None, None)
        promoted: list[TableTarget] = []
        for target in targets:
            try:
                count = self.backend.promote(target)
            except Exception as exc:  # noqa: BLE001
                return self._roll_back(target, exc, promoted, target_states, staged)
            promoted.append(target)
            self.log(target.name, "COMPLETED", count, None)

        cleanup_failures = self._cleanup_all(staged)
        if cleanup_failures:
            return self._completed_with_cleanup_failure(promoted, cleanup_failures)
        self.log("__run__", "COMPLETED", len(promoted), None)
        return PublicationOutcome(state=COMPLETED, promoted=[t.name for t in promoted])

    def _fail(
        self, target: TableTarget, exc: Exception, staged: list[TableTarget]
    ) -> PublicationOutcome:
        error = bound_error(exc)
        self.log(target.name, "FAILED", None, error)
        self.log("__run__", "FAILED", None, error)
        # Evidence (the STAGED/FAILED rows above) is logged before cleanup
        # runs. No data was touched, so the run stays FAILED regardless of
        # whether cleanup itself succeeds — but a cleanup failure is still
        # logged (not silently swallowed) and its staging left in place.
        cleanup_failures = self._cleanup_all(staged)
        preserved = [name for name, _ in cleanup_failures]
        if cleanup_failures:
            failure_text = "; ".join(f"{name}: {err}" for name, err in cleanup_failures)
            self.log(
                "__run__", "FAILED", None,
                f"staging cleanup also failed: {failure_text}"[:ERROR_TEXT_LIMIT],
            )
        return PublicationOutcome(
            state=FAILED,
            error=error,
            cleanup_failures=cleanup_failures,
            preserved_staging=preserved,
        )

    def _roll_back(
        self,
        failed_target: TableTarget,
        exc: Exception,
        promoted: list[TableTarget],
        target_states: dict[TableTarget, TargetState],
        staged: list[TableTarget],
    ) -> PublicationOutcome:
        error = bound_error(exc)
        self.log(failed_target.name, "FAILED", None, error)
        self.log("__run__", "ROLLING_BACK", None, error)

        # Roll back everything already promoted *and* the target whose
        # promote() call just raised: a Delta command can commit before the
        # client observes a network/driver error, so the failed target may
        # have partially or fully landed. Restoring/dropping it anyway is
        # safe and idempotent (RESTORE to the pre-run version is a no-op if
        # nothing committed; DROP TABLE IF EXISTS is a no-op if it was never
        # created) and keeps rollback fail-closed instead of trusting the
        # exception.
        attempted = [*promoted, failed_target]
        failures = self._attempt_rollback(attempted, target_states)

        if failures:
            preserved = [t.staging_name for t in staged]
            names = ", ".join(preserved)
            failure_text = "; ".join(f"{name}: {err}" for name, err in failures)
            self.log(
                "__run__", ROLLBACK_FAILED, None,
                f"{error}; rollback failures: {failure_text}; "
                f"staging preserved for manual recovery: {names}"[:ERROR_TEXT_LIMIT],
            )
            return PublicationOutcome(
                state=ROLLBACK_FAILED,
                error=error,
                rollback_failures=failures,
                preserved_staging=preserved,
            )

        cleanup_failures = self._cleanup_all(staged)
        if cleanup_failures:
            return self._rolled_back_with_cleanup_failure(error, cleanup_failures)
        self.log("__run__", ROLLED_BACK, None, error)
        return PublicationOutcome(state=ROLLED_BACK, error=error)

    def _attempt_rollback(
        self, targets: list[TableTarget], target_states: dict[TableTarget, TargetState]
    ) -> list[tuple[str, str]]:
        """Undo every given target in reverse (attempted) order.

        ``targets`` is every target that was promoted *plus* the one whose
        promotion just failed (see ``_roll_back``); continues past a
        restore/drop failure so one bad target can't stop the rest from
        being rolled back.
        """
        failures: list[tuple[str, str]] = []
        for target in reversed(targets):
            state = target_states[target]
            try:
                if state.existed:
                    self.backend.restore(target, state)
                else:
                    self.backend.drop(target)
                self.log(target.name, ROLLED_BACK, None, None)
            except Exception as exc:  # noqa: BLE001 — keep rolling back the rest
                error = bound_error(exc)
                failures.append((target.name, error))
                self.log(target.name, ROLLBACK_FAILED, None, error)
        return failures

    def _cleanup_all(self, staged: list[TableTarget]) -> list[tuple[str, str]]:
        """Best-effort cleanup of every staged artifact; never raises.

        Attempts every staged target even if one fails, and returns the
        failures instead of raising: an unhandled exception here would blow
        past an already-successful promotion or rollback and could mask it
        (the caller would never see the terminal log/outcome it earned).
        """
        failures: list[tuple[str, str]] = []
        for target in staged:
            try:
                self.backend.cleanup(target)
            except Exception as exc:  # noqa: BLE001 — collected, not swallowed
                failures.append((target.staging_name, bound_error(exc)))
        return failures

    def _completed_with_cleanup_failure(
        self, promoted: list[TableTarget], cleanup_failures: list[tuple[str, str]]
    ) -> PublicationOutcome:
        """Promotion fully succeeded, but staging cleanup did not.

        Never reported as plain COMPLETED (that would hide leftover
        staging) and never conflated with a data failure — the message is
        explicit that the promoted data is fine and only cleanup needs
        manual attention.
        """
        preserved = [name for name, _ in cleanup_failures]
        failure_text = "; ".join(f"{name}: {err}" for name, err in cleanup_failures)
        message = (
            f"data promoted successfully; staging cleanup failed: {failure_text}"
        )[:ERROR_TEXT_LIMIT]
        self.log("__run__", COMPLETED_CLEANUP_FAILED, len(promoted), message)
        return PublicationOutcome(
            state=COMPLETED_CLEANUP_FAILED,
            promoted=[t.name for t in promoted],
            error=message,
            cleanup_failures=cleanup_failures,
            preserved_staging=preserved,
        )

    def _rolled_back_with_cleanup_failure(
        self, error: str, cleanup_failures: list[tuple[str, str]]
    ) -> PublicationOutcome:
        """Rollback fully restored/dropped every target, but cleanup did not.

        Never reported as plain ROLLED_BACK (that would hide leftover
        staging) and never mislabeled as a data problem — the message states
        the rollback itself succeeded and only cleanup needs manual
        attention.
        """
        preserved = [name for name, _ in cleanup_failures]
        failure_text = "; ".join(f"{name}: {err}" for name, err in cleanup_failures)
        message = (
            f"{error}; data rolled back successfully; staging cleanup failed: "
            f"{failure_text}"
        )[:ERROR_TEXT_LIMIT]
        self.log("__run__", ROLLED_BACK_CLEANUP_FAILED, None, message)
        return PublicationOutcome(
            state=ROLLED_BACK_CLEANUP_FAILED,
            error=message,
            cleanup_failures=cleanup_failures,
            preserved_staging=preserved,
        )
