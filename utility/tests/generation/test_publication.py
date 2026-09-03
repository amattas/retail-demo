"""Pure-Python tests for the stage -> validate -> promote -> rollback
coordinator (no Spark/Delta/Fabric dependency — see publication.py's
module docstring for why).
"""

import pytest

from retail_setup.generation.publication import (
    COMPLETED,
    COMPLETED_CLEANUP_FAILED,
    FAILED,
    ROLLBACK_FAILED,
    ROLLED_BACK,
    ROLLED_BACK_CLEANUP_FAILED,
    PublicationCoordinator,
    TableTarget,
    TargetState,
)


class FakeBackend:
    """In-memory PublicationBackend recording every call for assertions."""

    def __init__(
        self,
        *,
        existing: frozenset[str] = frozenset(),
        row_counts: dict[str, int] | None = None,
        fail_stage: frozenset[str] = frozenset(),
        fail_validate: frozenset[str] = frozenset(),
        fail_promote: frozenset[str] = frozenset(),
        fail_restore: frozenset[str] = frozenset(),
        fail_drop: frozenset[str] = frozenset(),
        fail_cleanup: frozenset[str] = frozenset(),
    ) -> None:
        self.existing = existing
        self.row_counts = row_counts or {}
        self.fail_stage = fail_stage
        self.fail_validate = fail_validate
        self.fail_promote = fail_promote
        self.fail_restore = fail_restore
        self.fail_drop = fail_drop
        self.fail_cleanup = fail_cleanup
        self.calls: list[tuple[str, str]] = []
        self.promoted: list[str] = []
        self.restored: list[tuple[str, object]] = []
        self.dropped: list[str] = []
        self.cleaned: list[str] = []

    def _count(self, target: TableTarget) -> int:
        return self.row_counts.get(target.name, 10)

    def stage(self, target: TableTarget) -> int:
        self.calls.append(("stage", target.name))
        if target.name in self.fail_stage:
            raise RuntimeError(f"stage failed: {target.name}")
        return self._count(target)

    def validate(self, target: TableTarget, staged_row_count: int) -> None:
        self.calls.append(("validate", target.name))
        if target.name in self.fail_validate:
            raise ValueError(f"validate failed: {target.name}")

    def target_state(self, target: TableTarget) -> TargetState:
        self.calls.append(("target_state", target.name))
        if target.name in self.existing:
            return TargetState(existed=True, restore_token=7)
        return TargetState(existed=False)

    def promote(self, target: TableTarget) -> int:
        self.calls.append(("promote", target.name))
        if target.name in self.fail_promote:
            raise RuntimeError(f"promote failed: {target.name}")
        self.promoted.append(target.name)
        return self._count(target)

    def restore(self, target: TableTarget, state: TargetState) -> None:
        self.calls.append(("restore", target.name))
        if target.name in self.fail_restore:
            raise RuntimeError(f"restore failed: {target.name}")
        self.restored.append((target.name, state.restore_token))

    def drop(self, target: TableTarget) -> None:
        self.calls.append(("drop", target.name))
        if target.name in self.fail_drop:
            raise RuntimeError(f"drop failed: {target.name}")
        self.dropped.append(target.name)

    def cleanup(self, target: TableTarget) -> None:
        self.calls.append(("cleanup", target.name))
        if target.name in self.fail_cleanup:
            raise RuntimeError(f"cleanup failed: {target.name}")
        self.cleaned.append(target.name)


class LogRecorder:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str, int | None, str | None]] = []

    def __call__(self, table_name: str, status: str, row_count, error) -> None:
        self.entries.append((table_name, status, row_count, error))


def _targets(*names: str) -> list[TableTarget]:
    return [TableTarget(name=n, db="db", staging_name=f"stg_{n}") for n in names]


def test_success_promotes_all_and_logs_exact_sequence():
    backend = FakeBackend()
    log = LogRecorder()
    targets = _targets("a", "b", "c")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == COMPLETED
    assert outcome.ok
    assert outcome.promoted == ["a", "b", "c"]
    assert backend.promoted == ["a", "b", "c"]
    assert backend.cleaned == ["a", "b", "c"]
    assert log.entries == [
        ("__run__", "STARTED", None, None),
        ("a", "STAGED", 10, None),
        ("b", "STAGED", 10, None),
        ("c", "STAGED", 10, None),
        ("__run__", "VALIDATED", None, None),
        ("__run__", "PROMOTING", None, None),
        ("a", "COMPLETED", 10, None),
        ("b", "COMPLETED", 10, None),
        ("c", "COMPLETED", 10, None),
        ("__run__", "COMPLETED", 3, None),
    ]


def test_stage_failure_leaves_targets_untouched():
    backend = FakeBackend(fail_stage=frozenset({"b"}))
    log = LogRecorder()
    targets = _targets("a", "b", "c")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == FAILED
    assert not outcome.ok
    # c is never even attempted; no target/promote/restore/drop calls at all.
    assert [op for op, _ in backend.calls if op not in ("stage", "cleanup")] == []
    assert backend.promoted == []
    assert backend.dropped == []
    assert backend.restored == []
    # only "a" made it into staging before "b" failed -> only "a" is cleaned up.
    assert backend.cleaned == ["a"]
    assert log.entries == [
        ("__run__", "STARTED", None, None),
        ("a", "STAGED", 10, None),
        ("b", "FAILED", None, "RuntimeError: stage failed: b"),
        ("__run__", "FAILED", None, "RuntimeError: stage failed: b"),
    ]


def test_validation_failure_leaves_targets_untouched():
    backend = FakeBackend(fail_validate=frozenset({"b"}))
    log = LogRecorder()
    targets = _targets("a", "b", "c")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == FAILED
    # every candidate staged fine; validation for "b" fails before any promote.
    assert [op for op, _ in backend.calls if op in ("target_state", "promote", "restore", "drop")] == []
    assert backend.promoted == []
    # all three were staged, so all three get cleaned up (evidence already logged).
    assert set(backend.cleaned) == {"a", "b", "c"}
    assert log.entries == [
        ("__run__", "STARTED", None, None),
        ("a", "STAGED", 10, None),
        ("b", "STAGED", 10, None),
        ("c", "STAGED", 10, None),
        ("b", "FAILED", None, "ValueError: validate failed: b"),
        ("__run__", "FAILED", None, "ValueError: validate failed: b"),
    ]


def test_nth_promotion_restores_prior_existing_and_drops_new():
    # "a" pre-existed before this run; "b" and the failed target "c" are
    # both newly created by this run.
    backend = FakeBackend(existing=frozenset({"a"}), fail_promote=frozenset({"c"}))
    log = LogRecorder()
    targets = _targets("a", "b", "c")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == ROLLED_BACK
    assert not outcome.ok
    assert outcome.rollback_failures == []
    assert backend.promoted == ["a", "b"]
    # rollback undoes in reverse attempted order (c, b, a): "c" itself is
    # included even though its promote() raised, because a Delta command can
    # commit before the client observes the error — dropping it anyway is a
    # safe no-op if it never actually committed. "b" (new) is also dropped;
    # "a" (pre-existing) is restored.
    assert backend.dropped == ["c", "b"]
    assert backend.restored == [("a", 7)]
    # rollback succeeded cleanly -> staging is cleaned up for every staged table.
    assert set(backend.cleaned) == {"a", "b", "c"}
    assert log.entries == [
        ("__run__", "STARTED", None, None),
        ("a", "STAGED", 10, None),
        ("b", "STAGED", 10, None),
        ("c", "STAGED", 10, None),
        ("__run__", "VALIDATED", None, None),
        ("__run__", "PROMOTING", None, None),
        ("a", "COMPLETED", 10, None),
        ("b", "COMPLETED", 10, None),
        ("c", "FAILED", None, "RuntimeError: promote failed: c"),
        ("__run__", "ROLLING_BACK", None, "RuntimeError: promote failed: c"),
        ("c", "ROLLED_BACK", None, None),
        ("b", "ROLLED_BACK", None, None),
        ("a", "ROLLED_BACK", None, None),
        ("__run__", "ROLLED_BACK", None, "RuntimeError: promote failed: c"),
    ]


def test_rollback_continues_after_one_restore_error_and_returns_rollback_failed():
    # both "a" and "b" pre-existed; restoring "b" will fail during rollback.
    # "c" (the failed promotion target) did not pre-exist, so it is dropped.
    backend = FakeBackend(
        existing=frozenset({"a", "b"}),
        fail_promote=frozenset({"c"}),
        fail_restore=frozenset({"b"}),
    )
    log = LogRecorder()
    targets = _targets("a", "b", "c")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == ROLLBACK_FAILED
    assert not outcome.ok
    assert outcome.rollback_failures == [("b", "RuntimeError: restore failed: b")]
    # rollback kept going after the "b" restore failure: "c" was dropped
    # first (reverse attempted order) and "a" was still restored afterward.
    assert backend.dropped == ["c"]
    assert backend.restored == [("a", 7)]
    # never call success after a rollback failure, and never clean up staging —
    # it is preserved for manual recovery.
    assert backend.cleaned == []
    assert set(outcome.preserved_staging) == {"stg_a", "stg_b", "stg_c"}
    assert log.entries[:9] == [
        ("__run__", "STARTED", None, None),
        ("a", "STAGED", 10, None),
        ("b", "STAGED", 10, None),
        ("c", "STAGED", 10, None),
        ("__run__", "VALIDATED", None, None),
        ("__run__", "PROMOTING", None, None),
        ("a", "COMPLETED", 10, None),
        ("b", "COMPLETED", 10, None),
        ("c", "FAILED", None, "RuntimeError: promote failed: c"),
    ]
    assert log.entries[9] == ("__run__", "ROLLING_BACK", None, "RuntimeError: promote failed: c")
    assert log.entries[10] == ("c", "ROLLED_BACK", None, None)
    assert log.entries[11] == ("b", "ROLLBACK_FAILED", None, "RuntimeError: restore failed: b")
    assert log.entries[12] == ("a", "ROLLED_BACK", None, None)
    final_table, final_status, final_count, final_error = log.entries[13]
    assert (final_table, final_status, final_count) == ("__run__", "ROLLBACK_FAILED", None)
    assert "restore failed: b" in final_error
    assert "staging preserved for manual recovery" in final_error
    assert "stg_a" in final_error and "stg_b" in final_error and "stg_c" in final_error
    assert len(log.entries) == 14


def test_first_promotion_failure_restores_or_drops_the_failed_target_itself():
    # "a" is the very first (and only attempted) promotion, and it fails.
    # Even though nothing "already promoted" exists, "a" itself must still
    # be rolled back (dropped, since it didn't pre-exist) — a Delta command
    # can commit before the client observes the error.
    backend = FakeBackend(fail_promote=frozenset({"a"}))
    log = LogRecorder()
    targets = _targets("a", "b", "c")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == ROLLED_BACK
    assert backend.promoted == []
    assert backend.restored == []
    assert backend.dropped == ["a"]
    # b, c never attempted once a's promotion fails.
    assert [name for op, name in backend.calls if op == "promote"] == ["a"]
    assert set(backend.cleaned) == {"a", "b", "c"}


def test_first_promotion_failure_restores_pre_existing_failed_target():
    # "a" already existed before this run and its promotion fails; it must
    # be restored (not dropped) even though it was never appended to
    # "promoted" (the promote() call itself raised).
    backend = FakeBackend(existing=frozenset({"a"}), fail_promote=frozenset({"a"}))
    log = LogRecorder()
    targets = _targets("a")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == ROLLED_BACK
    assert backend.promoted == []
    assert backend.restored == [("a", 7)]
    assert backend.dropped == []


def test_error_text_is_bounded():
    class HugeError(RuntimeError):
        pass

    backend = FakeBackend(fail_stage=frozenset({"a"}))

    def _raise_huge(target):
        raise HugeError("x" * 5000)

    backend.stage = _raise_huge  # type: ignore[method-assign]
    log = LogRecorder()
    targets = _targets("a")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == FAILED
    assert outcome.error is not None
    assert len(outcome.error) <= 1000
    assert all(len(entry[3]) <= 1000 for entry in log.entries if entry[3] is not None)


def test_duplicate_target_states_captured_before_any_promotion():
    # target_state must be captured for every target before promote() is
    # called for any of them (order recorded via calls list).
    backend = FakeBackend()
    log = LogRecorder()
    targets = _targets("a", "b")

    PublicationCoordinator(backend, log).publish(targets)

    state_calls = [name for op, name in backend.calls if op == "target_state"]
    promote_calls = [name for op, name in backend.calls if op == "promote"]
    assert state_calls == ["a", "b"]
    assert promote_calls == ["a", "b"]
    # both target_state calls happen before the first promote call.
    first_promote_index = backend.calls.index(("promote", "a"))
    last_state_index = max(
        i for i, (op, _) in enumerate(backend.calls) if op == "target_state"
    )
    assert last_state_index < first_promote_index


@pytest.mark.parametrize("bad_kwargs", [{"fail_stage": frozenset({"z"})}])
def test_no_targets_still_starts_and_completes(bad_kwargs):
    # An empty target list is a degenerate but valid "nothing to publish" run.
    backend = FakeBackend(**bad_kwargs)
    log = LogRecorder()

    outcome = PublicationCoordinator(backend, log).publish([])

    assert outcome.state == COMPLETED
    assert outcome.promoted == []
    assert log.entries == [
        ("__run__", "STARTED", None, None),
        ("__run__", "VALIDATED", None, None),
        ("__run__", "PROMOTING", None, None),
        ("__run__", "COMPLETED", 0, None),
    ]


def test_same_name_targets_in_different_dbs_do_not_collide_on_validate():
    # A silver and a gold table can legitimately share a bare name. Keying
    # staged_counts by name alone would let the gold "dup"'s staged count
    # silently overwrite the silver "dup"'s before either is validated.
    class DualCountBackend:
        def __init__(self) -> None:
            self.validate_seen: list[tuple[str, int]] = []

        def stage(self, target: TableTarget) -> int:
            return 5 if target.db == "silver" else 8

        def validate(self, target: TableTarget, staged_row_count: int) -> None:
            self.validate_seen.append((target.db, staged_row_count))

        def target_state(self, target: TableTarget) -> TargetState:
            return TargetState(existed=False)

        def promote(self, target: TableTarget) -> int:
            return 5 if target.db == "silver" else 8

        def restore(self, target: TableTarget, state: TargetState) -> None:
            raise AssertionError("nothing pre-existed; restore should not be called")

        def drop(self, target: TableTarget) -> None:
            pass

        def cleanup(self, target: TableTarget) -> None:
            pass

    backend = DualCountBackend()
    log = LogRecorder()
    targets = [
        TableTarget(name="dup", db="silver", staging_name="stg_silver_dup"),
        TableTarget(name="dup", db="gold", staging_name="stg_gold_dup"),
    ]

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.ok
    assert sorted(backend.validate_seen) == [("gold", 8), ("silver", 5)]


def test_same_name_targets_target_state_not_shared_across_db_during_rollback():
    # Silver "dup" existed before this run (restore token 1); gold "dup" is
    # new (must be dropped). A third target's promotion fails, forcing
    # rollback of both "dup"s; restore/drop must resolve per (db, name), not
    # by bare name alone, or one "dup"'s pre-existence would shadow the
    # other's during target_state lookup.
    class PerDbBackend:
        def __init__(self) -> None:
            self.restored: list[tuple[str, str, object]] = []
            self.dropped: list[tuple[str, str]] = []

        def stage(self, target: TableTarget) -> int:
            return 1

        def validate(self, target: TableTarget, staged_row_count: int) -> None:
            pass

        def target_state(self, target: TableTarget) -> TargetState:
            if target.db == "silver":
                return TargetState(existed=True, restore_token=1)
            return TargetState(existed=False)

        def promote(self, target: TableTarget) -> int:
            if target.name == "trigger":
                raise RuntimeError("boom")
            return 1

        def restore(self, target: TableTarget, state: TargetState) -> None:
            self.restored.append((target.db, target.name, state.restore_token))

        def drop(self, target: TableTarget) -> None:
            self.dropped.append((target.db, target.name))

        def cleanup(self, target: TableTarget) -> None:
            pass

    backend = PerDbBackend()
    log = LogRecorder()
    targets = [
        TableTarget(name="dup", db="silver", staging_name="s1"),
        TableTarget(name="dup", db="gold", staging_name="s2"),
        TableTarget(name="trigger", db="gold", staging_name="s3"),
    ]

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == ROLLED_BACK
    # only the silver "dup" (existed=True) is restored...
    assert backend.restored == [("silver", "dup", 1)]
    # ...the gold "dup" (existed=False) and the failed "trigger" (also
    # existed=False) are both dropped, in reverse attempted order.
    assert backend.dropped == [("gold", "trigger"), ("gold", "dup")]


def test_cleanup_failure_after_success_is_not_reported_as_completed():
    backend = FakeBackend(fail_cleanup=frozenset({"b"}))
    log = LogRecorder()
    targets = _targets("a", "b", "c")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == COMPLETED_CLEANUP_FAILED
    assert not outcome.ok
    # the promotion itself fully succeeded — the cleanup failure never
    # touches the data outcome.
    assert outcome.promoted == ["a", "b", "c"]
    assert backend.promoted == ["a", "b", "c"]
    # cleanup is attempted for every staged table, not just up to the failure.
    assert set(backend.cleaned) == {"a", "c"}
    assert outcome.cleanup_failures == [("stg_b", "RuntimeError: cleanup failed: b")]
    assert outcome.preserved_staging == ["stg_b"]
    assert outcome.error is not None
    assert "data promoted successfully" in outcome.error
    assert "stg_b" in outcome.error
    assert len(outcome.error) <= 1000
    assert log.entries[-1] == (
        "__run__",
        COMPLETED_CLEANUP_FAILED,
        3,
        outcome.error,
    )


def test_cleanup_failure_after_successful_rollback_is_not_data_corruption():
    backend = FakeBackend(
        existing=frozenset({"a"}),
        fail_promote=frozenset({"c"}),
        fail_cleanup=frozenset({"b"}),
    )
    log = LogRecorder()
    targets = _targets("a", "b", "c")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == ROLLED_BACK_CLEANUP_FAILED
    assert not outcome.ok
    # the rollback itself fully succeeded (no restore/drop failures) — only
    # staging cleanup failed, and that must never be conflated with a data
    # problem such as ROLLBACK_FAILED.
    assert outcome.rollback_failures == []
    assert backend.restored == [("a", 7)]
    assert set(backend.dropped) == {"b", "c"}
    # cleanup is attempted for every staged table, not just up to the failure.
    assert set(backend.cleaned) == {"a", "c"}
    assert outcome.cleanup_failures == [("stg_b", "RuntimeError: cleanup failed: b")]
    assert outcome.preserved_staging == ["stg_b"]
    assert outcome.error is not None
    assert "data rolled back successfully" in outcome.error
    assert "stg_b" in outcome.error
    assert len(outcome.error) <= 1000
    assert log.entries[-1] == (
        "__run__",
        ROLLED_BACK_CLEANUP_FAILED,
        None,
        outcome.error,
    )


def test_cleanup_failure_after_stage_failure_still_reports_failed():
    # Cleanup failing after a stage/validate failure must not change the
    # already-correct FAILED outcome (no data was ever touched) — it just
    # needs to surface the residual staging instead of raising unhandled.
    backend = FakeBackend(fail_stage=frozenset({"b"}), fail_cleanup=frozenset({"a"}))
    log = LogRecorder()
    targets = _targets("a", "b", "c")

    outcome = PublicationCoordinator(backend, log).publish(targets)

    assert outcome.state == FAILED
    assert not outcome.ok
    assert outcome.cleanup_failures == [("stg_a", "RuntimeError: cleanup failed: a")]
    assert outcome.preserved_staging == ["stg_a"]
    assert log.entries[-1][:2] == ("__run__", "FAILED")
    assert "cleanup also failed" in log.entries[-1][3]
    assert "stg_a" in log.entries[-1][3]
