"""Failure-safety contracts for streaming Gold publication."""

from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "fabric" / "lakehouse" / "04-streaming-to-gold.ipynb"


def _code() -> str:
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )


def _function(source: str, name: str) -> str:
    tree = ast.parse(source)
    node = next(
        item
        for item in tree.body
        if isinstance(item, ast.FunctionDef) and item.name == name
    )
    segment = ast.get_source_segment(source, node)
    assert segment is not None
    return segment


def _assignment(source: str, name: str):
    tree = ast.parse(source)
    node = next(
        item
        for item in tree.body
        if isinstance(item, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == name
            for target in item.targets
        )
    )
    return ast.literal_eval(node.value)


def _load_promote_gold(globals_: dict):
    source = _code()
    tree = ast.parse(source)
    node = next(
        item
        for item in tree.body
        if isinstance(item, ast.FunctionDef) and item.name == "promote_gold"
    )
    module = ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[]))
    exec(compile(module, "<promote_gold>", "exec"), globals_)
    return globals_["promote_gold"]


class _FakeWriter:
    def __init__(self, target_calls: list[str], fail_target: str | None) -> None:
        self.target_calls = target_calls
        self.fail_target = fail_target

    def format(self, _format: str):
        return self

    def mode(self, _mode: str):
        return self

    def option(self, _key: str, _value: str):
        return self

    def saveAsTable(self, target: str) -> None:  # noqa: N802 - mirrors PySpark API
        self.target_calls.append(target)
        if target == self.fail_target:
            raise RuntimeError(f"failed promotion: {target}")


class _FakeFrame:
    def __init__(self, target_calls: list[str], fail_target: str | None) -> None:
        self.write = _FakeWriter(target_calls, fail_target)


class _FakeSpark:
    def __init__(
        self,
        *,
        fail_target: str | None = None,
        fail_rollback_target: str | None = None,
    ) -> None:
        self.fail_target = fail_target
        self.fail_rollback_target = fail_rollback_target
        self.target_calls: list[str] = []
        self.sql_calls: list[str] = []

    def table(self, _stage: str) -> _FakeFrame:
        return _FakeFrame(self.target_calls, self.fail_target)

    def sql(self, command: str) -> None:
        self.sql_calls.append(command)
        if self.fail_rollback_target and self.fail_rollback_target in command:
            raise RuntimeError(f"failed rollback: {command}")


def _promotion_fixture(
    *,
    fail_target: str | None = None,
    fail_rollback_target: str | None = None,
    cleanup_errors: list[str] | None = None,
    missing_stage: str | None = None,
):
    tables = ["table_a", "table_b", "table_c"]
    staged = {name: (f"stage.{name}", 1) for name in tables}
    if missing_stage is not None:
        staged.pop(missing_stage)
    spark = _FakeSpark(
        fail_target=fail_target,
        fail_rollback_target=fail_rollback_target,
    )
    logs: list[tuple[str, int | None, str, object | None]] = []
    cleanup_calls: list[bool] = []
    versions = {
        "retail.au.table_a": 3,
        "retail.au.table_b": None,
        "retail.au.table_c": 8,
    }

    def cleanup():
        cleanup_calls.append(True)
        return list(cleanup_errors or [])

    globals_ = {
        "EXPECTED_GOLD_TABLES": tables,
        "STAGED_GOLD": staged,
        "LAKEHOUSE_NAME": "retail",
        "GOLD_DB": "au",
        "spark": spark,
        "gold_target_version": versions.__getitem__,
        "append_gold_run_log": lambda table, count, status, error=None: logs.append(
            (table, count, status, error)
        ),
        "cleanup_gold_staging": cleanup,
    }
    return _load_promote_gold(globals_), spark, logs, cleanup_calls


def test_all_gold_tables_stage_before_any_promotion() -> None:
    source = _code()
    compile(source, "<04-streaming-to-gold>", "exec")
    expected = _assignment(source, "EXPECTED_GOLD_TABLES")

    assert len(expected) == 10
    assert "campaign_performance_daily" in expected
    save_source = _function(source, "save_gold")
    assert "GOLD_STAGING_DB" in save_source
    assert "GOLD_DB" not in save_source
    assert "staged row count" in save_source
    assert "staged schema does not match source" in save_source

    promote_call = source.rindex("promote_gold()")
    first_complete_banner = source.index('print("GOLD AGGREGATIONS COMPLETE")')
    assert promote_call < first_complete_banner


def test_gold_promotion_captures_versions_and_rolls_back_every_attempt() -> None:
    source = _code()
    promote_source = _function(source, "promote_gold")

    for expected in (
        "gold_target_version(target)",
        'append_gold_run_log("__gold_run__", None, "PROMOTING")',
        "attempted.append(target)",
        'spark.sql(f"RESTORE TABLE {target} TO VERSION AS OF {version}")',
        'spark.sql(f"DROP TABLE IF EXISTS {target}")',
        "for target in reversed(attempted)",
        "rollback_errors.append",
        '"ROLLING_BACK"',
        '"ROLLED_BACK"',
        '"ROLLBACK_FAILED"',
    ):
        assert expected in promote_source

    assert promote_source.index("for target in reversed(attempted)") < (
        promote_source.index("if rollback_errors:")
    )


def test_gold_run_evidence_and_cleanup_boundaries_are_explicit() -> None:
    source = _code()
    save_source = _function(source, "save_gold")
    promote_source = _function(source, "promote_gold")

    for status in (
        "STARTED",
        "STAGED",
        "VALIDATED",
        "PROMOTING",
        "COMPLETED",
        "FAILED",
        "ROLLING_BACK",
        "ROLLED_BACK",
        "ROLLBACK_FAILED",
    ):
        assert f'"{status}"' in source

    assert "cleanup_gold_staging()" not in save_source
    assert promote_source.count("cleanup_gold_staging()") == 3


def test_gold_promotion_success_promotes_every_table_and_cleans_staging() -> None:
    promote, spark, logs, cleanup = _promotion_fixture()

    promote()

    assert spark.target_calls == [
        "retail.au.table_a",
        "retail.au.table_b",
        "retail.au.table_c",
    ]
    assert cleanup == [True]
    assert logs[-1][:3] == ("__gold_run__", 3, "COMPLETED")


def test_incomplete_gold_staging_cleans_ready_tables_before_failing() -> None:
    promote, spark, logs, cleanup = _promotion_fixture(missing_stage="table_c")

    try:
        promote()
    except RuntimeError as exc:
        assert "staging incomplete" in str(exc)
    else:
        raise AssertionError("incomplete staging should fail")

    assert spark.target_calls == []
    assert cleanup == [True]
    assert logs[-1][2] == "FAILED"


def test_nth_gold_promotion_failure_restores_and_drops_attempted_targets() -> None:
    promote, spark, logs, cleanup = _promotion_fixture(fail_target="retail.au.table_b")

    try:
        promote()
    except RuntimeError as exc:
        assert "failed promotion" in str(exc)
    else:
        raise AssertionError("promotion should fail")

    assert spark.target_calls == ["retail.au.table_a", "retail.au.table_b"]
    assert spark.sql_calls == [
        "DROP TABLE IF EXISTS retail.au.table_b",
        "RESTORE TABLE retail.au.table_a TO VERSION AS OF 3",
    ]
    assert cleanup == [True]
    assert any(log[2] == "ROLLING_BACK" for log in logs)
    assert any(log[2] == "ROLLED_BACK" for log in logs)


def test_gold_rollback_continues_and_preserves_staging_on_restore_failure() -> None:
    promote, spark, logs, cleanup = _promotion_fixture(
        fail_target="retail.au.table_c",
        fail_rollback_target="retail.au.table_a",
    )

    try:
        promote()
    except RuntimeError as exc:
        assert "rollback was incomplete" in str(exc)
    else:
        raise AssertionError("rollback failure should be surfaced")

    assert spark.sql_calls == [
        "RESTORE TABLE retail.au.table_c TO VERSION AS OF 8",
        "DROP TABLE IF EXISTS retail.au.table_b",
        "RESTORE TABLE retail.au.table_a TO VERSION AS OF 3",
    ]
    assert cleanup == []
    assert logs[-1][2] == "ROLLBACK_FAILED"


def test_gold_cleanup_failure_prevents_completed_status() -> None:
    promote, spark, logs, cleanup = _promotion_fixture(
        cleanup_errors=["stage.table_b: drop failed"]
    )

    try:
        promote()
    except RuntimeError as exc:
        assert "staging cleanup failed" in str(exc)
    else:
        raise AssertionError("cleanup failure should be surfaced")

    assert len(spark.target_calls) == 3
    assert cleanup == [True]
    assert logs[-1][2] == "FAILED"
    assert not any(
        table == "__gold_run__" and status == "COMPLETED"
        for table, _count, status, _error in logs
    )
