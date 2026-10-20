"""Executable correctness contracts for the Eventhouse-to-Silver notebook."""

from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SILVER_NOTEBOOK = ROOT / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb"
SCHEMAS = ROOT / "utility" / "src" / "retail_setup" / "generation" / "schemas.py"


def _notebook_code() -> str:
    notebook = json.loads(SILVER_NOTEBOOK.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )


def _tree() -> ast.Module:
    return ast.parse(_notebook_code(), filename=str(SILVER_NOTEBOOK))


def _function(tree: ast.Module, name: str) -> ast.FunctionDef:
    function = next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == name
        ),
        None,
    )
    assert function is not None, f"missing function: {name}"
    return function


def _assignment(tree: ast.Module, name: str) -> ast.expr:
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            return node.value
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
        ):
            assert node.value is not None
            return node.value
    raise AssertionError(f"missing assignment: {name}")


def _literal(name: str) -> Any:
    return ast.literal_eval(_assignment(_tree(), name))


def _exec_functions(
    *names: str,
    extra_globals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tree = _tree()
    nodes = [_function(tree, name) for name in names]
    namespace: dict[str, Any] = {
        "datetime": datetime,
        "timedelta": timedelta,
        "timezone": timezone,
        "REPLAY_OVERLAP_SECONDS": _literal("REPLAY_OVERLAP_SECONDS"),
        "DEEP_RECONCILIATION_INTERVAL_SECONDS": _literal(
            "DEEP_RECONCILIATION_INTERVAL_SECONDS"
        ),
    }
    namespace.update(extra_globals or {})
    exec(
        compile(
            ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])),
            str(SILVER_NOTEBOOK),
            "exec",
        ),
        namespace,
    )
    return namespace


@dataclass(frozen=True, eq=False)
class _Expression:
    operation: str
    arguments: tuple[Any, ...]

    def __eq__(self, other: object) -> _Expression:  # type: ignore[override]
        return _Expression("==", (self, other))

    def __ge__(self, other: Any) -> _Expression:
        return _Expression(">=", (self, other))

    def __lt__(self, other: Any) -> _Expression:
        return _Expression("<", (self, other))

    def __gt__(self, other: Any) -> _Expression:
        return _Expression(">", (self, other))

    def __and__(self, other: Any) -> _Expression:
        return _Expression("and", (self, other))

    def __or__(self, other: Any) -> _Expression:
        return _Expression("or", (self, other))

    def __invert__(self) -> _Expression:
        return _Expression("not", (self,))

    def __add__(self, other: Any) -> _Expression:
        return _Expression("+", (self, other))

    def __neg__(self) -> _Expression:
        return _Expression("negate", (self,))

    def otherwise(self, other: Any) -> _Expression:
        return _Expression("otherwise", (self, other))

    def alias(self, name: str) -> _Expression:
        return _Expression("alias", (self, name))

    def asc(self) -> _Expression:
        return _Expression("asc", (self,))

    def cast(self, data_type: str) -> _Expression:
        return _Expression("cast", (self, data_type))

    def desc(self) -> _Expression:
        return _Expression("desc", (self,))

    def isNotNull(self) -> _Expression:  # noqa: N802 - mirrors PySpark
        return _Expression("isNotNull", (self,))

    def isNull(self) -> _Expression:  # noqa: N802 - mirrors PySpark
        return _Expression("isNull", (self,))

    def over(self, window: _WindowSpec) -> _Expression:
        return _Expression("over", (self, window))


@dataclass(frozen=True)
class _WindowSpec:
    operations: tuple[tuple[Any, ...], ...]

    def orderBy(self, *columns: Any) -> _WindowSpec:  # noqa: N802 - PySpark API
        return _WindowSpec((*self.operations, ("orderBy", columns)))

    def rowsBetween(self, start: Any, end: Any) -> _WindowSpec:  # noqa: N802
        return _WindowSpec((*self.operations, ("rowsBetween", start, end)))


class _RecordingWindow:
    unboundedPreceding = "unboundedPreceding"  # noqa: N815 - PySpark API
    currentRow = "currentRow"  # noqa: N815 - PySpark API

    @staticmethod
    def partitionBy(*columns: str) -> _WindowSpec:  # noqa: N802 - PySpark API
        return _WindowSpec((("partitionBy", columns),))


@dataclass(frozen=True)
class _Broadcast:
    frame: _RecordingFrame


def _column_name(column: Any) -> str | None:
    if isinstance(column, str):
        return column.rsplit(".", 1)[-1]
    if not isinstance(column, _Expression):
        return None
    if column.operation == "alias":
        return str(column.arguments[1])
    if column.operation == "column":
        return str(column.arguments[0]).rsplit(".", 1)[-1]
    if column.operation in {"asc", "cast", "desc"}:
        return _column_name(column.arguments[0])
    return None


@dataclass(frozen=True, eq=False)
class _RecordingFrame:
    name: str
    operations: tuple[tuple[Any, ...], ...] = ()
    columns: tuple[str, ...] = ()
    count_result: int = 0
    action_log: list[tuple[str, Any]] | None = None
    max_timestamp: datetime | None = None

    def _record(
        self,
        *operation: Any,
        columns: tuple[str, ...] | None = None,
    ) -> _RecordingFrame:
        return _RecordingFrame(
            self.name,
            (*self.operations, operation),
            self.columns if columns is None else columns,
            self.count_result,
            self.action_log,
            self.max_timestamp,
        )

    def filter(self, predicate: _Expression) -> _RecordingFrame:
        return self._record("filter", predicate)

    def select(self, *columns: Any) -> _RecordingFrame:
        selected = tuple(
            name for column in columns if (name := _column_name(column)) is not None
        )
        return self._record("select", columns, columns=selected)

    def join(
        self,
        other: _RecordingFrame | _Broadcast,
        keys: Any,
        how: str,
    ) -> _RecordingFrame:
        normalized_keys = tuple(keys) if isinstance(keys, list) else keys
        other_frame = other.frame if isinstance(other, _Broadcast) else other
        columns = self.columns
        if how not in {"left_anti", "left_semi"}:
            columns = tuple(dict.fromkeys((*columns, *other_frame.columns)))
        return self._record("join", other, normalized_keys, how, columns=columns)

    def agg(self, *expressions: Any) -> _RecordingFrame:
        aliases = tuple(
            name
            for expression in expressions
            if (name := _column_name(expression)) is not None
        )
        return self._record(
            "agg",
            expressions,
            columns=tuple(dict.fromkeys((*self.columns, *aliases))),
        )

    def alias(self, name: str) -> _RecordingFrame:
        return self._record("alias", name)

    def count(self) -> int | _RecordingFrame:
        if self.operations and self.operations[-1][0] == "groupBy":
            return self._record(
                "groupCount",
                columns=(*self.columns, "count"),
            )
        if self.action_log is not None:
            self.action_log.append(("count", self))
        return self.count_result

    def collect(self) -> list[list[datetime | None]]:
        return [[self.max_timestamp]]

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802
        if self.action_log is not None:
            self.action_log.append(("createTempView", name))

    def distinct(self) -> _RecordingFrame:
        return self._record("distinct")

    def drop(self, *columns: str) -> _RecordingFrame:
        dropped = set(columns)
        return self._record(
            "drop",
            columns,
            columns=tuple(column for column in self.columns if column not in dropped),
        )

    def dropDuplicates(self, columns: list[str]) -> _RecordingFrame:  # noqa: N802
        return self._record("dropDuplicates", tuple(columns))

    def groupBy(self, *columns: str) -> _RecordingFrame:  # noqa: N802 - PySpark API
        return self._record("groupBy", columns, columns=tuple(columns))

    def limit(self, count: int) -> _RecordingFrame:
        return self._record("limit", count)

    def persist(self) -> _RecordingFrame:
        return self._record("persist")

    def unpersist(self) -> _RecordingFrame:
        if self.action_log is not None:
            self.action_log.append(("unpersist", self))
        return self

    def unionByName(self, other: _RecordingFrame) -> _RecordingFrame:  # noqa: N802
        timestamps = [
            timestamp
            for timestamp in (self.max_timestamp, other.max_timestamp)
            if timestamp is not None
        ]
        return _RecordingFrame(
            self.name,
            (*self.operations, ("unionByName", other)),
            self.columns,
            self.count_result + other.count_result,
            self.action_log,
            max(timestamps) if timestamps else None,
        )

    def withColumn(self, name: str, expression: _Expression) -> _RecordingFrame:  # noqa: N802
        columns = tuple(column for column in self.columns if column != name)
        return self._record(
            "withColumn",
            name,
            expression,
            columns=(*columns, name),
        )


class _RecordingFunctions:
    @staticmethod
    def when(condition: _Expression, value: Any) -> _Expression:
        return _Expression("when", (condition, value))

    @staticmethod
    def greatest(*expressions: _Expression) -> _Expression:
        return _Expression("greatest", expressions)

    @staticmethod
    def col(name: str) -> _Expression:
        return _Expression("column", (name,))

    @staticmethod
    def lit(value: Any) -> _Expression:
        return _Expression("literal", (value,))

    @staticmethod
    def broadcast(frame: _RecordingFrame) -> _Broadcast:
        return _Broadcast(frame)

    @staticmethod
    def coalesce(*expressions: _Expression) -> _Expression:
        return _Expression("coalesce", expressions)

    @staticmethod
    def max(expression: Any) -> _Expression:
        return _Expression("max", (expression,))

    @staticmethod
    def min(expression: Any) -> _Expression:
        return _Expression("min", (expression,))

    @staticmethod
    def pmod(left: _Expression, right: _Expression) -> _Expression:
        return _Expression("pmod", (left, right))

    @staticmethod
    def row_number() -> _Expression:
        return _Expression("row_number", ())

    @staticmethod
    def struct(*expressions: _Expression) -> _Expression:
        return _Expression("struct", expressions)

    @staticmethod
    def sum(expression: Any) -> _Expression:
        return _Expression("sum", (expression,))

    @staticmethod
    def to_date(expression: _Expression) -> _Expression:
        return _Expression("to_date", (expression,))

    @staticmethod
    def xxhash64(*expressions: _Expression) -> _Expression:
        return _Expression("xxhash64", expressions)


class _RecordingSpark:
    def __init__(self, tables: dict[str, _RecordingFrame] | None = None) -> None:
        self.table_calls: list[str] = []
        self.actions: list[tuple[str, Any]] = []
        self.tables = tables or {}
        self.catalog = _RecordingCatalog(self.actions)

    def table(self, name: str) -> _RecordingFrame:
        self.table_calls.append(name)
        frame = self.tables.get(name, _RecordingFrame(name))
        return _RecordingFrame(
            frame.name,
            frame.operations,
            frame.columns,
            frame.count_result,
            self.actions,
            frame.max_timestamp,
        )

    def sql(self, query: str) -> None:
        self.actions.append(("sql", query))


class _RecordingCatalog:
    def __init__(self, actions: list[tuple[str, Any]]) -> None:
        self.actions = actions

    def dropTempView(self, name: str) -> None:  # noqa: N802 - PySpark API
        self.actions.append(("dropTempView", name))


def _plan(value: Any) -> Any:
    if isinstance(value, _Expression):
        return (value.operation, tuple(_plan(item) for item in value.arguments))
    if isinstance(value, _WindowSpec):
        return ("window", tuple(_plan(operation) for operation in value.operations))
    if isinstance(value, _Broadcast):
        return ("broadcast", _plan(value.frame))
    if isinstance(value, _RecordingFrame):
        return (
            "frame",
            value.name,
            tuple(_plan(operation) for operation in value.operations),
        )
    if isinstance(value, (list, tuple)):
        return tuple(_plan(item) for item in value)
    return value


def _historical_tables() -> dict[str, list[tuple[str, str]]]:
    tree = ast.parse(SCHEMAS.read_text(encoding="utf-8"), filename=str(SCHEMAS))
    return ast.literal_eval(_assignment(tree, "TABLES"))


def _source(name: str) -> str:
    code = _notebook_code()
    source = ast.get_source_segment(code, _function(ast.parse(code), name))
    assert source is not None
    return source


def test_replay_and_deep_reconciliation_configuration_defaults_are_explicit() -> None:
    notebook = json.loads(SILVER_NOTEBOOK.read_text(encoding="utf-8"))
    code = _notebook_code()

    assert _literal("REPLAY_OVERLAP_SECONDS") == 3 * 60 * 60
    assert _literal("DEEP_RECONCILIATION_INTERVAL_SECONDS") == 24 * 60 * 60
    parameter_cell = next(
        cell
        for cell in notebook["cells"]
        if "parameters" in cell.get("metadata", {}).get("tags", [])
    )
    parameter_source = "".join(parameter_cell["source"])
    assert "REPLAY_OVERLAP_SECONDS = 10800" in parameter_source
    assert "DEEP_RECONCILIATION_INTERVAL_SECONDS = 86400" in parameter_source
    assert 'get_env("REPLAY_OVERLAP_SECONDS"' in code
    assert '"DEEP_RECONCILIATION_INTERVAL_SECONDS",' in code


def test_fresh_checkpoint_accepts_naive_spark_timestamp() -> None:
    namespace = _exec_functions(
        "_normalize_checkpoint_timestamp",
        "_replay_lower_bound",
        "_next_watermark",
    )
    spark_timestamp = datetime(2026, 9, 4, 16, 0)
    aware_timestamp = spark_timestamp.replace(tzinfo=UTC)

    assert namespace["_replay_lower_bound"](None) is None
    assert namespace["_next_watermark"](None, spark_timestamp) == spark_timestamp
    assert (
        namespace["_next_watermark"](aware_timestamp, spark_timestamp)
        == spark_timestamp
    )


def test_missing_checkpoint_row_has_no_lower_bound() -> None:
    class _Result:
        @staticmethod
        def collect() -> list[Any]:
            return []

    class _Spark:
        @staticmethod
        def sql(_query: str) -> _Result:
            return _Result()

    namespace = _exec_functions(
        "_normalize_checkpoint_timestamp",
        "get_watermark",
        extra_globals={
            "spark": _Spark(),
            "WATERMARK_TABLE": "lakehouse.ag._watermarks",
        },
    )

    assert namespace["get_watermark"]("receipt_created") is None


def test_replay_window_reconsiders_equal_and_recent_events_without_regression() -> None:
    namespace = _exec_functions(
        "_normalize_checkpoint_timestamp",
        "_replay_lower_bound",
        "_next_watermark",
        "_max_observed_timestamp",
    )
    last_ts = datetime(2026, 9, 4, 16, 0)
    overlap = _literal("REPLAY_OVERLAP_SECONDS")
    assert overlap == 3 * 60 * 60

    lower_bound = namespace["_replay_lower_bound"](last_ts)
    rows = [
        ("already-written", last_ts),
        ("equal-timestamp-new-key", last_ts),
        ("late-visible", last_ts - timedelta(seconds=min(60, overlap))),
        ("too-old", lower_bound - timedelta(microseconds=1)),
    ]
    reconsidered = [(key, ts) for key, ts in rows if ts >= lower_bound]

    assert [key for key, _ in reconsidered] == [
        "already-written",
        "equal-timestamp-new-key",
        "late-visible",
    ]
    assert len({key for key, _ in reconsidered}) == len(reconsidered)
    assert namespace["_next_watermark"](
        last_ts, max(ts for _, ts in reconsidered)
    ) == last_ts
    assert namespace["_max_observed_timestamp"](None, None) is None
    assert namespace["_max_observed_timestamp"](None, last_ts) == last_ts


def test_deep_reconciliation_is_due_only_after_its_bounded_cadence() -> None:
    namespace = _exec_functions(
        "_normalize_checkpoint_timestamp",
        "_reconciliation_watermark_key",
        "_reconciliation_due",
    )
    now = datetime(2026, 9, 4, 16, 0)
    interval = _literal("DEEP_RECONCILIATION_INTERVAL_SECONDS")

    assert interval == 24 * 60 * 60
    assert (
        namespace["_reconciliation_watermark_key"]("receipt_created")
        == "receipt_created:reconciliation"
    )
    assert namespace["_reconciliation_due"](None, now)
    assert not namespace["_reconciliation_due"](
        now - timedelta(seconds=interval - 1),
        now,
    )
    assert namespace["_reconciliation_due"](
        now - timedelta(seconds=interval),
        now,
    )


def test_replay_and_deep_reconciliation_plans_execute_from_notebook_code() -> None:
    spark = _RecordingSpark()
    namespace = _exec_functions(
        "_split_replay_source",
        "_target_keys_for_candidates",
        "_late_event_key_gaps",
        "_missing_late_source_rows",
        extra_globals={
            "F": _RecordingFunctions,
            "spark": spark,
            "LAKEHOUSE_NAME": "lakehouse",
            "SILVER_DB": "ag",
            "STREAM_DEDUPE_KEYS": _literal("STREAM_DEDUPE_KEYS"),
            "SOURCE_DEDUPE_PROJECTIONS": _literal("SOURCE_DEDUPE_PROJECTIONS"),
            "deduplicate_candidates": lambda frame, _source, _keys: frame,
        },
    )
    source = _RecordingFrame(
        "online_order_picked",
        columns=("order_id", "picked_time", "ingest_timestamp"),
    )
    boundary = datetime(2026, 9, 4, 16, 0)

    replay, late = namespace["_split_replay_source"](
        source, "ingest_timestamp", boundary, True
    )

    assert _plan(replay.operations[-1]) == (
        "filter",
        (
            ">=",
            (
                ("column", ("ingest_timestamp",)),
                ("literal", (boundary,)),
            ),
        ),
    )
    assert late is not None
    assert _plan(late.operations[-1]) == (
        "filter",
        (
            "<",
            (
                ("column", ("ingest_timestamp",)),
                ("literal", (boundary,)),
            ),
        ),
    )

    candidate_keys = _RecordingFrame("late-online-order-keys")
    gaps = namespace["_late_event_key_gaps"](
        candidate_keys,
        "online_order_picked",
        "fact_online_order_status",
    )
    gap_join = gaps.operations[-1]
    assert gap_join[0] == "join"
    assert gap_join[2:] == (
        ("order_id_ext", "status"),
        "left_anti",
    )
    matching_target_keys = gap_join[1]
    assert isinstance(matching_target_keys, _RecordingFrame)
    semi_join = next(
        operation
        for operation in matching_target_keys.operations
        if operation[0] == "join"
    )
    assert semi_join[2:] == (
        ("order_id_ext", "status"),
        "left_semi",
    )
    assert semi_join[1] is candidate_keys
    assert spark.table_calls == ["lakehouse.ag.fact_online_order_status"]

    missing_rows = namespace["_missing_late_source_rows"](
        late,
        "online_order_picked",
        "fact_online_order_status",
    )
    source_join = next(
        operation
        for operation in missing_rows.operations
        if operation[0] == "join" and operation[3] == "left_semi"
    )
    assert source_join[0] == "join"
    assert source_join[3] == "left_semi"
    assert isinstance(source_join[1], _RecordingFrame)
    missing_key_join = next(
        operation
        for operation in source_join[1].operations
        if operation[0] == "join" and operation[3] == "left_anti"
    )
    assert missing_key_join[0] == "join"
    assert missing_key_join[3] == "left_anti"

    notebook_source = _notebook_code()
    assert "F.broadcast(" not in notebook_source


def test_deep_reconciliation_excludes_keys_already_present_in_silver() -> None:
    class _MemoryFrame:
        def __init__(self, rows: list[dict[str, str]]) -> None:
            self.rows = rows

        def select(self, *keys: str) -> _MemoryFrame:
            return _MemoryFrame(
                [{key: row[key] for key in keys} for row in self.rows]
            )

        def join(
            self,
            other: _MemoryFrame,
            keys: list[str],
            how: str,
        ) -> _MemoryFrame:
            other_keys = {
                tuple(row[key] for key in keys)
                for row in other.rows
            }
            if how == "left_semi":
                rows = [
                    row
                    for row in self.rows
                    if tuple(row[key] for key in keys) in other_keys
                ]
            elif how == "left_anti":
                rows = [
                    row
                    for row in self.rows
                    if tuple(row[key] for key in keys) not in other_keys
                ]
            else:
                raise AssertionError(f"unexpected join type: {how}")
            return _MemoryFrame(rows)

    class _MemorySpark:
        @staticmethod
        def table(_name: str) -> _MemoryFrame:
            return _MemoryFrame([{"receipt_id_ext": "already-present"}])

    namespace = _exec_functions(
        "_target_keys_for_candidates",
        "_late_event_key_gaps",
        extra_globals={
            "spark": _MemorySpark(),
            "AnalysisException": RuntimeError,
            "LAKEHOUSE_NAME": "lakehouse",
            "SILVER_DB": "ag",
            "STREAM_DEDUPE_KEYS": {"receipt_created": ["receipt_id_ext"]},
        },
    )
    candidates = _MemoryFrame(
        [
            {"receipt_id_ext": "already-present"},
            {"receipt_id_ext": "missing-old-row"},
        ]
    )

    missing = namespace["_late_event_key_gaps"](
        candidates,
        "receipt_created",
        "fact_receipts",
    )

    assert missing.rows == [{"receipt_id_ext": "missing-old-row"}]


def test_null_checkpoint_timestamp_fails_before_replay_split() -> None:
    source_name = "lakehouse.cusn.inventory_updated"
    source = _RecordingFrame(
        "inventory_updated",
        columns=("ingest_timestamp", "trace_id"),
        count_result=1,
    )
    spark = _RecordingSpark({source_name: source})
    split_calls: list[tuple[Any, ...]] = []

    def _unexpected_split(*args: Any) -> None:
        split_calls.append(args)

    namespace = _exec_functions(
        "_assert_non_null_checkpoint_timestamps",
        "process_events",
        extra_globals={
            "F": _RecordingFunctions,
            "spark": spark,
            "LAKEHOUSE_NAME": "lakehouse",
            "BRONZE_SCHEMA": "cusn",
            "SILVER_DB": "ag",
            "streaming_table_exists": lambda _source: True,
            "get_watermark": lambda _source: datetime(2026, 9, 4, 16, 0),
            "_replay_lower_bound": lambda timestamp: timestamp,
            "_reconciliation_watermark_key": lambda source: f"{source}:reconcile",
            "_reconciliation_due": lambda _last: False,
            "_source_snapshot_for_run": lambda frame, *_args: frame.persist(),
            "_split_replay_source": _unexpected_split,
        },
    )

    with pytest.raises(
        ValueError,
        match="inventory_updated: null ingest_timestamp checkpoint timestamp",
    ):
        namespace["process_events"](
            "inventory_updated",
            "fact_store_inventory_txn",
            lambda frame: frame,
        )

    assert not split_calls
    assert [action for action, _ in spark.actions] == [
        "count",
        "count",
        "unpersist",
    ]


def test_missing_late_event_target_fails_closed_with_clear_error() -> None:
    class _MissingTarget(Exception):
        pass

    class _MissingSpark:
        @staticmethod
        def table(_name: str) -> _RecordingFrame:
            raise _MissingTarget("missing")

    namespace = _exec_functions(
        "_target_keys_for_candidates",
        extra_globals={
            "F": _RecordingFunctions,
            "spark": _MissingSpark(),
            "AnalysisException": _MissingTarget,
            "LAKEHOUSE_NAME": "lakehouse",
            "SILVER_DB": "ag",
        },
    )

    with pytest.raises(
        ValueError,
        match=(
            "fact_receipts: target unavailable for key reconciliation; "
            "checkpoint not advanced"
        ),
    ) as error:
        namespace["_target_keys_for_candidates"](
            "fact_receipts",
            _RecordingFrame("late-keys"),
            ["receipt_id_ext"],
        )

    assert isinstance(error.value.__cause__, _MissingTarget)


def test_target_key_scope_and_null_checks_execute_from_notebook_code() -> None:
    spark = _RecordingSpark()
    namespace = _exec_functions(
        "_target_keys_for_candidates",
        extra_globals={
            "F": _RecordingFunctions,
            "spark": spark,
            "LAKEHOUSE_NAME": "lakehouse",
            "SILVER_DB": "ag",
        },
    )
    candidate_keys = _RecordingFrame("incoming")

    matching_target_keys = namespace["_target_keys_for_candidates"](
        "fact_receipts",
        candidate_keys,
        ["receipt_id_ext"],
    )

    assert matching_target_keys.operations[0] == (
        "select",
        ("receipt_id_ext",),
    )
    assert matching_target_keys.operations[1][2:] == (
        ("receipt_id_ext",),
        "left_semi",
    )
    assert matching_target_keys.operations[1][1] is candidate_keys
    assert spark.table_calls == ["lakehouse.ag.fact_receipts"]

    uniqueness_source = _source("validate_target_key_uniqueness")
    assert "deduplicate_candidates(" in uniqueness_source


def test_null_dedupe_key_fails_with_source_name() -> None:
    class _Predicate:
        def __or__(self, _other: Any) -> _Predicate:
            return self

        def isNull(self) -> _Predicate:  # noqa: N802 - mirrors PySpark
            return self

    class _Functions:
        @staticmethod
        def col(_name: str) -> _Predicate:
            return _Predicate()

    class _Count:
        @staticmethod
        def limit(_count: int) -> _Count:
            return _Count()

        @staticmethod
        def count() -> int:
            return 1

    class _Frame:
        columns = ["receipt_id_ext"]

        @staticmethod
        def filter(_predicate: _Predicate) -> _Count:
            return _Count()

    namespace = _exec_functions(
        "deduplicate_candidates",
        extra_globals={"F": _Functions},
    )

    with pytest.raises(ValueError, match="receipt_created: null value"):
        namespace["deduplicate_candidates"](
            _Frame(),
            "receipt_created",
            ["receipt_id_ext"],
        )


def test_late_detector_has_source_specific_key_projections() -> None:
    projections = _literal("SOURCE_DEDUPE_PROJECTIONS")
    dedupe_keys = _literal("STREAM_DEDUPE_KEYS")

    for source_table, keys in dedupe_keys.items():
        if source_table == "marketing_attribution":
            continue
        assert list(projections[source_table]) == keys

    assert projections["receipt_created"]["receipt_id_ext"] == (
        "column",
        "receipt_id",
    )
    assert projections["receipt_line_added"]["line_num"] == (
        "column",
        "line_number",
    )
    assert projections["online_order_picked"]["status"] == (
        "literal",
        "PICKED",
    )
    assert projections["online_order_shipped"]["status"] == (
        "literal",
        "SHIPPED",
    )


def test_non_reconciliation_runs_materialize_only_nulls_and_replay_window() -> None:
    namespace = _exec_functions(
        "_source_snapshot_for_run",
        extra_globals={"F": _RecordingFunctions},
    )
    source = _RecordingFrame("source")
    boundary = datetime(2026, 9, 4, 16, 0)

    bounded = namespace["_source_snapshot_for_run"](
        source,
        "ingest_timestamp",
        boundary,
        False,
    )
    deep = namespace["_source_snapshot_for_run"](
        source,
        "ingest_timestamp",
        boundary,
        True,
    )

    assert deep is source
    assert _plan(bounded.operations[-1]) == (
        "filter",
        (
            "or",
            (
                ("isNull", (("column", ("ingest_timestamp",)),)),
                (
                    ">=",
                    (
                        ("column", ("ingest_timestamp",)),
                        ("literal", (boundary,)),
                    ),
                ),
            ),
        ),
    )


def test_process_events_skips_deep_scan_before_reconciliation_cadence() -> None:
    now = datetime(2026, 9, 4, 16, 0)
    source_name = "lakehouse.cusn.receipt_created"
    source = _RecordingFrame(
        "source",
        columns=("ingest_timestamp", "receipt_id"),
    )
    spark = _RecordingSpark({source_name: source})
    snapshot_flags: list[bool] = []
    split_flags: list[bool] = []

    def source_snapshot(
        frame: _RecordingFrame,
        _ts_col: str,
        _replay_start: datetime,
        reconcile_history: bool,
    ) -> _RecordingFrame:
        snapshot_flags.append(reconcile_history)
        return frame

    def split_source(
        frame: _RecordingFrame,
        _ts_col: str,
        _replay_start: datetime,
        reconcile_history: bool,
    ) -> tuple[_RecordingFrame, None]:
        split_flags.append(reconcile_history)
        return frame, None

    namespace = _exec_functions(
        "process_events",
        extra_globals={
            "F": _RecordingFunctions,
            "spark": spark,
            "LAKEHOUSE_NAME": "lakehouse",
            "BRONZE_SCHEMA": "cusn",
            "SILVER_DB": "ag",
            "streaming_table_exists": lambda _source: True,
            "get_watermark": lambda _key: now,
            "_replay_lower_bound": lambda timestamp: timestamp,
            "_reconciliation_watermark_key": (
                lambda table: f"{table}:reconciliation"
            ),
            "_reconciliation_due": lambda _last: False,
            "_source_snapshot_for_run": source_snapshot,
            "_assert_non_null_checkpoint_timestamps": lambda *_args: None,
            "_split_replay_source": split_source,
            "_missing_late_source_rows": (
                lambda *_args: pytest.fail("deep reconciliation ran early")
            ),
        },
    )

    assert (
        namespace["process_events"](
            "receipt_created",
            "fact_receipts",
            lambda frame: frame,
        )
        == 0
    )
    assert snapshot_flags == [False]
    assert split_flags == [False]


def test_process_events_recovers_missing_old_rows_and_checkpoints_after_merge() -> None:
    now = datetime(2026, 9, 4, 16, 0)
    source_name = "lakehouse.cusn.receipt_created"
    source = _RecordingFrame(
        "source",
        columns=("ingest_timestamp", "receipt_id"),
        count_result=3,
        max_timestamp=now,
    )
    replay = _RecordingFrame(
        "replay",
        columns=source.columns,
        count_result=1,
        max_timestamp=now,
    )
    late = _RecordingFrame(
        "late",
        columns=source.columns,
        count_result=2,
        max_timestamp=now - timedelta(days=2),
    )
    missing = _RecordingFrame(
        "missing-late",
        columns=source.columns,
        count_result=1,
        max_timestamp=now - timedelta(days=2),
    )
    spark = _RecordingSpark({source_name: source})
    update_calls: list[tuple[str, datetime]] = []
    transformed_inputs: list[_RecordingFrame] = []
    merge_calls: list[_RecordingFrame] = []
    merge_error: list[Exception] = [RuntimeError("merge failed")]

    def get_watermark(key: str) -> datetime:
        if key.endswith(":reconciliation"):
            return now - timedelta(days=2)
        return now - timedelta(hours=1)

    def transform(frame: _RecordingFrame) -> _RecordingFrame:
        transformed_inputs.append(frame)
        return frame

    def merge(frame: _RecordingFrame, _source: str, _target: str) -> int:
        merge_calls.append(frame)
        if merge_error:
            raise merge_error.pop()
        return frame.count_result

    namespace = _exec_functions(
        "process_events",
        extra_globals={
            "F": _RecordingFunctions,
            "spark": spark,
            "LAKEHOUSE_NAME": "lakehouse",
            "BRONZE_SCHEMA": "cusn",
            "SILVER_DB": "ag",
            "streaming_table_exists": lambda _source: True,
            "get_watermark": get_watermark,
            "_replay_lower_bound": lambda timestamp: timestamp,
            "_reconciliation_watermark_key": (
                lambda table: f"{table}:reconciliation"
            ),
            "_reconciliation_due": lambda _last: True,
            "_source_snapshot_for_run": lambda frame, *_args: frame,
            "_assert_non_null_checkpoint_timestamps": lambda *_args: None,
            "_split_replay_source": lambda *_args: (replay, late),
            "_missing_late_source_rows": lambda *_args: missing,
            "project_target_schema": lambda frame, _target: frame,
            "merge_new_rows": merge,
            "_next_watermark": lambda _previous, observed: observed,
            "update_watermark": (
                lambda key, value: update_calls.append((key, value))
            ),
        },
    )

    with pytest.raises(RuntimeError, match="merge failed"):
        namespace["process_events"](
            "receipt_created",
            "fact_receipts",
            transform,
        )

    assert not update_calls
    assert transformed_inputs
    processed = transformed_inputs[-1]
    union = next(
        operation for operation in processed.operations if operation[0] == "unionByName"
    )
    assert union[1] is missing

    result = namespace["process_events"](
        "receipt_created",
        "fact_receipts",
        transform,
    )

    assert result == 2
    assert len(merge_calls) == 2
    assert [key for key, _ in update_calls] == [
        "receipt_created",
        "receipt_created:reconciliation",
    ]


def test_fresh_run_processes_source_once_and_seeds_reconciliation_state() -> None:
    process_source = _source("process_events")
    process = _function(_tree(), "process_events")

    assert "reconcile_history = last_ts is not None and" in process_source
    assert "_missing_late_source_rows" in process_source
    assert "replay_source.unionByName(missing_late_source)" in process_source
    merge_call = next(
        node
        for node in ast.walk(process)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "merge_new_rows"
    )
    reconciliation_update = next(
        node
        for node in ast.walk(process)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "update_watermark"
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "reconciliation_key"
    )
    assert merge_call.lineno < reconciliation_update.lineno
    assert "assert_no_unmerged_late_events" not in process_source


def test_process_events_checkpoints_only_the_materialized_snapshot() -> None:
    tree = _tree()
    process = _function(tree, "process_events")

    persist_assignments = [
        node
        for node in ast.walk(process)
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "persist"
    ]
    assert persist_assignments, "the replay window must be persisted as one snapshot"
    snapshot_assignment = next(
        assignment
        for assignment in persist_assignments
        if any(
            isinstance(target, ast.Name) and target.id == "snapshot"
            for target in assignment.targets
        )
    )
    snapshot_name = next(
        target.id
        for target in snapshot_assignment.targets
        if isinstance(target, ast.Name)
    )

    aggregate_receivers = {
        node.func.value.id
        for node in ast.walk(process)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "agg"
        and isinstance(node.func.value, ast.Name)
    }
    assert aggregate_receivers == {snapshot_name}

    calls = [
        node
        for node in ast.walk(process)
        if isinstance(node, ast.Call)
        and (
            isinstance(node.func, ast.Name)
            and node.func.id in {"merge_new_rows", "update_watermark"}
        )
    ]
    call_names = [
        node.func.id for node in sorted(calls, key=lambda node: (node.lineno, node.col_offset))
    ]
    assert call_names.index("merge_new_rows") < call_names.index("update_watermark")
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "unpersist"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == snapshot_name
        for node in ast.walk(process)
    )


def test_truck_checkpoint_uses_only_materialized_source_snapshots() -> None:
    source = _source("process_truck_lifecycles")
    assert "arrivals_snapshot" in source
    assert "departures_snapshot" in source
    assert 'arrivals_snapshot.agg(F.max("ingest_timestamp"))' in source
    assert 'departures_snapshot.agg(F.max("ingest_timestamp"))' in source
    assert "paired.agg" not in source
    assert source.index('if new_count == 0:') < source.index("update_watermark")


def test_event_time_rules_are_explicit_and_payload_first() -> None:
    assert _literal("EVENT_TIME_FIELDS") == {
        "receipt_created": ("ingest_timestamp",),
        "receipt_line_added": ("ingest_timestamp",),
        "payment_processed": ("processing_time", "ingest_timestamp"),
        "inventory_updated": ("ingest_timestamp",),
        "customer_entered": ("ingest_timestamp",),
        "stockout_detected": ("detection_time", "ingest_timestamp"),
        "reorder_triggered": ("ingest_timestamp",),
        "truck_arrived": ("arrival_time", "ingest_timestamp"),
        "truck_departed": ("departure_time", "ingest_timestamp"),
        "store_opened": ("operation_time", "ingest_timestamp"),
        "store_closed": ("operation_time", "ingest_timestamp"),
        "ad_impression": ("ingest_timestamp",),
        "promotion_applied": ("ingest_timestamp",),
        "customer_zone_changed": ("timestamp", "ingest_timestamp"),
        "ble_ping_detected": ("ingest_timestamp",),
        "online_order_created": ("ingest_timestamp",),
        "online_order_picked": ("picked_time", "ingest_timestamp"),
        "online_order_shipped": ("shipped_time", "ingest_timestamp"),
    }

    helper_source = _source("_event_time_columns")
    assert "_event_time_expr" in helper_source
    assert "F.to_date(event_ts)" in helper_source


def test_truck_lifecycle_uses_payload_times_with_ingest_fallbacks() -> None:
    source = _source("transform_truck_lifecycle")
    process_source = _source("process_truck_lifecycles")
    assert '_event_time_expr("truck_arrived", "arrived.")' in source
    assert '_event_time_expr("truck_departed", "departed.")' in source
    assert 'F.to_date(departure_event_ts).alias("event_date")' in source
    assert "_checkpoint_ingest_timestamp" not in source
    assert "_checkpoint_ingest_timestamp" not in process_source


def test_target_schema_rules_match_authoritative_historical_contracts() -> None:
    actual = _literal("SILVER_TARGET_SCHEMAS")
    historical = _historical_tables()
    expected_targets = {
        "fact_receipts",
        "fact_receipt_lines",
        "fact_payments",
        "fact_store_inventory_txn",
        "fact_foot_traffic",
        "fact_truck_moves",
        "fact_stockouts",
        "fact_reorders",
        "fact_store_ops",
        "fact_marketing",
        "fact_promotions",
        "fact_customer_zone_changes",
        "fact_ble_pings",
        "fact_online_order_headers",
        "fact_marketing_attribution",
    }

    for table in expected_targets:
        assert actual[table] == historical[table]

    assert actual["fact_online_order_status"] == [
        ("order_id_ext", "string"),
        ("event_ts", "timestamp"),
        ("event_date", "date"),
        ("node_type", "string"),
        ("node_id", "long"),
        ("fulfillment_mode", "string"),
        ("picked_time", "timestamp"),
        ("shipped_time", "timestamp"),
        ("status", "string"),
    ]

    forbidden = {
        "fact_receipts": {"return_for_receipt_id_ext"},
        "fact_payments": {"payment_id"},
        "fact_foot_traffic": {"customer_id"},
        "fact_store_ops": {"operation_time"},
        "fact_online_order_headers": {
            "fulfillment_mode",
            "node_type",
            "node_id",
            "item_count",
        },
    }
    for table, extra_columns in forbidden.items():
        assert extra_columns.isdisjoint(dict(actual[table]))


def test_process_event_routes_have_maintained_target_contracts() -> None:
    tree = _tree()
    target_schemas = _literal("SILVER_TARGET_SCHEMAS")
    routes: list[tuple[str, str]] = []

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "process_events"
        ):
            source = ast.literal_eval(node.args[0])
            target = ast.literal_eval(node.args[1])
            routes.append((source, target))

    assert routes
    assert all(target in target_schemas for _, target in routes)
    assert (
        "online_order_picked",
        "fact_online_order_status",
    ) in routes
    assert (
        "online_order_shipped",
        "fact_online_order_status",
    ) in routes


def test_streaming_driver_does_not_swallow_individual_source_failures() -> None:
    notebook = json.loads(SILVER_NOTEBOOK.read_text(encoding="utf-8"))
    execution = next(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
        and "STREAMING TO SILVER" in "".join(cell["source"])
    )
    execution_tree = ast.parse(execution)

    assert not any(isinstance(node, ast.Try) for node in ast.walk(execution_tree))
    assert sum(
        1
        for node in ast.walk(execution_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"process_events", "process_truck_lifecycles"}
    ) == 17


def test_merges_project_explicit_contracts_without_schema_auto_merge() -> None:
    merge_source = _source("merge_new_rows")
    process_source = _source("process_events")

    assert "project_target_schema" in process_source
    assert "mergeSchema" not in merge_source
    assert "schema.autoMerge.enabled" not in merge_source
    assert "INSERT *" not in merge_source
    assert "SILVER_TARGET_SCHEMAS" in merge_source


def test_existing_target_schema_is_validated_before_merge() -> None:
    source = _source("validate_target_schema")
    merge_source = _source("merge_new_rows")

    assert "SILVER_TARGET_SCHEMAS" in source
    assert "schema mismatch" in source
    assert "validate_target_schema(target_table)" in merge_source


def test_target_schema_accepts_timestamp_ntz_alias() -> None:
    class _DataType:
        @staticmethod
        def simpleString() -> str:  # noqa: N802 - mirrors PySpark
            return "timestamp_ntz"

    field = type("_Field", (), {"name": "event_ts", "dataType": _DataType()})()
    table = type("_Table", (), {"schema": type("_Schema", (), {"fields": [field]})()})()

    class _Spark:
        @staticmethod
        def table(_name: str) -> Any:
            return table

    namespace = _exec_functions(
        "validate_target_schema",
        extra_globals={
            "spark": _Spark(),
            "LAKEHOUSE_NAME": "lakehouse",
            "SILVER_DB": "ag",
            "SILVER_TARGET_SCHEMAS": {"events": [("event_ts", "timestamp")]},
        },
    )

    namespace["validate_target_schema"]("events")


def test_target_key_validation_scopes_duplicates_to_incoming_keys() -> None:
    source = _source("validate_target_key_uniqueness")
    merge_source = _source("merge_new_rows")

    assert "candidate_df" in source
    assert "_target_keys_for_candidates" in source
    assert source.index("_target_keys_for_candidates") < source.index(".groupBy")
    assert "source_table" in source
    assert "validate_target_key_uniqueness(target_table, source_table, deduped)" in (
        merge_source
    )


def test_process_events_refuses_to_advance_checkpoint_on_empty_transform() -> None:
    process_source = _source("process_events")
    guard = "if projected_count == 0"

    assert guard in process_source
    assert process_source.index(guard) < process_source.index("merge_new_rows")
    assert process_source.index(guard) < process_source.index("update_watermark")


def test_process_events_materializes_the_exact_transformed_snapshot() -> None:
    process_source = _source("process_events")
    assert "projected_snapshot = project_target_schema(" in process_source
    assert ").persist()" in process_source
    assert "projected_count = projected_snapshot.count()" in process_source
    assert "projected_snapshot, source_table, target_table" in process_source
    assert "projected_snapshot.unpersist()" in process_source


def _inventory_namespace(spark: _RecordingSpark) -> dict[str, Any]:
    return _exec_functions(
        "_event_time_expr",
        "_legacy_index",
        "deduplicate_candidates",
        "_inventory_before_boundary",
        "_inventory_recompute_boundaries",
        "_prior_inventory_balances",
        "transform_inventory_updated",
        extra_globals={
            "F": _RecordingFunctions,
            "Window": _RecordingWindow,
            "spark": spark,
            "LAKEHOUSE_NAME": "lakehouse",
            "SILVER_DB": "ag",
            "EVENT_TIME_FIELDS": _literal("EVENT_TIME_FIELDS"),
            "STREAM_DEDUPE_KEYS": _literal("STREAM_DEDUPE_KEYS"),
            "INVENTORY_ORDER_FIELDS": _literal("INVENTORY_ORDER_FIELDS"),
            "target_table_exists": lambda _target: True,
        },
    )


def _inventory_transform_plan() -> tuple[_RecordingFrame, _RecordingSpark]:
    target_name = "lakehouse.ag.fact_store_inventory_txn"
    target_columns = tuple(
        name for name, _ in _historical_tables()["fact_store_inventory_txn"]
    )
    spark = _RecordingSpark(
        {
            target_name: _RecordingFrame(
                target_name,
                columns=target_columns,
            )
        }
    )
    source = _RecordingFrame(
        "inventory_updated",
        columns=(
            "ingest_timestamp",
            "trace_id",
            "store_id",
            "product_id",
            "quantity_delta",
            "reason",
            "payload_source",
        ),
    )
    transformed = _inventory_namespace(spark)["transform_inventory_updated"](source)
    return transformed, spark


def test_inventory_prior_balance_is_latest_non_null_before_boundary() -> None:
    namespace = _exec_functions(
        "_inventory_before_boundary",
        "_prior_inventory_balances",
        extra_globals={
            "F": _RecordingFunctions,
            "Window": _RecordingWindow,
        },
    )
    result = namespace["_prior_inventory_balances"](
        _RecordingFrame(
            "existing-with-boundary",
            columns=(
                "event_ts",
                "trace_id",
                "store_id",
                "product_id",
                "balance",
                "_first_replay",
            ),
        )
    )

    assert [operation[0] for operation in result.operations] == [
        "filter",
        "filter",
        "withColumn",
        "filter",
        "select",
    ]
    assert _plan(result.operations[0][1]) == (
        "or",
        (
            (
                "<",
                (
                    ("column", ("existing.event_ts",)),
                    ("column", ("_first_replay.event_ts",)),
                ),
            ),
            (
                "and",
                (
                    (
                        "==",
                        (
                            ("column", ("existing.event_ts",)),
                            ("column", ("_first_replay.event_ts",)),
                        ),
                    ),
                    (
                        "<",
                        (
                            ("column", ("existing.trace_id",)),
                            ("column", ("_first_replay.trace_id",)),
                        ),
                    ),
                ),
            ),
        ),
    )
    assert _plan(result.operations[1][1]) == (
        "isNotNull",
        (("column", ("balance",)),),
    )
    rank_expression = result.operations[2][2]
    assert _plan(rank_expression) == (
        "over",
        (
            ("row_number", ()),
            (
                "window",
                (
                    ("partitionBy", ("store_id", "product_id")),
                    (
                        "orderBy",
                        (
                            ("desc", (("column", ("event_ts",)),)),
                            ("desc", (("column", ("trace_id",)),)),
                        ),
                    ),
                ),
            ),
        ),
    )
    assert _plan(result.operations[3][1]) == (
        "==",
        (("column", ("_balance_rank",)), 1),
    )
    assert result.columns == ("store_id", "product_id", "_prior_balance")


def test_inventory_legacy_null_expands_to_earliest_recompute_boundary() -> None:
    namespace = _exec_functions(
        "_inventory_recompute_boundaries",
        extra_globals={
            "F": _RecordingFunctions,
            "INVENTORY_ORDER_FIELDS": _literal("INVENTORY_ORDER_FIELDS"),
        },
    )
    replay = _RecordingFrame(
        "replay",
        columns=("event_ts", "trace_id", "store_id", "product_id"),
    )
    existing = _RecordingFrame(
        "existing",
        columns=("event_ts", "trace_id", "store_id", "product_id", "balance"),
    )

    boundaries = namespace["_inventory_recompute_boundaries"](replay, existing)

    assert _literal("INVENTORY_ORDER_FIELDS") == ("event_ts", "trace_id")
    union = boundaries.operations[2]
    assert union[0] == "unionByName"
    legacy_boundaries = union[1]
    assert isinstance(legacy_boundaries, _RecordingFrame)
    assert _plan(legacy_boundaries.operations[0]) == (
        "filter",
        ("isNull", (("column", ("balance",)),)),
    )
    expected_first = (
        "alias",
        (
            (
                "min",
                (
                    (
                        "struct",
                        (
                            ("column", ("event_ts",)),
                            ("column", ("trace_id",)),
                        ),
                    ),
                ),
            ),
            "_first_replay",
        ),
    )
    assert _plan(boundaries.operations[1][1][0]) == expected_first
    assert _plan(legacy_boundaries.operations[-1][1][0]) == expected_first
    assert _plan(boundaries.operations[-1][1][0]) == (
        "alias",
        (("min", ("_first_replay",)), "_first_replay"),
    )


def test_inventory_replay_excludes_existing_trace_ids_and_dedupes_idempotently() -> None:
    transformed, spark = _inventory_transform_plan()
    anti_join_index, anti_join = next(
        (index, operation)
        for index, operation in enumerate(transformed.operations)
        if operation[0] == "join" and operation[3] == "left_anti"
    )
    union_index, union = next(
        (index, operation)
        for index, operation in enumerate(transformed.operations)
        if operation[0] == "unionByName"
    )

    assert anti_join_index < union_index
    assert anti_join[2:] == ("trace_id", "left_anti")
    replayed_trace_ids = anti_join[1]
    replay_rows = union[1]
    assert isinstance(replayed_trace_ids, _RecordingFrame)
    assert isinstance(replay_rows, _RecordingFrame)
    assert [operation[0] for operation in replayed_trace_ids.operations[-2:]] == [
        "select",
        "distinct",
    ]
    assert replayed_trace_ids.operations[-2][1] == ("trace_id",)
    assert _plan(replayed_trace_ids.operations[:-2]) == _plan(
        replay_rows.operations
    )
    assert "source" in replay_rows.columns

    namespace = _inventory_namespace(spark)
    candidates = _RecordingFrame(
        "merge-candidates",
        columns=("event_ts", "trace_id", "quantity", "balance"),
    )
    deduped = namespace["deduplicate_candidates"](
        candidates,
        "inventory_updated",
        ["trace_id"],
    )
    dedupe_operations = [
        operation for operation in deduped.operations if operation[0] == "dropDuplicates"
    ]
    assert dedupe_operations == []
    assert any(
        operation[:2] == ("withColumn", "_dedupe_row")
        for operation in deduped.operations
    )
    assert ("drop", ("_dedupe_row",)) in deduped.operations


def test_inventory_running_balance_uses_prior_plus_ordered_cumulative_quantity() -> None:
    transformed, _ = _inventory_transform_plan()
    balance_operation = next(
        operation
        for operation in transformed.operations
        if operation[0] == "withColumn" and operation[1] == "balance"
    )
    cast_expression = balance_operation[2]

    assert cast_expression.operation == "cast"
    addition = cast_expression.arguments[0]
    assert isinstance(addition, _Expression)
    assert addition.operation == "+"
    assert _plan(addition.arguments[0]) == (
        "coalesce",
        (
            ("column", ("_prior_balance",)),
            ("cast", (("literal", (0,)), "long")),
        ),
    )
    cumulative = addition.arguments[1]
    assert isinstance(cumulative, _Expression)
    assert cumulative.operation == "over"
    assert _plan(cumulative.arguments[0]) == ("sum", ("quantity",))
    assert _plan(cumulative.arguments[1]) == (
        "window",
        (
            ("partitionBy", ("store_id", "product_id")),
            (
                "orderBy",
                (
                    ("asc", (("column", ("event_ts",)),)),
                    ("asc", (("column", ("trace_id",)),)),
                ),
            ),
            ("rowsBetween", "unboundedPreceding", "currentRow"),
        ),
    )


def test_inventory_post_merge_repair_scopes_keys_and_rejects_nulls() -> None:
    target_table = "fact_store_inventory_txn"
    target_name = f"lakehouse.ag.{target_table}"
    spark = _RecordingSpark(
        {
            target_name: _RecordingFrame(
                target_name,
                columns=("store_id", "product_id", "balance"),
                count_result=1,
            )
        }
    )
    candidate = _RecordingFrame(
        "inventory-candidates",
        columns=("trace_id", "store_id", "product_id", "balance"),
        count_result=1,
        action_log=spark.actions,
    )
    namespace = _exec_functions(
        "_assert_inventory_balances_repaired",
        "merge_new_rows",
        extra_globals={
            "F": _RecordingFunctions,
            "spark": spark,
            "LAKEHOUSE_NAME": "lakehouse",
            "SILVER_DB": "ag",
            "STREAM_DEDUPE_KEYS": {"inventory_updated": ["trace_id"]},
            "SILVER_TARGET_SCHEMAS": {
                target_table: [
                    ("trace_id", "string"),
                    ("store_id", "long"),
                    ("product_id", "long"),
                    ("balance", "long"),
                ]
            },
            "deduplicate_candidates": lambda frame, _source, _keys: frame,
            "target_table_exists": lambda _target: True,
            "validate_target_schema": lambda _target: None,
            "validate_target_key_uniqueness": (
                lambda _target, _source, _candidates: None
            ),
        },
    )

    with pytest.raises(
        ValueError,
        match="inventory_updated: null balance remains after deterministic repair",
    ):
        namespace["merge_new_rows"](
            candidate,
            "inventory_updated",
            target_table,
        )

    sql_index = next(
        index for index, (operation, _) in enumerate(spark.actions) if operation == "sql"
    )
    repair_index, repaired_frame = next(
        (index, payload)
        for index, (operation, payload) in enumerate(spark.actions)
        if operation == "count"
        and isinstance(payload, _RecordingFrame)
        and payload.name == target_name
    )
    assert sql_index < repair_index
    assert isinstance(repaired_frame, _RecordingFrame)
    assert [operation[0] for operation in repaired_frame.operations] == [
        "join",
        "filter",
        "limit",
    ]
    repair_join = repaired_frame.operations[0]
    assert repair_join[2:] == (("store_id", "product_id"), "left_semi")
    assert isinstance(repair_join[1], _RecordingFrame)
    affected_keys = repair_join[1]
    assert [operation[0] for operation in affected_keys.operations[-2:]] == [
        "select",
        "distinct",
    ]
    assert affected_keys.operations[-2][1] == ("store_id", "product_id")
    assert _plan(repaired_frame.operations[1][1]) == (
        "isNull",
        (("column", ("balance",)),),
    )


def test_live_alert_and_customer_transforms_emit_compatibility_columns() -> None:
    stockout = _source("transform_stockout_detected")
    reorder = _source("transform_reorder_triggered")
    zone = _source("transform_customer_zone_changed")
    ble = _source("transform_ble_ping")

    for expected in ("StoreID", "DCID", "ProductID", "LastKnownQuantity"):
        assert expected in stockout
    for expected in ("event_date", "__index_level_0__"):
        assert expected in stockout
        assert expected in reorder
        assert expected in zone
        assert expected in ble
    for expected in ("StoreID", "CustomerBLEId", "FromZone", "ToZone"):
        assert expected in zone
    assert "_resolved_ble_customer_id" in ble
    assert "CustomerId" in ble
    assert 'cast("double")' in ble
    assert 'cast("long")' in reorder


def test_online_attribution_works_without_live_only_header_fields() -> None:
    source = _source("process_marketing_attribution")
    assert '"fact_online_order_lines"' not in source
    assert 'F.col("node_type")' not in source
    assert 'F.col("node_id")' not in source
    assert 'F.lit(None).cast("long").alias("store_id")' in source
    assert 'F.col("store_id").cast("long").alias("payment_store_id")' in source
    assert 'F.coalesce(F.col("store_id"), F.col("payment_store_id"))' in source


def test_marketing_attribution_projects_its_authoritative_target_schema() -> None:
    source = _source("process_marketing_attribution")
    assert (
        'project_target_schema(attribution, "fact_marketing_attribution")'
        in source
    )
