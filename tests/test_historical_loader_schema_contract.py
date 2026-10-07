"""Physical-schema contracts for the historical Lakehouse loader."""

from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_NOTEBOOK = ROOT / "fabric" / "lakehouse" / "02-historical-data-load.ipynb"
SCHEMAS = ROOT / "utility" / "src" / "retail_setup" / "generation" / "schemas.py"


def _notebook_code() -> str:
    notebook = json.loads(HISTORICAL_NOTEBOOK.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )


def _tree(path: Path | None = None) -> ast.Module:
    if path is None:
        return ast.parse(_notebook_code(), filename=str(HISTORICAL_NOTEBOOK))
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


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


def _tables() -> dict[str, list[tuple[str, str]]]:
    return ast.literal_eval(_assignment(_tree(SCHEMAS), "TABLES"))


@pytest.mark.parametrize("streaming_initialized", [False, True])
def test_legacy_loader_refuses_to_drop_silver_after_streaming_initializes(
    streaming_initialized,
):
    spark = Mock()
    spark.catalog.tableExists.return_value = streaming_initialized
    namespace = {"spark": spark, "LAKEHOUSE_NAME": "lh", "SILVER_DB": "ag"}
    exec(
        compile(
            ast.Module(
                body=[_function(_tree(), "drop_table_if_exists")], type_ignores=[]
            ),
            str(HISTORICAL_NOTEBOOK),
            "exec",
        ),
        namespace,
    )
    if streaming_initialized:
        with pytest.raises(RuntimeError, match="Historical replacement is blocked"):
            namespace["drop_table_if_exists"]("ag", "fact_receipts")
        spark.sql.assert_not_called()
    else:
        namespace["drop_table_if_exists"]("ag", "fact_receipts")
        spark.sql.assert_called_once_with("DROP TABLE IF EXISTS lh.ag.fact_receipts")


@dataclass(frozen=True)
class _DataType:
    value: str

    def simpleString(self) -> str:  # noqa: N802 - mirrors PySpark
        return self.value


@dataclass(frozen=True)
class _Field:
    name: str
    dataType: _DataType  # noqa: N815 - mirrors PySpark


class _Frame:
    def __init__(self, fields: list[tuple[str, str]]) -> None:
        self.schema = type(
            "_Schema",
            (),
            {"fields": [_Field(name, _DataType(kind)) for name, kind in fields]},
        )()
        self.columns = [name for name, _ in fields]
        self.selected: tuple[str, ...] | None = None

    def select(self, *columns: str) -> _Frame:
        self.selected = columns
        return self


class _DictRowFrame:
    def __init__(self, fields: list[tuple[str, str]]) -> None:
        self.schema = type(
            "_Schema",
            (),
            {"fields": [_Field(name, _DataType(kind)) for name, kind in fields]},
        )()
        self.columns = [name for name, _ in fields]

    def select(self, *columns: str) -> _DictRowFrame:
        fields_by_name = {field.name: field for field in self.schema.fields}
        self.schema.fields = [fields_by_name[name] for name in columns]
        self.columns = list(columns)
        return self


class _DictRowSpark:
    def __init__(self) -> None:
        self.inferred_columns: list[str] = []

    def createDataFrame(  # noqa: N802 - mirrors PySpark
        self, rows: list[dict[str, Any]]
    ) -> _DictRowFrame:
        assert rows
        self.inferred_columns = sorted(rows[0])
        return _DictRowFrame(
            [(name, self._infer_type(rows[0][name])) for name in self.inferred_columns]
        )

    @staticmethod
    def _infer_type(value: Any) -> str:
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "long"
        if isinstance(value, datetime):
            return "timestamp"
        if isinstance(value, date):
            return "date"
        if isinstance(value, float):
            return "double"
        if isinstance(value, str):
            return "string"
        raise AssertionError(f"unsupported test value: {value!r}")


def _schema_namespace() -> dict[str, Any]:
    tree = _tree()
    nodes = [
        ast.Assign(
            targets=[ast.Name(id="HISTORICAL_TABLE_SCHEMAS", ctx=ast.Store())],
            value=_assignment(tree, "HISTORICAL_TABLE_SCHEMAS"),
        ),
        _function(tree, "validate_historical_schema"),
    ]
    namespace: dict[str, Any] = {}
    exec(
        compile(
            ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])),
            str(HISTORICAL_NOTEBOOK),
            "exec",
        ),
        namespace,
    )
    return namespace


def test_historical_writer_contract_matches_tables_exactly() -> None:
    tree = _tree()
    authoritative = _tables()
    notebook_contract = ast.literal_eval(_assignment(tree, "HISTORICAL_TABLE_SCHEMAS"))
    loaded_tables = {
        *ast.literal_eval(_assignment(tree, "dimensions")),
        *ast.literal_eval(_assignment(tree, "facts")),
        "dim_date",
    }

    assert notebook_contract == {
        table: authoritative[table] for table in sorted(loaded_tables)
    }


def test_historical_schema_validation_preserves_generator_types_and_order() -> None:
    namespace = _schema_namespace()
    validate = namespace["validate_historical_schema"]

    for table, schema in namespace["HISTORICAL_TABLE_SCHEMAS"].items():
        frame = _Frame(schema)
        result = validate(frame, table)

        assert result is frame
        assert frame.selected == tuple(name for name, _ in schema)
        assert [
            (field.name, field.dataType.simpleString())
            for field in result.schema.fields
        ] == schema


def test_historical_schema_rejects_type_drift_instead_of_coercing_it() -> None:
    namespace = _schema_namespace()
    validate = namespace["validate_historical_schema"]
    drifted = _Frame(
        [
            ("event_ts", "timestamp"),
            ("trace_id", "string"),
            ("store_id", "long"),
            ("beacon_id", "string"),
            ("customer_ble_id", "string"),
            ("customer_id", "long"),
            ("CustomerId", "long"),
            ("rssi", "int"),
            ("zone", "string"),
            ("event_date", "date"),
            ("__index_level_0__", "long"),
        ]
    )

    with pytest.raises(ValueError, match="fact_ble_pings: schema mismatch"):
        validate(drifted, "fact_ble_pings")


def test_historical_contract_keeps_compatibility_and_count_types() -> None:
    schemas = _tables()

    assert dict(schemas["dim_trucks"])["DCID"] == "double"
    assert dict(schemas["fact_stockouts"])["StoreID"] == "double"
    assert dict(schemas["fact_stockouts"])["DCID"] == "double"
    assert dict(schemas["fact_ble_pings"])["CustomerId"] == "double"
    assert dict(schemas["fact_marketing"])["CustomerId"] == "double"

    assert dict(schemas["fact_foot_traffic"])["count"] == "long"
    assert dict(schemas["fact_ble_pings"])["rssi"] == "long"
    for table in (
        "fact_promo_lines",
        "fact_online_order_lines",
        "fact_truck_inventory",
        "fact_dc_inventory_txn",
        "fact_store_inventory_txn",
    ):
        assert dict(schemas[table])["quantity"] == "long"
    assert dict(schemas["fact_reorders"])["reorder_quantity"] == "long"


def test_historical_loader_validates_before_write_and_propagates_failures() -> None:
    code = _notebook_code()
    tree = ast.parse(code)
    loader = _function(tree, "load_to_silver")
    loader_source = ast.get_source_segment(code, loader)
    assert loader_source is not None

    assert not any(isinstance(node, ast.ExceptHandler) for node in ast.walk(loader))
    assert "cast_id_columns" not in code
    assert loader_source.index("validate_historical_schema") < loader_source.index(
        "drop_table_if_exists"
    )
    assert 'validate_historical_schema(dim_date_df, "dim_date")' in code


def test_dim_date_dict_rows_are_ordered_before_strict_validation() -> None:
    notebook = json.loads(HISTORICAL_NOTEBOOK.read_text(encoding="utf-8"))
    source = next(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
        and "CREATING DIM_DATE" in "".join(cell["source"])
    )
    tree = ast.parse(source)
    executable_nodes: list[ast.stmt] = []
    collecting = False
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "min_date"
            for target in node.targets
        ):
            collecting = True
        if collecting:
            executable_nodes.append(node)
        if any(
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Name)
            and child.func.id == "validate_historical_schema"
            for child in ast.walk(node)
        ):
            break

    spark = _DictRowSpark()
    namespace = _schema_namespace() | {
        "date": date,
        "datetime": datetime,
        "spark": spark,
        "timedelta": timedelta,
    }
    exec(
        compile(
            ast.fix_missing_locations(
                ast.Module(body=executable_nodes, type_ignores=[])
            ),
            str(HISTORICAL_NOTEBOOK),
            "exec",
        ),
        namespace,
    )

    expected = namespace["HISTORICAL_TABLE_SCHEMAS"]["dim_date"]
    expected_columns = [name for name, _ in expected]
    actual = [
        (field.name, field.dataType.simpleString())
        for field in namespace["dim_date_df"].schema.fields
    ]

    assert spark.inferred_columns == sorted(expected_columns)
    assert spark.inferred_columns != expected_columns
    assert actual == expected
