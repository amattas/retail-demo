"""Runtime contracts for the Delta table maintenance notebook."""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAINTENANCE_NOTEBOOK = ROOT / "fabric" / "lakehouse" / "05-maintain-delta-tables.ipynb"


def _notebook_code() -> str:
    notebook = json.loads(MAINTENANCE_NOTEBOOK.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )


def _assignment(tree: ast.Module, name: str) -> ast.Assign:
    assignment = next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == name
                for target in node.targets
            )
        ),
        None,
    )
    assert assignment is not None, f"missing assignment: {name}"
    return assignment


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


def test_get_env_is_defined_before_maintenance_configuration(
    monkeypatch: Any,
) -> None:
    tree = ast.parse(_notebook_code(), filename=str(MAINTENANCE_NOTEBOOK))
    helper = _function(tree, "get_env")
    lakehouse_assignment = _assignment(tree, "LAKEHOUSE_NAME")

    assert helper.lineno < lakehouse_assignment.lineno
    assert isinstance(lakehouse_assignment.value, ast.Call)
    assert isinstance(lakehouse_assignment.value.func, ast.Name)
    assert lakehouse_assignment.value.func.id == "get_env"

    namespace = {"os": os}
    exec(
        compile(
            ast.fix_missing_locations(ast.Module(body=[helper], type_ignores=[])),
            str(MAINTENANCE_NOTEBOOK),
            "exec",
        ),
        namespace,
    )
    get_env = namespace["get_env"]

    monkeypatch.delenv("LAKEHOUSE_NAME", raising=False)
    assert get_env("LAKEHOUSE_NAME", default="retail_lakehouse") == ("retail_lakehouse")
    monkeypatch.setenv("LAKEHOUSE_NAME", "custom_lakehouse")
    assert get_env("LAKEHOUSE_NAME", default="retail_lakehouse") == ("custom_lakehouse")


@pytest.mark.parametrize("operation", ["optimize_table", "vacuum_table"])
@pytest.mark.parametrize(
    ("exists", "error", "expected"),
    [
        (False, None, None),
        (True, None, True),
        (True, ValueError("invalid column"), False),
    ],
)
def test_maintenance_distinguishes_missing_tables_from_operation_failures(
    operation,
    exists,
    error,
    expected,
) -> None:
    spark = Mock()
    spark.catalog.tableExists.return_value = exists
    spark.sql.side_effect = error
    tree = ast.parse(_notebook_code())
    namespace = {"spark": spark, "LAKEHOUSE_NAME": "custom_lh", "DRY_RUN": False}
    exec(
        compile(
            ast.Module(body=[_function(tree, operation)], type_ignores=[]), "", "exec"
        ),
        namespace,
    )
    arguments = ["ag", "fact_receipts"]
    if operation == "vacuum_table":
        arguments.append(168)

    assert namespace[operation](*arguments) is expected
    spark.catalog.tableExists.assert_called_once_with("custom_lh.ag.fact_receipts")
    if not exists:
        spark.sql.assert_not_called()
    else:
        assert "custom_lh.ag.fact_receipts" in spark.sql.call_args.args[0]


@pytest.mark.parametrize("failures", [0, 1])
def test_maintenance_summary_fails_only_for_unsuccessful_operations(failures) -> None:
    notebook = json.loads(MAINTENANCE_NOTEBOOK.read_text(encoding="utf-8"))
    summary = "".join(notebook["cells"][-1]["source"])
    namespace = {
        "datetime": SimpleNamespace(
            now=lambda: SimpleNamespace(isoformat=lambda: "now")
        ),
        "silver_success": 1,
        "gold_success": 1,
        "vacuum_success": 2,
        "silver_failed": failures,
        "gold_failed": 0,
        "vacuum_failed": 0,
        "silver_missing": 2,
        "gold_missing": 3,
        "vacuum_missing": 5,
        "silver_tables": ["one"],
        "gold_tables": ["two"],
        "DRY_RUN": False,
    }
    if failures:
        with pytest.raises(RuntimeError, match="Maintenance failed"):
            exec(compile(summary, "", "exec"), namespace)
    else:
        exec(compile(summary, "", "exec"), namespace)


def test_maintenance_zorder_columns_match_authoritative_physical_tables() -> None:
    schemas_path = (
        ROOT / "utility" / "src" / "retail_setup" / "generation" / "schemas.py"
    )
    schema_tree = ast.parse(schemas_path.read_text(encoding="utf-8"))
    tables_node = next(
        node
        for node in schema_tree.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "TABLES"
    )
    schemas = ast.literal_eval(tables_node.value)
    tree = ast.parse(_notebook_code())
    for name in ("silver_tables", "gold_tables"):
        for table, columns in ast.literal_eval(_assignment(tree, name).value):
            if table in schemas:
                assert set(columns or []) <= {field for field, _ in schemas[table]}, (
                    table
                )


def test_maintenance_fails_when_wrong_lakehouse_has_no_base_tables():
    tree = ast.parse(_notebook_code())
    start = _assignment(tree, "required_base_tables").lineno
    end = _assignment(tree, "silver_success").lineno
    nodes = [node for node in tree.body if start <= node.lineno < end]
    spark = Mock()
    spark.catalog.tableExists.return_value = False
    namespace = {
        "spark": spark,
        "LAKEHOUSE_NAME": "wrong",
        "SILVER_DB": "ag",
        "GOLD_DB": "au",
    }
    with pytest.raises(RuntimeError, match="Required historical tables are missing"):
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "", "exec"), namespace)
