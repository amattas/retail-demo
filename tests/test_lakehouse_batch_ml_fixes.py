"""Contracts for Lakehouse batch repair and ML correctness fixes."""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = ROOT / "fabric" / "lakehouse"
NOTEBOOKS = {
    "shortcuts": NOTEBOOK_DIR / "01-create-bronze-shortcuts.ipynb",
    "stockout": NOTEBOOK_DIR / "12-ml-stockout-prediction.ipynb",
    "delivery": NOTEBOOK_DIR / "13-ml-delivery-prediction.ipynb",
    "pricing": NOTEBOOK_DIR / "14-ml-dynamic-pricing.ipynb",
    "receipts": NOTEBOOK_DIR / "90-augment-and-dedupe-receipts.ipynb",
    "reset": NOTEBOOK_DIR / "99-reset-lakehouse.ipynb",
}


def _notebook(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _code(path: Path) -> str:
    return "\n".join(
        "".join(cell.get("source", []))
        for cell in _notebook(path)["cells"]
        if cell.get("cell_type") == "code"
    )


def _tree(path: Path) -> ast.Module:
    return ast.parse(_code(path), filename=str(path))


def _function(path: Path, name: str) -> ast.FunctionDef:
    function = next(
        (
            node
            for node in _tree(path).body
            if isinstance(node, ast.FunctionDef) and node.name == name
        ),
        None,
    )
    assert function is not None, f"missing function: {name}"
    return function


def _function_source(path: Path, name: str) -> str:
    source = _code(path)
    function = next(
        (
            node
            for node in ast.parse(source).body
            if isinstance(node, ast.FunctionDef) and node.name == name
        ),
        None,
    )
    assert function is not None, f"missing function: {name}"
    return ast.get_source_segment(source, function) or ""


def _load_functions(
    path: Path,
    names: list[str],
    namespace: dict[str, Any],
) -> dict[str, Any]:
    tree = _tree(path)
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    assert {function.name for function in functions} == set(names)
    module = ast.fix_missing_locations(
        ast.Module(body=functions, type_ignores=[])
    )
    exec(compile(module, str(path), "exec"), namespace)
    return namespace


@pytest.mark.parametrize("path", NOTEBOOKS.values(), ids=NOTEBOOKS.keys())
def test_changed_notebooks_are_valid_json_and_python(path: Path) -> None:
    notebook = _notebook(path)
    assert notebook["nbformat"] == 4
    for index, cell in enumerate(notebook["cells"]):
        if cell.get("cell_type") == "code":
            ast.parse(
                "".join(cell.get("source", [])),
                filename=f"{path.name}:cell-{index}",
            )


def test_shortcut_configuration_defines_get_env_before_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = NOTEBOOKS["shortcuts"]
    tree = _tree(path)
    helper = _function(path, "get_env")
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "LAKEHOUSE_NAME"
            for target in node.targets
        )
    )

    assert helper.lineno < assignment.lineno
    namespace = {"os": os}
    exec(
        compile(
            ast.fix_missing_locations(ast.Module(body=[helper], type_ignores=[])),
            str(path),
            "exec",
        ),
        namespace,
    )
    monkeypatch.delenv("LAKEHOUSE_NAME", raising=False)
    assert (
        namespace["get_env"]("LAKEHOUSE_NAME", default="retail_lakehouse")
        == "retail_lakehouse"
    )


@pytest.mark.parametrize(
    "path",
    [
        NOTEBOOKS["stockout"],
        NOTEBOOKS["delivery"],
        NOTEBOOKS["pricing"],
    ],
    ids=["stockout", "delivery", "pricing"],
)
def test_ml_notebook_has_exactly_one_lakehouse_parameter_cell(
    path: Path,
) -> None:
    parameter_cells = []
    parameter_tag_count = 0
    for cell in _notebook(path)["cells"]:
        tags = cell.get("metadata", {}).get("tags", [])
        parameter_tag_count += tags.count("parameters")
        if "parameters" in tags:
            parameter_cells.append(cell)

    assert parameter_tag_count == 1
    assert len(parameter_cells) == 1
    assert "LAKEHOUSE_NAME" in "".join(parameter_cells[0]["source"])


def test_receipt_repair_fails_on_conflicts_and_dedupes_all_receipt_grains() -> None:
    path = NOTEBOOKS["receipts"]
    code = _code(path)
    dedupe_source = _function_source(path, "deduplicate_exact_rows")

    assert "conflicting rows share dedupe key" in dedupe_source
    assert ".groupBy(*key_columns)" in dedupe_source
    assert ".dropDuplicates(key_columns)" in dedupe_source
    assert (
        "deduplicate_exact_rows(\n"
        "    receipts,\n"
        '    ["receipt_id_ext"],\n'
        '    "Receipt headers",'
    ) in code
    assert (
        'deduplicate_exact_rows(\n    lines_aug,\n    ["receipt_id_ext", "line_num"],'
    ) in code
    assert (
        'deduplicate_exact_rows(\n    payments_aug,\n    ["transaction_id"],'
    ) in code
    assert "header_dates = receipts_clean.select(" in code


def test_receipt_repair_materializes_and_validates_before_promotion() -> None:
    path = NOTEBOOKS["receipts"]
    code = _code(path)
    promotion_source = _function_source(path, "stage_and_promote")

    assert "__candidate" in promotion_source
    assert "__backup" in promotion_source
    assert promotion_source.index(".saveAsTable(candidate_table)") < (
        promotion_source.index("CREATE OR REPLACE TABLE")
    )
    assert "validate_materialized_table(" in promotion_source
    assert "for item in reversed(promoted):" in promotion_source
    assert "any changed targets were restored" in promotion_source
    assert promotion_source.index("promoted.append(item)") < (
        promotion_source.index(
            'f"CREATE OR REPLACE TABLE {item[\'target\']} "'
        )
    )
    assert ".saveAsTable(RECEIPTS_TABLE)" not in code
    assert ".saveAsTable(LINES_TABLE)" not in code
    assert ".saveAsTable(PAYMENTS_TABLE)" not in code


def test_receipt_promotion_rolls_back_commit_then_client_error() -> None:
    target_a = "retail.ag.fact_receipts"
    target_b = "retail.ag.fact_receipt_lines"
    commit_error = ConnectionError(
        "client lost response after committed replace"
    )

    class FakeWriter:
        def __init__(self, frame: FakeFrame) -> None:
            self.frame = frame

        def format(self, _format: str) -> FakeWriter:
            return self

        def mode(self, _mode: str) -> FakeWriter:
            return self

        def option(self, _name: str, _value: str) -> FakeWriter:
            return self

        def saveAsTable(self, table_name: str) -> None:  # noqa: N802
            self.frame.spark.tables[table_name] = (
                self.frame.value,
                self.frame.row_count,
            )

    class FakeFrame:
        def __init__(
            self,
            spark: FakeSpark,
            value: str,
            row_count: int,
        ) -> None:
            self.spark = spark
            self.value = value
            self.row_count = row_count
            data_type = SimpleNamespace(simpleString=lambda: "string")
            self.schema = SimpleNamespace(
                fields=[SimpleNamespace(name="value", dataType=data_type)]
            )

        def count(self) -> int:
            return self.row_count

        @property
        def write(self) -> FakeWriter:
            return FakeWriter(self)

    class FakeSpark:
        def __init__(self) -> None:
            self.tables = {
                target_a: ("original-header", 2),
                target_b: ("original-lines", 3),
            }
            self.failed = False
            self.restore_targets: list[str] = []

        def table(self, table_name: str) -> FakeFrame:
            value, row_count = self.tables[table_name]
            return FakeFrame(self, value, row_count)

        def sql(self, statement: str) -> SimpleNamespace:
            if statement.startswith("CREATE OR REPLACE TABLE"):
                parts = statement.split()
                target = parts[4]
                source = parts[-1]
                self.tables[target] = self.tables[source]
                if source.endswith("__backup"):
                    self.restore_targets.append(target)
                if (
                    target == target_b
                    and source.endswith("__candidate")
                    and not self.failed
                ):
                    self.failed = True
                    raise commit_error
            elif statement.startswith("DROP TABLE IF EXISTS"):
                self.tables.pop(statement.split()[-1], None)
            return SimpleNamespace()

    spark = FakeSpark()
    namespace = _load_functions(
        NOTEBOOKS["receipts"],
        [
            "schema_signature",
            "validate_materialized_table",
            "stage_and_promote",
        ],
        {
            "spark": spark,
            "LAKEHOUSE_NAME": "retail",
            "SILVER_STAGE_DB": "ag_stage",
            "RUN_TOKEN": "run1",
        },
    )

    with pytest.raises(RuntimeError, match=str(commit_error)) as raised:
        namespace["stage_and_promote"](
            [
                (target_a, FakeFrame(spark, "repaired-header", 1)),
                (target_b, FakeFrame(spark, "repaired-lines", 2)),
            ]
        )

    assert raised.value.__cause__ is commit_error
    assert spark.tables[target_a] == ("original-header", 2)
    assert spark.tables[target_b] == ("original-lines", 3)
    assert spark.restore_targets == [target_b, target_a]


def test_delivery_history_features_use_keyed_asof_windows() -> None:
    path = NOTEBOOKS["delivery"]
    code = _code(path)
    asof_source = _function_source(path, "build_asof_history_stats")

    assert "unionByName(history_rows)" in asof_source
    assert "Window.partitionBy(*key_columns)" in asof_source
    assert 'F.col("_feature_ts"), F.col("_event_priority")' in asof_source
    assert '.withColumn("_event_priority", F.lit(0))' in asof_source
    assert '.withColumn("_event_priority", F.lit(1))' in asof_source
    assert (
        'F.col("history.history_available_ts") < F.col("target.arrived_ts")' not in code
    )
    assert '["location_type", "location_id"]' in code
    assert '["arrival_hour"]' in code


def test_dynamic_pricing_uses_only_positive_sale_receipt_lines() -> None:
    code = _code(NOTEBOOKS["pricing"])

    assert 'RECEIPTS_TABLE = get_env("RECEIPTS_TABLE"' in code
    assert "receipt_lines\n        .join(receipt_headers" in code
    assert '(F.col("receipt_type") == "SALE")' in code
    assert '(F.col("quantity") > 0)' in code
    assert '"receipt_type"' in code
    assert 'default="fact_receipts"' in code


def test_stockout_clamps_deficits_without_masking_unknown_balances() -> None:
    path = NOTEBOOKS["stockout"]
    code = _code(path)
    eod_source = _function_source(path, "build_end_of_day_inventory")

    assert 'F.col("balance").isNotNull()' in eod_source
    assert 'F.greatest(F.col("balance"), F.lit(0.0))' in eod_source
    assert 'inventory_history.filter(F.col("balance").isNull())' in code
    assert "repair the inventory history instead of treating null as zero" in code


def test_reset_removes_primary_and_both_staging_schema_conventions() -> None:
    path = NOTEBOOKS["reset"]
    code = _code(path)

    assert 'f"{GOLD_DB}_stage"' in code
    assert 'f"{GOLD_DB}_staging"' in code
    assert 'f"{SILVER_DB}_stage"' in code
    assert 'f"{SILVER_DB}_staging"' in code
    show_databases_call = next(
        node
        for node in ast.walk(_tree(path))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "sql"
        and isinstance(node.args[0], ast.JoinedStr)
        and "SHOW DATABASES IN" in ast.unparse(node.args[0])
    )
    assert isinstance(show_databases_call.args[0], ast.JoinedStr)


@pytest.mark.parametrize("column", ["namespace", "databaseName"])
def test_reset_verification_rejects_surviving_target_namespace(column):
    notebook = _notebook(NOTEBOOKS["reset"])
    source = next(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if "RESET COMPLETE" in "".join(cell.get("source", []))
    )
    row = SimpleNamespace(asDict=lambda: {column: "ag"})
    spark = SimpleNamespace(sql=lambda _: SimpleNamespace(collect=lambda: [row]))
    namespace = {
        "spark": spark,
        "LAKEHOUSE_NAME": "lh",
        "DATABASES_TO_DROP": ["ag", "au"],
    }
    with pytest.raises(RuntimeError, match="Reset incomplete"):
        exec(compile(source, str(NOTEBOOKS["reset"]), "exec"), namespace)
