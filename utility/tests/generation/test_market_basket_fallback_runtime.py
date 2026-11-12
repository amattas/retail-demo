"""Real Spark execution of the market-basket notebook's co-occurrence fallback."""

import ast
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

pytestmark = pytest.mark.spark_no_filesystem

NOTEBOOK_PATH = (
    Path(__file__).resolve().parents[3] / "fabric" / "lakehouse" / "07-ml-market-basket.ipynb"
)


def _cooccurrence_rules():
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    source = "\n".join(
        "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
    )
    function = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == "cooccurrence_rules"
    )
    namespace = {"F": F}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(NOTEBOOK_PATH), "exec"), namespace)
    return namespace["cooccurrence_rules"]


def test_small_baskets_still_yield_valid_recommendation_rules(spark):
    # Luxury-like baskets: few items, so no pair reaches FP-Growth's thresholds.
    baskets = spark.createDataFrame(
        [
            ("r1", [1, 2]),
            ("r2", [1, 2]),
            ("r3", [1, 3]),
            ("r4", [2, 4]),
            ("r5", [3, 4]),
        ],
        "receipt_id_ext string, items array<long>",
    )

    rules = _cooccurrence_rules()(baskets, 5, 2)

    rows = {
        (row.antecedent[0], row.consequent[0]): row
        for row in rules.collect()
    }
    # Only the pair bought together twice qualifies, in both directions.
    assert set(rows) == {(1, 2), (2, 1)}
    rule = rows[(1, 2)]
    assert rule.support == pytest.approx(2 / 5)
    assert rule.confidence == pytest.approx(2 / 3)  # product 1 is in 3 baskets
    assert rule.lift == pytest.approx((2 / 3) / (3 / 5))  # product 2 is in 3 baskets
    assert all(row.lift > 0 and 0 <= row.confidence <= 1 for row in rows.values())
    assert [field.dataType.simpleString() for field in rules.schema.fields] == [
        "array<bigint>",
        "array<bigint>",
        "double",
        "double",
        "double",
    ]
