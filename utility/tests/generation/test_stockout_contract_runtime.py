"""Exercise the stockout producer and its downstream domain gate together."""

import ast
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

pytestmark = pytest.mark.spark_no_filesystem
ROOT = Path(__file__).resolve().parents[3]
PRODUCER = ROOT / "fabric" / "lakehouse" / "12-ml-stockout-prediction.ipynb"
GATE = ROOT / "fabric" / "lakehouse" / "15-validate-required-ml-contract.ipynb"


def _tree(path):
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return ast.parse(
        "\n".join(
            "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
        )
    )


def _functions(path, names):
    nodes = [
        node
        for node in _tree(path).body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    namespace = {"F": F, "Window": Window, "LOOKBACK_DAYS": 30}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


def _inventory(spark):
    return spark.sql("""
        SELECT 1L AS store_id, 1L AS product_id,
               timestamp'2026-09-01 18:00:00' AS event_ts,
               date'2026-09-01' AS event_date, 'latest' AS trace_id, -7.0 AS balance
        UNION ALL SELECT 1L, 1L, timestamp'2026-09-01 08:00:00',
               date'2026-09-01', 'earlier', 20.0
        UNION ALL SELECT 1L, 2L, timestamp'2026-09-01 18:00:00',
               date'2026-09-01', 'positive', 6.0
        UNION ALL SELECT 1L, 3L, timestamp'2026-09-01 18:00:00',
               date'2026-09-01', 'zero', 0.0
    """)


def test_signed_ledger_produces_nonnegative_features_accepted_by_real_gate(spark):
    producer = _functions(PRODUCER, {"build_end_of_day_inventory", "build_snapshot_features"})
    validator = _functions(GATE, {"_validate_stockout_risk", "_fail_if"})
    inventory = _inventory(spark)
    snapshots = producer["build_end_of_day_inventory"](inventory)
    sales = spark.sql("""
        SELECT 1L AS store_id, 1L AS product_id, 30L AS quantity,
               timestamp'2026-09-01 16:00:00' AS sales_event_ts,
               date'2026-09-01' AS sales_event_date
    """)
    products = spark.range(1, 4).select(F.col("id").alias("product_id"))
    features = producer["build_snapshot_features"](snapshots, sales, products)
    output = (
        features.withColumn("stockout_predicted", F.lit(1))
        .withColumn("forecast_horizon_days", F.lit(3))
        .withColumn("ranking", F.lit(1))
        .withColumn("predicted_at", F.to_timestamp(F.lit("2026-09-01 18:00:00")))
        .withColumn("generated_at", F.to_timestamp(F.lit("2026-09-02 08:00:00")))
    ).cache()
    try:
        validator["_validate_stockout_risk"](output)
        rows = {row.product_id: row for row in output.collect()}
        assert rows[1].current_inventory == 0
        assert rows[1].days_of_inventory == 0
        assert rows[2].current_inventory == 6
        assert rows[3].current_inventory == 0
        assert inventory.filter("balance < 0").count() == 1
    finally:
        output.unpersist()


def test_unknown_balance_remains_unknown_and_is_rejected_before_training(spark):
    inventory = _inventory(spark).withColumn("balance", F.lit(None).cast("double"))
    producer = _functions(PRODUCER, {"build_end_of_day_inventory"})
    assert (
        producer["build_end_of_day_inventory"](inventory)
        .filter(F.col("current_inventory").isNotNull())
        .isEmpty()
    )
    tree = _tree(PRODUCER)
    index = next(
        index
        for index, node in enumerate(tree.body)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "null_balance_count"
            for target in node.targets
        )
    )
    namespace = {"inventory_history": inventory, "F": F, "INVENTORY_TXN_TABLE": "inventory"}
    with pytest.raises(RuntimeError, match="contains null balances"):
        exec(
            compile(
                ast.Module(body=tree.body[index : index + 2], type_ignores=[]),
                str(PRODUCER),
                "exec",
            ),
            namespace,
        )


def test_stockout_gate_reports_which_constraints_failed(spark):
    validator = _functions(GATE, {"_validate_stockout_risk", "_fail_if"})
    invalid = spark.sql("""
        SELECT -7L AS current_inventory, -7.0 AS days_of_inventory,
               -1.0 AS demand_velocity_daily, 7 AS stockout_predicted,
               0 AS forecast_horizon_days, 0 AS ranking,
               timestamp'2026-09-01 19:00:00' AS inventory_as_of,
               timestamp'2026-09-01 18:00:00' AS predicted_at,
               timestamp'2026-09-01 17:00:00' AS generated_at
    """)
    with pytest.raises(RuntimeError) as error:
        validator["_validate_stockout_risk"](invalid)
    for name in (
        "negative_current_inventory",
        "negative_days_of_inventory",
        "negative_demand_velocity",
        "invalid_class",
        "invalid_horizon",
        "invalid_ranking",
        "inventory_after_prediction",
        "prediction_after_generation",
    ):
        assert f"{name}=1" in str(error.value)
