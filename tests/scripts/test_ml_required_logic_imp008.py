"""Deterministic IMP-008 contracts for the required ML notebooks."""

from __future__ import annotations

import ast
import json
import math
from datetime import date, timedelta
from functools import reduce
from pathlib import Path
from types import FunctionType, SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS = {
    "demand": REPO_ROOT / "fabric" / "lakehouse" / "06-ml-demand-forecast.ipynb",
    "segments": (
        REPO_ROOT / "fabric" / "lakehouse" / "08-ml-customer-segmentation.ipynb"
    ),
    "churn": REPO_ROOT / "fabric" / "lakehouse" / "09-ml-churn-prediction.ipynb",
    "stockout": (
        REPO_ROOT / "fabric" / "lakehouse" / "12-ml-stockout-prediction.ipynb"
    ),
    "validator": (
        REPO_ROOT
        / "fabric"
        / "lakehouse"
        / "15-validate-required-ml-contract.ipynb"
    ),
}


def _notebook(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _code(path: Path) -> str:
    return "\n".join(
        "".join(cell.get("source", []))
        for cell in _notebook(path)["cells"]
        if cell.get("cell_type") == "code"
    )


def _function(
    path: Path,
    function_name: str,
    namespace: dict[str, Any] | None = None,
) -> FunctionType:
    for cell in _notebook(path)["cells"]:
        if cell.get("cell_type") != "code":
            continue
        tree = ast.parse("".join(cell.get("source", [])))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                module = ast.Module(body=[node], type_ignores=[])
                ast.fix_missing_locations(module)
                globals_dict = dict(namespace or {})
                exec(compile(module, str(path), "exec"), globals_dict)
                return globals_dict[function_name]
    raise AssertionError(f"{function_name} was not found in {path.name}")


@pytest.mark.parametrize("path", NOTEBOOKS.values(), ids=NOTEBOOKS.keys())
def test_notebook_json_and_python_cells_are_valid(path: Path) -> None:
    notebook = _notebook(path)
    assert notebook["nbformat"] == 4
    for index, cell in enumerate(notebook["cells"]):
        if cell.get("cell_type") == "code":
            ast.parse(
                "".join(cell.get("source", [])),
                filename=f"{path.name}:cell-{index}",
            )


def test_demand_uses_dense_rolling_origins_and_recursive_state() -> None:
    path = NOTEBOOKS["demand"]
    code = _code(path)
    boundaries = _function(
        path,
        "rolling_origin_boundaries",
        {"timedelta": timedelta},
    )

    assert boundaries(date(2026, 7, 20), 3) == {
        "train_end": date(2026, 7, 14),
        "calibration_start": date(2026, 7, 15),
        "calibration_end": date(2026, 7, 17),
        "test_start": date(2026, 7, 18),
        "test_end": date(2026, 7, 20),
    }
    assert "def build_dense_daily_calendar" in code
    assert "F.sequence(" in code
    assert 'subset=["units_sold", "revenue"]' in code
    assert "def forecast_recursively" in code
    assert 'F.col("predicted_units").alias("units_sold")' in code
    assert "def calibration_residual_quantiles" in code
    assert "F.percentile_approx(" in code
    assert "FORECAST_INTERVAL_Z" not in code
    assert "F.lit(float(overall_mape))" in code
    assert "source_as_of_timestamp" in code
    assert "datetime.now" not in code
    assert 'F.current_timestamp()' in code
    assert (
        "df_evaluation_base, df_evaluation_cohort = "
        "build_dense_daily_calendar(\n"
        "    df_daily_sales,\n"
        "    source_as_of_date,\n"
        "    train_end_date,"
    ) in code
    assert (
        "df_inference_base, df_inference_cohort = "
        "build_dense_daily_calendar(\n"
        "    df_daily_sales,\n"
        "    source_as_of_date,\n"
        "    source_as_of_date,"
    ) in code
    assert "add_demand_features(df_evaluation_base)" in code
    assert "    df_inference_base,\n    forecast_start_date," in code

    mape = _function(path, "mape_ratio_column")
    mape_source = ast.unparse(ast.parse(_function_source(path, mape.__name__)))
    assert " * 100" not in mape_source


def _function_source(path: Path, function_name: str) -> str:
    for cell in _notebook(path)["cells"]:
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"{function_name} was not found in {path.name}")


def _literal_assignment(path: Path, name: str) -> Any:
    for cell in _notebook(path)["cells"]:
        if cell.get("cell_type") != "code":
            continue
        tree = ast.parse("".join(cell.get("source", [])))
        for node in tree.body:
            if (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(target, ast.Name) and target.id == name
                    for target in node.targets
                )
            ):
                return ast.literal_eval(node.value)
    raise AssertionError(f"{name} was not found in {path.name}")


def test_segmentation_is_source_anchored_and_canonical() -> None:
    path = NOTEBOOKS["segments"]
    code = _code(path)
    canonical_mapping = _function(path, "canonical_cluster_mapping")
    select_best_k = _function(path, "select_best_k")

    assert canonical_mapping([(2.0, 0.0), (1.0, 9.0), (1.0, 2.0)]) == {
        2: 0,
        1: 1,
        0: 2,
    }
    assert select_best_k([(5, 0.4), (4, 0.4), (6, 0.3)]) == (4, 0.4)
    assert 'F.max(F.col("event_ts").cast("timestamp"))' in code
    assert 'F.lit(analysis_timestamp).cast("timestamp")' in code
    assert "datetime.now" not in code
    assert "seed=RANDOM_SEED" in code
    assert '.orderBy("customer_id")' in code
    assert 'assert_unique_keys(df_output, ["customer_id"]' in code


def test_churn_snapshots_labels_and_probabilities_are_non_leaky() -> None:
    path = NOTEBOOKS["churn"]
    code = _code(path)
    boundaries = _function(
        path,
        "chronological_split_boundaries",
        {"timedelta": timedelta},
    )
    dates = [date(2026, 1, day) for day in range(1, 11)]

    assert boundaries(dates) == {
        "train_end": date(2026, 1, 6),
        "calibration_start": date(2026, 1, 7),
        "calibration_end": date(2026, 1, 8),
        "test_start": date(2026, 1, 9),
    }
    purged = boundaries(
        [date(2025, 1, 1) + timedelta(days=offset) for offset in range(400)],
        embargo_days=90,
    )
    assert purged["calibration_start"] > purged["train_end"] + timedelta(days=90)
    assert purged["test_start"] > purged["calibration_end"] + timedelta(days=90)
    feature_source = _function_source(path, "build_feature_snapshots")
    label_source = _function_source(path, "attach_forward_churn_labels")
    assert '< F.col("snapshot.snapshot_date")' in feature_source
    assert '>= F.col("snapshot.snapshot_date")' in label_source
    assert "F.date_add(" in label_source
    assert "days_since_last_purchase" not in code
    assert "IsotonicRegression(" in code
    assert "def score_with_calibration" in code
    assert "calibrated_probability" in code
    assert '"label_available_date"' in label_source
    assert (
        'F.lit(None).cast("int").alias("is_churned_actual")'
        in code
    )
    assert 'assert_unique_keys(df_final, ["customer_id"]' in code


def test_stockout_is_eod_future_labeled_and_calibrated() -> None:
    path = NOTEBOOKS["stockout"]
    code = _code(path)
    boundaries = _function(
        path,
        "chronological_split_boundaries",
        {"timedelta": timedelta},
    )
    dates = [date(2026, 2, day) for day in range(1, 11)]

    assert boundaries(dates) == {
        "train_end": date(2026, 2, 6),
        "calibration_start": date(2026, 2, 7),
        "calibration_end": date(2026, 2, 8),
        "test_start": date(2026, 2, 9),
    }
    purged = boundaries(
        [date(2026, 1, 1) + timedelta(days=offset) for offset in range(30)],
        embargo_days=3,
    )
    assert purged["calibration_start"] > purged["train_end"] + timedelta(days=3)
    assert purged["test_start"] > purged["calibration_end"] + timedelta(days=3)
    eod_source = _function_source(path, "build_end_of_day_inventory")
    label_source = _function_source(path, "attach_future_stockout_labels")
    assert '"store_id", "product_id", "event_date"' in eod_source
    assert "F.row_number()" in eod_source
    assert '> F.col("snapshot.snapshot_date")' in label_source
    assert '"label_available_date"' in label_source
    assert "IsotonicRegression(" in code
    assert '"raw_stockout_score"' in code
    assert 'F.col("calibrated_probability")' in code
    assert 'F.col("inventory_as_of").cast("timestamp")' in code
    assert '["store_id", "product_id", "forecast_horizon_days"]' in code


def test_required_outputs_separate_generation_time_from_source_as_of() -> None:
    expected_source_columns = {
        "demand": "source_as_of",
        "segments": "segmented_at",
        "churn": "prediction_date",
        "stockout": "predicted_at",
    }
    for name, source_column in expected_source_columns.items():
        code = _code(NOTEBOOKS[name])
        contracts = _literal_assignment(NOTEBOOKS[name], "ML_OUTPUT_CONTRACTS")
        fields = next(iter(contracts.values()))
        field_names = [field[0] for field in fields]

        assert source_column in field_names
        assert "generated_at" in field_names
        assert "model_run_id" in field_names
        assert "F.current_timestamp()" in code
        assert '"overwriteSchema", "true"' in code

    rules = _literal_assignment(NOTEBOOKS["validator"], "REQUIRED_ML_RULES")
    for table, source_column in {
        "demand_forecast": "source_as_of",
        "customer_segments": "segmented_at",
        "churn_predictions": "prediction_date",
        "stockout_risk": "predicted_at",
    }.items():
        assert rules[table]["as_of"] == "generated_at"
        assert source_column in rules[table]["lineage"]
        assert "model_run_id" in rules[table]["lineage"]


def test_required_validator_rejects_incompatible_schema() -> None:
    path = NOTEBOOKS["validator"]
    contracts = _literal_assignment(path, "ML_OUTPUT_CONTRACTS")
    normalize = _function(path, "_normalize_ml_type")
    validate_schema = _function(
        path,
        "_validate_schema",
        {
            "ML_OUTPUT_CONTRACTS": contracts,
            "_normalize_ml_type": normalize,
        },
    )

    fields = [
        SimpleNamespace(
            name=name,
            dataType=SimpleNamespace(simpleString=lambda value=data_type: value),
        )
        for name, data_type, _ in contracts["demand_forecast"]
    ]
    validate_schema(SimpleNamespace(schema=SimpleNamespace(fields=fields)), "demand_forecast")

    with pytest.raises(RuntimeError, match="Required ML schema mismatch"):
        validate_schema(
            SimpleNamespace(schema=SimpleNamespace(fields=fields[:-1])),
            "demand_forecast",
        )


def test_required_validator_declares_all_fail_closed_checks() -> None:
    code = _code(NOTEBOOKS["validator"])

    assert "Required ML table is missing" in code
    assert "Required ML table is empty" in code
    assert "_any_null(required_values)" in code
    assert "_any_null(non_nullable_outputs)" in code
    assert 'groupBy(*rule["grain"]).count()' in code
    assert "contains duplicate grain keys" in code
    assert "_any_non_finite(floating_outputs)" in code
    assert "contains NaN or infinite floating-point values" in code
    assert "is null or outside [0, 1]" in code
    assert "incomplete or inconsistent forecast horizon" in code


@pytest.mark.parametrize(
    "invalid_value",
    [float("nan"), float("inf"), float("-inf")],
    ids=["nan", "positive-infinity", "negative-infinity"],
)
def test_required_validator_detects_every_non_finite_float(
    invalid_value: float,
) -> None:
    class FakeColumn:
        def __init__(self, value: float) -> None:
            self.value = value

        def isin(self, *values: float) -> bool:
            return self.value in values

    class FakeFunctions:
        values = {"metric": invalid_value}

        @classmethod
        def col(cls, name: str) -> FakeColumn:
            return FakeColumn(cls.values[name])

        @staticmethod
        def isnan(column: FakeColumn) -> bool:
            return math.isnan(column.value)

    any_non_finite = _function(
        NOTEBOOKS["validator"],
        "_any_non_finite",
        {"F": FakeFunctions, "reduce": reduce},
    )

    assert any_non_finite(("metric",))
    FakeFunctions.values["metric"] = 1.25
    assert not any_non_finite(("metric",))
