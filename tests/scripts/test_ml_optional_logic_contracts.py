from __future__ import annotations

import ast
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK_DIR = REPO_ROOT / "fabric" / "lakehouse"
NOTEBOOKS = {
    "market_basket": "07-ml-market-basket.ipynb",
    "promotion": "10-ml-promotion-effectiveness.ipynb",
    "journey": "11-ml-journey-analysis.ipynb",
    "delivery": "13-ml-delivery-prediction.ipynb",
    "pricing": "14-ml-dynamic-pricing.ipynb",
}


def _notebook(name: str) -> dict[str, object]:
    return json.loads((NOTEBOOK_DIR / NOTEBOOKS[name]).read_text(encoding="utf-8"))


def _source(name: str) -> str:
    notebook = _notebook(name)
    return "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )


def _literal_assignment(source: str, name: str) -> object:
    tree = ast.parse(source)
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if any(isinstance(target, ast.Name) and target.id == name for target in targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"Assignment {name!r} not found")


def _load_function(source: str, name: str):
    tree = ast.parse(source)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    namespace: dict[str, object] = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "<notebook-helper>", "exec"), namespace)
    return namespace[name]


def test_optional_ml_notebooks_are_valid_json_and_python() -> None:
    for notebook_name, filename in NOTEBOOKS.items():
        notebook = _notebook(notebook_name)
        assert notebook["nbformat"] == 4, filename
        for index, cell in enumerate(notebook["cells"]):
            if cell.get("cell_type") == "code":
                compile(
                    "".join(cell.get("source", [])),
                    f"{filename}:cell-{index}",
                    "exec",
                )


def test_market_basket_uses_union_frequency_and_replaces_empty_outputs() -> None:
    source = _source("market_basket")

    assert "canonicalize_itemset(F.collect_set(\"product_id\"))" in source
    assert "canonicalize_itemset(F.array_union(" in source
    assert '(F.size(F.col("antecedent")) == 1)' in source
    assert '(F.size(F.col("consequent")) == 1)' in source
    assert 'F.col("rule_itemset_freq") / F.lit(float(total_baskets))' in source
    assert 'F.col("confidence").alias("support")' not in source
    assert '.agg(F.max("support")' not in source
    assert '.agg(F.max("confidence")' not in source
    assert '.agg(F.max("lift")' not in source
    assert "model = None" in source
    assert "recommendations_table, recommendations_count = save_gold(" in source
    assert "Skipping: no rules to create recommendations" not in source


def test_promotion_logic_uses_exact_grain_calendar_and_correct_ols_se() -> None:
    source = _source("promotion")
    episode_key_cols = _literal_assignment(source, "episode_key_cols")
    contracts = _literal_assignment(source, "ML_OUTPUT_CONTRACTS")
    promotion_fields = {
        name: data_type
        for name, data_type, _ in contracts["promotion_lift"]
    }

    assert episode_key_cols == [
        "store_id",
        "promo_code",
        "discount_type",
        "promo_episode_id",
        "product_id",
    ]
    assert 'F.col("promo.line_num") == F.col("sales.line_num")' in source
    assert 'F.col("promo.store_id") == F.col("sales.store_id")' in source
    assert "F.explode(F.sequence(" in source
    assert 'F.count(F.lit(1)).alias(f"{prefix}_calendar_days")' in source
    assert promotion_fields["promo_episode_id"] == "string"
    assert (
        'F.col("ext_cents").cast("double")\n'
        '            / (F.col("quantity").cast("double") * F.lit(100.0))'
        in source
    )
    assert '"baseline_start_date") >= F.col("first_observed_date")' in source
    assert '"post_promo_end_date") <= F.col("last_observed_date")' in source
    assert "excluded_incomplete_periods" in source
    assert (
        'F.sqrt((F.col("sse") / (F.col("n_observations") - F.lit(2.0))) '
        '/ F.col("sxx"))'
    ) in source
    assert "OBSERVATIONAL_CALENDAR_COMPARISON" in source
    assert "observed_promo_lift_pct" in source
    assert "incremental_lift_pct" not in source
    assert "cannibalization_pct" not in source
    assert "roi_category" not in source


def test_journey_receipts_require_resolved_identity_and_single_assignment() -> None:
    source = _source("journey")

    assert 'CUSTOMERS_TABLE = get_env("CUSTOMERS_TABLE", default="dim_customers")' in source
    assert '"BLEId", "CustomerBLEId", "customer_ble_id"' in source
    assert 'F.col("paths.customer_id") == F.col("receipts.customer_id")' in source
    assert 'F.col("paths.store_id") == F.col("receipts.store_id")' in source
    assert 'Window.partitionBy("receipt_id_ext").orderBy(' in source
    assert 'F.col("receipt_assignment_rank") == 1' in source
    assert "unmatched_identity_count" in source
    assert "Customer identifiers may not align across sources" not in source


def test_delivery_logic_is_chronological_calibrated_and_inference_only() -> None:
    source = _source("delivery")
    output_cols = set(_literal_assignment(source, "inference_output_cols"))

    assert "randomSplit" not in source
    assert 'BRONZE_SCHEMA = get_env("BRONZE_SCHEMA", default="cusn")' in source
    assert "read_bronze(TRUCK_ARRIVED_TABLE)" in source
    assert "read_bronze(TRUCK_DEPARTED_TABLE)" in source
    assert "FACT_TRUCK_MOVES_TABLE" not in source
    assert (
        'F.col("history.history_available_ts") < F.col("target.arrived_ts")'
        in source
    )
    assert 'Window.orderBy(F.col("label_available_ts")' in source
    assert "timedelta(hours=SPLIT_PURGE_HOURS)" in source
    assert "df_calibration_pred = model_point.transform(df_calibration)" in source
    assert "calibration_residuals_df = df_calibration_pred.select(" in source
    assert 'how="left_anti"' in source
    assert {"departed_ts", "dwell_minutes", "prediction_error"}.isdisjoint(output_cols)
    assert {
        "shipment_id",
        "arrived_ts",
        "predicted_dwell_minutes",
        "lower_bound_minutes",
        "upper_bound_minutes",
    }.issubset(output_cols)
    assert source.index("if inference_ready_count == 0:") < source.index(
        'save_gold(df_predictions, OUTPUT_TABLE)'
    )
    assert "existing output is retained" in source


def test_delivery_split_helper_is_deterministic() -> None:
    split = _load_function(_source("delivery"), "chronological_split_boundaries")

    assert split(10, 0.2, 0.2) == (6, 8)
    assert split(3, 0.1, 0.2) == (1, 2)


def test_dynamic_pricing_propagates_evidence_without_numeric_confidence() -> None:
    promotion_source = _source("promotion")
    pricing_source = _source("pricing")
    required_evidence = _literal_assignment(
        pricing_source, "required_elasticity_evidence_columns"
    )

    assert required_evidence <= {
        "product_id",
        "elasticity_coefficient",
        "elasticity_category",
        "confidence_interval_lower",
        "confidence_interval_upper",
        "standard_error",
        "r_squared",
        "n_observations",
        "elasticity_evidence_status",
        "analysis_design",
    }
    assert all(f'"{column}"' in promotion_source for column in required_evidence)
    assert "10-ml-promotion-effectiveness.ipynb" in pricing_source
    assert "EXPERIMENTAL_NOT_FOR_AUTOMATED_PRICING" in pricing_source
    assert "elasticity_evidence_valid" in pricing_source
    assert "elasticity_standard_error" in pricing_source
    assert "elasticity_r_squared" in pricing_source
    assert 'F.lit("NO_ELASTICITY_ESTIMATE")' in pricing_source
    assert (
        'F.when(F.col("accepted_price_change"), F.col("recommendation_ts"))'
        in pricing_source
    )
    assert '.otherwise(F.col("previous_effective_price_change_ts"))' in pricing_source
    assert ").localCheckpoint(eager=True)" in pricing_source
    assert (
        'F.col("recommended_price") / F.col("SalePrice"),\n'
        '                F.col("elasticity_coefficient"),\n'
        '            ) - F.lit(1.0)'
        in pricing_source
    )
    assert 'F.lit(None).cast("double").alias("ml_confidence")' in pricing_source
    assert "confidence_score" not in pricing_source
    assert "confidence_level" not in pricing_source
    assert "F.lit(0.85)" not in pricing_source
    assert "F.lit(0.60)" not in pricing_source
    assert "F.lit(0.35)" not in pricing_source
