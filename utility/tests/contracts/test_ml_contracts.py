"""Executable ML contract and negative-drift tests for IMP-008."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from retail_setup.contracts import (
    ManifestSourceError,
    SolutionManifest,
    load_repository_manifest,
    validate_manifest_repository,
)
from retail_setup.contracts.source_parsers import notebook_ml_output_schemas

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "contracts" / "retail-demo.json"


def _document() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _contract(document: dict, table_name: str) -> dict:
    return next(
        contract
        for contract in document["ml_contracts"]
        if contract["output"]["table"] == table_name
    )


def _write_ml_notebook(
    tmp_path: Path,
    statements: str,
    *,
    validate_nulls: bool = True,
) -> Path:
    null_validation = (
        """
    non_nullable = tuple(name for name, _, nullable in contract if not nullable)
    if any(frame.filter(name).isNull() for name in non_nullable):
        raise RuntimeError("required value is null")
"""
        if validate_nulls
        else ""
    )
    code = f"""
ML_OUTPUT_CONTRACTS = {{
    "example_output": (("id", "long", False),),
}}

def validate_ml_output(frame, table_name):
    contract = ML_OUTPUT_CONTRACTS[table_name]
    expected = contract
    actual = frame.schema.fields
    if actual != expected:
        raise RuntimeError("schema mismatch")
{null_validation}
    return frame

{statements}
"""
    path = tmp_path / "producer.ipynb"
    path.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "metadata": {},
                        "source": code.splitlines(keepends=True),
                        "outputs": [],
                        "execution_count": None,
                    }
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
        ),
        encoding="utf-8",
    )
    return path


def test_ml_tier_inventory_is_exact_and_reporting_requires_only_required() -> None:
    manifest, validation = load_repository_manifest(REPO_ROOT)

    assert validation.ml_contracts.required_tables == {
        "demand_forecast",
        "customer_segments",
        "churn_predictions",
        "stockout_risk",
    }
    assert validation.ml_contracts.optional_tables == {
        "product_associations",
        "product_recommendations",
        "journey_patterns",
        "zone_transitions",
        "zone_dwell_stats",
        "dwell_predictions",
    }
    assert validation.ml_contracts.experimental_tables == {
        "price_elasticity",
        "promotion_lift",
        "pricing_constraints",
        "pricing_recommendations",
    }
    assert {
        contract.output.table
        for contract in manifest.ml_contracts
        if contract.reporting_required
    } == validation.ml_contracts.required_tables


def test_corrected_notebook_output_shapes_are_manifested() -> None:
    manifest, _ = load_repository_manifest(REPO_ROOT)
    contracts = {contract.output.table: contract for contract in manifest.ml_contracts}

    churn_fields = {
        field.name: field for field in contracts["churn_predictions"].output.fields
    }
    assert churn_fields["is_churned_actual"].data_type == "int"
    assert churn_fields["is_churned_actual"].nullable
    assert "inventory_as_of" in {
        field.name for field in contracts["stockout_risk"].output.fields
    }
    assert contracts["promotion_lift"].output.grain == (
        "store_id",
        "promo_code",
        "promo_episode_id",
        "product_id",
        "discount_type",
    )
    dwell_fields = {
        field.name for field in contracts["dwell_predictions"].output.fields
    }
    assert not {
        "actual_dwell_minutes",
        "departed_ts",
        "prediction_error_minutes",
    } & dwell_fields
    pricing_fields = {
        field.name: field
        for field in contracts["pricing_recommendations"].output.fields
    }
    assert pricing_fields["ml_confidence"].nullable
    assert {
        "upstream_evidence_status",
        "elasticity_validation_status",
        "projection_basis",
        "effective_price_change_ts",
    } <= pricing_fields.keys()
    assert pricing_fields["upstream_evidence_status"].nullable is False
    assert pricing_fields["effective_price_change_ts"].nullable


def test_required_schemas_retain_all_legacy_reporting_bindings() -> None:
    manifest, _ = load_repository_manifest(REPO_ROOT)
    contracts = {contract.output.table: contract for contract in manifest.ml_contracts}
    legacy_bindings = {
        "demand_forecast": {
            "store_id",
            "product_id",
            "forecast_date",
            "predicted_units",
            "lower_bound",
            "upper_bound",
            "mape",
            "generated_at",
        },
        "customer_segments": {
            "customer_id",
            "cluster_id",
            "segment_label",
            "recency_days",
            "frequency",
            "monetary_value",
            "avg_order_value",
            "first_purchase_date",
            "last_purchase_date",
            "segmented_at",
        },
        "churn_predictions": {
            "customer_id",
            "churn_probability",
            "churn_prediction",
            "risk_category",
            "is_churned_actual",
            "prediction_date",
            "model_version",
            "churn_window_days",
        },
        "stockout_risk": {
            "store_id",
            "product_id",
            "current_inventory",
            "demand_velocity_daily",
            "days_of_inventory",
            "demand_trend",
            "stockout_probability",
            "stockout_predicted",
            "risk",
            "ranking",
            "risk_level",
            "predicted_at",
            "forecast_horizon_days",
            "Department",
            "Category",
            "Subcategory",
        },
    }

    for table_name, legacy_fields in legacy_bindings.items():
        current_fields = {
            field.name for field in contracts[table_name].output.fields
        }
        assert legacy_fields <= current_fields


def test_contract_rejects_nullable_grain_key() -> None:
    document = _document()
    demand = _contract(document, "demand_forecast")
    demand["output"]["fields"][0]["nullable"] = True

    with pytest.raises(
        ValidationError,
        match="grain/as-of/lineage/probability fields cannot be nullable",
    ):
        SolutionManifest.model_validate(document)


def test_optional_output_cannot_be_required_by_reporting() -> None:
    document = _document()
    _contract(document, "product_associations")["reporting_required"] = True

    with pytest.raises(
        ValidationError,
        match="only required-tier ML outputs may be required by Reporting",
    ):
        SolutionManifest.model_validate(document)


def test_repository_validation_rejects_producer_schema_drift() -> None:
    document = _document()
    demand = _contract(document, "demand_forecast")
    predicted_units = next(
        field
        for field in demand["output"]["fields"]
        if field["name"] == "predicted_units"
    )
    predicted_units["data_type"] = "long"
    manifest = SolutionManifest.model_validate(document)

    with pytest.raises(
        ManifestSourceError,
        match="producer/manifest schema mismatch for 'demand_forecast'",
    ):
        validate_manifest_repository(manifest, REPO_ROOT)


@pytest.mark.parametrize(
    "semantic",
    ["grain", "as_of", "lineage", "probabilities", "horizon"],
)
def test_repository_validation_rejects_runtime_semantic_drift(
    semantic: str,
) -> None:
    document = _document()
    output = _contract(document, "demand_forecast")["output"]
    if semantic == "grain":
        output["grain"] = ["product_id", "store_id", "forecast_date"]
    elif semantic == "as_of":
        output["as_of_column"] = "forecast_date"
    elif semantic == "lineage":
        output["lineage_columns"] = [
            "model_run_id",
            "source_as_of",
            "schema_version",
        ]
    elif semantic == "probabilities":
        output["probability_columns"] = ["predicted_units"]
    else:
        output.pop("forecast_horizon_column")
        output["forecast_horizon_days"] = 14
    manifest = SolutionManifest.model_validate(document)

    with pytest.raises(
        ManifestSourceError,
        match="required ML runtime/manifest semantics mismatch for 'demand_forecast'",
    ):
        validate_manifest_repository(manifest, REPO_ROOT)


def test_notebook_contract_rejects_wrong_physical_write_target(
    tmp_path: Path,
) -> None:
    path = _write_ml_notebook(
        tmp_path,
        """
output = validate_ml_output(output, "example_output")
output.write.format("delta").mode("overwrite").saveAsTable("au.wrong_output")
""",
    )

    with pytest.raises(ValueError, match="writes 'au.wrong_output'"):
        notebook_ml_output_schemas(path)


def test_notebook_contract_rejects_wrong_physical_schema(
    tmp_path: Path,
) -> None:
    path = _write_ml_notebook(
        tmp_path,
        """
output = validate_ml_output(output, "example_output")
output.write.format("delta").mode("overwrite").saveAsTable("ag.example_output")
""",
    )

    with pytest.raises(ValueError, match="writes 'ag.example_output'"):
        notebook_ml_output_schemas(path)


def test_notebook_contract_requires_validation_immediately_before_write(
    tmp_path: Path,
) -> None:
    path = _write_ml_notebook(
        tmp_path,
        """
output = validate_ml_output(output, "example_output")
print("intervening operation")
output.write.format("delta").mode("overwrite").saveAsTable("au.example_output")
""",
    )

    with pytest.raises(ValueError, match="is not immediately written"):
        notebook_ml_output_schemas(path)


def test_notebook_contract_requires_non_nullable_value_validation(
    tmp_path: Path,
) -> None:
    path = _write_ml_notebook(
        tmp_path,
        """
output = validate_ml_output(output, "example_output")
output.write.format("delta").mode("overwrite").saveAsTable("au.example_output")
""",
        validate_nulls=False,
    )

    with pytest.raises(ValueError, match="must reject schema drift and nulls"):
        notebook_ml_output_schemas(path)


def test_required_output_cannot_drop_active_tmdl_authority() -> None:
    document = _document()
    demand = _contract(document, "demand_forecast")
    demand["sources"] = [
        source
        for source in demand["sources"]
        if source.get("selector", {}).get("kind") != "tmdl_active_table_schemas"
    ]
    manifest = SolutionManifest.model_validate(document)

    with pytest.raises(
        ManifestSourceError,
        match="must reference one active TMDL model",
    ):
        validate_manifest_repository(manifest, REPO_ROOT)
