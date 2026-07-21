"""Required ML semantic metadata guards for IMP-008."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TABLES = (
    REPO_ROOT
    / "fabric"
    / "powerbi"
    / "retail_model.SemanticModel"
    / "definition"
    / "tables"
)
EXPECTED = {
    "demand_forecast": {
        "hidden": {"generated_at", "model_run_id", "schema_version"},
        "freshness": "Forecast Freshness (Hours)",
        "limitation": "Forecast Limitation",
    },
    "customer_segments": {
        "hidden": {"segmented_at", "model_run_id", "schema_version"},
        "freshness": "Segmentation Freshness (Hours)",
        "limitation": "Segmentation Limitation",
    },
    "churn_predictions": {
        "hidden": {
            "is_churned_actual",
            "prediction_date",
            "model_version",
            "model_run_id",
            "schema_version",
        },
        "freshness": "Churn Freshness (Hours)",
        "limitation": "Churn Limitation",
    },
    "stockout_risk": {
        "hidden": {
            "inventory_as_of",
            "predicted_at",
            "model_run_id",
            "schema_version",
        },
        "freshness": "Stockout Freshness (Hours)",
        "limitation": "Stockout Limitation",
    },
}


def _column_blocks(text: str) -> dict[str, str]:
    return {
        source.group(1): match.group("body")
        for match in re.finditer(
            r"^\tcolumn .+?\n(?P<body>.*?)(?=^\t(?:column|measure|partition) |\Z)",
            text,
            re.MULTILINE | re.DOTALL,
        )
        if (
            source := re.search(
                r"^\t\tsourceColumn: (.+)$",
                match.group("body"),
                re.MULTILINE,
            )
        )
    }


def test_required_ml_metadata_is_hidden_and_exposes_freshness_limits() -> None:
    for table_name, expected in EXPECTED.items():
        text = (TABLES / f"{table_name}.tmdl").read_text(encoding="utf-8")
        columns = _column_blocks(text)

        assert expected["hidden"] <= columns.keys()
        assert all(
            "isHidden" in columns[source_name]
            for source_name in expected["hidden"]
        )
        assert f"measure '{expected['freshness']}' = DATEDIFF(" in text
        assert "UTCNOW()" in text
        assert f"measure '{expected['limitation']}' = \"" in text


def test_churn_semantic_contract_retains_hidden_compatibility_label() -> None:
    text = (TABLES / "churn_predictions.tmdl").read_text(encoding="utf-8")

    columns = _column_blocks(text)
    compatibility = columns["is_churned_actual"]
    assert "isHidden" in compatibility
    assert "sourceColumn: is_churned_actual" in compatibility
    assert "Deprecated compatibility projection" in text
