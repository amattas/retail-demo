"""Cross-layer label-vocabulary guards for IMP-009.

Categorical label columns are produced with a fixed casing/vocabulary by the
generator and ML notebooks. DAX measures that filter on these columns must use
exactly those literals, or the filter silently matches nothing (as happened
with stockout ``Risk Level = "High"`` while the producer emits ``"HIGH"`` and
reorder ``Priority IN {"High", "Critical"}`` while the producer emits
``URGENT``/``HIGH``/``NORMAL``).

Producer references:
- ``fabric/lakehouse/12-ml-stockout-prediction.ipynb`` -> risk_level HIGH/MEDIUM/LOW
- ``utility/src/retail_setup/generation/inventory.py`` -> priority URGENT/HIGH/NORMAL
- ``fabric/lakehouse/09-ml-churn-prediction.ipynb`` -> risk_category Very Low/Low/Medium/High/Very High
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TABLES_DIR = (
    REPO_ROOT
    / "fabric"
    / "powerbi"
    / "retail_model.SemanticModel"
    / "definition"
    / "tables"
)

# (table, column display name) -> the exact set of producer label literals.
LABEL_VOCABULARIES = {
    ("stockout_risk", "Risk Level"): {"HIGH", "MEDIUM", "LOW"},
    ("fact_reorders", "Priority"): {"URGENT", "HIGH", "NORMAL"},
    ("churn_predictions", "Risk Category"): {
        "Very Low",
        "Low",
        "Medium",
        "High",
        "Very High",
    },
}

MEASURE_RE = re.compile(r"^\tmeasure '(?P<name>[^']+)' = (?P<expr>.+)$", re.MULTILINE)


def _measures(table: str) -> list[tuple[str, str]]:
    text = (TABLES_DIR / f"{table}.tmdl").read_text(encoding="utf-8")
    return [(m.group("name"), m.group("expr")) for m in MEASURE_RE.finditer(text)]


def test_label_filters_match_producer_vocabulary() -> None:
    offenders = []
    checked = 0
    for (table, column), valid in LABEL_VOCABULARIES.items():
        col_token = f"[{column}]"
        for name, expr in _measures(table):
            if col_token not in expr:
                continue
            checked += 1
            literals = re.findall(r'"([^"]*)"', expr)
            for literal in literals:
                if literal not in valid:
                    offenders.append(
                        f"{table}.{name}: {literal!r} not in {sorted(valid)}"
                    )
    assert checked, "Expected at least one label-filter measure to guard."
    assert not offenders, (
        "DAX label filters must match the producer vocabulary; offenders: "
        f"{offenders}"
    )
