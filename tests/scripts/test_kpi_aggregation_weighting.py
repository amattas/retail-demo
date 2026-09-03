"""Aggregation-weighting guards for IMP-009.

Pre-computed per-row averages (source columns like ``avg_basket`` /
``avg_dwell``) must never auto-sum, and roll-up "average" measures must be
volume-weighted recomputations rather than a naive ``AVERAGE`` of per-row
averages (which weights every grain row equally regardless of volume). These
tests pin both properties on the Direct Lake aggregate tables.
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

PRECOMPUTED_AVG_NAME_RE = re.compile(r"^(Average|Avg)\b")


def _columns() -> list[tuple[str, str, str, str]]:
    """(table, column display name, sourceColumn, column body)."""
    found: list[tuple[str, str, str, str]] = []
    for path in sorted(TABLES_DIR.glob("*.tmdl")):
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(
            r"\tcolumn '?(?P<name>[^'\n]+?)'?\n(?P<body>(?:\t\t.+\n)+)", text
        ):
            body = match.group("body")
            src = re.search(r"^\t\tsourceColumn: (.+)$", body, re.MULTILINE)
            found.append(
                (path.stem, match.group("name"), src.group(1) if src else "", body)
            )
    return found


def _measure_expression(table: str, measure: str) -> str:
    text = (TABLES_DIR / f"{table}.tmdl").read_text(encoding="utf-8")
    match = re.search(
        rf"^\tmeasure '{re.escape(measure)}' = (.+)$", text, re.MULTILINE
    )
    assert match, f"Missing measure {measure!r} on {table}."
    return match.group(1)


def test_precomputed_average_columns_do_not_sum() -> None:
    # A stored per-row average summed across rows is meaningless; such columns
    # must use summarizeBy: none so a bare drag-and-drop cannot roll them up.
    offenders = []
    for table, name, src, body in _columns():
        is_avg = src.startswith("avg_") or bool(PRECOMPUTED_AVG_NAME_RE.match(name))
        if is_avg and re.search(r"^\t\tsummarizeBy: sum$", body, re.MULTILINE):
            offenders.append(f"{table}.{name}")
    assert not offenders, (
        "Pre-computed average columns must not summarizeBy sum; offenders: "
        f"{offenders}"
    )


def test_rollup_average_measures_are_volume_weighted() -> None:
    # Each measure must recompute from base totals (DIVIDE) and must not be a
    # naive AVERAGE of the stored per-row average column.
    expectations = {
        ("sales_minute_store", "Avg Store Basket"): {
            "required": ["DIVIDE(", "[Total Store Sales]", "[Total Store Receipts]"],
            "forbidden": "AVERAGE('sales_minute_store'[Average Basket])",
        },
        ("zone_dwell_minute", "Avg Zone Dwell"): {
            "required": ["DIVIDE(", "SUMX(", "[Customers]", "[Zone Customers]"],
            "forbidden": "AVERAGE('zone_dwell_minute'[Average Dwell])",
        },
        ("truck_dwell_daily", "Avg Truck Dwell Minutes"): {
            "required": ["DIVIDE(", "SUMX(", "[Trucks]", "[Total Trucks]"],
            "forbidden": "AVERAGE('truck_dwell_daily'[Average Dwell Minutes])",
        },
    }
    for (table, measure), spec in expectations.items():
        expr = _measure_expression(table, measure)
        for token in spec["required"]:
            assert token in expr, f"{table}.{measure} must contain {token!r}: {expr}"
        assert spec["forbidden"] not in expr, (
            f"{table}.{measure} must not naively average the stored column."
        )
