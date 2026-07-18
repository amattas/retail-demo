"""Technical-column visibility guards for the semantic model (IMP-009).

Direct Lake source parquet carries a pandas write artifact ``__index_level_0__``
that has no analytical meaning. Such technical columns must be hidden and must
never auto-sum so they cannot leak into report field lists or totals.
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

# Source columns that are pure technical artifacts and must stay hidden.
TECHNICAL_SOURCE_COLUMNS = ("__index_level_0__",)


def _artifact_column_blocks() -> list[tuple[str, str, str]]:
    """(table, column display name, column body) for technical-artifact columns."""
    found: list[tuple[str, str, str]] = []
    for path in sorted(TABLES_DIR.glob("*.tmdl")):
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(
            r"\tcolumn '?(?P<name>[^'\n]+?)'?\n(?P<body>(?:\t\t.+\n)+)", text
        ):
            body = match.group("body")
            src = re.search(r"^\t\tsourceColumn: (.+)$", body, re.MULTILINE)
            if src and src.group(1) in TECHNICAL_SOURCE_COLUMNS:
                found.append((path.stem, match.group("name"), body))
    return found


def test_technical_artifact_columns_exist() -> None:
    # Guard sanity: the artifact columns are still present in the model so the
    # visibility assertions below stay meaningful.
    assert _artifact_column_blocks(), (
        "Expected at least one technical-artifact column to guard."
    )


def test_technical_artifact_columns_are_hidden() -> None:
    offenders = [
        f"{table}.{name}"
        for table, name, body in _artifact_column_blocks()
        if "isHidden" not in body
    ]
    assert not offenders, (
        "Technical-artifact columns (e.g. __index_level_0__) must be hidden; "
        f"offenders: {offenders}"
    )


def test_technical_artifact_columns_do_not_sum() -> None:
    offenders = [
        f"{table}.{name}"
        for table, name, body in _artifact_column_blocks()
        if re.search(r"^\t\tsummarizeBy: sum$", body, re.MULTILINE)
    ]
    assert not offenders, (
        "Technical-artifact columns must not summarizeBy sum; offenders: "
        f"{offenders}"
    )
