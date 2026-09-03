"""Grain guards for surrogate-key columns (IMP-009).

Surrogate identifier columns (source columns ending in ``_id`` such as
``store_id``/``product_id``/``customer_id``) are foreign keys, not additive
facts. If they default to ``summarizeBy: sum`` a bare drag-and-drop produces a
meaningless key total, so they must default to ``summarizeBy: none``.
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

COLUMN_RE = re.compile(
    r"\tcolumn '?(?P<name>[^'\n]+?)'?\n(?P<body>(?:\t\t.+\n)+)"
)


def _key_columns() -> list[tuple[str, str, str]]:
    """(table, column display name, column body) for *_id-sourced columns."""
    found: list[tuple[str, str, str]] = []
    for path in sorted(TABLES_DIR.glob("*.tmdl")):
        text = path.read_text(encoding="utf-8")
        for match in COLUMN_RE.finditer(text):
            body = match.group("body")
            src = re.search(r"^\t\tsourceColumn: (.+)$", body, re.MULTILINE)
            if src and src.group(1).endswith("_id"):
                found.append((path.stem, match.group("name"), body))
    return found


def test_surrogate_key_columns_exist() -> None:
    assert _key_columns(), "Expected at least one *_id key column to guard."


def test_surrogate_key_columns_do_not_sum() -> None:
    offenders = [
        f"{table}.{name}"
        for table, name, body in _key_columns()
        if re.search(r"^\t\tsummarizeBy: sum$", body, re.MULTILINE)
    ]
    assert not offenders, (
        "Surrogate key columns (*_id) must not summarizeBy sum; offenders: "
        f"{offenders}"
    )
