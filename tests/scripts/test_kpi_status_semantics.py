"""Cross-layer KPI status-semantics guards for IMP-009.

Status enumerations (payment ``status``, ``attribution_status``, pricing
``status``) are produced by the Spark generator in canonical UPPERCASE
(e.g. ``APPROVED``/``DECLINED``/``ATTRIBUTED``/``PENDING`` -- see
``utility/src/retail_setup/generation/receipts.py`` and ``online_orders.py``).
Any KQL function/materialized view or DAX measure that compares against a
lowercase literal silently matches nothing, so these tests pin the casing
contract on both layers. They also assert that orphan ``event_date``
technical columns (raw date columns that are not dim_date relationship keys)
stay hidden to avoid ambiguous date slicers.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
KQL_DIR = REPO_ROOT / "fabric" / "kql_database"
SEMANTIC_DEFINITION = (
    REPO_ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition"
)
TABLES_DIR = SEMANTIC_DEFINITION / "tables"

# Status columns whose comparison literals must stay UPPERCASE across layers.
KQL_STATUS_COLUMNS = ("payment_status", "attribution_status", "status")
KQL_STATUS_RE = re.compile(
    r"(?<![\w])(payment_status|attribution_status|status)\s*(==|!=)\s*\"([^\"]+)\""
)
DAX_STATUS_RE = re.compile(r"\[(Status|Attribution Status)\]\s*(=|<>)\s*\"([^\"]+)\"")


def _kql_status_comparisons() -> list[tuple[Path, str, str]]:
    hits: list[tuple[Path, str, str]] = []
    for path in sorted(KQL_DIR.glob("*.kql")):
        text = path.read_text(encoding="utf-8")
        for column, _op, literal in KQL_STATUS_RE.findall(text):
            hits.append((path, column, literal))
    return hits


def _dax_status_comparisons() -> list[tuple[Path, str, str]]:
    hits: list[tuple[Path, str, str]] = []
    for path in sorted(TABLES_DIR.glob("*.tmdl")):
        text = path.read_text(encoding="utf-8")
        for column, _op, literal in DAX_STATUS_RE.findall(text):
            hits.append((path, column, literal))
    return hits


def test_kql_status_literals_are_canonical_uppercase() -> None:
    comparisons = _kql_status_comparisons()
    assert comparisons, "Expected at least one KQL status comparison to guard."
    offenders = [
        f"{path.name}: {column} == {literal!r}"
        for path, column, literal in comparisons
        if literal != literal.upper()
    ]
    assert not offenders, (
        "KQL status comparisons must use canonical UPPERCASE literals "
        f"(generator emits uppercase); offenders: {offenders}"
    )


def test_dax_status_literals_are_canonical_uppercase() -> None:
    comparisons = _dax_status_comparisons()
    assert comparisons, "Expected at least one DAX status comparison to guard."
    offenders = [
        f"{path.name}: [{column}] = {literal!r}"
        for path, column, literal in comparisons
        if literal != literal.upper()
    ]
    assert not offenders, (
        "DAX status measures must use canonical UPPERCASE literals "
        f"to match the KQL/Lakehouse contract; offenders: {offenders}"
    )


def test_payment_declined_anomaly_filter_matches_source_casing() -> None:
    # Regression for the IMP-009 bug: the payment-anomaly function filtered on
    # lowercase "declined", which never matches the uppercase source values.
    text = (KQL_DIR / "06-ml-anomaly-detection.kql").read_text(encoding="utf-8")
    assert 'status == "DECLINED"' in text
    assert 'status == "declined"' not in text


def test_orphan_payment_event_date_is_hidden() -> None:
    # fact_payments has no dim_date relationship (it inherits date context via
    # the active Payments-to-Receipts relationship), so the raw event_date
    # column must be hidden to avoid a duplicate/ambiguous date slicer.
    text = (TABLES_DIR / "fact_payments.tmdl").read_text(encoding="utf-8")
    block = re.search(
        r"\tcolumn event_date\n(?P<body>(?:\t\t.+\n)+)",
        text,
    )
    assert block, "Missing fact_payments event_date column block."
    assert "isHidden" in block.group("body"), (
        "fact_payments.event_date must be hidden (isHidden)."
    )


def _dim_date_relationship_keys() -> set[tuple[str, str]]:
    """(table, column display name) pairs that are legitimate dim_date keys."""
    text = (SEMANTIC_DEFINITION / "relationships.tmdl").read_text(encoding="utf-8")
    keys: set[tuple[str, str]] = set()
    for block in re.split(r"(?=^relationship )", text, flags=re.MULTILINE):
        to_match = re.search(r"^\ttoColumn: (.+)$", block, re.MULTILINE)
        from_match = re.search(r"^\tfromColumn: (.+)$", block, re.MULTILINE)
        if not to_match or not from_match:
            continue
        if not to_match.group(1).strip().startswith("dim_date."):
            continue
        table, _, column = from_match.group(1).strip().partition(".")
        keys.add((table.strip("'"), column.strip("'")))
    return keys


def _event_date_columns() -> list[tuple[str, str, str]]:
    """(table, column display name, column body) for every event_date column."""
    found: list[tuple[str, str, str]] = []
    for path in sorted(TABLES_DIR.glob("*.tmdl")):
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(
            r"\tcolumn '?(?P<name>[^'\n]+?)'?\n(?P<body>(?:\t\t.+\n)+)", text
        ):
            body = match.group("body")
            if re.search(r"^\t\tsourceColumn: event_date$", body, re.MULTILINE):
                found.append((path.stem, match.group("name"), body))
    return found


def test_orphan_event_date_columns_are_hidden() -> None:
    # Any fact-table column sourced from raw ``event_date`` that is NOT used as
    # a dim_date relationship key duplicates the event timestamp and produces an
    # ambiguous date slicer, so it must be hidden. Relationship-key date columns
    # (e.g. fact_receipts.'Event Date') stay visible.
    relationship_keys = _dim_date_relationship_keys()
    columns = _event_date_columns()
    assert columns, "Expected at least one event_date column to guard."
    offenders = [
        f"{table}.{name}"
        for table, name, body in columns
        if (table, name) not in relationship_keys and "isHidden" not in body
    ]
    assert not offenders, (
        "Orphan raw event_date columns (not dim_date relationship keys) must be "
        f"hidden to avoid ambiguous date slicers; offenders: {offenders}"
    )

