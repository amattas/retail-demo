"""Date-key visibility guards for IMP-009.

Redundant date foreign-key columns on facts and daily aggregates must be hidden
so report authors slice dates through the ``dim_date`` dimension instead of a
per-table date column (which fragments cross-table filtering). Columns that are
genuinely referenced by report visuals (``Forecast Date``) or carry sub-day
detail (``Timestamp``) stay visible.
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

# Redundant date FK columns that must be hidden.
HIDDEN_DATE_KEYS = {
    "fact_foot_traffic": "Event Date",
    "fact_marketing_attribution": "Event Date",
    "fact_online_order_headers": "Event Date",
    "fact_payments": "Event Date",
    "fact_receipts": "Event Date",
    "fact_reorders": "Event Date",
    "fact_stockouts": "Event Date",
    "fact_store_ops": "Event Date",
    "campaign_performance_daily": "Day",
    "marketing_cost_daily": "Day",
    "online_sales_daily": "Day",
    "tender_mix_daily": "Day",
    "truck_dwell_daily": "Day",
}

REQUIRED_ACTIVE_DATE_RELATIONSHIPS = {
    "Payments to Date": ("fact_payments", "Event Date"),
    "Reorders to Date": ("fact_reorders", "Event Date"),
    "Stockouts to Date": ("fact_stockouts", "Event Date"),
}


def _column_block(table: str, column: str) -> list[str]:
    lines = (TABLES_DIR / f"{table}.tmdl").read_text(encoding="utf-8").splitlines()
    hdr = re.compile(rf"^\tcolumn '?{re.escape(column)}'?\s*$")
    for i, line in enumerate(lines):
        if hdr.match(line):
            block = [line]
            j = i + 1
            while j < len(lines) and (lines[j].startswith("\t\t") or not lines[j].strip()):
                block.append(lines[j])
                j += 1
            return block
    raise AssertionError(f"column {column!r} not found in {table}.tmdl")


def _relationship_block(name: str) -> str:
    path = TABLES_DIR.parent / "relationships.tmdl"
    text = path.read_text(encoding="utf-8")
    match = re.search(
        rf"^relationship '{re.escape(name)}'\n(?P<body>(?:\t.+\n)+)",
        text,
        re.MULTILINE,
    )
    assert match, f"relationship {name!r} not found"
    return match.group("body")


def test_redundant_date_keys_hidden() -> None:
    missing = []
    for table, column in HIDDEN_DATE_KEYS.items():
        block = _column_block(table, column)
        if not any(line.strip() == "isHidden" for line in block):
            missing.append(f"{table}.{column}")
    assert not missing, f"Redundant date-FK columns must be hidden: {missing}"


def test_event_facts_have_active_date_relationships() -> None:
    for relationship, (table, column) in REQUIRED_ACTIVE_DATE_RELATIONSHIPS.items():
        column_block = _column_block(table, column)
        assert any(
            line.strip() == "dataType: dateTime" for line in column_block
        )
        assert any(
            line.strip() == "sourceColumn: event_date" for line in column_block
        )
        assert any(line.strip().startswith("lineageTag: ") for line in column_block)

        block = _relationship_block(relationship)
        assert "\tisActive: false\n" not in block
        assert f"\tfromColumn: {table}.'{column}'\n" in block
        assert "\ttoColumn: dim_date.Date\n" in block
