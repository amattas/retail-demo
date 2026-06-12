"""Verify schemas.py against the semantic model's TMDL bindings.

For every Plan-2a table that exists in the model: every TMDL sourceColumn must
exist in our schema with a compatible type. (Our schema MAY have extra columns
the model doesn't bind — Direct Lake ignores them.)
"""

import re
from pathlib import Path

import pytest

from retail_setup.generation.schemas import TABLES

TMDL_DIR = (
    Path(__file__).resolve().parents[3]
    / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition" / "tables"
)

# TMDL dataType -> acceptable spark types in schemas.py
TYPE_COMPAT = {
    "int64": {"long", "int"},
    "string": {"string"},
    "double": {"double"},
    "boolean": {"boolean"},
    "dateTime": {"timestamp", "date"},
    "decimal": {"double"},
}

def parse_tmdl_columns(text: str) -> list[tuple[str, str]]:
    """Return [(sourceColumn, dataType)] for every column block in a TMDL file.

    Splits on tab-indented 'column' lines (the real TMDL format uses tabs).
    Each block is scanned for dataType and sourceColumn properties.
    """
    out = []
    blocks = re.split(r"^\tcolumn\s+", text, flags=re.MULTILINE)[1:]
    for block in blocks:
        dt = re.search(r"dataType:\s*(\w+)", block)
        sc = re.search(r"sourceColumn:\s*(\S+)", block)
        name = block.splitlines()[0].strip().strip("'")
        if dt is None:
            continue
        # DAX calculated columns have no physical sourceColumn — skip them
        if sc is None and re.search(r"^\t\t(?:expression\s*=|\s*=)", block, re.MULTILINE):
            continue
        source = sc.group(1).strip("'\"") if sc else name
        out.append((source, dt.group(1)))
    return out


@pytest.mark.parametrize("table", sorted(TABLES))
def test_schema_covers_tmdl_bindings(table):
    tmdl_path = TMDL_DIR / f"{table}.tmdl"
    if not tmdl_path.exists():
        pytest.skip(f"{table} not in semantic model")
    ours = dict(TABLES[table])
    missing, mismatched = [], []
    for source_col, tmdl_type in parse_tmdl_columns(tmdl_path.read_text()):
        if source_col not in ours:
            missing.append(source_col)
        elif tmdl_type in TYPE_COMPAT and ours[source_col] not in TYPE_COMPAT[tmdl_type]:
            mismatched.append((source_col, tmdl_type, ours[source_col]))
    assert not missing, f"{table}: TMDL binds columns we don't generate: {missing}"
    assert not mismatched, f"{table}: type mismatches (col, tmdl, ours): {mismatched}"


def test_spark_schema_builds(spark):
    from retail_setup.generation.schemas import spark_schema

    for table in TABLES:
        schema = spark_schema(table)
        assert len(schema.fields) == len(TABLES[table])


def test_no_case_insensitive_duplicate_columns():
    """Every table must have case-insensitively unique column names.

    Delta on Fabric rejects tables where two column names differ only in case
    (e.g. 'source' and 'Source'). This test is the permanent guard against
    re-introducing such pairs.
    """
    for table, cols in TABLES.items():
        names = [c for c, _ in cols]
        lower_names = [c.lower() for c in names]
        assert len(set(lower_names)) == len(names), (
            f"{table}: case-insensitive duplicate column names detected: "
            f"{[n for n in names if lower_names.count(n.lower()) > 1]}"
        )
