"""The manual Bronze shortcut notebook fails when required shortcuts are missing."""

import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from deploy.scripts.ensure_eventhouse_shortcuts import expected_event_tables

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "fabric" / "lakehouse" / "01-create-bronze-shortcuts.ipynb"


def _cells() -> list[str]:
    cells = json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]
    return ["".join(cell["source"]) for cell in cells if cell["cell_type"] == "code"]


def _event_tables() -> list[str]:
    for source in _cells():
        for node in ast.parse(source).body:
            if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "event_tables":
                return ast.literal_eval(node.value)
    raise AssertionError("event_tables not found")


def test_required_shortcuts_are_the_stream_event_tables():
    assert sorted(_event_tables()) == list(expected_event_tables(ROOT))


@pytest.mark.parametrize("present", [18, 17])
def test_missing_eventhouse_shortcut_fails_the_notebook(present):
    tables = _event_tables()
    verify = next(source for source in _cells() if "def missing_event_shortcuts" in source)
    rows = [SimpleNamespace(tableName=name) for name in tables[:present]]
    namespace = {
        "event_tables": tables,
        "LAKEHOUSE_NAME": "retail_lakehouse",
        "BRONZE_SCHEMA": "cusn",
        "spark": SimpleNamespace(sql=lambda _query: SimpleNamespace(collect=lambda: rows)),
    }
    if present == len(tables):
        exec(verify, namespace)  # noqa: S102 - repository notebook cell
    else:
        with pytest.raises(RuntimeError, match=tables[-1]):
            exec(verify, namespace)  # noqa: S102 - repository notebook cell
