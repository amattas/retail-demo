"""Guard: the gold loader list must cover every fact table generate_gold reads.

Regression for the ``KeyError: 'fact_marketing_attribution'`` at gold-build time: the
driver's ``GOLD_SOURCE_TABLES`` (which tables are read back from silver) drifted from the
``tables["..."]`` accesses in ``generate_gold``. This test fails closed on any such drift.
"""

import ast
import re
from pathlib import Path

UTILITY = Path(__file__).resolve().parents[1]
GOLD_PY = UTILITY / "src" / "retail_setup" / "generation" / "gold.py"
GOLD_DRIVER = UTILITY / "notebooks" / "templates" / "driver-04-gold.py"


def _tables_read_by_generate_gold() -> set[str]:
    source = GOLD_PY.read_text(encoding="utf-8")
    return set(re.findall(r'tables\[\s*["\']([A-Za-z0-9_]+)["\']\s*\]', source))


def _gold_source_tables() -> set[str]:
    tree = ast.parse(GOLD_DRIVER.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "GOLD_SOURCE_TABLES" for t in node.targets
        ):
            elements = node.value.elts if isinstance(node.value, ast.List) else []
            return {el.value for el in elements if isinstance(el, ast.Constant) and isinstance(el.value, str)}
    raise AssertionError("GOLD_SOURCE_TABLES not found in driver-04-gold.py")


def test_gold_source_tables_cover_generate_gold_reads():
    read = _tables_read_by_generate_gold()
    loaded = _gold_source_tables()
    missing = read - loaded
    assert not missing, f"GOLD_SOURCE_TABLES is missing tables read by generate_gold: {sorted(missing)}"


def test_fact_marketing_attribution_is_a_gold_source():
    # The specific table whose omission caused the build-gold KeyError.
    assert "fact_marketing_attribution" in _gold_source_tables()
