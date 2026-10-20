"""Rule values must match the code that enforces them."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

from deploy.scripts import _ontology_rules as rules

ROOT = Path(__file__).resolve().parents[2]


def _notebook(name: str) -> str:
    cells = json.loads((ROOT / "fabric" / "lakehouse" / name).read_text(encoding="utf-8"))
    return "".join("".join(cell["source"]) for cell in cells["cells"] if cell["cell_type"] == "code")


def _module_constant(relative: str, name: str):
    tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {relative}")


@pytest.mark.parametrize(
    "name",
    [
        "MIN_MARGIN_PCT", "MAX_PRICE_FACTOR", "MAX_CHANGE_PCT_WEEKLY", "MIN_DURATION_DAYS",
        "AGE_THRESHOLD_MODERATE", "AGE_THRESHOLD_AGGRESSIVE", "AGE_MARKDOWN_MODERATE",
        "AGE_MARKDOWN_AGGRESSIVE", "INVENTORY_HIGH_THRESHOLD",
    ],
)
def test_pricing_rules_match_the_pricing_notebook(name):
    source = _notebook("14-ml-dynamic-pricing.ipynb")
    value = float(re.search(rf"^{name} = ([0-9.]+)", source, re.M).group(1))
    assert getattr(rules, name) == value


def test_return_window_matches_generator():
    assert rules.RETURN_WINDOW_DAYS == _module_constant(
        "utility/src/retail_setup/generation/returns.py", "RETURN_WINDOW_DAYS"
    )


def test_stockout_rule_matches_stockout_notebook():
    source = _notebook("12-ml-stockout-prediction.ipynb")
    assert f'"STOCKOUT_THRESHOLD", default="{rules.STOCKOUT_THRESHOLD}"' in source
    assert f'"FORECAST_HORIZON_DAYS", default="{rules.FORECAST_HORIZON_DAYS}"' in source
    assert f'>= {rules.STOCKOUT_HIGH_PROBABILITY}, "HIGH"' in source
    assert f'>= {rules.STOCKOUT_MEDIUM_PROBABILITY}, "MEDIUM"' in source
    assert '.otherwise("LOW")' in source


def test_replenishment_rule_matches_inventory_policy():
    relative = "utility/src/retail_setup/generation/inventory_policy.py"
    source = (ROOT / relative).read_text(encoding="utf-8")
    assert rules.CASE_PACK == _module_constant(relative, "CASE_PACK")
    assert f"point = max(CASE_PACK, ceil(rate * {rules.REORDER_POINT_DAYS}))" in source
    assert f"target = max(point + CASE_PACK, ceil(rate * {rules.ORDER_UP_TO_DAYS}))" in source
    assert f"closing + timedelta(days={rules.NEAR_TERM_RETURN_DAYS})" in source
    assert f'"HIGH" if on_hand < rate * {rules.HIGH_PRIORITY_DEMAND_DAYS}' in source
    assert '"URGENT" if on_hand == 0' in source


def test_churn_rule_matches_churn_notebook():
    source = _notebook("09-ml-churn-prediction.ipynb")
    assert f'"CHURN_WINDOW_DAYS", default="{rules.CHURN_WINDOW_DAYS}"' in source


def test_rendered_rules_follow_the_tmdl_shape():
    names = [rule.name for rule in rules.RULES]
    assert len(names) == len(set(names)) == 9
    text = rules.render_rule(rules.RULES[0], "tag")
    assert text.startswith(f"rule {rules.RULES[0].name}\n\tstatement: ")
    assert "\t\tpropertyScope: specific\n" in text
    assert "description" not in text and "synonym" not in text


def test_render_rule_rejects_oversized_references():
    rule = rules.Rule("Big", "x", (rules.RuleReference("E", tuple(f"p{i}" for i in range(11))),))
    with pytest.raises(ValueError, match="10 properties"):
        rules.render_rule(rule, "tag")
