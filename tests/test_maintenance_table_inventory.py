"""Notebook 05 maintains exactly the contract's Silver, Gold and ML tables."""

import ast
import json
from pathlib import Path

from retail_setup.generation.gold import GOLD_TABLES
from retail_setup.generation.schemas import TABLES

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "fabric" / "lakehouse" / "05-maintain-delta-tables.ipynb"
# Tables in the Silver schema that are owned outside schemas.TABLES.
SILVER_CONTROL_TABLES = {
    "_watermarks": "fabric/lakehouse/03-streaming-to-silver.ipynb",
    "_stream_state_stores": "utility/src/retail_setup/generation/stream_state.py",
    "_stream_state_visits": "utility/src/retail_setup/generation/stream_state.py",
    "_stream_state_meta": "utility/src/retail_setup/generation/stream_state.py",
    "fact_online_order_status": "fabric/lakehouse/03-streaming-to-silver.ipynb",
    "setup_run_log": "fabric/lakehouse/04-streaming-to-gold.ipynb",
}


def _maintenance_lists() -> dict[str, list[tuple[str, list[str] | None]]]:
    cells = json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]
    lists = {}
    for cell in cells:
        if cell["cell_type"] != "code":
            continue
        for node in ast.parse("".join(cell["source"])).body:
            if (
                isinstance(node, ast.Assign)
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id in {"silver_tables", "gold_tables"}
            ):
                lists[node.targets[0].id] = ast.literal_eval(node.value)
    return lists


def _ml_outputs() -> dict[str, set[str]]:
    manifest = json.loads((ROOT / "contracts" / "retail-demo.json").read_text(encoding="utf-8"))
    return {
        contract["output"]["table"]: {field["name"] for field in contract["output"]["fields"]}
        for contract in manifest["ml_contracts"]
    }


def test_silver_list_is_the_contract_plus_owned_control_tables():
    silver = dict(_maintenance_lists()["silver_tables"])
    expected = {name for name in TABLES if name not in GOLD_TABLES} | set(SILVER_CONTROL_TABLES)
    assert set(silver) == expected
    for table, owner in SILVER_CONTROL_TABLES.items():
        assert table in (ROOT / owner).read_text(encoding="utf-8")


def test_gold_list_is_every_gold_aggregate_and_ml_output():
    gold = dict(_maintenance_lists()["gold_tables"])
    assert set(gold) == set(GOLD_TABLES) | set(_ml_outputs())


def test_zorder_columns_exist_in_their_tables():
    columns = {name: {column for column, _ in fields} for name, fields in TABLES.items()}
    columns |= _ml_outputs()
    lists = _maintenance_lists()
    for table, zorder in lists["silver_tables"] + lists["gold_tables"]:
        if zorder and table in columns:
            assert set(zorder) <= columns[table], (table, zorder)
