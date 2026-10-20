"""Name enrichment must query the semantic model rather than silently stay empty."""

import ast
import functools
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


def test_name_indexes_use_semantic_model_rows():
    path = Path(__file__).resolve().parents[2] / "app" / "backend" / "main.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    assert any(
        isinstance(node, ast.ImportFrom)
        and node.level == 1
        and any(alias.name == "dax" for alias in node.names)
        for node in tree.body
    ), "The live module must bind the dax client used by the enrichment helpers"
    query = Mock(
        side_effect=[
            [
                {
                    "id": 4,
                    "number": "S000004",
                    "address": "Demo address",
                    "format": "small",
                }
            ],
            [{"id": 7, "name": "Demo product"}],
        ]
    )
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in {"_store_index", "_product_index"}
    ]
    namespace = {"dax": SimpleNamespace(query_rows=query), "functools": functools}
    exec(
        compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"),
        namespace,
    )
    assert namespace["_store_index"]()[4]["number"] == "S000004"
    assert namespace["_product_index"]()[7] == "Demo product"
    assert query.call_count == 2
