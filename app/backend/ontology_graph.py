"""Build the ontology graph: entity nodes (live from the MCP) + relationship
edges (parsed from the ``30-create-ontology`` source notebook, which is the
authoritative relationship design for the deployed ontology)."""

from __future__ import annotations

import ast
import json
from functools import lru_cache
from typing import Any

from . import config, mcp_client


def _extract_list_literal(text: str, var_name: str) -> list[dict[str, Any]]:
    """Extract ``var_name = [ ... ]`` from notebook source and literal-eval it."""
    marker = f"{var_name} = ["
    start = text.find(marker)
    if start == -1:
        return []
    bracket_start = text.find("[", start)
    depth = 0
    for i in range(bracket_start, len(text)):
        ch = text[i]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                literal = text[bracket_start : i + 1]
                try:
                    return ast.literal_eval(literal)
                except (ValueError, SyntaxError):
                    return []
    return []


@lru_cache(maxsize=1)
def _notebook_source() -> str:
    """Return the concatenated code source of the ontology notebook."""
    if not config.ONTOLOGY_NOTEBOOK.exists():
        return ""
    nb = json.loads(config.ONTOLOGY_NOTEBOOK.read_text(encoding="utf-8"))
    chunks = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") == "code":
            chunks.append("".join(cell.get("source", [])))
    return "\n".join(chunks)


def get_edges() -> list[dict[str, str]]:
    """Relationship edges from the notebook, de-duplicated by (source, target, name)."""
    src = _notebook_source()
    rels = _extract_list_literal(src, "RELATIONSHIPS")
    eh = _extract_list_literal(src, "EVENTHOUSE_RELATIONSHIP_CONTEXTS")
    seen: set[tuple[str, str, str]] = set()
    edges: list[dict[str, str]] = []
    for rel in rels + eh:
        s = rel.get("source_entity")
        t = rel.get("target_entity")
        name = rel.get("name", "")
        if not s or not t:
            continue
        key = (s, t, name)
        if key in seen:
            continue
        seen.add(key)
        edges.append(
            {
                "source": s,
                "target": t,
                "label": name,
                "kind": rel.get("source", "lakehouse"),
            }
        )
    return edges


def get_nodes() -> list[dict[str, Any]]:
    """Entity types live from the ontology MCP."""
    text = mcp_client.call_tool(
        config.ONTOLOGY_MCP_URL,
        "list_ontology_entity_types",
        {"includeProperties": False},
        timeout=60,
    )
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    nodes = []
    for ent in data.get("values", []):
        nodes.append(
            {
                "id": ent.get("name"),
                "label": ent.get("name"),
                "namespace": ent.get("namespace", ""),
                "property_count": len(ent.get("properties", [])),
                "timeseries_count": len(ent.get("timeseriesProperties", [])),
            }
        )
    return nodes


def build_graph() -> dict[str, Any]:
    """Merge nodes and edges; synthesise any node referenced only by an edge."""
    nodes = get_nodes()
    node_ids = {n["id"] for n in nodes if n.get("id")}
    edges = get_edges()
    for edge in edges:
        for end in ("source", "target"):
            name = edge[end]
            if name not in node_ids:
                node_ids.add(name)
                nodes.append(
                    {
                        "id": name,
                        "label": name,
                        "namespace": "",
                        "property_count": 0,
                        "timeseries_count": 0,
                        "inferred": True,
                    }
                )
    return {"nodes": nodes, "edges": edges}
