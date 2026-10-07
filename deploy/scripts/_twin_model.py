"""Merge only twin measures and their missing columns into a live TMDL definition."""

from __future__ import annotations

import base64
import copy
import json
import re
from pathlib import Path
from typing import Any

from deploy.scripts.fabric_runtime import decode_definition_parts

_HEADER = re.compile(r"^\t(?P<kind>measure|column) (?P<name>'(?:[^']|'')+'|[^\s=]+)", re.M)
_LEVEL_ONE = re.compile(r"^\t[^\s/]", re.M)
_REFERENCE = re.compile(r"(?:'(?P<quoted>(?:[^']|'')+)'|(?P<plain>[A-Za-z_]\w*))\[(?P<field>[^\]]+)\]")
_TOKENS = re.compile(
    r'"(?:[^"]|"")*"|\'(?:[^\']|\'\')*\'|//[^\n]*|/\*[\s\S]*?\*/|'
    r"\[[^\]]+\]|[A-Za-z_]\w*|\d+(?:\.\d+)?|\S"
)


def _objects(text: str) -> dict[tuple[str, str], tuple[int, int, str]]:
    result = {}
    for match in _HEADER.finditer(text):
        description = re.search(r"(?:^\t///[^\n]*\n)+\Z", text[:match.start()], re.M)
        start = description.start() if description else match.start()
        end = _LEVEL_ONE.search(text, match.end())
        stop = end.start() if end else len(text)
        # Descriptions immediately before the next object belong to that next object.
        tail = re.search(r"(?:\n\t///[^\n]*)+\s*\Z", text[match.start():stop])
        if tail:
            stop = match.start() + tail.start() + 1
        name = match["name"]
        if name.startswith("'"):
            name = name[1:-1].replace("''", "'")
        key = (match["kind"], name)
        if key in result:
            raise ValueError(f"Duplicate TMDL object: {key}")
        result[key] = (start, stop, text[start:stop].rstrip() + "\n")
    return result


def _expression(block: str) -> str:
    header = _HEADER.search(block)
    if header is None:
        raise ValueError("A twin measure block has no TMDL header.")
    body = block[header.end():].split("=", 1)[1]
    if body.lstrip().startswith("```"):
        return body.split("```", 2)[1]
    metadata = re.search(r"^\t\t[^\s/]", body, re.M)
    return body[:metadata.start()] if metadata else body


def _signature(block: str, kind: str) -> tuple[Any, ...]:
    properties = dict(re.findall(
        r"^\t\t(dataType|sourceColumn|formatString|displayFolder):\s*(.*?)\s*$",
        block, re.M,
    ))
    tokens = (
        tuple(token for token in _TOKENS.findall(_expression(block))
              if not token.startswith(("//", "/*")))
        if kind == "measure" else ()
    )
    description = " ".join(
        line.strip()[3:].strip() for line in block.splitlines()
        if line.strip().startswith("///")
    )
    return tokens, properties, description


def load_metrics(repo_root: Path) -> list[dict[str, str]]:
    data = json.loads((repo_root / "fabric" / "twins" / "metrics.json").read_text(encoding="utf-8"))
    metrics = data.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        raise ValueError("The twin metric catalog is empty or invalid.")
    names = set()
    for metric in metrics:
        if (
            not isinstance(metric, dict)
            or not isinstance(metric.get("name"), str)
            or not metric["name"].startswith("Twin ")
            or not isinstance(metric.get("table"), str)
            or not re.fullmatch(r"[A-Za-z_]\w*", metric.get("table", ""))
            or metric["name"] in names
        ):
            raise ValueError("Twin metric identities must be unique, named and table-bound.")
        names.add(metric["name"])
    return metrics


def merge_twin_model(
    definition: dict[str, Any], repo_root: Path,
) -> tuple[dict[str, Any], list[str]]:
    """Preserve live partitions, roles, expressions and all unrelated objects."""
    metrics = load_metrics(repo_root)
    decoded = decode_definition_parts(definition)
    source_root = repo_root / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition" / "tables"
    sources: dict[str, dict] = {}
    desired: dict[str, dict[tuple[str, str], str]] = {}

    def source(table: str) -> dict:
        if not re.fullmatch(r"[A-Za-z_]\w*", table):
            raise ValueError("Twin source table references must be repository table names.")
        if table not in sources:
            sources[table] = _objects((source_root / f"{table}.tmdl").read_text(encoding="utf-8"))
        return sources[table]

    for metric in metrics:
        key = ("measure", metric["name"])
        objects = source(metric["table"])
        if key not in objects:
            raise ValueError(f"Missing source twin measure: {metric['name']}")
        block = objects[key][2]
        desired.setdefault(metric["table"], {})[key] = block
        for reference in _REFERENCE.finditer(_expression(block)):
            table = (reference["quoted"] or reference["plain"]).replace("''", "'")
            name = reference["field"]
            column_key = ("column", name)
            referenced = source(table)
            if column_key in referenced:
                desired.setdefault(table, {})[column_key] = referenced[column_key][2]
            elif ("measure", name) not in referenced:
                raise ValueError(f"Unknown twin source reference: {table}[{name}]")

    result = copy.deepcopy(definition)
    changes = []
    for table, objects in desired.items():
        paths = [path for path in decoded if path.endswith(f"/tables/{table}.tmdl")]
        if len(paths) != 1:
            raise ValueError(f"Expected one deployed TMDL table for {table}.")
        path = paths[0]
        text = decoded[path].decode("utf-8").replace("\r\n", "\n")
        original = text
        for key, block in objects.items():
            current = _objects(text).get(key)
            if current:
                if key[0] == "column":
                    for prop in ("dataType", "sourceColumn"):
                        wanted = _signature(block, "column")[1].get(prop)
                        actual = _signature(current[2], "column")[1].get(prop)
                        if not wanted or wanted != actual:
                            raise ValueError(f"Conflicting deployed column: {table}[{key[1]}]")
                    continue
                if _signature(block, key[0]) == _signature(current[2], key[0]):
                    continue
                text = text[:current[0]] + block + "\n" + text[current[1]:]
            else:
                partition = re.search(r"^\tpartition ", text, re.M)
                position = partition.start() if partition else len(text)
                if key[0] == "measure":
                    columns = [value[0] for name, value in _objects(text).items() if name[0] == "column"]
                    position = min(columns, default=position)
                text = text[:position].rstrip() + "\n\n" + block + "\n" + text[position:]
            changes.append(f"{table}[{key[1]}]")
        if text != original:
            part = next(part for part in result["parts"] if part["path"] == path)
            part["payload"] = base64.b64encode(text.encode("utf-8")).decode("ascii")
            part["payloadType"] = "InlineBase64"
    result["format"] = "TMDL"
    return result, changes
