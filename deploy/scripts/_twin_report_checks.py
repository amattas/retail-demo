"""Check report field references against the target semantic model."""

from __future__ import annotations

import json
from typing import Any

from deploy.scripts._twin_model import _objects
from deploy.scripts.fabric_runtime import decode_definition_parts


def report_references(definition: dict[str, Any]) -> set[tuple[str, str, str]]:
    references: set[tuple[str, str, str]] = set()

    def walk(value: Any, aliases: dict[str, str]) -> None:
        if isinstance(value, list):
            for child in value:
                walk(child, aliases)
        elif isinstance(value, dict):
            local = {**aliases}
            for binding in value.get("From", []):
                if isinstance(binding, dict) and "Name" in binding and "Entity" in binding:
                    local[binding["Name"]] = binding["Entity"]
            for kind in ("Column", "Measure"):
                field = value.get(kind)
                if not isinstance(field, dict) or "Property" not in field:
                    continue
                source = field.get("Expression", {}).get("SourceRef", {})
                table = source.get("Entity") or local.get(source.get("Source"))
                if not isinstance(table, str):
                    raise ValueError("Twin report contains an unresolved model field source.")
                references.add((table, kind.casefold(), field["Property"]))
            for child in value.values():
                walk(child, local)

    for path, payload in decode_definition_parts(definition).items():
        if path.startswith("definition/") and path.endswith(".json"):
            walk(json.loads(payload), {})
    if not references:
        raise ValueError("A twin report must contain real model field references.")
    return references


def validate_report_model_fields(
    report_definition: dict[str, Any], model_definition: dict[str, Any],
) -> None:
    tables = {
        path.rsplit("/", 1)[-1].removesuffix(".tmdl"): _objects(value.decode("utf-8"))
        for path, value in decode_definition_parts(model_definition).items()
        if "/tables/" in path and path.endswith(".tmdl")
    }
    missing = sorted(
        f"{table}[{name}]" for table, kind, name in report_references(report_definition)
        if (kind, name) not in tables.get(table, {})
    )
    if missing:
        raise ValueError("Twin report references missing live model fields: " + ", ".join(missing))
