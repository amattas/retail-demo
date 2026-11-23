"""Regenerate the Data Agent source snapshots from the models they describe.

The two Data Agents carry snapshots of their sources in
``fabric/data-agents/*/Files/Config/{draft,published}/``. Fabric fills them on
export, but a model change does not, so they drift. This module rebuilds them
offline and deterministically:

- the semantic-model agent's ``datasource.json`` elements come from the
  ``retail_model`` TMDL tables: every table, measure and column (hidden ones
  included), keyed by lineage tag, in TMDL order, all selected;
- the ontology agent's ``ontology.json`` element descriptions come from
  ``_ontology_model.build_definition``: each entity's Lakehouse properties.

Instance fields (artifact and workspace IDs, instructions) are kept from the
committed files. ``--check`` reports drift without writing; CI runs it through
``tests/test_ancillary_fabric_assets.py``.

Example:
    python -m deploy.scripts.agent_snapshots           # rewrite the snapshots
    python -m deploy.scripts.agent_snapshots --check   # fail on drift
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from deploy.scripts import _ontology_model as ontology

ROOT = Path(__file__).resolve().parents[2]
TABLES_DIR = ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition" / "tables"
MODEL_TMDL = TABLES_DIR.parent / "model.tmdl"
TWIN_METRICS = ROOT / "fabric" / "twins" / "metrics.json"
AGENTS = ROOT / "fabric" / "data-agents"
STAGES = ("draft", "published")
SEMANTIC_SNAPSHOT = (
    "retail-semantic-model-agent.DataAgent/Files/Config/{stage}/"
    "semantic-model-retail_model/datasource.json"
)
ONTOLOGY_SNAPSHOT = (
    "retail-ontology-agent.DataAgent/Files/Config/{stage}/ontologies/"
    f"{ontology.ONTOLOGY_NAME}/ontology.json"
)

_COLUMN_TYPES = {
    "int64": "Int64",
    "double": "Double",
    "decimal": "Double",
    "string": "String",
    "dateTime": "DateTime",
    "boolean": "Boolean",
}
# A measure's result type follows its format string; a measure without one is
# text. Measures that return a sum of a double column with a whole-number
# format are listed by name.
_FORMAT_TYPES = {"#,0": "Int64", "0": "Int64", "General Date": "DateTime", "Short Date": "DateTime"}
_DOUBLE_MEASURES = frozenset(
    {
        ("demand_forecast", "Forecasted Units"),
        ("demand_forecast", "Forecast Lower Bound"),
        ("demand_forecast", "Forecast Upper Bound"),
        ("demand_forecast", "Forecasted Units Next 7 Days"),
        ("demand_forecast", "Forecast Lower Bound Next 7 Days"),
        ("demand_forecast", "Forecast Upper Bound Next 7 Days"),
    }
)
_OBJECT = re.compile(
    r"^\t(?P<kind>measure|column) (?P<name>'(?:[^']|'')+'|[^\s=]+)(?P<rest>.*?)(?=^\t\S|\Z)",
    re.MULTILINE | re.DOTALL,
)


def _unquote(name: str) -> str:
    return name[1:-1].replace("''", "'") if name.startswith("'") else name


def _property(block: str, name: str) -> str | None:
    match = re.search(rf"^\t\t{name}: (.*)$", block, re.MULTILINE)
    return match.group(1).strip() if match else None


def _measure_type(table: str, name: str, block: str, twins: Mapping[tuple[str, str], str]) -> str:
    if (table, name) in twins:
        return twins[(table, name)]
    if (table, name) in _DOUBLE_MEASURES:
        return "Double"
    fmt = _property(block, "formatString")
    if fmt is None or fmt == "General":
        return "String"
    return _FORMAT_TYPES.get(fmt, "Double")


def _model_tables() -> list[str]:
    """Active tables of model.tmdl, by name (the order Fabric exports them in)."""
    text = MODEL_TMDL.read_text(encoding="utf-8")
    return sorted(
        _unquote(name) for name in re.findall(r"^ref table (.+?)\s*$", text, re.MULTILINE)
    )


def semantic_model_elements() -> list[dict[str, Any]]:
    """Datasource elements for every table, measure and column of retail_model."""
    twins = {
        (metric["table"], metric["name"]): metric["data_type"]
        for metric in json.loads(TWIN_METRICS.read_text(encoding="utf-8"))["metrics"]
    }
    elements = []
    for table in _model_tables():
        text = (TABLES_DIR / f"{table}.tmdl").read_text(encoding="utf-8").replace("\r\n", "\n")
        table_tag = re.search(r"^\tlineageTag: (\S+)", text, re.MULTILINE)
        if table_tag is None:
            raise ValueError(f"{table}.tmdl has no lineage tag")
        measures, columns = [], []
        for match in _OBJECT.finditer(text):
            kind, name, block = match["kind"], _unquote(match["name"]), match["rest"]
            tag = _property(block, "lineageTag")
            if tag is None:
                raise ValueError(f"{table}[{name}] has no lineage tag")
            if kind == "measure":
                data_type = _measure_type(table, name, block, twins)
                bucket = measures
            else:
                data_type = _COLUMN_TYPES[_property(block, "dataType") or ""]
                bucket = columns
            bucket.append(
                {
                    "id": tag,
                    "is_selected": True,
                    "display_name": name,
                    "type": f"semantic_model.{kind}",
                    "data_type": data_type,
                    "description": None,
                    "children": [],
                }
            )
        elements.append(
            {
                "id": table_tag.group(1),
                "is_selected": True,
                "display_name": table,
                "type": "semantic_model.table",
                "description": None,
                "children": measures + columns,
            }
        )
    return elements


def ontology_entity_properties() -> dict[str, list[str]]:
    """Each ontology entity's Lakehouse properties, as ``build_definition`` binds them."""
    zero = "00000000-0000-0000-0000-000000000000"
    parts = ontology.build_definition(
        sql_endpoint="snapshot.invalid",
        sql_database_id=zero,
        source=ontology.SourceItem(zero, "snapshot", zero, "snapshot", "1970-01-01T00:00:00Z"),
        available_bronze_tables=(),
        include_rules=False,
    )
    return {
        entity.name: re.findall(
            r"^\tproperty (\S+)$", parts[f"entities/{entity.name}.tmdl"], re.MULTILINE
        )
        for entity in ontology.ENTITIES
    }


def ontology_elements() -> list[dict[str, Any]]:
    return [
        {
            "id": name,
            "is_selected": True,
            "display_name": name,
            "type": "ontology.entity",
            "description": ",".join(properties),
            "children": [],
        }
        for name, properties in ontology_entity_properties().items()
    ]


def _dump(data: Mapping[str, Any]) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def render(root: Path = AGENTS) -> dict[Path, str]:
    """``{path: content}`` of every snapshot, rebuilt from its source."""
    semantic = semantic_model_elements()
    entities = ontology_elements()
    rendered: dict[Path, str] = {}
    for stage in STAGES:
        path = root / SEMANTIC_SNAPSHOT.format(stage=stage)
        current = json.loads(path.read_text(encoding="utf-8"))
        rendered[path] = _dump({**current, "elements": semantic})
        path = root / ONTOLOGY_SNAPSHOT.format(stage=stage)
        current = json.loads(path.read_text(encoding="utf-8"))
        rendered[path] = _dump(
            {
                **current,
                "elements": entities,
                "entitiesWithUserAuthoredDescriptions": [e["id"] for e in entities],
            }
        )
    return rendered


def stale(root: Path = AGENTS) -> list[Path]:
    """Snapshots whose committed content differs from the rebuilt one."""
    return [
        path
        for path, content in render(root).items()
        if path.read_text(encoding="utf-8").replace("\r\n", "\n") != content
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="Report drift without writing.")
    args = parser.parse_args(argv)
    if args.check:
        drifted = stale()
        for path in drifted:
            print(f"stale: {path.relative_to(ROOT)}")
        return 1 if drifted else 0
    for path, content in render().items():
        path.write_text(content, encoding="utf-8", newline="\n")
        print(f"wrote {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
