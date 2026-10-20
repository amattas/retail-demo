"""Render the native PBIR connected retail-twins report without live calls."""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from deploy.scripts._twin_model import load_metrics

ROOT = Path(__file__).resolve().parents[2]
_SOURCE = ROOT / "fabric" / "twins" / "retail-twins.Report"
_TABLES = ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition" / "tables"
_SEMANTIC_MODEL_MARKER = "__SEMANTIC_MODEL_ID__"

PAGE_ORDER = [
    "networkoverview01",
    "storetwin00000001",
    "warehousetwin001",
    "trucktwin00000001",
]
DRILL_TARGETS = {
    "storetwin00000001": ("dim_stores", "Store Number"),
    "warehousetwin001": ("dim_distribution_centers", "DC Number"),
    "trucktwin00000001": ("dim_trucks", "License Plate"),
}

ALLOWED_VISUAL_TYPES = {
    "textbox",
    "cardVisual",
    "tableEx",
    "clusteredBarChart",
    "clusteredColumnChart",
    "slicer",
}

REQUIRED_TWIN_MEASURES: dict[str, set[str]] = {
    "dim_stores": {
        "Twin Store State",
        "Twin Store State As Of",
        "Twin Store Signed Stock",
        "Twin Store Available Stock",
        "Twin Store Inventory As Of",
        "Twin Store Stockout Risk",
        "Twin Store Zone Activity",
    },
    "dim_distribution_centers": {
        "Twin Warehouse Signed Stock",
        "Twin Warehouse Available Stock",
        "Twin Warehouse Inventory As Of",
        "Twin Warehouse Shipment Count",
    },
    "dim_trucks": {
        "Twin Truck Last Status",
        "Twin Truck State As Of",
        "Twin Truck Last Shipment",
        "Twin Truck Last Store",
        "Twin Truck Last DC",
        "Twin Truck Recorded ETA",
        "Twin Truck Recorded ETD",
        "Twin Truck Cargo Balance",
        "Twin Truck Cargo As Of",
        "Twin Truck Delay Scenario ETA",
    },
}

_TMDL_HEADER = re.compile(r"^\t(?P<kind>measure|column) (?P<name>'(?:[^']|'')+'|[^\s=]+)", re.M)


def _identifier(value: str, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a non-nil UUID string")
    try:
        identifier = UUID(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a non-nil UUID string") from exc
    if identifier.int == 0:
        raise ValueError(f"{field} must be a non-nil UUID string")
    return str(identifier)


def _name(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")
    return value


def _utc_datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a UTC ISO-8601 timestamp string")
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field} must be a UTC ISO-8601 timestamp string") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must include the UTC offset")
    return parsed


def _int_list(values: Any, field: str, *, min_len: int, max_len: int) -> list[int]:
    if not isinstance(values, list) or not (min_len <= len(values) <= max_len):
        raise ValueError(f"{field} must contain between {min_len} and {max_len} IDs")
    if any(type(value) is not int or value < 1 for value in values):
        raise ValueError(f"{field} must contain positive source integer IDs")
    if len(set(values)) != len(values):
        raise ValueError(f"{field} must not contain duplicate IDs")
    return list(values)


def _shipment_list(values: Any) -> list[str]:
    if not isinstance(values, list) or not (1 <= len(values) <= 1000):
        raise ValueError("shipment_ids must contain between 1 and 1000 recorded IDs")
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > 128
            or any(ord(character) < 32 for character in value)
        ):
            raise ValueError("shipment_ids must contain nonempty recorded ID strings")
        if value in seen:
            raise ValueError("shipment_ids must not contain duplicates")
        seen.add(value)
        result.append(value)
    return result


def _validate_cohort(cohort: dict[str, Any]) -> dict[str, Any]:
    required = {
        "dc_id",
        "truck_ids",
        "store_ids",
        "product_ids",
        "shipment_ids",
        "source_as_of",
        "window_start",
    }
    if not isinstance(cohort, dict) or not required <= cohort.keys():
        raise ValueError("cohort must include the connected twin runtime keys")
    if type(cohort["dc_id"]) is not int or cohort["dc_id"] < 1:
        raise ValueError("dc_id must be a positive source integer ID")
    source_as_of = _utc_datetime(cohort["source_as_of"], "source_as_of")
    window_start = _utc_datetime(cohort["window_start"], "window_start")
    if window_start > source_as_of:
        raise ValueError("window_start must not be after source_as_of")
    return {
        "dc_id": cohort["dc_id"],
        "truck_ids": _int_list(cohort["truck_ids"], "truck_ids", min_len=2, max_len=2),
        "store_ids": _int_list(cohort["store_ids"], "store_ids", min_len=1, max_len=5),
        "product_ids": _int_list(cohort["product_ids"], "product_ids", min_len=1, max_len=20),
        "shipment_ids": _shipment_list(cohort["shipment_ids"]),
        "source_as_of": cohort["source_as_of"],
        "window_start": cohort["window_start"],
    }


def _literal(value: int | str) -> dict[str, dict[str, str]]:
    if type(value) is int:
        return {"Literal": {"Value": f"{value}L"}}
    return {"Literal": {"Value": "'" + value.replace("'", "''") + "'"}}


def _column_expr(table: str, column: str, *, source: str | None = None) -> dict[str, Any]:
    source_ref = {"Source": source} if source else {"Entity": table}
    return {
        "Column": {
            "Expression": {"SourceRef": source_ref},
            "Property": column,
        }
    }


def _cohort_filter(name: str, table: str, column: str, values: list[int] | list[str]) -> dict[str, Any]:
    alias = name[:1]
    typed_values = [_literal(value) for value in values]
    return {
        "name": name,
        "field": _column_expr(table, column),
        "type": "Categorical",
        "howCreated": "User",
        "isHiddenInViewMode": True,
        "isLockedInViewMode": True,
        "filter": {
            "Version": 2,
            "From": [{"Name": alias, "Entity": table, "Type": 0}],
            "Where": [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [_column_expr(table, column, source=alias)],
                            "Values": [[value] for value in typed_values],
                        }
                    }
                }
            ],
        },
    }


def _cohort_filters(cohort: dict[str, Any]) -> list[dict[str, Any]]:
    # Shipment IDs are rendered as relationship evidence in visuals but are not
    # page/report filters; freezing them would hide later recorded shipments for
    # the selected DC/truck/store/SKU cohort.
    return [
        _cohort_filter("cohort_dc_id", "dim_distribution_centers", "ID", [cohort["dc_id"]]),
        _cohort_filter("cohort_truck_ids", "dim_trucks", "ID", cohort["truck_ids"]),
        _cohort_filter("cohort_store_ids", "dim_stores", "ID", cohort["store_ids"]),
        _cohort_filter("cohort_product_ids", "dim_products", "ID", cohort["product_ids"]),
    ]


def _source_window_paragraph(cohort: dict[str, Any]) -> dict[str, Any]:
    return {
        "textRuns": [
            {
                "value": (
                    "Cohort selection anchor: "
                    + cohort["source_as_of"]
                    + "; source window starts "
                    + cohort["window_start"]
                    + " (UTC; not a live refresh time). Metric-specific actual as-of timestamps are shown in the visuals."
                ),
                "textStyle": {
                    "fontFamily": "Segoe UI",
                    "fontSize": "9pt",
                    "color": "#5A6478",
                },
            }
        ]
    }


def _append_source_window(documents: dict[str, Any], cohort: dict[str, Any]) -> None:
    paragraph = _source_window_paragraph(cohort)
    for path, document in documents.items():
        if not path.endswith("/visual.json") or not document.get("name", "").endswith("_header"):
            continue
        visual = document.get("visual", {})
        if visual.get("visualType") != "textbox":
            continue
        general = visual.get("objects", {}).get("general", [])
        if general:
            general[0].setdefault("properties", {}).setdefault("paragraphs", []).append(paragraph)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_source_documents(source: Path = _SOURCE) -> dict[str, Any]:
    files = [source / ".platform", source / "definition.pbir"]
    files.extend(sorted((source / "definition").rglob("*.json")))
    files.extend(sorted((source / "StaticResources").rglob("*.json")))
    return {
        path.relative_to(source).as_posix(): _read_json(path)
        for path in files
    }


def _decode_tmdl_name(value: str) -> str:
    if value.startswith("'"):
        return value[1:-1].replace("''", "'")
    return value


def _model_fields() -> dict[str, dict[str, set[str]]]:
    fields: dict[str, dict[str, set[str]]] = {}
    for path in _TABLES.glob("*.tmdl"):
        text = path.read_text(encoding="utf-8")
        table = path.stem
        fields[table] = {"column": set(), "measure": set()}
        for match in _TMDL_HEADER.finditer(text):
            fields[table][match["kind"]].add(_decode_tmdl_name(match["name"]))
    return fields


def _walk_fields(value: Any) -> Iterator[tuple[str, str, str]]:
    if isinstance(value, dict):
        if "Column" in value and isinstance(value["Column"], dict):
            column = value["Column"]
            ref = column.get("Expression", {}).get("SourceRef", {})
            table = ref.get("Entity")
            if isinstance(table, str) and isinstance(column.get("Property"), str):
                yield "column", table, column["Property"]
        if "Measure" in value and isinstance(value["Measure"], dict):
            measure = value["Measure"]
            ref = measure.get("Expression", {}).get("SourceRef", {})
            table = ref.get("Entity")
            if isinstance(table, str) and isinstance(measure.get("Property"), str):
                yield "measure", table, measure["Property"]
        for child in value.values():
            yield from _walk_fields(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_fields(child)


def _validate_field_refs(documents: dict[str, Any]) -> None:
    fields = _model_fields()
    allowed_twin_measures = {
        (metric["table"], metric["name"]) for metric in load_metrics(ROOT)
    }
    for path, document in documents.items():
        if not path.endswith("/visual.json"):
            continue
        for kind, table, field in _walk_fields(document):
            if kind == "measure" and (table, field) not in allowed_twin_measures:
                raise ValueError(
                    f"Unsupported non-catalog measure reference in {path}: {table}[{field}]"
                )
            if table not in fields or field not in fields[table][kind]:
                raise ValueError(f"Unknown model {kind} reference in {path}: {table}[{field}]")


def _validate_source(documents: dict[str, Any]) -> None:
    required = {".platform", "definition.pbir", "definition/report.json", "definition/version.json", "definition/pages/pages.json"}
    if not required <= documents.keys():
        raise ValueError("PBIR source is missing required report definition parts")
    platform = documents[".platform"]
    if platform.get("metadata", {}).get("type") != "Report":
        raise ValueError("PBIR platform metadata must declare a Report")
    if platform.get("metadata", {}).get("displayName") != "retail-twins":
        raise ValueError("PBIR platform metadata must be named retail-twins")
    connection = (
        documents["definition.pbir"]
        .get("datasetReference", {})
        .get("byConnection", {})
        .get("connectionString")
    )
    if not isinstance(connection, str) or _SEMANTIC_MODEL_MARKER not in connection:
        raise ValueError("PBIR source must use the semantic model binding marker")
    pages = documents["definition/pages/pages.json"]
    if pages.get("pageOrder") != PAGE_ORDER or pages.get("activePageName") != PAGE_ORDER[0]:
        raise ValueError("PBIR pages must use the approved stable page order")
    for page in PAGE_ORDER:
        if f"definition/pages/{page}/page.json" not in documents:
            raise ValueError(f"PBIR page is missing: {page}")
        visuals = [
            path for path in documents
            if path.startswith(f"definition/pages/{page}/visuals/") and path.endswith("/visual.json")
        ]
        if not visuals:
            raise ValueError(f"PBIR page has no visuals: {page}")
        if len(visuals) > (9 if page == "storetwin00000001" else 8):
            raise ValueError(f"PBIR page exceeds the approved visual density: {page}")
    for path, document in documents.items():
        if path.endswith("/visual.json"):
            visual_type = document.get("visual", {}).get("visualType")
            if visual_type not in ALLOWED_VISUAL_TYPES:
                raise ValueError(f"Unsupported visual type in {path}: {visual_type}")
    _validate_field_refs(documents)


def _part(path: str, payload: Any) -> dict[str, str]:
    return {
        "path": path,
        "payload": base64.b64encode(
            json.dumps(payload, ensure_ascii=False, allow_nan=False, sort_keys=False).encode("utf-8")
        ).decode("ascii"),
        "payloadType": "InlineBase64",
    }


def render_twins_report(
    *, semantic_model_id: str, cohort: dict[str, Any], display_name: str = "retail-twins"
) -> dict[str, Any]:
    """Bind the source PBIR report to a semantic model and runtime cohort.

    The renderer performs no authentication, Fabric calls, credential changes, or
    publication. Runtime values are assigned to parsed JSON structures; they are
    not interpolated into source files or JSON text templates.
    """
    model_id = _identifier(semantic_model_id, "semantic_model_id")
    display_name = _name(display_name, "display_name")
    normalized = _validate_cohort(cohort)
    documents = _load_source_documents()
    _validate_source(documents)

    documents[".platform"]["metadata"]["displayName"] = display_name
    documents["definition.pbir"]["datasetReference"] = {
        "byConnection": {
            "connectionString": f"semanticmodelid={model_id}",
        }
    }
    _append_source_window(documents, normalized)
    filters = _cohort_filters(normalized)
    for page in PAGE_ORDER:
        page_doc = documents[f"definition/pages/{page}/page.json"]
        page_filters = list(filters)
        if page in DRILL_TARGETS:
            table, column = DRILL_TARGETS[page]
            name = f"drill_{page}"
            expression = _column_expr(table, column)
            page_doc["type"] = "Drillthrough"
            page_doc["visibility"] = "AlwaysVisible"
            page_filters.append({
                "name": name, "field": expression,
                "type": "Categorical", "howCreated": "Drillthrough",
            })
            page_doc["pageBinding"] = {
                "name": f"binding_{page}", "type": "Drillthrough",
                "parameters": [{"name": f"parameter_{page}", "boundFilter": name, "fieldExpr": expression}],
            }
        page_doc["filterConfig"] = {"filters": page_filters}

    return {"format": "PBIR", "parts": [_part(path, documents[path]) for path in sorted(documents)]}
