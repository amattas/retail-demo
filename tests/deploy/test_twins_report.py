"""Offline contracts for the native PBIR connected retail twins report."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from deploy.scripts import _twins_report as report
from deploy.scripts.fabric_runtime import decode_definition_parts

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "fabric" / "twins" / "retail-twins.Report"
TABLES = ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition" / "tables"
MODEL_ID = "11111111-1111-4111-8111-111111111111"


def cohort(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "dc_id": 9,
        "truck_ids": [10, 20],
        "store_ids": [50, 60, 70],
        "product_ids": [101, 102, 103],
        "shipment_ids": ["ship-actual-1", "ship-O'Brien-2"],
        "source_as_of": "2026-09-08T10:00:00+00:00",
        "window_start": "2026-09-01T10:00:00+00:00",
    }
    value.update(changes)
    return value


def render(**overrides: object) -> dict[str, dict]:
    definition = report.render_twins_report(
        semantic_model_id=overrides.pop("semantic_model_id", MODEL_ID),
        cohort=overrides.pop("cohort", cohort()),
        display_name=overrides.pop("display_name", "retail-twins"),
    )
    assert not overrides
    return {
        path: json.loads(payload)
        for path, payload in decode_definition_parts(definition).items()
    }


def visual_documents(parts: dict[str, dict]) -> list[tuple[str, dict]]:
    return [
        (path, document)
        for path, document in parts.items()
        if path.endswith("/visual.json")
    ]


def walk_fields(value: object):
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
            yield from walk_fields(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_fields(child)


def tmdl_name(value: str) -> str:
    if value.startswith("'"):
        return value[1:-1].replace("''", "'")
    return value


def tmdl_fields() -> dict[str, dict[str, set[str]]]:
    header = re.compile(r"^\t(?P<kind>measure|column) (?P<name>'(?:[^']|'')+'|[^\s=]+)", re.M)
    result: dict[str, dict[str, set[str]]] = {}
    for path in TABLES.glob("*.tmdl"):
        text = path.read_text(encoding="utf-8")
        result[path.stem] = {"column": set(), "measure": set()}
        for match in header.finditer(text):
            result[path.stem][match["kind"]].add(tmdl_name(match["name"]))
    return result


def literal_values(filter_config: dict) -> list[str]:
    condition = filter_config["filter"]["Where"][0]["Condition"]["In"]
    return [row[0]["Literal"]["Value"] for row in condition["Values"]]


def textbox_text(parts: dict[str, dict]) -> str:
    runs = []
    for _path, document in visual_documents(parts):
        visual = document["visual"]
        if visual["visualType"] != "textbox":
            continue
        for general in visual["objects"]["general"]:
            for paragraph in general["properties"]["paragraphs"]:
                runs.extend(run["value"] for run in paragraph["textRuns"])
    return "\n".join(runs)


def test_complete_multipart_graph_has_native_report_parts_and_stable_pages() -> None:
    definition = report.render_twins_report(semantic_model_id=MODEL_ID, cohort=cohort())
    assert definition["format"] == "PBIR"
    assert all(part["payloadType"] == "InlineBase64" for part in definition["parts"])
    parts = render()
    assert parts[".platform"]["metadata"]["type"] == "Report"
    assert parts[".platform"]["metadata"]["displayName"] == "retail-twins"
    assert parts["definition.pbir"]["datasetReference"] == {
        "byConnection": {"connectionString": f"semanticmodelid={MODEL_ID}"}
    }
    pages = parts["definition/pages/pages.json"]
    assert pages["pageOrder"] == report.PAGE_ORDER
    assert pages["activePageName"] == "networkoverview01"
    for page in report.PAGE_ORDER:
        assert parts[f"definition/pages/{page}/page.json"]["displayOption"] == "FitToPage"
        visuals = [path for path, _ in visual_documents(parts) if f"/pages/{page}/" in path]
        assert 1 <= len(visuals) <= (9 if page == "storetwin00000001" else 8)
    assert report.render_twins_report(semantic_model_id=MODEL_ID, cohort=cohort()) == definition


def test_source_uses_marker_and_runtime_binding_is_assigned_as_json_values() -> None:
    source = json.loads((SOURCE / "definition.pbir").read_text(encoding="utf-8"))
    assert source["datasetReference"]["byConnection"]["connectionString"] == "semanticmodelid=__SEMANTIC_MODEL_ID__"
    assert MODEL_ID not in json.dumps(source)
    renamed = render(display_name="Twin demo for Contoso")
    assert renamed[".platform"]["metadata"]["displayName"] == "Twin demo for Contoso"
    assert renamed["definition.pbir"]["datasetReference"]["byConnection"]["connectionString"] == f"semanticmodelid={MODEL_ID}"


def test_routes_can_be_supplied_but_required_contract_does_not_need_them() -> None:
    without_routes = render()
    with_routes = render(
        cohort=cohort(
            routes=[
                {
                    "dc_id": 9,
                    "truck_id": 10,
                    "store_id": 50,
                    "shipment_id": "ship-actual-1",
                    "event_ts": "2026-09-08T09:30:00+00:00",
                }
            ]
        )
    )
    assert with_routes["definition/pages/pages.json"] == without_routes["definition/pages/pages.json"]
    assert with_routes["definition/pages/networkoverview01/page.json"]["filterConfig"] == without_routes["definition/pages/networkoverview01/page.json"]["filterConfig"]


def test_accepts_up_to_twenty_actual_nonsequential_cargo_skus() -> None:
    product_ids = [3, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946]
    parts = render(cohort=cohort(product_ids=product_ids))
    filters = {
        item["name"]: item
        for item in parts["definition/pages/networkoverview01/page.json"]["filterConfig"]["filters"]
    }
    assert literal_values(filters["cohort_product_ids"]) == [f"{value}L" for value in product_ids]


def test_cohort_filters_limit_default_scope_without_becoming_authorization() -> None:
    parts = render()
    expected = {
        "cohort_dc_id": ["9L"],
        "cohort_truck_ids": ["10L", "20L"],
        "cohort_store_ids": ["50L", "60L", "70L"],
        "cohort_product_ids": ["101L", "102L", "103L"],
    }
    for page in report.PAGE_ORDER:
        filters = {
            item["name"]: item
            for item in parts[f"definition/pages/{page}/page.json"]["filterConfig"]["filters"]
            if item["name"].startswith("cohort_")
        }
        assert set(filters) == set(expected)
        assert "cohort_move_shipment_ids" not in filters
        assert "cohort_cargo_shipment_ids" not in filters
        for name, values in expected.items():
            assert filters[name]["isLockedInViewMode"] is True
            assert filters[name]["isHiddenInViewMode"] is True
            assert filters[name]["filter"]["Version"] == 2
            assert literal_values(filters[name]) == values


def test_entity_pages_have_source_field_drillthrough_bindings():
    parts = render()
    for page, (table, column) in report.DRILL_TARGETS.items():
        document = parts[f"definition/pages/{page}/page.json"]
        assert document["type"] == "Drillthrough"
        assert document["visibility"] == "AlwaysVisible"
        binding = document["pageBinding"]["parameters"][0]
        assert binding["fieldExpr"]["Column"]["Property"] == column
        assert binding["fieldExpr"]["Column"]["Expression"]["SourceRef"]["Entity"] == table
        assert any(row["name"] == binding["boundFilter"] for row in document["filterConfig"]["filters"])


def test_store_activity_is_not_misattributed_to_skus():
    parts = render()
    sku = parts["definition/pages/storetwin00000001/visuals/store_sku_table/visual.json"]
    activity = parts["definition/pages/storetwin00000001/visuals/store_zone_activity/visual.json"]
    assert "Twin Store Zone Activity" not in json.dumps(sku)
    assert ("column", "fact_foot_traffic", "Zone") in set(walk_fields(activity))
    assert not any(table == "dim_products" for _kind, table, _name in walk_fields(activity))


@pytest.mark.parametrize(
    "overrides, match",
    [
        ({"semantic_model_id": "not-a-guid"}, "semantic_model_id"),
        ({"display_name": " \t"}, "display_name"),
        ({"cohort": cohort(dc_id=0)}, "dc_id"),
        ({"cohort": cohort(truck_ids=[10])}, "truck_ids"),
        ({"cohort": cohort(truck_ids=[10, 10])}, "truck_ids"),
        ({"cohort": cohort(store_ids=[1, 2, 3, 4, 5, 6])}, "store_ids"),
        ({"cohort": cohort(product_ids=list(range(1, 22)))}, "product_ids"),
        ({"cohort": cohort(shipment_ids=[])}, "shipment_ids"),
        ({"cohort": cohort(shipment_ids=["x", "x"])}, "shipment_ids"),
        ({"cohort": cohort(source_as_of="2026-09-08T10:00:00-05:00")}, "source_as_of"),
        ({"cohort": cohort(window_start="2026-09-09T10:00:00+00:00")}, "window_start"),
    ],
)
def test_invalid_runtime_inputs_fail_closed(overrides: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        render(**overrides)


def test_visible_guardrails_make_demo_limits_and_asofs_clear() -> None:
    parts = render()
    text = textbox_text(parts).casefold()
    for phrase in [
        "synthetic/historical demo",
        "latest recorded within the selected cohort",
        "not live gps",
        "live temperature",
        "exact occupancy",
        "physical actuation",
        "purchasing",
        "sso remains enforced",
        "source/as-of",
        "metric-specific actual as-of timestamps",
        "zone activity",
        "not available-to-promise",
        "+60 min delay what-if",
        "cohort selection anchor",
        "not a live refresh time",
        "shipment ids are relationship evidence",
        "missing/not modeled",
        "not green or zero risk",
        "2026-09-01t10:00:00+00:00",
        "2026-09-08t10:00:00+00:00",
    ]:
        assert phrase in text
    measures = {
        field
        for _, visual in visual_documents(parts)
        for kind, _table, field in walk_fields(visual)
        if kind == "measure"
    }
    assert {
        "Twin Store State As Of",
        "Twin Store Inventory As Of",
        "Twin Warehouse Inventory As Of",
        "Twin Truck State As Of",
        "Twin Truck Cargo As Of",
        "Twin Truck Recorded ETA",
        "Twin Truck Delay Scenario ETA",
    } <= measures


def test_no_custom_visuals_and_crossfilter_story_is_source_bound() -> None:
    parts = render()
    visual_types = {visual["visual"]["visualType"] for _path, visual in visual_documents(parts)}
    assert visual_types <= report.ALLOWED_VISUAL_TYPES
    assert "publicCustomVisuals" not in json.dumps(parts["definition/report.json"])
    for _path, visual in visual_documents(parts):
        assert visual["visual"].get("drillFilterOtherVisuals") is True
    network_path = parts[
        "definition/pages/networkoverview01/visuals/net_path_table/visual.json"
    ]
    refs = {(table, field) for _kind, table, field in walk_fields(network_path)}
    assert {
        ("dim_distribution_centers", "DC Number"),
        ("fact_truck_moves", "Shipment ID"),
        ("dim_trucks", "License Plate"),
        ("fact_truck_moves", "Store ID"),
    } <= refs
    cargo = parts[
        "definition/pages/networkoverview01/visuals/net_cargo_table/visual.json"
    ]
    cargo_refs = {(table, field) for _kind, table, field in walk_fields(cargo)}
    assert {("fact_truck_inventory", "Shipment ID"), ("dim_products", "Product Name")} <= cargo_refs


def test_model_references_use_case_sensitive_tmdl_columns_and_exact_twin_measures() -> None:
    existing = tmdl_fields()
    required = {
        (table, measure)
        for table, measures in report.REQUIRED_TWIN_MEASURES.items()
        for measure in measures
    }
    used_required: set[tuple[str, str]] = set()
    for path, document in render().items():
        if not path.endswith((".json", ".pbir")):
            continue
        for kind, table, field in walk_fields(document):
            if kind == "measure":
                assert (table, field) in required, (path, table, field)
                used_required.add((table, field))
            assert table in existing, (path, table, field)
            assert field in existing[table][kind], (path, kind, table, field)
    assert required <= used_required


def test_source_parts_have_schema_urls_and_no_runtime_ids() -> None:
    source_documents = {
        path.relative_to(SOURCE).as_posix(): json.loads(path.read_text(encoding="utf-8"))
        for path in [SOURCE / ".platform", SOURCE / "definition.pbir", *sorted((SOURCE / "definition").rglob("*.json"))]
    }
    assert report._SEMANTIC_MODEL_MARKER in json.dumps(source_documents["definition.pbir"])
    serialized = json.dumps(source_documents)
    assert MODEL_ID not in serialized
    assert "2026-09-08T10:00:00+00:00" not in serialized
    assert "2026-09-01T10:00:00+00:00" not in serialized
    for path, document in source_documents.items():
        assert "$schema" in document, path
