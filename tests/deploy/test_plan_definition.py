"""Offline contracts for the native PlanV1 workbook (not service execution)."""

from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.util
import json
import re
import shutil
from pathlib import Path
from uuid import UUID

import pytest

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "fabric/planning/retail-replenishment.Plan"
SCHEMAS = ROOT / "fabric/planning/schemas"
TABLES = ROOT / "fabric/powerbi/retail_model.SemanticModel/definition/tables"
MODULE = "deploy.scripts._plan_definition"
TARGET = {
    "workspace_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    "workspace_name": "Test workspace",
    "semantic_model_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    "semantic_model_name": "retail_model",
    "connection_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    "display_name": "Protect weekend availability",
}


def _render(**overrides: object) -> dict:
    assert importlib.util.find_spec(MODULE), "Native Plan renderer is missing"
    renderer = importlib.import_module(MODULE).render_plan_definition
    return renderer(**(TARGET | overrides))


def _decode(definition: dict) -> dict[str, dict]:
    return {
        part["path"]: json.loads(base64.b64decode(part["payload"], validate=True))
        for part in definition["parts"]
    }


def _visuals(parts: dict[str, dict]) -> list[tuple[dict, dict, dict, str]]:
    result = []
    for sheet in parts["definition.json"]["sheets"]:
        for visual in sheet["workloadItemEntityVisuals"]:
            prefix = f"sheets/{sheet['recordGuid']}/visuals/{visual['visualId']}"
            props = parts[f"{prefix}/properties.json"]["visuals"]
            result.append(
                (
                    sheet,
                    props[visual["visualId"]]["properties"],
                    parts[f"{prefix}/dataInput.json"],
                    prefix,
                )
            )
    return result


def test_complete_multipart_graph_has_real_planning_visuals() -> None:
    rendered = _render()
    assert rendered["format"] == "PlanV1"
    assert all(p["payloadType"] == "InlineBase64" for p in rendered["parts"])
    parts = _decode(rendered)
    assert len(parts) == len(rendered["parts"])
    assert {".platform", "definition.json", "planProperties.json"} <= parts.keys()
    assert parts[".platform"]["metadata"]["type"] == "Plan"
    sheets = parts["definition.json"]["sheets"]
    assert len(sheets) == 3
    identifiers = []
    expected = {".platform", "definition.json", "planProperties.json"}
    for sheet, props, data, prefix in _visuals(parts):
        assert sheet["sheetType"] == "PLANNING"
        identifiers.extend([sheet["recordGuid"], prefix.split("/")[-1]])
        expected.update(
            {
                f"sheets/{sheet['recordGuid']}/sheet.json",
                f"sheets/{sheet['recordGuid']}/commentSettings.json",
                f"{prefix}/properties.json",
                f"{prefix}/dataInput.json",
            }
        )
        assert props["pivotAssignments"]
        assert data["columns"]
        for column in data["columns"]:
            assert column["visualId"] == prefix.split("/")[-1]
            assert column["columnMeta"]["id"] == column["measureGuid"]
        if "what-if" in sheet["displayName"].lower():
            expected.add(f"{prefix}/scenarios.json")
    assert len(set(identifiers)) == len(identifiers)
    assert all(str(UUID(value)) == value for value in identifiers)
    assert set(parts) == expected
    assert _render() == rendered


def test_runtime_targets_are_bound_as_json_values_and_direct_lake() -> None:
    parts = _decode(_render())
    definition = parts["definition.json"]
    assert definition["workloadItemName"] == TARGET["display_name"]
    assert parts[".platform"]["metadata"]["displayName"] == TARGET["display_name"]
    assert definition["semanticModelReference"] == {
        "connectionId": TARGET["connection_id"],
        "semanticModelId": TARGET["semantic_model_id"],
        "semanticModelName": TARGET["semantic_model_name"],
        "semanticModelWorkspaceId": TARGET["workspace_id"],
        "semanticModelWorkspaceName": TARGET["workspace_name"],
        "directLakeMode": True,
        "directQueryMode": False,
        "sourceType": "WORKLOAD",
    }


@pytest.mark.parametrize(
    "field", ["workspace_id", "semantic_model_id", "connection_id"]
)
@pytest.mark.parametrize(
    "value",
    [
        "",
        None,
        123,
        "not-a-guid",
        "../definition.json",
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","sheets":[]',
        "00000000-0000-0000-0000-000000000000",
    ],
)
def test_rejects_invalid_or_missing_target_identifiers(
    field: str, value: object
) -> None:
    with pytest.raises(ValueError, match=field):
        _render(**{field: value})


@pytest.mark.parametrize(
    "field", ["workspace_name", "semantic_model_name", "display_name"]
)
@pytest.mark.parametrize("value", ["", " \t\n", None, 42])
def test_names_must_be_nonempty_strings(field: str, value: object) -> None:
    with pytest.raises(ValueError, match=field):
        _render(**{field: value})


def test_names_cannot_inject_json_paths_or_nested_template_values() -> None:
    name = '${CONNECTION_ID} "../sheets/evil" \\ \n雪", "sheets": []'
    original_files = {p: p.read_bytes() for p in SOURCE.rglob("*") if p.is_file()}
    parts = _decode(
        _render(
            workspace_name=name,
            semantic_model_name=name,
            display_name=name,
        )
    )
    definition = parts["definition.json"]
    assert definition["workloadItemName"] == name
    assert definition["semanticModelReference"]["semanticModelName"] == name
    assert definition["semanticModelReference"]["semanticModelWorkspaceName"] == name
    assert parts[".platform"]["metadata"]["displayName"] == name
    assert set(parts) == set(_decode(_render()))
    assert len(definition["sheets"]) == 3
    assert {p: p.read_bytes() for p in original_files} == original_files
    assert TARGET["workspace_id"] not in "".join(
        p.read_text(encoding="utf-8") for p in original_files
    )


def test_render_calls_are_independent_and_content_ids_survive_rebinding() -> None:
    first = _decode(_render())
    changed = _decode(
        _render(
            workspace_id=TARGET["connection_id"],
            semantic_model_id=TARGET["workspace_id"],
            connection_id=TARGET["semantic_model_id"],
            display_name="Another plan",
        )
    )
    assert first["definition.json"]["sheets"] == changed["definition.json"]["sheets"]
    assert first[".platform"]["config"] == changed[".platform"]["config"]
    assert first == _decode(_render())


@pytest.mark.parametrize("damage", ["missing-visual", "empty-sheets", "extra-part"])
def test_incomplete_or_unexpected_sources_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    damage: str,
) -> None:
    source = tmp_path / "source.Plan"
    shutil.copytree(SOURCE, source)
    module = importlib.import_module(MODULE)
    monkeypatch.setattr(module, "_SOURCE", source)
    if damage == "missing-visual":
        next(source.rglob("dataInput.json")).unlink()
    elif damage == "empty-sheets":
        path = source / "definition.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        document["sheets"] = []
        path.write_text(json.dumps(document), encoding="utf-8")
    else:
        (source / "writeback.json").write_text("{}", encoding="utf-8")
    with pytest.raises((ValueError, FileNotFoundError)):
        _render()


def test_schema_snapshots_match_downloaded_microsoft_content() -> None:
    hashes = {
        "commentSettings": "66118033408e12a45139e769662a1e2f4306280703bff4a98439a9a3d160acdf",
        "dataInput": "9426039e1a616b3d5e6774b65c3022018e2958aa0e5d025934a6f6458600be01",
        "definition": "775859de01b1bdf57297ad00ce797e6547631a933290d3c0380e7a19ab46e8c9",
        "planProperties": "7722419d298e8d581d700e4ac49e07bea8daa58f5d25d2e3a21c398052060344",
        "platform": "610c7c4bdc7e1d8e58c195bf6b1ac9fca269edab93d428f433cb438c9a9dd699",
        "properties": "9739ba078cf73990bfa8d758357634d6100f65f3f673bff90a21a4e597f76502",
        "scenarios": "558af830e0861f39224aea3a816e989702659c2fe616b032173be79aa04e4247",
        "sheet": "df7a05b3886b3a937316f141b96a81d6dc7ff0309fbfa22130b94fea2e777579",
    }
    for name, digest in hashes.items():
        # Preserve content while allowing Git's platform-specific newline conversion.
        content = (SCHEMAS / f"{name}.schema.json").read_text(encoding="utf-8")
        assert hashlib.sha256(content.encode("utf-8")).hexdigest() == digest


def test_all_parts_validate_against_unmodified_published_schemas() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    referencing = pytest.importorskip("referencing")
    schemas = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in SCHEMAS.glob("*.schema.json")
    ]
    by_id = {schema["$id"]: schema for schema in schemas}
    assert len(by_id) == 8
    # No implicit network retrieval: every used schema must be an offline snapshot.
    registry = referencing.Registry()
    for path, document in _decode(_render()).items():
        assert document["$schema"] in by_id, path
        schema = by_id[document["$schema"]]
        jsonschema.Draft7Validator.check_schema(schema)
        validator = jsonschema.Draft7Validator(
            schema,
            registry=registry,
            format_checker=jsonschema.Draft7Validator.FORMAT_CHECKER,
        )
        errors = list(validator.iter_errors(document))
        assert not errors, f"{path}: {[error.message for error in errors]}"


def test_model_bindings_use_exact_tmdl_names_and_link_native_columns() -> None:
    tables_used = set()
    measures_used = set()
    for _, props, data, _ in _visuals(_decode(_render())):
        native = {
            column["measureGuid"]: column
            for column in data["columns"]
            if "Native" in column["columnMeta"]["measure_type"]
        }
        linked = set()
        assignments = list(props["pivotAssignments"])
        for filter_assignment in props["superFilterAssignments"]:
            assignments.extend(filter_assignment["pivotAssignments"])
        for assignment in assignments:
            table, field = re.fullmatch(r"(\w+)\[(.+)\]", assignment["id"]).groups()
            assert assignment["sourceId"] == assignment["id"]
            assert assignment["tableName"] == table
            assert assignment["columnName"] == field
            text = (TABLES / f"{table}.tmdl").read_text(encoding="utf-8")
            kind = "measure" if assignment["columnType"] == "Measure" else "column"
            assert re.search(
                rf"^\t{kind} (?:'{re.escape(field)}'|{re.escape(field)})(?: =.*)?$",
                text,
                re.MULTILINE,
            ), (table, field, kind)
            tables_used.add(table)
            if assignment["bucketId"] == "ameasure":
                column = native[assignment["measureGuid"]]
                assert column["disableWriteAccess"] is True
                linked.add(column["measureGuid"])
                measures_used.add(field)
        assert linked == set(native)
    assert tables_used == {
        "stockout_risk",
        "demand_forecast",
        "dim_stores",
        "dim_products",
        "inventory_position_current",
        "dc_inventory_position_current",
    }
    assert {
        "Avg Stockout Probability",
        "Avg Days of Inventory",
        "Forecasted Units Next 7 Days",
        "Forecast Lower Bound Next 7 Days",
        "Forecast Upper Bound Next 7 Days",
        "Total Units on Hand",
        "DC Units on Hand",
    } <= measures_used


def test_scope_filters_declare_bounded_synthetic_cohort_without_dc_duplication() -> (
    None
):
    for sheet, props, _, _ in _visuals(_decode(_render())):
        filters = {
            item["pivotAssignments"][0]["id"]: item["configuration"]
            for item in props["superFilterAssignments"]
        }
        for config in filters.values():
            assert config["locked"] is True
            assert config["selectAll"] is False
        product = filters["dim_products[ID]"]["alphaNumericValues"]
        assert (product["minValue"], product["maxValue"]) == (1, 20)
        ids = {assignment["id"] for assignment in props["pivotAssignments"]}
        if "DC" in sheet["displayName"]:
            assert (
                filters["dc_inventory_position_current[DC ID]"]["alphaNumericValues"][
                    "equalValue"
                ]
                == 1
            )
            assert "dim_stores[ID]" not in ids
        else:
            store = filters["dim_stores[ID]"]["alphaNumericValues"]
            assert (store["minValue"], store["maxValue"]) == (1, 5)
            assert "demand_forecast[Forecast Horizon Days]" not in filters
            assert "stockout_risk[Forecast Horizon Days]" not in filters
            assert "dc_inventory_position_current[DC Units on Hand]" not in ids


def test_editable_assumptions_are_zero_initialized_not_observed_stock() -> None:
    inputs = []
    for _, _, data, _ in _visuals(_decode(_render())):
        for column in data["columns"]:
            measure_type = column["columnMeta"]["measure_type"]
            if "DataInput" not in measure_type:
                continue
            inputs.append(column)
            assert "assumption" in column["description"].lower()
            assert column["disableWriteAccess"] is False
            config = measure_type["DataInput"]
            assert config["allow_input"] == "ReadAndEdit"
            assert config["disable_write_access"] is False
            numeric = config["column_type"]["Number"]
            assert numeric["default_value"] == 0
            assert numeric["min_value"] == 0
            assert numeric["distribute_parent_value_to_children"] is False
    assert {column["name"] for column in inputs} == {
        "Transfer units (assumption)",
        "Expedite units (assumption)",
        "Budget per store-SKU (assumption)",
        "Demand uplift % (assumption)",
    }


def test_scenarios_reference_columns_and_never_enable_execution_or_writeback() -> None:
    parts = _decode(_render())
    scenarios = [
        (path, part["scenarios"])
        for path, part in parts.items()
        if path.endswith("/scenarios.json")
    ]
    assert len(scenarios) == 1
    path, records = scenarios[0]
    columns = parts[path.replace("scenarios.json", "dataInput.json")]["columns"]
    column_ids = {column["measureGuid"] for column in columns}
    assert [record["name"] for record in records] == [
        "Baseline",
        "Transfer",
        "Expedite",
    ]
    assert records[0]["status"] == "LOCK"
    for record in records:
        assert record["autoWritebackEnabled"] == "INACTIVE"
        assert record["meta"]["measureIds"]
        assert set(record["meta"]["measureIds"]) <= column_ids
        assert not record.get("simulations")
        UUID(record["meta"]["scenarioGuid"])
    assert not any(
        term in path
        for path in parts
        for term in ("writeback", "approvals", "automations", "source.json", "cube/")
    )
    for path, part in parts.items():
        if path.endswith("/commentSettings.json"):
            assert part["allowComment"] is False
            assert part["notification"] is False
