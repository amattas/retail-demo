"""Contract tests for the new-experience (TMDL) ontology definition."""

from __future__ import annotations

import ast
import json
import re
from dataclasses import replace
from pathlib import Path

import pytest

from deploy.scripts import _ontology_model as model
from deploy.scripts import deploy_ontology
from deploy.scripts._ontology_rules import RULES

ROOT = Path(__file__).resolve().parents[2]
ALL_BRONZE = {binding.table for binding in model.EVENT_BINDINGS}
DATABASE = "11111111-2222-3333-4444-555555555555"
SOURCE = model.SourceItem(
    workspace_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    workspace_name="retail-demo",
    item_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    item_name="retail_lakehouse",
    pinned_at="2026-10-23T00:00:00Z",
)


def _build(**overrides):
    options = {
        "sql_endpoint": "abc.datawarehouse.fabric.microsoft.com",
        "sql_database_id": DATABASE,
        "source": SOURCE,
        "available_bronze_tables": ALL_BRONZE,
    }
    options.update(overrides)
    return model.build_definition(**options)


@pytest.fixture(scope="module")
def parts():
    return _build()


def _columns(parts):
    columns = set()
    for path, text in parts.items():
        if path.startswith("tables/"):
            table = re.search(r"^table (\S+)$", text, re.M).group(1)
            columns |= {f"{table}.{name}" for name in re.findall(r"^\tcolumn (\S+)$", text, re.M)}
    return columns


def test_definition_has_required_parts_and_tmdl_platform(parts):
    for required in (".platform", "database.tmdl", "model.tmdl", "namespaces/default.tmdl",
                     "expressions.tmdl", "relationships.tmdl", "entityRelationships.tmdl"):
        assert required in parts
    assert '"version": "2.0"' in parts[".platform"]
    assert "compatibilityLevel: 1000000" in parts["database.tmdl"]
    assert f'Sql.Database("abc.datawarehouse.fabric.microsoft.com", "{DATABASE}")' in (
        parts["expressions.tmdl"]
    )
    assert not any(path.startswith("EntityTypes/") or path == "definition.json" for path in parts)


def test_model_references_every_element_part(parts):
    refs = set(re.findall(r"^ref (table|entity|rule) (\S+)$", parts["model.tmdl"], re.M))
    expected = {
        ({"tables": "table", "entities": "entity", "rules": "rule"}[path.split("/")[0]],
         path.split("/", 1)[1][:-5])
        for path in parts if path.startswith(("tables/", "entities/", "rules/"))
    }
    assert refs == expected
    assert "ref namespace default" in parts["model.tmdl"]


def test_entities_keys_and_bindings_resolve_to_declared_columns(parts):
    columns = _columns(parts)
    entities = {path for path in parts if path.startswith("entities/")}
    assert {path[9:-5] for path in entities} == {entity.name for entity in model.ENTITIES}
    for path in entities:
        text = parts[path]
        properties = re.findall(r"^\tproperty (\S+)$", text, re.M)
        assert len(properties) == len(set(properties))
        key = re.search(r"^\tkeyProperty: (\S+)$", text, re.M).group(1)
        assert key in properties
        for reference in re.findall(r"^\t\t\t(?:valueColumn|orderingColumn): (\S+)$", text, re.M):
            assert reference in columns, (path, reference)
    assert "__index_level_0__" not in "".join(parts.values())


def test_stockout_risk_is_generator_discoverable_entity(parts):
    entity = parts["entities/StockoutRisk.tmdl"]
    assert "\tkeyProperty: risk_id\n" in entity
    for property_name in (
        "store_id",
        "product_id",
        "inventory_as_of",
        "current_inventory",
        "days_of_inventory",
        "stockout_probability",
        "forecast_horizon_days",
    ):
        assert f"\tproperty {property_name}\n" in entity


def test_time_series_properties_are_ordered_by_ingest_timestamp(parts):
    store = parts["entities/Store.tmdl"]
    blocks = re.findall(r"dataType: TimeSeries<\w+>.*?(?=\n\t///|\n\tadditionalBackingTable|\Z)",
                        store, re.S)
    assert blocks
    assert all("type: timeSeries" in block and ".ingest_timestamp" in block for block in blocks)
    assert store.count("\tadditionalBackingTable\n") == sum(
        1 for binding in model.EVENT_BINDINGS if binding.entity == "Store"
    )


def test_relationships_reference_existing_tom_relationships(parts):
    model.validate_definition(parts)
    names = re.findall(r"^entityRelationship (\S+)$", parts["entityRelationships.tmdl"], re.M)
    assert len(names) == len(model.RELATIONSHIPS) + len(model.JUNCTIONS) == 20


def _notebook_model() -> dict:
    notebook = json.loads(
        (ROOT / "fabric" / "lakehouse" / "30-create-ontology.ipynb").read_text(encoding="utf-8")
    )
    source = next(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if "ENTITY_CONFIG = {" in "".join(cell["source"])
    )
    tree = ast.parse(source)
    values: dict = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            try:
                values[node.targets[0].id] = ast.literal_eval(node.value)
            except ValueError:
                continue
    return values


def _resolve(columns: list[str], candidates: list[str]) -> str:
    lookup = {column.lower(): column for column in columns}
    return next(lookup[c.lower()] for c in candidates if c.lower() in lookup)


def test_notebook_30_defines_the_same_ontology_as_the_deploy_builder():
    schemas = model.load_schemas(ROOT)
    columns = {
        name: [column for column, _ in fields]
        for name, fields in {**schemas.silver, **schemas.gold}.items()
    }
    notebook = _notebook_model()
    entities = {
        cfg["entity_name"]: (table, _resolve(columns[table], cfg["key_candidates"]))
        for table, cfg in notebook["ENTITY_CONFIG"].items()
    }
    assert entities == {entity.name: (entity.table, entity.key) for entity in model.ENTITIES}

    keys = {entity.name: entity.key for entity in model.ENTITIES}
    expected = {
        rel.name: (rel.source, rel.target, rel.from_table, keys[rel.source], rel.from_column)
        for rel in model.RELATIONSHIPS
    } | {
        junction.name: (
            junction.source, junction.target, junction.table,
            junction.source_column, junction.target_column,
        )
        for junction in model.JUNCTIONS
    }
    actual = {
        rel["name"]: (
            rel["source_entity"], rel["target_entity"], rel["source_table"],
            _resolve(columns[rel["source_table"]], rel["source_column_candidates"]),
            _resolve(columns[rel["source_table"]], rel["target_column_candidates"]),
        )
        for rel in notebook["RELATIONSHIPS"]
    }
    assert actual == expected
    # Historical-only until the deploy builder publishes event backing tables.
    assert notebook["ENABLE_EVENTHOUSE_BINDINGS"] is deploy_ontology._ENABLE_EVENT_BACKING_TABLES


def test_relationship_keys_must_share_a_type():
    broken = model.Relationship(
        "TruckAssignedToDistributionCenter", "Truck", "DistributionCenter",
        "dim_trucks", "DCID", "dim_distribution_centers", "ID",
    )
    with pytest.raises(model.OntologyModelError, match="DCID is double but .*ID is int64"):
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(model, "RELATIONSHIPS", (*model.RELATIONSHIPS, broken))
            _build()


def test_promotion_and_stockout_keys_identify_one_row_of_their_grain():
    schemas = model.load_schemas(ROOT)
    promotions = dict(schemas.silver["fact_promotions"])
    assert promotions["trace_id"] == "string"
    source = (ROOT / "utility" / "src" / "retail_setup" / "generation" / "promotions.py").read_text(
        encoding="utf-8"
    )
    # trace_id is built from exactly the (receipt_id_ext, promo_code) grain.
    assert 'lines.groupBy("receipt_id_ext", "promo_code")' in source
    assert (
        'F.concat(F.lit("TRC-PRM-"), F.col("receipt_id_ext"), F.lit("-"), F.col("promo_code"))'
        in source
    )
    manifest = json.loads((ROOT / "contracts" / "retail-demo.json").read_text(encoding="utf-8"))
    stockout = next(
        contract["output"]
        for contract in manifest["ml_contracts"]
        if contract["output"]["table"] == "stockout_risk"
    )
    assert stockout["grain"] == ["store_id", "product_id", "forecast_horizon_days"]
    notebook = (ROOT / "fabric" / "lakehouse" / "12-ml-stockout-prediction.ipynb").read_text(
        encoding="utf-8"
    )
    assert 'for name in (\\"store_id\\", \\"product_id\\", \\"forecast_horizon_days\\")' in notebook
    assert '.alias(\\"risk_id\\")' in notebook


def test_inactive_flags_mirror_the_semantic_model():
    tmdl = (ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition"
            / "relationships.tmdl").read_text(encoding="utf-8")
    inactive_pairs = set()
    for block in re.split(r"\nrelationship ", tmdl):
        if "isActive: false" in block:
            frm = re.search(r"fromColumn: (\w+)\.", block).group(1)
            to = re.search(r"toColumn: (\w+)\.", block).group(1)
            inactive_pairs.add((frm, to))
    for rel in model.RELATIONSHIPS:
        if not rel.active:
            assert (rel.from_table, rel.to_table) in inactive_pairs, rel.name


def test_each_event_table_has_one_active_relationship(parts):
    active: dict[str, int] = {}
    for block in parts["relationships.tmdl"].split("\n\n"):
        match = re.search(r"fromColumn: (\w+)\.", block)
        if match and match.group(1) in ALL_BRONZE and "isActive: false" not in block:
            active[match.group(1)] = active.get(match.group(1), 0) + 1
    assert active and set(active.values()) == {1}


def test_missing_bronze_tables_drop_only_their_bindings():
    reduced = _build(available_bronze_tables={"receipt_created"})
    assert "tables/receipt_created.tmdl" in reduced
    assert "tables/payment_processed.tmdl" not in reduced
    model.validate_definition(reduced)
    assert "payment_processed" not in reduced["entities/Store.tmdl"]
    none = _build(available_bronze_tables=set())
    assert not any(path[7:-5] in ALL_BRONZE for path in none if path.startswith("tables/"))


def test_definition_is_deterministic():
    assert _build() == _build()


def test_indentation_uses_tabs_outside_the_m_expression(parts):
    for path, text in parts.items():
        if path.endswith(".tmdl") and path != "expressions.tmdl":
            assert not re.search(r"^ +\S", text, re.M), path


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"sql_database_id": "not-a-guid"}, "badly formed"),
        ({"sql_endpoint": 'bad"host'}, "SQL endpoint"),
        ({"source": replace(SOURCE, item_name="bad\nname")}, "ONT_ItemName"),
        ({"source": replace(SOURCE, item_id="not-a-guid")}, "badly formed"),
    ],
)
def test_invalid_endpoint_inputs_are_rejected(overrides, message):
    with pytest.raises(ValueError, match=message):
        _build(**overrides)


def test_every_partition_identifies_its_lakehouse_source(parts):
    expected = {
        "ONT_WorkspaceId": SOURCE.workspace_id,
        "ONT_ItemId": SOURCE.item_id,
        "ONT_ItemKind": "Lakehouse",
        "ONT_ItemName": "retail_lakehouse",
        "ONT_WorkspaceName": "retail-demo",
        "ONT_SqlEndpoint": "abc.datawarehouse.fabric.microsoft.com",
        "ONT_SqlDatabase": "retail_lakehouse",
        "ONT_PinnedAtUtc": "2026-10-23T00:00:00Z",
    }
    tables = [text for path, text in parts.items() if path.startswith("tables/")]
    assert tables
    for text in tables:
        partition = text.split("\tpartition ", 1)[1]
        assert dict(re.findall(r"^\t\tannotation (\S+) = (.+)$", partition, re.M)) == expected


def test_schema_drift_fails_closed():
    schemas = model.load_schemas()
    silver = dict(schemas.silver)
    silver["dim_stores"] = tuple(c for c in silver["dim_stores"] if c[0] != "GeographyID")
    with pytest.raises(model.OntologyModelError, match="GeographyID"):
        _build(schemas=replace(schemas, silver=silver))


def test_rules_reference_existing_concepts(parts):
    relationships = set(
        re.findall(r"^entityRelationship (\S+)$", parts["entityRelationships.tmdl"], re.M)
    )
    for rule in RULES:
        assert f"rules/{rule.name}.tmdl" in parts
        for reference in rule.entities:
            entity = parts[f"entities/{reference.entity}.tmdl"]
            for prop in reference.properties:
                assert f"\tproperty {prop}\n" in entity, (rule.name, prop)
        assert set(rule.relationships) <= relationships
