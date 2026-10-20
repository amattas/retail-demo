import base64
import copy
import json

import pytest

from deploy.scripts._twin_model import _signature, merge_twin_model
from deploy.scripts.fabric_runtime import decode_definition_parts


def part(path, text):
    return {
        "path": path, "payload": base64.b64encode(text.encode()).decode(),
        "payloadType": "InlineBase64",
    }


@pytest.fixture
def model(tmp_path):
    tables = tmp_path / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition" / "tables"
    tables.mkdir(parents=True)
    twins = tmp_path / "fabric" / "twins"
    twins.mkdir()
    (twins / "metrics.json").write_text(json.dumps({
        "metrics": [{"table": "dim_stores", "name": "Twin Store State As Of"}],
    }))
    (tables / "dim_stores.tmdl").write_text(
        "table dim_stores\n"
        "\t/// Source-as-of = latest recorded event, not current time.\n"
        "\tmeasure 'Twin Store State As Of' = MAX('fact_state'[Event Timestamp])\n"
        "\t\tformatString: General Date\n"
        "\t\tdisplayFolder: Twins\n"
        "\tcolumn ID\n\t\tdataType: int64\n\t\tsourceColumn: ID\n"
    )
    (tables / "fact_state.tmdl").write_text(
        "table fact_state\n"
        "\t/// Source event time.\n"
        "\tcolumn 'Event Timestamp'\n\t\tdataType: dateTime\n\t\tsourceColumn: event_ts\n"
        "\tcolumn ID\n\t\tdataType: int64\n\t\tsourceColumn: ID\n"
    )
    definition = {"parts": [
        part("definition/tables/dim_stores.tmdl",
             "table dim_stores\n\tmeasure Existing = 42\n\t\tformatString: 0\n"
             "\tcolumn ID\n\t\tdataType: int64\n\t\tsourceColumn: ID\n"
             "\tpartition dim_stores = entity\n\t\tmode: directLake\n\t\tsource\n"
             "\t\t\tentityName: dim_stores\n\t\t\texpressionSource: LIVE_SOURCE\n"),
        part("definition/tables/fact_state.tmdl",
             "table fact_state\n\tcolumn ID\n\t\tdataType: int64\n\t\tsourceColumn: ID\n"
             "\tpartition fact_state = entity\n\t\tmode: directLake\n"),
        part("definition/expressions.tmdl", "expression LIVE_SOURCE = ORIGINAL_SOURCE"),
        part("definition/roles/Viewer.tmdl", "role Viewer\n\tmodelPermission: read"),
    ]}
    return tmp_path, definition


def test_additive_merge_preserves_bindings_roles_and_unrelated_measures(model):
    root, original = model
    before = copy.deepcopy(original)
    result, changes = merge_twin_model(original, root)
    parts = decode_definition_parts(result)
    assert original == before
    assert changes == ["dim_stores[Twin Store State As Of]", "fact_state[Event Timestamp]"]
    assert b"measure Existing = 42" in parts["definition/tables/dim_stores.tmdl"]
    assert b"expressionSource: LIVE_SOURCE" in parts["definition/tables/dim_stores.tmdl"]
    assert b"Source-as-of = latest" in parts["definition/tables/dim_stores.tmdl"]
    for path in ("definition/expressions.tmdl", "definition/roles/Viewer.tmdl"):
        assert parts[path] == decode_definition_parts(before)[path]
    again, changes = merge_twin_model(result, root)
    assert changes == []
    assert again == result


def test_conflicting_source_column_is_never_replaced(model):
    root, definition = model
    definition["parts"][1] = part(
        "definition/tables/fact_state.tmdl",
        "table fact_state\n\tcolumn 'Event Timestamp'\n"
        "\t\tdataType: dateTime\n\t\tsourceColumn: wrong_source\n",
    )
    with pytest.raises(ValueError, match="Conflicting"):
        merge_twin_model(definition, root)


def test_missing_live_table_never_creates_a_new_model_table(model):
    root, definition = model
    definition["parts"].pop(1)
    with pytest.raises(ValueError, match="one deployed TMDL table"):
        merge_twin_model(definition, root)


def test_owned_measure_update_preserves_neighbor_description(model):
    root, definition = model
    current = (
        "table dim_stores\n\t/// Old twin description.\n"
        "\tmeasure 'Twin Store State As Of' = 1\n\t\tformatString: General Date\n"
        "\t/// Existing user measure.\n\tmeasure Existing = 42\n\t\tformatString: 0\n"
    )
    definition["parts"][0] = part("definition/tables/dim_stores.tmdl", current)
    result, _changes = merge_twin_model(definition, root)
    text = decode_definition_parts(result)["definition/tables/dim_stores.tmdl"].decode()
    assert "Old twin description" not in text
    assert "/// Existing user measure.\n\tmeasure Existing = 42" in text
    assert text.count("measure 'Twin Store State As Of'") == 1


def test_service_unfenced_multiline_expression_matches_source():
    source = (
        "\tmeasure 'Twin Example' = ```\n\t\t\tVAR x = 1\n\t\t\tRETURN x\n"
        "\t\t\t```\n\t\tformatString: 0\n"
    )
    actual = (
        "\tmeasure 'Twin Example' =\n\t\t\tVAR x = 1\n\t\t\tRETURN x\n"
        "\t\tformatString: 0\n"
    )
    assert _signature(source, "measure") == _signature(actual, "measure")
