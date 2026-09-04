"""Static checks for the generated retail ontology notebook."""

from __future__ import annotations

import ast
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ONTOLOGY_NOTEBOOK = REPO_ROOT / "fabric" / "lakehouse" / "30-create-ontology.ipynb"

EXPECTED_BUSINESS_ENTITIES = {
    "Geography",
    "Store",
    "DistributionCenter",
    "Truck",
    "Customer",
    "Product",
    "Receipt",
    "OnlineOrder",
    "Promotion",
    "Payment",
    "CustomerSegment",
    "ChurnPrediction",
}

EXPECTED_EVENTHOUSE_BINDINGS = {
    ("Receipt", "receipt_created"),
    ("Receipt", "receipt_line_added"),
    ("Payment", "payment_processed"),
    ("Promotion", "promotion_applied"),
    ("OnlineOrder", "online_order_created"),
    ("OnlineOrder", "online_order_picked"),
    ("OnlineOrder", "online_order_shipped"),
    ("Store", "receipt_created"),
    ("Store", "payment_processed"),
    ("Store", "promotion_applied"),
    ("Store", "inventory_updated"),
    ("Store", "stockout_detected"),
    ("Store", "reorder_triggered"),
    ("Store", "customer_entered"),
    ("Store", "customer_zone_changed"),
    ("Store", "ble_ping_detected"),
    ("Store", "truck_arrived"),
    ("Store", "truck_departed"),
    ("Store", "store_opened"),
    ("Store", "store_closed"),
    ("Customer", "receipt_created"),
    ("Customer", "payment_processed"),
    ("Customer", "promotion_applied"),
    ("Customer", "online_order_created"),
    ("Product", "receipt_line_added"),
    ("Product", "inventory_updated"),
    ("Product", "stockout_detected"),
    ("Product", "reorder_triggered"),
    ("DistributionCenter", "inventory_updated"),
    ("DistributionCenter", "stockout_detected"),
    ("DistributionCenter", "reorder_triggered"),
    ("DistributionCenter", "truck_arrived"),
    ("DistributionCenter", "truck_departed"),
}

EXPECTED_EVENTHOUSE_RELATIONSHIP_CONTEXTS = {
    ("ReceiptPlacedByCustomer", "receipt_created"),
    ("ReceiptAtStore", "receipt_created"),
    ("ReceiptContainsProduct", "receipt_line_added"),
    ("PaymentForReceipt", "payment_processed"),
    ("PaymentForOnlineOrder", "payment_processed"),
    ("PromotionAppliedToReceipt", "promotion_applied"),
    ("PromotionAtStore", "promotion_applied"),
    ("PromotionByCustomer", "promotion_applied"),
    ("OnlineOrderPlacedByCustomer", "online_order_created"),
}


def _notebook_source() -> str:
    notebook = json.loads(ONTOLOGY_NOTEBOOK.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )


def _literal_assignment(source: str, name: str) -> object:
    tree = ast.parse(source)
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"Assignment {name!r} not found")


def test_ontology_entities_are_business_entities_not_event_log_rows() -> None:
    source = _notebook_source()
    entity_config = _literal_assignment(source, "ENTITY_CONFIG")
    entity_names = {config["entity_name"] for config in entity_config.values()}

    assert entity_names == EXPECTED_BUSINESS_ENTITIES
    assert not any(name.endswith("Event") for name in entity_names)


def test_eventhouse_tables_bind_to_existing_business_entities() -> None:
    source = _notebook_source()
    bindings = _literal_assignment(source, "EVENTHOUSE_ENTITY_BINDINGS")
    actual = {
        (binding["entity_name"], binding["source_table"])
        for binding in bindings
    }

    assert EXPECTED_EVENTHOUSE_BINDINGS.issubset(actual)
    assert all(binding["source"] == "eventhouse" for binding in bindings)
    assert all(binding["timestamp_candidates"] == ["ingest_timestamp"] for binding in bindings)


def test_eventhouse_contextualizes_existing_business_relationships() -> None:
    source = _notebook_source()
    contexts = _literal_assignment(source, "EVENTHOUSE_RELATIONSHIP_CONTEXTS")
    relationships = _literal_assignment(source, "RELATIONSHIPS")
    context_pairs = {
        (context["name"], context["source_table"])
        for context in contexts
    }
    relationship_names = {relationship["name"] for relationship in relationships}

    assert EXPECTED_EVENTHOUSE_RELATIONSHIP_CONTEXTS.issubset(context_pairs)
    assert all(context["name"] in relationship_names for context in contexts)
    assert all(context["source"] == "eventhouse" for context in contexts)


def test_business_entities_emit_multiple_data_bindings() -> None:
    source = _notebook_source()

    assert "for event_binding in metadata['eventhouse_bindings']" in source
    assert "'dataBindingType': 'TimeSeries'" in source
    assert "'sourceType': 'KustoTable'" in source
    assert "relationship_groups" in source


def test_first_create_requires_successful_derived_graph_rebuild() -> None:
    source = _notebook_source()

    assert "GRAPH_SETTLE_SECONDS" in source
    assert "def _validate_created_graph():" in source
    assert source.count("_validate_created_graph()") == 3
    assert "Re-applied ontology definition to rebuild the derived graph." in source
    assert "the derived graph is not proven ready." in source
    assert "if the graph shows no valid content, re-run this notebook." not in source


def test_ontology_generates_descriptions_for_every_property_kind() -> None:
    source = _notebook_source()

    assert "def ontology_field_description(" in source
    assert "'description': ontology_field_description(" in source
    assert source.count("'semanticEnrichment': {") >= 2
    assert source.count("'description': column['description']") == 2


def test_ontology_applies_descriptions_through_authoring_api() -> None:
    source = _notebook_source()

    assert "notebookutils.credentials.getToken('pbi')" in source
    assert "'Authorization': f'Bearer {pbi_token}'" in source
    assert "/metadata/v201606/generatemwctokenv2" in source
    assert "'Authorization': f'MwcToken {workload_token}'" in source
    assert "f'entityTypes/{entity_type_id}'" in source
    assert "'properties': described_properties" in source
    assert "'timeseriesProperties': described_timeseries_properties" in source
    assert "field.get('semanticEnrichment', {}).get('description')" in source


def test_ontology_verifies_persisted_field_descriptions() -> None:
    source = _notebook_source()

    assert "def _verify_field_descriptions(" in source
    assert "missing_descriptions" in source
    assert "_verify_field_descriptions(authoring_context)" in source
