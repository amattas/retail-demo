"""Contracts for manually maintained ancillary Fabric assets."""

from __future__ import annotations

import json
import re
from pathlib import Path

from deploy.scripts._ontology_model import ENTITIES, ONTOLOGY_NAME

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = ROOT / "fabric" / "dashboards"
DATA_AGENTS = ROOT / "fabric" / "data-agents"
QUERYSETS = ROOT / "fabric" / "querysets"
RULES = ROOT / "fabric" / "rules" / "definitions.kql"
TASKFLOW = ROOT / "fabric" / "taskflow" / "taskflow.json"
SEMANTIC_TABLES = (
    ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition" / "tables"
)

SAFE_SEMANTIC_TABLES = {
    "campaign_performance_daily",
    "dc_inventory_position_current",
    "demand_forecast",
    "dim_date",
    "dim_distribution_centers",
    "dim_geographies",
    "dim_products",
    "dim_stores",
    "inventory_position_current",
    "marketing_cost_daily",
    "online_sales_daily",
    "price_elasticity",
    "product_recommendations",
    "sales_minute_store",
    "stockout_risk",
    "tender_mix_daily",
    "top_products_15m",
    "truck_dwell_daily",
    "zone_dwell_minute",
}
DIRECT_IDENTIFIER_FIELDS = {
    "Ad ID",
    "Attribution ID",
    "Attribution Journey ID",
    "BLE ID",
    "Beacon ID",
    "Customer Ad ID",
    "Customer BLE ID",
    "Customer ID",
    "First Name",
    "Impression ID",
    "Last Name",
    "License Plate",
    "Loyalty Card",
    "Order ID",
    "Phone",
    "Receipt ID",
    "Shipment ID",
    "Trace ID",
    "Transaction ID",
    "Truck ID",
}


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dashboard_tiles(name: str) -> list[dict[str, object]]:
    return _load_json(DASHBOARDS / name)["tiles"]  # type: ignore[return-value]


def _semantic_datasource(stage: str) -> dict[str, object]:
    return _load_json(
        DATA_AGENTS
        / "retail-semantic-model-agent.DataAgent"
        / "Files"
        / "Config"
        / stage
        / "semantic-model-retail_model"
        / "datasource.json"
    )


def _ontology_datasource(stage: str) -> dict[str, object]:
    return _load_json(
        DATA_AGENTS
        / "retail-ontology-agent.DataAgent"
        / "Files"
        / "Config"
        / stage
        / "ontologies"
        / ONTOLOGY_NAME
        / "ontology.json"
    )


def _selected_names(datasource: dict[str, object]) -> set[str]:
    elements = datasource["elements"]
    assert isinstance(elements, list)
    return {
        str(element["display_name"])
        for element in elements
        if isinstance(element, dict) and element.get("is_selected") is True
    }


def _element_names(datasource: dict[str, object]) -> set[str]:
    elements = datasource["elements"]
    assert isinstance(elements, list)
    return {
        str(element["display_name"])
        for element in elements
        if isinstance(element, dict)
    }


def _current_ontology_entities() -> set[str]:
    return {entity.name for entity in ENTITIES}


def _semantic_table_ids() -> dict[str, str]:
    table_ids: dict[str, str] = {}
    for path in SEMANTIC_TABLES.glob("*.tmdl"):
        source = path.read_text(encoding="utf-8")
        match = re.search(r"^\tlineageTag:\s*(\S+)", source, re.MULTILINE)
        assert match is not None, path.name
        table_ids[path.stem] = match.group(1)
    return table_ids


def test_fulfillment_counts_do_not_use_an_unsupported_constant_join() -> None:
    queryset = (QUERYSETS / "q_fulfillment_pipeline_24h.kql").read_text(
        encoding="utf-8"
    )
    dashboard_query = next(
        str(tile["query"])
        for tile in _dashboard_tiles("retail-ops.template.json")
        if tile["title"] == "Fulfillment Pipeline (24h)"
    )

    for query in (queryset, dashboard_query):
        assert not re.search(r"\bjoin\b.*\bon\s+1\s*==\s*1", query)
        assert query.count("toscalar(") == 3
        assert "print created" in query


def test_truck_dwell_alerts_are_driven_by_completed_nonfuture_departures() -> None:
    rules = RULES.read_text(encoding="utf-8")
    queryset = (QUERYSETS / "q_truck_dwell_by_site.kql").read_text(encoding="utf-8")
    dashboard_query = next(
        str(tile["query"])
        for tile in _dashboard_tiles("retail-ops.template.json")
        if tile["title"] == "Truck Dwell by Site (24h)"
    )

    for query in (rules, queryset, dashboard_query):
        assert "departure_ingest_timestamp > ago(" in query
        assert "departure_ingest_timestamp <= now()" in query
        assert "departure_time <= now()" in query
    assert "arrival_ingest_timestamp > ago(5m)" not in rules


def test_stockout_and_customer_arrival_assets_do_not_claim_current_occupancy() -> None:
    assert not (QUERYSETS / "q_stockouts_open_by_store.kql").exists()
    detections = (QUERYSETS / "q_stockout_detections_by_store.kql").read_text(
        encoding="utf-8"
    )
    rules = RULES.read_text(encoding="utf-8")
    dashboard = _dashboard_tiles("retail-ops.template.json")

    assert "no resolution event exists" in detections
    assert "Stockout Detections (24h)" in {str(tile["title"]) for tile in dashboard}
    assert "Open Stockouts" not in json.dumps(dashboard)
    assert "arrival surge" in rules.lower()
    assert "not an occupancy estimate" in rules.lower()
    assert "overcrowd" not in rules.lower()


def test_zone_dwell_rollups_are_weighted_by_customer_count() -> None:
    queryset = (QUERYSETS / "q_zone_dwell_heatmap.kql").read_text(encoding="utf-8")
    rules = RULES.read_text(encoding="utf-8")
    dashboard_query = next(
        str(tile["query"])
        for tile in _dashboard_tiles("retail-ops.template.json")
        if tile["title"] == "Zone Arrivals and Weighted Dwell (30m)"
    )

    for query in (queryset, rules, dashboard_query):
        assert "todouble(" in query
        assert "* todouble(" in query or "*todouble(" in query
        assert "/ todouble(" in query or "/todouble(" in query
        assert "arrivals" in query
    assert "avg(avg_dwell)" not in queryset


def test_pricing_catalog_uses_current_state_and_labels_expected_impact() -> None:
    tiles = _dashboard_tiles("pricing-approval.template.json")
    pending_titles = {
        "Pending Recommendations Summary",
        "Recommendations by Department (Pending)",
        "Top Pending Recommendations by Revenue Impact",
        "Price Change Distribution (Pending)",
        "Confidence Level Breakdown",
        "Constraint Compliance Status",
    }
    pending_queries = [
        str(tile["query"]) for tile in tiles if tile["title"] in pending_titles
    ]
    impact = next(
        tile
        for tile in tiles
        if tile["title"] == "Expected Revenue Impact of Approvals (7d)"
    )

    assert len(pending_queries) == len(pending_titles)
    assert all("fn_pending_recommendations()" in query for query in pending_queries)
    assert all(" by 1" not in str(tile["query"]) for tile in tiles)
    assert "Realized Revenue Impact" not in json.dumps(tiles)
    assert "arg_max(event_ts, *) by recommendation_id" in str(impact["query"])
    assert "expected_revenue_impact=sum(expected_revenue_impact)" in str(
        impact["query"]
    )


def test_dashboard_query_catalogs_are_explicitly_not_deployable() -> None:
    for path in sorted(DASHBOARDS.glob("*.template.json")):
        catalog = _load_json(path)
        assert catalog["format"] == "retail-demo.dashboard-query-catalog/v1"
        assert catalog["deployable"] is False
        assert "$schema" not in catalog

    manifest = _load_json(ROOT / "contracts" / "retail-demo.json")
    assets = {
        str(asset["id"]): asset
        for asset in manifest["assets"]  # type: ignore[index]
        if isinstance(asset, dict)
    }
    assert assets["asset.dashboard-templates"]["support_status"] == "manual"
    build_source = (ROOT / "deploy" / "scripts" / "build_artifacts.py").read_text(
        encoding="utf-8"
    )
    readiness_source = (
        ROOT / "deploy" / "scripts" / "verify_readiness.py"
    ).read_text(encoding="utf-8")
    assert "stage_dashboard" not in build_source
    assert 'ExpectedItem("KQLDashboard"' not in readiness_source


def test_semantic_agent_selection_matches_current_model_and_excludes_pii_tables() -> None:
    draft = _semantic_datasource("draft")
    published = _semantic_datasource("published")
    model_table_ids = _semantic_table_ids()
    elements = draft["elements"]
    assert isinstance(elements, list)
    datasource_table_ids = {
        str(table["display_name"]): str(table["id"])
        for table in elements
        if isinstance(table, dict)
    }

    assert draft == published
    assert datasource_table_ids == model_table_ids
    assert _selected_names(draft) == SAFE_SEMANTIC_TABLES
    assert not {
        "dim_customers",
        "churn_predictions",
        "customer_segments",
        "fact_ble_pings",
        "fact_marketing",
        "fact_marketing_attribution",
        "fact_online_order_headers",
        "fact_online_order_lines",
        "fact_payments",
        "fact_receipts",
    } & _selected_names(draft)
    assert all(
        child.get("is_selected") is False
        for table in elements
        if isinstance(table, dict) and table.get("is_selected") is False
        for child in table.get("children", [])
        if isinstance(child, dict)
    )
    exposed_fields = {
        str(child["display_name"])
        for table in elements
        if isinstance(table, dict) and table.get("is_selected") is True
        for child in table.get("children", [])
        if isinstance(child, dict) and child.get("is_selected") is True
    }
    assert DIRECT_IDENTIFIER_FIELDS.isdisjoint(exposed_fields)
    instructions = str(draft["dataSourceInstructions"]).lower()
    assert "aggregate" in instructions
    assert "at least 10" in instructions
    assert "refuse" in instructions


def test_ontology_agent_selection_is_current_private_and_publish_explicit() -> None:
    draft = _ontology_datasource("draft")
    published = _ontology_datasource("published")
    agent_root = DATA_AGENTS / "retail-ontology-agent.DataAgent" / "Files" / "Config"

    assert draft == published
    assert _element_names(draft) == _current_ontology_entities()
    # Fabric doesn't enforce selection for ontology sources, so every entity is
    # selected and the stage instructions carry the personal-data guard.
    assert _selected_names(draft) == _current_ontology_entities()
    instructions = str(
        _load_json(agent_root / "draft" / "stage_config.json")["aiInstructions"]
    ).lower()
    for identifier in ("names", "phone", "loyalty card", "customer ids", "license plates"):
        assert identifier in instructions
    assert "re-identify" in instructions
    assert (agent_root / "publish_info.json").is_file()
    # TMDL ontologies are only queryable through the ontology-as-context
    # source; the legacy datasource.json binding calls an API the new
    # ontology format rejects.
    assert str(draft["$schema"]).endswith("/dataAgent/definition/ontology/1.0.0/schema.json")
    assert draft["displayName"] == ONTOLOGY_NAME
    assert not list(agent_root.glob("*/ontology-*/datasource.json"))


def test_agent_stage_instructions_enforce_privacy_and_match_published_state() -> None:
    for agent in (
        "retail-semantic-model-agent.DataAgent",
        "retail-ontology-agent.DataAgent",
    ):
        config_root = DATA_AGENTS / agent / "Files" / "Config"
        draft = _load_json(config_root / "draft" / "stage_config.json")
        published = _load_json(config_root / "published" / "stage_config.json")
        instructions = str(draft["aiInstructions"]).lower()

        assert draft == published
        assert "never" in instructions
        assert "row-level" in instructions
        assert "at least 10" in instructions
        assert "refuse" in instructions
        assert (config_root / "publish_info.json").is_file()


def test_taskflow_uses_matching_type_and_identifier_prefixes() -> None:
    taskflow = _load_json(TASKFLOW)
    items = [
        item
        for task in taskflow["tasks"]  # type: ignore[index]
        if isinstance(task, dict)
        for item in task["items"]
        if isinstance(item, dict)
    ]
    for item in items:
        unique_type, separator, _identifier = str(item["artifactUniqueId"]).partition(
            ":"
        )
        assert separator
        assert unique_type == item["artifactType"]

    semantic_item = next(
        item for item in items if item.get("artifactName") == "retail_model"
        and item.get("artifactType") == "dataset"
    )
    datasource = _semantic_datasource("draft")
    assert semantic_item["artifactUniqueId"] == f"dataset:{datasource['artifactId']}"
