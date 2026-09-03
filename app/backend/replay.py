"""Deterministic synthetic replay for the Retail CX Decision Studio."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from . import proposals

BRAND = "Aster & Pine Outfitters"
SCENARIO_ID = "protect-the-winner"

REPORT = {
    "brand": BRAND,
    "scenario": "Protect the Winner",
    "period": "FY26 P08",
    "headline": (
        "Performance Footwear is down 4.8%, while Momentum Runner is up 18.6%."
    ),
    "kpis": [
        {"label": "Net sales", "value": "$42.8M", "delta": "-1.9% vs prior year"},
        {"label": "Momentum Runner", "value": "$6.4M", "delta": "+18.6%"},
        {"label": "Stores at risk", "value": "3", "delta": "under 1.5 weeks cover"},
        {"label": "Sales protected", "value": "$286K", "delta": "modeled 14-day impact"},
    ],
    "stores": [
        {
            "store": "STORE-014",
            "market": "Central Metro",
            "velocity": 34.2,
            "weeks_cover": 0.8,
            "recommendation": "Replenish",
        },
        {
            "store": "STORE-022",
            "market": "Lakeside",
            "velocity": 28.7,
            "weeks_cover": 1.1,
            "recommendation": "Transfer",
        },
        {
            "store": "STORE-031",
            "market": "North Ridge",
            "velocity": 8.4,
            "weeks_cover": 7.6,
            "recommendation": "Source candidate",
        },
        {
            "store": "STORE-009",
            "market": "River District",
            "velocity": 25.1,
            "weeks_cover": 4.2,
            "recommendation": "Activate",
        },
    ],
}

GRAPH = {
    "nodes": [
        {"id": "TradingSignal", "label": "TradingSignal", "property_count": 8,
         "timeseries_count": 0},
        {"id": "ProductFamily", "label": "ProductFamily", "property_count": 7,
         "timeseries_count": 0},
        {"id": "Product", "label": "Product", "property_count": 12,
         "timeseries_count": 0},
        {"id": "SKU", "label": "SKU", "property_count": 9,
         "timeseries_count": 0},
        {"id": "Store", "label": "Store", "property_count": 10,
         "timeseries_count": 4},
        {"id": "InventorySnapshot", "label": "InventorySnapshot",
         "property_count": 8, "timeseries_count": 0},
        {"id": "FulfillmentNode", "label": "FulfillmentNode",
         "property_count": 6, "timeseries_count": 2},
        {"id": "Recommendation", "label": "Recommendation",
         "property_count": 10, "timeseries_count": 0},
        {"id": "Evidence", "label": "Evidence", "property_count": 8,
         "timeseries_count": 0},
        {"id": "Decision", "label": "Decision", "property_count": 9,
         "timeseries_count": 1},
        {"id": "Override", "label": "Override", "property_count": 7,
         "timeseries_count": 0},
    ],
    "edges": [
        {"source": "TradingSignal", "target": "ProductFamily",
         "label": "identifies opportunity", "kind": "lakehouse"},
        {"source": "ProductFamily", "target": "Product",
         "label": "contains", "kind": "lakehouse"},
        {"source": "Product", "target": "SKU",
         "label": "has variant", "kind": "lakehouse"},
        {"source": "SKU", "target": "InventorySnapshot",
         "label": "has inventory", "kind": "lakehouse"},
        {"source": "InventorySnapshot", "target": "Store",
         "label": "measured at", "kind": "lakehouse"},
        {"source": "Store", "target": "FulfillmentNode",
         "label": "served by", "kind": "lakehouse"},
        {"source": "TradingSignal", "target": "Recommendation",
         "label": "supports", "kind": "lakehouse"},
        {"source": "Recommendation", "target": "Evidence",
         "label": "grounded by", "kind": "lakehouse"},
        {"source": "Recommendation", "target": "Decision",
         "label": "reviewed as", "kind": "eventhouse"},
        {"source": "Decision", "target": "Override",
         "label": "may include", "kind": "eventhouse"},
    ],
}

ENTITY_PROPERTIES = {
    "TradingSignal": ["signal_id", "period", "metric", "baseline", "actual",
                      "variance", "materiality", "status"],
    "ProductFamily": ["family_id", "name", "category", "brand", "season",
                      "net_sales", "growth_rate"],
    "Product": ["product_id", "name", "family_id", "category", "department",
                "list_price", "unit_cost", "margin_rate", "status",
                "launch_date", "channel", "description"],
    "SKU": ["sku_id", "product_id", "color", "size", "barcode", "status",
            "unit_cost", "list_price", "compatibility_group"],
    "Store": ["store_id", "store_name", "market", "region", "format", "channel",
              "capacity_band", "manager_role", "open_date", "status"],
    "InventorySnapshot": ["snapshot_date", "store_id", "sku_id", "on_hand",
                          "in_transit", "sales_velocity", "weeks_cover",
                          "risk_band"],
    "FulfillmentNode": ["node_id", "node_name", "node_type", "region",
                        "service_level", "status"],
    "Recommendation": ["recommendation_id", "scenario_id", "action_type",
                       "target_id", "source_id", "quantity", "expected_value",
                       "risk", "status", "created_at"],
    "Evidence": ["evidence_id", "scenario_id", "source", "grain", "period",
                 "metric", "value", "quality_status"],
    "Decision": ["decision_id", "recommendation_id", "status", "owner_role",
                 "reason", "approved_at", "review_date", "expected_value",
                 "scenario_id"],
    "Override": ["override_id", "decision_id", "excluded_candidate", "reason",
                 "replacement_candidate", "owner_role", "created_at"],
}

_override: dict[str, str] | None = None
_studio_state = {
    "signalStatus": "active",
    "completed": [],
    "decisionStatus": "draft",
}

IQ_STAGES = {
    "work": {
        "name": "Work IQ",
        "promise": "Understand the assignment and how the team works.",
        "prompt": (
            "What outcomes and review criteria matter for this merchandising decision?"
        ),
        "answer": (
            "The fictional planning brief prioritizes profitable growth, avoids "
            "promoting inventory-constrained stores, requires a named decision "
            "owner, and asks for an explicit review date and override trail."
        ),
        "sources": ["Mocked email brief", "Mocked Teams decision thread",
                    "Synthetic operating calendar"],
        "next": "Use Fabric IQ to find the governed business opportunity.",
    },
    "fabric": {
        "name": "Fabric IQ",
        "promise": "Understand governed business data and relationships.",
        "prompt": (
            "Which product family is growing inside a declining category, and "
            "where can inventory support the opportunity?"
        ),
        "answer": (
            "Momentum Runner is up 18.6% while Performance Footwear is down 4.8%. "
            "STORE-009 can support activation, STORE-014 and STORE-022 need "
            "inventory intervention, and STORE-031 is an initial transfer source."
        ),
        "sources": ["Direct Lake semantic model", "Fabric Ontology",
                    "Synthetic Eventhouse signals"],
        "next": "Use Foundry IQ to apply the decision policy and trigger agents.",
    },
    "foundry": {
        "name": "Foundry IQ",
        "promise": "Ground the orchestrator in policy and invoke action tools.",
        "prompt": (
            "Apply the supply-aware activation policy and prepare reviewable actions."
        ),
        "answer": (
            "Policy requires at least 3 weeks of modeled cover for activation, "
            "evidence for every action, and approval before any external write. "
            "The orchestrator prepares one activation, one replenishment, and "
            "one transfer draft."
        ),
        "sources": ["Synthetic merchandising policy", "Agent instructions",
                    "Typed draft-action tools"],
        "next": "Use Web IQ to test the recommendation against external context.",
    },
    "web": {
        "name": "Web IQ",
        "promise": "Add current public context without replacing internal truth.",
        "prompt": (
            "What fictional market signals could strengthen or challenge the activation?"
        ),
        "answer": (
            "The replay market brief shows increased interest in lightweight "
            "daily trainers and value-focused bundles. It supports a targeted "
            "activation but does not change the internal inventory constraints."
        ),
        "sources": ["Synthetic market brief", "Mocked trend digest",
                    "Fictional competitor summary"],
        "next": "Review the recommendation, override if needed, and approve the draft.",
    },
}


def config_payload() -> dict[str, Any]:
    return {
        "workspaceId": "",
        "reportId": "",
        "dataAgentId": "",
        "ontologyItemId": "",
        "mode": "replay",
        "synthetic": True,
        "brand": BRAND,
        "scenario": REPORT["scenario"],
    }


def report_payload() -> dict[str, Any]:
    return REPORT


def studio_payload() -> dict[str, Any]:
    return {
        "brand": BRAND,
        "scenario": REPORT["scenario"],
        "signal": {
            "status": _studio_state["signalStatus"],
            "title": "Momentum Runner growth opportunity",
            "detail": REPORT["headline"],
            "impact": "$286K modeled value protected",
        },
        "completed": list(_studio_state["completed"]),
        "decisionStatus": _studio_state["decisionStatus"],
        "stages": [
            {"id": stage_id, **stage}
            for stage_id, stage in IQ_STAGES.items()
        ],
    }


def run_iq_stage(stage_id: str) -> dict[str, Any]:
    if stage_id not in IQ_STAGES:
        raise ValueError(f"Unknown IQ stage: {stage_id}")
    if stage_id not in _studio_state["completed"]:
        _studio_state["completed"].append(stage_id)
    return {"stage": {"id": stage_id, **IQ_STAGES[stage_id]},
            "studio": studio_payload()}


def set_signal_status(status: str) -> dict[str, Any]:
    if status not in ("active", "resolved"):
        raise ValueError("Signal status must be active or resolved")
    _studio_state["signalStatus"] = status
    return studio_payload()


def set_decision_status(status: str) -> dict[str, Any]:
    if status not in ("draft", "approved", "dismissed"):
        raise ValueError("Decision status must be draft, approved, or dismissed")
    _studio_state["decisionStatus"] = status
    return studio_payload()


def decision_payload() -> dict[str, Any]:
    source = "STORE-031"
    replacement = "STORE-027"
    return {
        "scenarioId": SCENARIO_ID,
        "brand": BRAND,
        "status": "ready for review",
        "signal": {
            "title": "Protect the Momentum Runner opportunity",
            "detail": REPORT["headline"],
            "period": REPORT["period"],
        },
        "diagnosis": [
            "Growth is concentrated in Central Metro and Lakeside.",
            "STORE-014 and STORE-022 have less than 1.5 weeks of cover.",
            "STORE-009 has enough cover for a local activation.",
        ],
        "constraint": (
            "Reserved inventory is certified at fulfillment-node/product grain "
            "and is not allocated to stores in this decision."
        ),
        "recommendation": {
            "activate": ["STORE-009"],
            "replenish": ["STORE-014"],
            "transfer": [{"from": source, "to": "STORE-022", "units": 180}],
            "expectedValue": "$286K modeled value protected",
        },
        "override": _override,
        "effectiveTransferSource": replacement if _override else source,
        "actionPackage": [
            "Replenishment review list",
            "Inter-store transfer review",
            "Supply-aware activation brief",
            "Evidence and grain statement",
            "Decision and override log",
        ],
    }


def apply_override(reason: str) -> dict[str, Any]:
    global _override
    clean_reason = reason.strip()
    if not clean_reason:
        raise ValueError("An override reason is required")
    _override = {
        "excludedCandidate": "STORE-031",
        "replacementCandidate": "STORE-027",
        "reason": clean_reason,
        "owner": "Regional Merchandising Lead",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    return decision_payload()


def ontology_entity(name: str) -> dict[str, Any]:
    properties = ENTITY_PROPERTIES.get(name, ["id", "name", "status"])
    telemetry = (
        ["sales_velocity", "traffic_count", "stockout_events", "decision_events"]
        if name == "Store"
        else ["action_status"] if name == "Decision"
        else []
    )
    return {
        "name": name,
        "namespace": "AsterPine.SyntheticRetail",
        "properties": [{"name": prop, "valueType": "String"} for prop in properties],
        "timeseriesProperties": [
            {"name": prop, "valueType": "Double"} for prop in telemetry
        ],
    }


def _scenario_proposals() -> list[dict[str, Any]]:
    return [
        item for item in proposals.list_all()
        if (item.get("payload") or {}).get("scenario_id") == SCENARIO_ID
    ]


def _ensure_action(
    kind: str,
    title: str,
    detail: str,
    payload: dict[str, Any],
    impact: str,
) -> dict[str, Any]:
    existing = next((item for item in _scenario_proposals()
                     if item.get("title") == title), None)
    if existing:
        return existing
    return proposals.add(
        kind,
        title,
        detail,
        {"scenario_id": SCENARIO_ID, **payload},
        impact,
    )


def _trace(
    source: str,
    decision: str,
    steps: list[str],
    *,
    entities: list[str] | None = None,
    relationships: list[str] | None = None,
) -> dict[str, Any]:
    trace: dict[str, Any] = {
        "source": source,
        "basis": source,
        "router": "deterministic replay router",
        "decision": decision,
        "steps": steps,
        "fellBack": False,
        "note": (
            "Replay response from a deterministic synthetic fixture. The live "
            "mode uses the same response contract with Fabric services."
        ),
    }
    if entities:
        trace["entities"] = entities
    if relationships:
        trace["relationships"] = relationships
    return trace


def _action_response() -> dict[str, Any]:
    actions = [
        _ensure_action(
            "reorder",
            "Replenish Momentum Runner at STORE-014",
            "420 units from FC-02; raises modeled cover from 0.8 to 3.6 weeks.",
            {
                "agent": "inventory",
                "store_id": "STORE-014",
                "product_id": "MOMENTUM-RUNNER",
                "reorder_quantity": 420,
            },
            "$148K sales protected",
        ),
        _ensure_action(
            "transfer",
            "Transfer compatible stock to STORE-022",
            "Move 180 units from STORE-031 after operator review.",
            {
                "agent": "inventory",
                "store_id": "STORE-022",
                "source_store_id": "STORE-031",
                "product_id": "MOMENTUM-RUNNER",
                "reorder_quantity": 180,
            },
            "$76K sales protected",
        ),
        _ensure_action(
            "activation",
            "Activate Momentum Runner at STORE-009",
            "Launch the synthetic local activation only where cover exceeds 4 weeks.",
            {
                "agent": "merchandising",
                "store_id": "STORE-009",
                "product_id": "MOMENTUM-RUNNER",
            },
            "$62K incremental sales",
        ),
    ]
    evidence = [
        {
            "store": "STORE-014",
            "market": "Central Metro",
            "velocity": "34.2/day",
            "cover": "0.8 weeks",
            "decision": "REPLENISH",
        },
        {
            "store": "STORE-022",
            "market": "Lakeside",
            "velocity": "28.7/day",
            "cover": "1.1 weeks",
            "decision": "TRANSFER",
        },
        {
            "store": "STORE-031",
            "market": "North Ridge",
            "velocity": "8.4/day",
            "cover": "7.6 weeks",
            "decision": "SOURCE",
        },
        {
            "store": "STORE-009",
            "market": "River District",
            "velocity": "25.1/day",
            "cover": "4.2 weeks",
            "decision": "ACTIVATE",
        },
    ]
    return {
        "answer": (
            "Momentum Runner is the growth opportunity, but a broad activation "
            "would create avoidable stockouts. Replenish STORE-014, transfer "
            "compatible stock to STORE-022, and activate STORE-009. Keep "
            "STORE-031 as a reviewed source candidate. Distribution-center "
            "reserved inventory remains excluded because it is not certified at "
            "store grain."
        ),
        "surface": "action-agent",
        "tool": "draft_supply_aware_activation",
        "routedTo": "inventory-agent",
        "trace": _trace(
            "Inventory + Merchandising agents (synthetic replay)",
            "The question requests a supply-aware decision and draft actions.",
            [
                "Read Momentum Runner contribution from the semantic-model fixture.",
                "Compared store/SKU velocity with the latest inventory snapshot.",
                "Applied the minimum-cover and source-store rules.",
                "Excluded fulfillment-node reserved inventory from store coverage.",
                "Created draft-only replenishment, transfer, and activation actions.",
            ],
            entities=["ProductFamily", "SKU", "InventorySnapshot", "Store",
                      "FulfillmentNode", "Recommendation"],
            relationships=[
                "ProductFamily -> Product (contains)",
                "Product -> SKU (has variant)",
                "SKU -> InventorySnapshot (has inventory)",
                "InventorySnapshot -> Store (measured at)",
                "Store -> FulfillmentNode (served by)",
            ],
        ),
        "recommendation": {
            "title": "Supply-aware Momentum Runner activation",
            "impact": "$286K modeled value protected",
            "summary": (
                "Three draft actions balance growth capture with inventory risk. "
                "All values and records are synthetic."
            ),
            "columns": [
                ["store", "Store"],
                ["market", "Market"],
                ["velocity", "Velocity"],
                ["cover", "Weeks cover"],
                ["decision", "Decision"],
            ],
            "evidence": evidence,
            "actions": actions,
        },
    }


def chat(message: str) -> dict[str, Any]:
    low = message.lower()
    if any(term in low for term in (
        "what should", "what can we do", "recommend", "replenish",
        "transfer", "activate", "action",
    )):
        return _action_response()
    if any(term in low for term in (
        "connected", "relationship", "serve", "serves", "carry",
        "which stores", "graph", "evidence",
    )):
        return {
            "answer": (
                "Momentum Runner is carried by 12 synthetic stores. STORE-014 "
                "and STORE-022 combine high velocity with low coverage. "
                "STORE-031 is a compatible transfer source, and FC-02 is the "
                "serving fulfillment node for the two risk stores."
            ),
            "surface": "ontology",
            "tool": "search_ontology",
            "routedTo": "ontology",
            "trace": _trace(
                "Fabric Ontology (synthetic replay)",
                "This is an entity and relationship question.",
                [
                    "Resolved the Momentum Runner product family.",
                    "Traversed products, compatible SKUs, inventory snapshots, and stores.",
                    "Followed each risk store to its serving fulfillment node.",
                ],
                entities=["ProductFamily", "Product", "SKU",
                          "InventorySnapshot", "Store", "FulfillmentNode"],
                relationships=[
                    "ProductFamily -> Product (contains)",
                    "Product -> SKU (has variant)",
                    "SKU -> InventorySnapshot (has inventory)",
                    "InventorySnapshot -> Store (measured at)",
                    "Store -> FulfillmentNode (served by)",
                ],
            ),
            "followUp": {
                "label": "Draft the supply-aware action plan ->",
                "message": (
                    "What should we do to protect Momentum Runner growth without "
                    "creating stockouts?"
                ),
            },
        }
    if any(term in low for term in ("traffic", "conversion")):
        answer = (
            "Traffic increased 9.4% in Central Metro, but conversion fell 2.1 "
            "points. STORE-014 contributes most of the gap, making inventory "
            "availability and floor execution the next diagnostic steps."
        )
    elif any(term in low for term in ("category", "gainer", "drainer", "family")):
        answer = (
            "Performance Footwear is down 4.8% versus the comparable fictional "
            "fiscal period. Momentum Runner is the strongest positive family at "
            "+18.6%, contributing $6.4M in synthetic net sales."
        )
    else:
        answer = (
            "Aster & Pine generated $42.8M in synthetic net sales for FY26 P08. "
            "The key decision signal is Momentum Runner growth (+18.6%) inside a "
            "declining Performance Footwear category (-4.8%)."
        )
    return {
        "answer": answer,
        "surface": "data-agent",
        "tool": "ask_semantic_model",
        "routedTo": "data-agent",
        "trace": _trace(
            "Semantic model Data Agent (synthetic replay)",
            "This question asks for governed metrics, comparison, or ranking.",
            [
                "Applied the fictional fiscal-period filter.",
                "Read certified measures from the deterministic scenario fixture.",
                "Returned the synthetic metric with its comparison context.",
            ],
        ),
    }


def actions_dashboard() -> dict[str, Any]:
    items = _scenario_proposals()
    status_counts = Counter(item.get("status", "draft") for item in items)
    approved = [item for item in items if item.get("status") == "approved"]
    now = datetime.now(timezone.utc).isoformat()
    by_agent: dict[tuple[str, str], int] = Counter()
    for item in items:
        payload = item.get("payload") or {}
        agent = payload.get("agent") or "inventory"
        status = "drafted" if item.get("status") == "draft" else item.get("status")
        by_agent[(agent, status)] += 1
    return {
        "enabled": True,
        "mode": "replay",
        "kpis": {
            "total": len(items),
            "approved": status_counts["approved"],
            "dismissed": status_counts["dismissed"],
            "pending": status_counts["draft"],
            "approved_reorders": sum(
                1 for item in approved if item.get("kind") in ("reorder", "transfer")
            ),
            "approved_reorder_units": sum(
                int((item.get("payload") or {}).get("reorder_quantity") or 0)
                for item in approved
            ),
            "approved_campaigns": sum(
                1 for item in approved if item.get("kind") == "activation"
            ),
            "addressed_ltv": 0,
        },
        "byAgent": [
            {"agent": agent, "action_status": status, "n": count}
            for (agent, status), count in sorted(by_agent.items())
        ],
        "recent": [
            {
                "action_ts": item.get("updated_at") or item.get("created_at") or now,
                "agent": (item.get("payload") or {}).get("agent") or "inventory",
                "action_kind": item.get("kind"),
                "action_status": (
                    "drafted" if item.get("status") == "draft"
                    else item.get("status")
                ),
                "action_title": item.get("title"),
                "action_impact": item.get("impact"),
            }
            for item in items[:25]
        ],
        "timeline": [
            {
                "action_ts": (
                    item.get("updated_at") or item.get("created_at") or now
                )[:16] + ":00",
                "action_status": (
                    "drafted" if item.get("status") == "draft"
                    else item.get("status")
                ),
                "events": 1,
            }
            for item in items
        ],
    }
