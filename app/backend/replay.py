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
    "persona": "Dana Reyes",
    "scope": {
        "territory": "Central Region",
        "salesPeriod": "Retail Fiscal Month 11, Fall 2026",
        "comparisonPeriod": "Retail Fiscal Month 11, Fall 2025",
        "inventorySnapshot": "2026-08-28",
        "mode": "Certified synthetic replay",
    },
    "period": "Retail Fiscal Month 11, Fall 2026",
    "headline": (
        "Footwear is down 6.2% YoY in Central Region. Momentum Runner is up "
        "38% inside that decline, and three stores will run out of core sizes "
        "before the next receipt window."
    ),
    "kpis": [
        {"label": "Net sales", "value": "$42.8M", "delta": "-1.9% vs prior year"},
        {"label": "Footwear", "value": "$18.7M", "delta": "-6.2% YoY"},
        {"label": "Momentum Runner", "value": "$6.4M", "delta": "+38.0%"},
        {"label": "Stores at risk", "value": "3", "delta": "under 1.5 weeks cover"},
        {"label": "Demand protected", "value": "740 units", "delta": "before next receipt"},
    ],
    "waterfall": [
        {"label": "Central footwear", "value": -6.2},
        {"label": "Outlet stores", "value": -11.0},
        {"label": "Full-price stores", "value": 2.0},
        {"label": "Momentum Runner", "value": 38.0},
        {"label": "Everything else", "value": 0.2},
    ],
    "concentration": {
        "storesUp": 7,
        "storesTotal": 10,
        "storesAbove50": 3,
    },
    "stores": [
        {
            "store": "Store A",
            "market": "Central Metro",
            "velocity": 34.2,
            "weeks_cover": 0.8,
            "quadrant": "Strong velocity / low coverage",
            "recommendation": "Hold + replenish",
        },
        {
            "store": "Store B",
            "market": "Lakeside",
            "velocity": 28.7,
            "weeks_cover": 1.1,
            "quadrant": "Strong velocity / low coverage",
            "recommendation": "Hold + transfer",
        },
        {
            "store": "Store C",
            "market": "River District",
            "velocity": 31.4,
            "weeks_cover": 1.3,
            "quadrant": "Strong velocity / low coverage",
            "recommendation": "Hold + replenish",
        },
        {
            "store": "Store D",
            "market": "Market Square",
            "velocity": 25.1,
            "weeks_cover": 4.2,
            "quadrant": "Strong velocity / adequate coverage",
            "recommendation": "Activate",
        },
        {"store": "Store E", "market": "West End", "velocity": 24.2,
         "weeks_cover": 4.8, "quadrant": "Strong velocity / adequate coverage",
         "recommendation": "Activate"},
        {"store": "Store F", "market": "University", "velocity": 23.5,
         "weeks_cover": 5.1, "quadrant": "Strong velocity / adequate coverage",
         "recommendation": "Activate"},
        {"store": "Store G", "market": "South Loop", "velocity": 22.9,
         "weeks_cover": 3.9, "quadrant": "Strong velocity / adequate coverage",
         "recommendation": "Activate"},
        {"store": "Store H", "market": "North Ridge", "velocity": 8.8,
         "weeks_cover": 7.4, "quadrant": "Weak velocity / adequate coverage",
         "recommendation": "Transfer source"},
        {"store": "Store I", "market": "Airport", "velocity": 7.9,
         "weeks_cover": 8.1, "quadrant": "Weak velocity / adequate coverage",
         "recommendation": "Transfer source"},
        {"store": "Store J", "market": "Old Town", "velocity": 7.2,
         "weeks_cover": 8.5, "quadrant": "Weak velocity / adequate coverage",
         "recommendation": "Transfer source"},
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

_override: dict[str, Any] | None = None
_studio_state = {
    "signalStatus": "active",
    "completed": [],
    "decisionStatus": "draft",
    "packageStatus": "not built",
    "reviewStatus": "not sent",
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
            "Momentum Runner is up 38% while Footwear is down 6.2%. Stores D-G "
            "can support activation, Stores A-C need inventory intervention, "
            "and Stores H-J are initial transfer candidates."
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
        "persona": REPORT["persona"],
        "scope": REPORT["scope"],
        "waterfall": REPORT["waterfall"],
        "concentration": REPORT["concentration"],
        "stores": REPORT["stores"],
        "signal": {
            "status": _studio_state["signalStatus"],
            "title": "A hidden winner is at risk of stocking out",
            "summary": REPORT["headline"],
            "detail": REPORT["headline"],
            "impact": "740 units of demand protected",
        },
        "completed": list(_studio_state["completed"]),
        "decisionStatus": _studio_state["decisionStatus"],
        "packageStatus": _studio_state["packageStatus"],
        "reviewStatus": _studio_state["reviewStatus"],
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


def set_package_status(status: str) -> dict[str, Any]:
    if status not in ("not built", "built", "sent for review"):
        raise ValueError(
            "Package status must be not built, built, or sent for review"
        )
    _studio_state["packageStatus"] = (
        "built" if status == "sent for review" else status
    )
    _studio_state["reviewStatus"] = (
        "sent for review" if status == "sent for review" else "not sent"
    )
    return studio_payload()


def decision_payload() -> dict[str, Any]:
    sources = ["Store H", "Store I", "Store J"]
    effective_sources = ["Store H", "Store I"] if _override else sources
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
            "Growth is broad across seven stores and exceeds 50% in three.",
            "Stores A-C combine strong velocity with less than 1.5 weeks of cover.",
            "Stores D-G can support activation; Stores H-J are source candidates.",
        ],
        "constraint": (
            "Reserved inventory is validated at location, style, size, and "
            "snapshot-date grain before it can enter a transfer plan."
        ),
        "recommendation": {
            "activate": ["Store D", "Store E", "Store F", "Store G"],
            "hold": ["Store A", "Store B", "Store C"],
            "replenish": ["Store A", "Store B", "Store C"],
            "transfer": [{
                "from": sources,
                "to": ["Store A", "Store B", "Store C"],
                "units": 420,
            }],
            "expectedValue": "740 units of demand protected",
        },
        "override": _override,
        "effectiveTransferSources": effective_sources,
        "packageStatus": _studio_state["packageStatus"],
        "reviewStatus": _studio_state["reviewStatus"],
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
        "excludedCandidate": "Store J",
        "replacementCandidates": ["Store H", "Store I"],
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
            "Protect Momentum Runner at Stores A-C",
            "Replenish core sizes before the next receipt window.",
            {
                "agent": "inventory",
                "store_id": "Stores A-C",
                "product_id": "MOMENTUM-RUNNER",
                "reorder_quantity": 420,
            },
            "420 units of demand protected",
        ),
        _ensure_action(
            "transfer",
            "Transfer compatible stock from Stores H-I",
            "Move 420 compatible units into Stores A-C after operator review.",
            {
                "agent": "inventory",
                "store_id": "Stores A-C",
                "source_store_id": "Stores H-I",
                "product_id": "MOMENTUM-RUNNER",
                "reorder_quantity": 420,
            },
            "Core style-size gaps covered",
        ),
        _ensure_action(
            "activation",
            "Activate Momentum Runner at Stores D-G",
            "Launch only where strong velocity and adequate coverage intersect.",
            {
                "agent": "merchandising",
                "store_id": "Stores D-G",
                "product_id": "MOMENTUM-RUNNER",
            },
            "Selective regional activation",
        ),
    ]
    evidence = [
        {
            "store": "Store A",
            "market": "Central Metro",
            "velocity": "34.2/day",
            "cover": "0.8 weeks",
            "decision": "HOLD + REPLENISH",
        },
        {
            "store": "Store B",
            "market": "Lakeside",
            "velocity": "28.7/day",
            "cover": "1.1 weeks",
            "decision": "HOLD + TRANSFER",
        },
        {
            "store": "Stores H-I",
            "market": "North Ridge",
            "velocity": "8.4/day",
            "cover": "7.8 weeks",
            "decision": "SOURCE",
        },
        {
            "store": "Stores D-G",
            "market": "River District",
            "velocity": "25.1/day",
            "cover": "4.2 weeks",
            "decision": "ACTIVATE",
        },
    ]
    return {
        "answer": (
            "Momentum Runner is the growth opportunity, but a broad activation "
            "would create avoidable stockouts. Hold and replenish Stores A-C, "
            "activate Stores D-G, and transfer compatible stock from Stores H-I "
            "after Dana excludes Store J. Reserved inventory is validated at "
            "location, style, size, and snapshot-date grain."
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
            "impact": "740 units of demand protected",
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
                "Momentum Runner is carried by 10 synthetic Central Region stores. "
                "Stores A-C combine strong velocity with low coverage, Stores D-G "
                "can support activation, and Stores H-I are compatible transfer "
                "sources after Dana excludes Store J."
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
            "points. Store A contributes most of the gap, making inventory "
            "availability and floor execution the next diagnostic steps."
        )
    elif any(term in low for term in ("category", "gainer", "drainer", "family")):
        answer = (
            "Footwear is down 6.2% versus the comparable fictional "
            "fiscal period. Momentum Runner is the strongest positive family at "
            "+38%, contributing $6.4M in synthetic net sales."
        )
    else:
        answer = (
            "Aster & Pine generated $42.8M in synthetic net sales for Retail "
            "Fiscal Month 11. The key decision signal is Momentum Runner growth "
            "(+38%) inside declining Footwear (-6.2%)."
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
