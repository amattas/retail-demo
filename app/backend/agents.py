"""Action agents: Inventory Replenishment and Customer Retention.

Each agent follows the same loop:

    perceive  -> read real rows from the semantic model (via DAX)
    reason    -> apply numeric trigger rules to find what needs action
    recommend -> draft proposals (status="draft") in the proposal store
    respond   -> return a narrative answer + a structured recommendation card

Nothing is executed against a system of record — the agent only *drafts*. A
human approves drafts from the UI. All thresholds live in the ``*_CONFIG``
dicts so they are easy to tune for a demo.
"""

from __future__ import annotations

import math
import re
from typing import Any

from . import dax, proposals

# --------------------------------------------------------------------------- #
# Inventory & Replenishment Agent
# --------------------------------------------------------------------------- #

INVENTORY_CONFIG = {
    "cover_days_target": 14,   # reorder enough to reach this many days of cover
    "max_drafts": 8,           # scope limit: never fan out into too many writes
    "urgent_days": 3.0,        # days-of-cover at/below this = urgent
}

_INVENTORY_DAX = """
EVALUATE
TOPN(
    30,
    FILTER(
        SELECTCOLUMNS(
            stockout_risk,
            "store_id", stockout_risk[Store ID],
            "product_id", stockout_risk[Product ID],
            "prob", stockout_risk[Stockout Probability],
            "doi", stockout_risk[Days of Inventory],
            "cur", stockout_risk[Current Inventory],
            "vel", stockout_risk[Demand Velocity Daily],
            "dept", stockout_risk[Department],
            "risk", stockout_risk[Risk Level],
            "name", LOOKUPVALUE('dim_products'[Product Name], 'dim_products'[ID], stockout_risk[Product ID]),
            "price", LOOKUPVALUE('dim_products'[Sale Price], 'dim_products'[ID], stockout_risk[Product ID])
        ),
        [risk] = "High"
    ),
    [prob], DESC
)
"""

_REORDER_PAIRS_DAX = """
EVALUATE
SELECTCOLUMNS(
    SUMMARIZE(fact_reorders, fact_reorders[Store ID], fact_reorders[Product ID], fact_reorders[DC ID]),
    "store_id", fact_reorders[Store ID],
    "product_id", fact_reorders[Product ID],
    "dc_id", fact_reorders[DC ID]
)
"""


# When a follow-up / user message names a specific product ("draft
# replenishment for White Truffle Powder at the at-risk stores"), scope the
# reorder drafts to that product instead of the global top-30 high-risk list.
_PRODUCT_SCOPE_RE = re.compile(
    r"\b(?:reorders?\s+for|replenish(?:ment)?\s+(?:reorders?\s+)?for|"
    r"running\s+low\s+on|low\s+on|restock(?:\s+of)?|reorder|replenish)\s+"
    r"(?P<product>[A-Za-z0-9][\w .&'\-/]*?)"
    r"(?:\s+(?:at|across|in|to|for|and|that|which)\b|[?.!,]|$)",
    re.IGNORECASE,
)

_GENERIC_PRODUCT_WORDS = {
    "these", "them", "it", "this", "the", "stock", "items", "products",
    "high-risk items", "similar high-risk items", "everything", "all",
    "stores", "skus", "inventory", "reorders", "those",
}


def _extract_product(message: str) -> str | None:
    """Pull a specific product name out of an inventory request, if present."""
    m = _PRODUCT_SCOPE_RE.search(message or "")
    if not m:
        return None
    product = m.group("product").strip().strip("\"'\u201c\u201d. ")
    if not product or len(product) > 60 or product.lower() in _GENERIC_PRODUCT_WORDS:
        return None
    return product


def _inventory_dax_for_product(name: str, include_medium: bool = False) -> str:
    """Stockout-risk rows filtered to a single product (case-insensitive match)."""
    safe = name.replace('"', '""')
    risk = '[risk] IN {"High", "Medium"}' if include_medium else '[risk] = "High"'
    return f'''
EVALUATE
TOPN(
    30,
    FILTER(
        SELECTCOLUMNS(
            stockout_risk,
            "store_id", stockout_risk[Store ID],
            "product_id", stockout_risk[Product ID],
            "prob", stockout_risk[Stockout Probability],
            "doi", stockout_risk[Days of Inventory],
            "cur", stockout_risk[Current Inventory],
            "vel", stockout_risk[Demand Velocity Daily],
            "dept", stockout_risk[Department],
            "risk", stockout_risk[Risk Level],
            "name", LOOKUPVALUE('dim_products'[Product Name], 'dim_products'[ID], stockout_risk[Product ID]),
            "price", LOOKUPVALUE('dim_products'[Sale Price], 'dim_products'[ID], stockout_risk[Product ID])
        ),
        {risk} && SEARCH("{safe}", [name], 1, 0) > 0
    ),
    [prob], DESC
)
'''


def _money(x: float) -> str:
    return f"${x:,.0f}"


def inventory_agent(message: str) -> dict[str, Any]:
    """Find high stockout-risk SKUs and draft priority reorders.

    If the message names a specific product (e.g. the ontology hand-off
    "draft replenishment for White Truffle Powder"), the agent scopes the
    drafts to that product across its at-risk stores instead of the global
    top-30 high-risk list.
    """
    product = _extract_product(message)
    scoped_dax = ""
    if product:
        scoped_dax = _inventory_dax_for_product(product)
        rows = dax.query_rows(scoped_dax)
        if not rows:
            scoped_dax = _inventory_dax_for_product(product, include_medium=True)
            rows = dax.query_rows(scoped_dax)
        if not rows:
            product = None
            scoped_dax = ""
            rows = dax.query_rows(_INVENTORY_DAX)
    else:
        rows = dax.query_rows(_INVENTORY_DAX)
    scoped = bool(product)
    pairs = dax.query_rows(_REORDER_PAIRS_DAX)

    covered: set[tuple[int, int]] = set()
    store_dc: dict[int, int] = {}
    for p in pairs:
        try:
            sid, pid, dc = int(p["store_id"]), int(p["product_id"]), int(p["dc_id"])
        except (TypeError, ValueError):
            continue
        covered.add((sid, pid))
        store_dc.setdefault(sid, dc)

    cover_target = INVENTORY_CONFIG["cover_days_target"]
    items: list[dict[str, Any]] = []
    for r in rows:
        sid = int(r["store_id"])
        pid = int(r["product_id"])
        vel = float(r["vel"] or 0)
        cur = int(r["cur"] or 0)
        price = float(r["price"] or 0)
        doi = float(r["doi"] or 0)
        qty = max(0, math.ceil(vel * cover_target) - cur)
        items.append({
            "store_id": sid,
            "product_id": pid,
            "name": r["name"] or f"Product {pid}",
            "dept": r["dept"] or "",
            "prob": float(r["prob"] or 0),
            "doi": doi,
            "cur": cur,
            "vel": vel,
            "price": price,
            "reorder_qty": qty,
            "dc_id": store_dc.get(sid),
            "has_open_reorder": (sid, pid) in covered,
            "at_risk_per_day": vel * price,
        })

    # Most urgent first: no open reorder, then thinnest days of cover.
    items.sort(key=lambda x: (x["has_open_reorder"], x["doi"]))

    n_sku = len(items)
    n_stores = len({i["store_id"] for i in items})
    n_uncovered = sum(1 for i in items if not i["has_open_reorder"])
    units_at_risk = sum(i["vel"] for i in items)
    dollars_at_risk = sum(i["at_risk_per_day"] for i in items)

    # Draft reorders for the actionable items (positive qty), capped.
    actions: list[dict[str, Any]] = []
    drafted_units = 0
    for it in items:
        if len(actions) >= INVENTORY_CONFIG["max_drafts"]:
            break
        if it["reorder_qty"] <= 0:
            continue
        dc_txt = f" from DC {it['dc_id']}" if it["dc_id"] is not None else ""
        cover_txt = f"{it['doi']:.1f} days cover"
        flag = "no open reorder" if not it["has_open_reorder"] else "top-up"
        title = (
            f"Reorder {it['reorder_qty']:,} units · {it['name']} → Store "
            f"{it['store_id']}{dc_txt}"
        )
        detail = (
            f"{it['dept']} · stockout prob {it['prob']:.0%} · {cover_txt} · "
            f"on hand {it['cur']:,} · sells {it['vel']:.0f}/day · {flag}"
        )
        proposal = proposals.add(
            kind="reorder",
            title=title,
            detail=detail,
            impact=f"~{_money(it['at_risk_per_day'])}/day at risk",
            payload={
                "store_id": it["store_id"],
                "product_id": it["product_id"],
                "product_name": it["name"],
                "dc_id": it["dc_id"],
                "reorder_quantity": it["reorder_qty"],
                "priority": "High",
                "current_quantity": it["cur"],
                "days_of_inventory": round(it["doi"], 2),
                "stockout_probability": round(it["prob"], 4),
            },
        )
        actions.append(proposal)
        drafted_units += it["reorder_qty"]

    evidence_cols = [
        ("name", "Product"), ("store_id", "Store"), ("dept", "Dept"),
        ("prob", "Stockout prob"), ("doi", "Days cover"),
        ("cur", "On hand"), ("reorder_qty", "Reorder qty"),
        ("status", "Status"),
    ]
    evidence = [{
        "name": i["name"],
        "store_id": i["store_id"],
        "dept": i["dept"],
        "prob": f"{i['prob']:.0%}",
        "doi": f"{i['doi']:.1f}",
        "cur": f"{i['cur']:,}",
        "reorder_qty": f"{i['reorder_qty']:,}",
        "status": "open reorder" if i["has_open_reorder"] else "UNCOVERED",
    } for i in items[:10]]

    if n_sku == 0:
        answer = ("Good news — no products are currently flagged at high stockout "
                  "risk, so there's nothing to replenish right now.")
        trace = {
            "source": "Inventory & Replenishment Agent",
            "basis": "Semantic model (DAX over stockout_risk)",
            "decision": "Action intent on inventory; agent invoked.",
            "steps": [
                "Read stockout_risk — found 0 SKUs at High risk.",
                "No reorder drafts required.",
            ],
            "tables": ["stockout_risk"],
        }
        return {"answer": answer, "surface": "inventory-agent",
                "routedTo": "inventory-agent", "recommendation": None,
                "trace": trace}

    product_label = (items[0]["name"] if scoped and items else product) or "this product"
    if scoped:
        answer = (
            f"**{product_label} is at stockout risk in {n_stores} "
            f"store{'s' if n_stores != 1 else ''}** — {n_uncovered} with no open "
            f"reorder on record. That's about **{units_at_risk:,.0f} units/day "
            f"(~{_money(dollars_at_risk)}/day)** of {product_label} sales exposed if "
            f"those stores stock out. I've **drafted {len(actions)} reorders "
            f"({drafted_units:,} units)** for {product_label}, prioritizing the "
            f"thinnest-cover stores. Review and approve below."
        )
    else:
        answer = (
            f"**{n_sku} SKUs across {n_stores} stores are at high stockout risk** — "
            f"{n_uncovered} have no open reorder on record. Combined, that's about "
            f"**{units_at_risk:,.0f} units/day (~{_money(dollars_at_risk)}/day)** of "
            f"sales exposed if they stock out. I've **drafted {len(actions)} priority "
            f"reorders ({drafted_units:,} units)**, starting with the uncovered, "
            f"lowest-days-of-cover positions. Review and approve below."
        )
    recommendation = {
        "title": (f"Inventory replenishment — {product_label}" if scoped
                  else "Inventory replenishment — recommended actions"),
        "impact": f"{units_at_risk:,.0f} units/day (~{_money(dollars_at_risk)}/day) at risk",
        "summary": (
            (f"{product_label}: {n_stores} at-risk store"
             f"{'s' if n_stores != 1 else ''} · {n_uncovered} uncovered · "
             f"{len(actions)} reorders drafted ({drafted_units:,} units, "
             f"{cover_target}-day cover target).") if scoped else
            (f"{n_sku} high-risk SKUs · {n_uncovered} uncovered · "
             f"{len(actions)} reorders drafted ({drafted_units:,} units, "
             f"{cover_target}-day cover target).")
        ),
        "columns": evidence_cols,
        "evidence": evidence,
        "actions": actions,
    }
    trace = {
        "source": "Inventory & Replenishment Agent",
        "basis": "Semantic model (DAX over stockout_risk, fact_reorders, dim_products)",
        "decision": (
            (f"Action intent on inventory, scoped to {product_label} → drafted "
             f"reorders for that product at its at-risk stores.") if scoped else
            "Action intent on inventory ('what can we do' + stock/replenish cues) → "
            "routed to the Inventory agent instead of a read-only answer."
        ),
        "steps": [
            (f"Filtered stockout_risk to {product_label} → {n_sku} at-risk "
             f"store rows (ML stockout model)." if scoped else
             f"Read {n_sku} High-risk SKUs from stockout_risk (ML stockout model)."),
            f"Cross-checked {len(covered):,} open positions in fact_reorders to flag {n_uncovered} uncovered.",
            "Joined dim_products for sell price → quantified ~"
            f"{_money(dollars_at_risk)}/day exposed.",
            f"Rule: reorder to a {cover_target}-day cover target; prioritized uncovered + lowest days-of-cover.",
            f"Drafted {len(actions)} reorders → logged to Eventhouse agent_actions (status=drafted).",
        ],
        "tables": ["stockout_risk", "fact_reorders", "dim_products"],
        "queries": [
            {"label": (f"{product_label} stockout-risk by store (ML model)" if scoped
                       else "High stockout-risk SKUs (ML model)"),
             "lang": "DAX",
             "text": (scoped_dax.strip() if scoped else _INVENTORY_DAX.strip())},
            {"label": "Open reorder positions", "lang": "DAX",
             "text": _REORDER_PAIRS_DAX.strip()},
        ],
    }
    return {"answer": answer, "surface": "inventory-agent",
            "routedTo": "inventory-agent", "recommendation": recommendation,
            "trace": trace}


# --------------------------------------------------------------------------- #
# Customer Retention Agent
# --------------------------------------------------------------------------- #

RETENTION_CONFIG = {
    "target_size": 50,         # customers in the drafted win-back campaign
    "winback_items": 3,        # cross-sell anchors to suggest
}

_CHURN_AGG_DAX = """
EVALUATE
ROW(
    "cohort_count", CALCULATE(COUNTROWS(churn_predictions), churn_predictions[Churn Prediction] = 1),
    "total_ltv", CALCULATE(
        SUMX(churn_predictions, LOOKUPVALUE(customer_segments[Monetary Value], customer_segments[Customer ID], churn_predictions[Customer ID])),
        churn_predictions[Churn Prediction] = 1
    ),
    "avg_prob", CALCULATE(AVERAGE(churn_predictions[Churn Probability]), churn_predictions[Churn Prediction] = 1)
)
"""

_CHURN_TARGETS_DAX = """
EVALUATE
TOPN(
    {N},
    FILTER(
        SELECTCOLUMNS(
            FILTER(churn_predictions, churn_predictions[Churn Prediction] = 1),
            "customer_id", churn_predictions[Customer ID],
            "prob", churn_predictions[Churn Probability],
            "segment", LOOKUPVALUE(customer_segments[Segment Label], customer_segments[Customer ID], churn_predictions[Customer ID]),
            "ltv", LOOKUPVALUE(customer_segments[Monetary Value], customer_segments[Customer ID], churn_predictions[Customer ID]),
            "recency", LOOKUPVALUE(customer_segments[Recency Days], customer_segments[Customer ID], churn_predictions[Customer ID])
        ),
        NOT ISBLANK([ltv])
    ),
    [ltv], DESC
)
"""

_WINBACK_DAX = """
EVALUATE
TOPN(
    {N},
    SUMMARIZE(
        FILTER(product_recommendations, product_recommendations[Product ID] <> product_recommendations[Recommended Product ID]),
        product_recommendations[Recommended Product ID],
        "rec", LOOKUPVALUE('dim_products'[Product Name], 'dim_products'[ID], product_recommendations[Recommended Product ID]),
        "lift", MAXX(FILTER(product_recommendations, product_recommendations[Recommended Product ID] = EARLIER(product_recommendations[Recommended Product ID])), product_recommendations[Lift]),
        "conf", MAXX(FILTER(product_recommendations, product_recommendations[Recommended Product ID] = EARLIER(product_recommendations[Recommended Product ID])), product_recommendations[Confidence])
    ),
    [lift], DESC
)
"""


def retention_agent(message: str) -> dict[str, Any]:
    """Quantify churn revenue-at-risk and draft a targeted win-back campaign."""
    agg = dax.query_rows(_CHURN_AGG_DAX)
    agg = agg[0] if agg else {}
    cohort_count = int(agg.get("cohort_count") or 0)
    total_ltv = float(agg.get("total_ltv") or 0)
    avg_prob = float(agg.get("avg_prob") or 0)

    if cohort_count == 0:
        answer = ("No customers are currently predicted to churn, so there's no "
                  "retention campaign to launch right now.")
        trace = {
            "source": "Customer Retention Agent",
            "basis": "Semantic model (DAX over churn_predictions)",
            "decision": "Action intent on churn/retention; agent invoked.",
            "steps": ["Read churn_predictions — 0 customers predicted to churn."],
            "tables": ["churn_predictions"],
        }
        return {"answer": answer, "surface": "retention-agent",
                "routedTo": "retention-agent", "recommendation": None,
                "trace": trace}

    n = RETENTION_CONFIG["target_size"]
    targets = dax.query_rows(_CHURN_TARGETS_DAX.replace("{N}", str(n)))
    winback = dax.query_rows(_WINBACK_DAX.replace("{N}", "8"))

    # Dedupe winback to distinct *recommended* products (the item we'd feature).
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for w in winback:
        rec_name = w.get("rec") or ""
        if not rec_name or rec_name in seen:
            continue
        seen.add(rec_name)
        items.append({
            "rec": rec_name,
            "lift": float(w["lift"] or 0),
            "conf": float(w["conf"] or 0),
        })
        if len(items) >= RETENTION_CONFIG["winback_items"]:
            break

    target_ltv = sum(float(t.get("ltv") or 0) for t in targets)
    customer_ids = [int(t["customer_id"]) for t in targets]

    anchor_txt = (
        ", ".join(f"{i['rec']} (lift {i['lift']:.1f})" for i in items)
        if items else "the strongest market-basket associations"
    )

    proposal = proposals.add(
        kind="campaign",
        title=(
            f"Win-back campaign · {len(targets)} highest-value at-risk customers "
            f"({_money(target_ltv)} LTV)"
        ),
        detail=(
            f"Target the top {len(targets)} customers predicted to churn, ranked "
            f"by lifetime value. Offer: personalized win-back incentive + a "
            f"cross-sell anchored on {anchor_txt}."
        ),
        impact=f"{_money(total_ltv)} total LTV at risk across {cohort_count:,} customers",
        payload={
            "cohort_count": cohort_count,
            "total_ltv_at_risk": round(total_ltv, 2),
            "target_customer_ids": customer_ids,
            "target_ltv": round(target_ltv, 2),
            "avg_churn_probability": round(avg_prob, 4),
            "winback_items": [i["rec"] for i in items],
            "offer_type": "personalized win-back + cross-sell",
        },
    )

    evidence_cols = [
        ("customer_id", "Customer"), ("segment", "Segment"),
        ("prob", "Churn prob"), ("ltv", "Lifetime value"),
        ("recency", "Days since last buy"),
    ]
    evidence = [{
        "customer_id": int(t["customer_id"]),
        "segment": t.get("segment") or "—",
        "prob": f"{float(t.get('prob') or 0):.0%}",
        "ltv": _money(float(t.get("ltv") or 0)),
        "recency": f"{float(t.get('recency') or 0):.0f}",
    } for t in targets[:10]]

    answer = (
        f"**{cohort_count:,} customers are predicted to churn**, representing "
        f"**{_money(total_ltv)} of lifetime value at risk** (avg churn probability "
        f"{avg_prob:.0%}). I've **drafted a win-back campaign** targeting the "
        f"**top {len(targets)} highest-value at-risk customers ({_money(target_ltv)} "
        f"LTV)**, with a cross-sell anchored on {anchor_txt}. Review and approve below."
    )
    recommendation = {
        "title": "Customer retention — recommended campaign",
        "impact": f"{_money(total_ltv)} LTV at risk · {cohort_count:,} customers",
        "summary": (
            f"Top {len(targets)} targeted ({_money(target_ltv)} LTV) · "
            f"avg churn prob {avg_prob:.0%} · "
            f"cross-sell anchors: {', '.join(i['rec'] for i in items) or 'n/a'}."
        ),
        "columns": evidence_cols,
        "evidence": evidence,
        "actions": [proposal],
    }
    trace = {
        "source": "Customer Retention Agent",
        "basis": "Semantic model (DAX over churn_predictions, customer_segments, product_recommendations)",
        "decision": "Action intent on churn/retention ('what can we do' + churn/campaign cues) → routed to the Retention agent instead of a read-only answer.",
        "steps": [
            f"Read churn_predictions (ML churn model) → {cohort_count:,} customers predicted to churn (avg prob {avg_prob:.0%}).",
            f"Joined customer_segments[Monetary Value] → quantified {_money(total_ltv)} lifetime value at risk.",
            f"Ranked by LTV and selected the top {len(targets)} ({_money(target_ltv)}) as the campaign cohort.",
            "Pulled cross-sell anchors from product_recommendations (market-basket lift): "
            f"{', '.join(i['rec'] for i in items) or 'n/a'}.",
            "Drafted 1 win-back campaign → logged to Eventhouse agent_actions (status=drafted).",
        ],
        "tables": ["churn_predictions", "customer_segments", "product_recommendations"],
        "queries": [
            {"label": "Churn cohort + LTV at risk", "lang": "DAX",
             "text": _CHURN_AGG_DAX.strip()},
            {"label": "Top at-risk customers by LTV", "lang": "DAX",
             "text": _CHURN_TARGETS_DAX.replace("{N}", str(n)).strip()},
            {"label": "Cross-sell anchors (market-basket lift)", "lang": "DAX",
             "text": _WINBACK_DAX.replace("{N}", "8").strip()},
        ],
    }
    return {"answer": answer, "surface": "retention-agent",
            "routedTo": "retention-agent", "recommendation": recommendation,
            "trace": trace}
