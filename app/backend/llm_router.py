"""LLM-based intent router.

Replaces the brittle keyword routing with a small, fast classification call that
*understands* what the user is really asking and picks the right surface:

* ``data-agent``  — aggregate metrics, KPIs, rankings, trends, ML predictions
                    (answered by the Fabric Data Agent over the semantic model).
* ``ontology``    — relationship / graph-traversal / multi-hop questions
                    ("which X connects to which Y", "for the customers who…").
* ``inventory``   — an *action* request about stock/replenishment.
* ``retention``   — an *action* request about churn/win-back.

The router returns a structured decision **with a short reason** that feeds the
chat transparency trace. On any failure (auth, timeout, bad JSON) it returns
``None`` so the caller transparently falls back to the deterministic keyword
router — the LLM is an enhancement, never a single point of failure.
"""

from __future__ import annotations

import json
from typing import Any

import requests

from . import config

_ROUTES = {"data-agent", "ontology", "inventory", "retention"}

_SYSTEM_PROMPT = """\
You are the intent router for a retail analytics assistant built on Microsoft \
Fabric. You decide which backend should handle each user question. Choose exactly \
one route:

- "ontology": questions about ONE specific entity in context — fusing its \
business attributes, its ML predictions, and its telemetry through the knowledge \
graph. The ontology shines when the user names a particular Customer (by loyalty \
card), Product, or Store and wants the related facts pulled together across \
links: e.g. "what segment is the customer with loyalty card LC0123 in and what \
is their churn probability", "give me the 360 view of this customer", "what \
segment and churn risk does this loyalty member have", "what geography is this \
store in". It is also right for simple 1-hop relationship lookups about a named \
entity — INCLUDING "which stores are at stockout risk for / at risk of selling \
out <a named Product>", "which stores stock product X", "which stores is this \
product at risk at". Those are graph LOOKUPS that resolve a named Product's ML \
stockout-risk prediction to the linked Store entities — NOT action requests. \
Choose this when the question is about a SPECIFIC named entity and wants its \
connected business + ML context, rather than an aggregate over many rows. \
Prefer it for Customer-360 / single-entity enrichment that blends a segment, a \
churn prediction, and profile attributes in one answer. ALSO choose ontology \
when the user asks about ONE named Store's or Product's own LIVE EVENT telemetry \
from the stream — e.g. "for store x, how many stockout_detected events and \
which products were affected", "which products had stockout_detected at this store". These walk \
the graph from the single named entity to its event stream and resolve the linked \
product/store names; they are single-entity lookups, not aggregates, even though \
they may include a count.

- "data-agent": aggregate metrics, KPIs, totals, averages, rankings, trends, \
time windows, comparisons, and ML rollups across MANY rows (sales, margin, \
revenue, basket size, churn counts/rates/LTV, stockout counts across all stores, \
forecasts, segment sizes, elasticity, "top/bottom N", "% of", "right now / last \
15 minutes / which sold the most"). Also use it for broad, deep multi-hop \
traversals that scan large fact tables — e.g. "the distinct products customers \
bought across in-store and online", "products that sell together" — which are \
answered far more reliably by the curated semantic model than by a graph walk. \
This is the default for "how much / what trend / what is predicted in aggregate" \
and for event aggregates that span MANY stores/products or a global time window \
("which products sold the most in the last 15 minutes"), but NOT for a single \
named entity's own event telemetry (that is "ontology").

- "inventory": the user wants a RECOMMENDATION OR ACTION about stock / \
replenishment / stockouts — i.e. asks what to DO about it (e.g. "stock is low, \
what should we do", "which items need reordering and draft the reorders", "how \
do we prevent stockouts", "create the replenishment orders"). The signal is an \
imperative/advice verb: should, do, prevent, fix, draft, create, reorder, \
replenish, recommend. Do NOT choose inventory merely because a question mentions \
"stockout" or "at risk" — "WHICH stores/products are at risk" (no action verb) \
is a LOOKUP for "ontology" or "data-agent", not an action.

- "retention": the user wants a RECOMMENDATION OR ACTION about customer churn / \
retention (e.g. "churn is rising, what can we do", "launch a win-back campaign", \
"how do we keep at-risk customers"). Only when asking what to DO. As with \
inventory, "which customers are at churn risk" (no action verb) is a LOOKUP, not \
an action.

Rules:
- Choose "ontology" when the user names a SPECIFIC entity (a customer's loyalty \
card, a product, a store) and wants its connected business + ML context pulled \
together (segment + churn + profile), OR asks which entities are LINKED to a \
named entity — including "which stores are at risk of selling out <Product>". \
Choose "data-agent" for aggregates, rankings, trends, and broad multi-hop scans \
over many rows.
- Use "inventory"/"retention" ONLY when the user asks what to DO (an action verb: \
should/do/prevent/fix/draft/create/reorder/replenish/recommend/launch). A "which \
… are at risk" question with no action verb is a lookup, never an action.
- Respond ONLY as compact JSON: {"route": "<one of data-agent|ontology|\
inventory|retention>", "reason": "<one short sentence explaining the choice>"}.
"""

_FEWSHOT = [
    ("What were total net sales and gross margin company-wide?",
     {"route": "data-agent", "reason": "Aggregate financial metric over the semantic model."}),
    ("What segment is the customer with loyalty card LC012304678 in and what is their churn probability?",
     {"route": "ontology", "reason": "Single-customer 360 fusing segment + churn through the graph."}),
    ("Give me the 360 view of the customer with loyalty card LC012783099.",
     {"route": "ontology", "reason": "Named-entity enrichment: profile + segment + churn in one answer."}),
    ("How many customers are predicted to churn and their total lifetime value?",
     {"route": "data-agent", "reason": "Aggregate count/value over the ML churn table."}),
    ("List the distinct products customers bought across in-store and online.",
     {"route": "data-agent", "reason": "Broad multi-hop scan over large fact tables — semantic model is more reliable."}),
    ("Stock is running low — what should we do about it?",
     {"route": "inventory", "reason": "Action request about replenishment."}),
    ("Which stores are at risk of selling out White Truffle Powder?",
     {"route": "ontology", "reason": "Lookup of the stores linked to a named product's stockout-risk prediction via the graph."}),
    ("Which stores are at stockout risk for product 1446?",
     {"route": "ontology", "reason": "Named product's ML stockout risk resolved to linked stores — a graph lookup, not an action."}),
    ("For store S000028, how many stockout_detected events have occurred, and which products were affected?",
     {"route": "ontology", "reason": "One named store's own live event telemetry, traversing the graph to its affected products."}),
    ("Which products sold the most in the last 15 minutes?",
     {"route": "data-agent", "reason": "Global top-N sales velocity over a time window across all products."}),
    ("Which items need reordering — draft the reorders for me.",
     {"route": "inventory", "reason": "Action verb (draft/reorder) — wants the replenishment orders created."}),
    ("Churn is rising — how do we retain customers?",
     {"route": "retention", "reason": "Action request about customer retention."}),
    ("What is the churn risk and customer segment for loyalty member LC012385934?",
     {"route": "ontology", "reason": "Named customer enriched with churn + segment via the ontology graph."}),
]


def _headers() -> dict[str, str]:
    """Prefer an API key if provided; otherwise mint a keyless AAD token."""
    if config.AOAI_KEY:
        return {"api-key": config.AOAI_KEY, "Content-Type": "application/json"}
    from azure.identity import AzureCliCredential

    token = AzureCliCredential().get_token(config.COGNITIVE_SCOPE).token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def classify(message: str) -> dict[str, Any] | None:
    """Return {"route", "reason", "via"} or None to signal fall back to keywords."""
    if not config.LLM_ROUTER:
        return None

    system = _SYSTEM_PROMPT
    if _use_anthropic():
        decision, model = _classify_anthropic(system, message)
    else:
        decision, model = _classify_aoai(system, message)
    if decision is None:
        return None

    route = str(decision.get("route", "")).strip().lower()
    if route not in _ROUTES:
        return None
    reason = str(decision.get("reason", "")).strip() or "LLM intent classification."
    return {"route": route, "reason": reason, "via": "LLM", "model": model}


def _use_anthropic() -> bool:
    return config.LLM_PROVIDER == "anthropic" and bool(config.ANTHROPIC_API_KEY)


def _build_messages(message: str) -> list[dict[str, str]]:
    msgs: list[dict[str, str]] = []
    for q, a in _FEWSHOT:
        msgs.append({"role": "user", "content": q})
        msgs.append({"role": "assistant", "content": json.dumps(a)})
    msgs.append({"role": "user", "content": message})
    return msgs


def _classify_aoai(system: str, message: str) -> tuple[dict[str, Any] | None, str]:
    """Classify with the Azure OpenAI deployment (gpt-4o-mini by default)."""
    messages = [{"role": "system", "content": system}, *_build_messages(message)]
    url = (f"{config.AOAI_ENDPOINT}/openai/deployments/{config.AOAI_DEPLOYMENT}"
           f"/chat/completions?api-version={config.AOAI_API_VERSION}")
    body = {
        "messages": messages,
        "temperature": 0,
        "max_tokens": 80,
        "response_format": {"type": "json_object"},
    }
    try:
        resp = requests.post(url, headers=_headers(), json=body, timeout=15)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        return json.loads(content), config.AOAI_DEPLOYMENT
    except Exception:
        return None, config.AOAI_DEPLOYMENT


def _classify_anthropic(system: str, message: str) -> tuple[dict[str, Any] | None, str]:
    """Classify with Claude via the Anthropic Messages API."""
    headers = {
        "x-api-key": config.ANTHROPIC_API_KEY,
        "anthropic-version": config.ANTHROPIC_VERSION,
        "content-type": "application/json",
    }
    body = {
        "model": config.ANTHROPIC_MODEL,
        "max_tokens": 120,
        "temperature": 0,
        "system": system + "\nRespond with ONLY the JSON object, no prose.",
        "messages": _build_messages(message),
    }
    try:
        resp = requests.post(config.ANTHROPIC_ENDPOINT, headers=headers,
                             json=body, timeout=15)
        resp.raise_for_status()
        blocks = resp.json().get("content", [])
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
        text = text.strip()
        # Be tolerant of stray prose around the JSON object.
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end != -1:
            text = text[start:end + 1]
        return json.loads(text), config.ANTHROPIC_MODEL
    except Exception:
        return None, config.ANTHROPIC_MODEL
