"""Ontology Explorer backend.

A small FastAPI app that powers a local single-page demo over the deployed
``retail-demo`` Fabric workspace. It exposes:

* ``GET  /api/config``          – IDs the frontend needs.
* ``GET  /api/embed``           – Power BI embed URL + an AAD token (user-owns-data).
* ``GET  /api/ontology/graph``  – entity/relationship graph for the canvas.
* ``GET  /api/ontology/entity`` – properties + telemetry for one entity.
* ``POST /api/chat``            – orchestrates a question to the Data Agent or Ontology MCP.

All tokens are minted server-side from the developer's ``az login`` session, so
no Entra app registration or service principal is required for the demo.
"""

from __future__ import annotations

import functools
import json
import re
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import agents, config, llm_router, mcp_client, ontology_graph, proposals, replay

app = FastAPI(title="Retail Ontology Explorer")

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"


class ChatRequest(BaseModel):
    message: str
    # "auto" lets the backend orchestrator choose; "data-agent"/"ontology"
    # force a specific backend (kept for debugging / power users).
    surface: str = "auto"
    # Which router to use in auto mode: "llm" (default, smart) or "keyword"
    # (deterministic). Lets the UI compare the two side by side.
    router: str = "llm"


class OverrideRequest(BaseModel):
    reason: str


@app.get("/api/config")
def get_config() -> dict[str, Any]:
    if config.REPLAY_MODE:
        return replay.config_payload()
    return {
        "workspaceId": config.WORKSPACE_ID,
        "reportId": config.REPORT_ID,
        "dataAgentId": config.DATA_AGENT_ID,
        "ontologyItemId": config.ONTOLOGY_ITEM_ID,
        "mode": "live",
        "synthetic": True,
        "brand": replay.BRAND,
        "scenario": replay.REPORT["scenario"],
    }


@app.get("/api/demo/dashboard")
def demo_dashboard() -> dict[str, Any]:
    return replay.report_payload()


@app.get("/api/demo/decision")
def demo_decision() -> dict[str, Any]:
    return replay.decision_payload()


@app.post("/api/demo/decision/override")
def demo_decision_override(req: OverrideRequest) -> dict[str, Any]:
    try:
        return replay.apply_override(req.reason)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/embed")
def get_embed() -> dict[str, str]:
    """Resolve the report's embed URL and return an AAD token for the PBI client."""
    if config.REPLAY_MODE:
        raise HTTPException(409, "Power BI embedding is disabled in replay mode")
    token = mcp_client.get_token(config.POWERBI_SCOPE)
    headers = {"Authorization": f"Bearer {token}"}
    report_id = config.REPORT_ID
    base = f"{config.POWERBI_HOST}/v1.0/myorg/groups/{config.WORKSPACE_ID}"
    try:
        if report_id:
            resp = requests.get(f"{base}/reports/{report_id}", headers=headers,
                                timeout=30)
            resp.raise_for_status()
            report = resp.json()
        else:
            resp = requests.get(f"{base}/reports", headers=headers, timeout=30)
            resp.raise_for_status()
            reports = resp.json().get("value", [])
            report = next(
                (r for r in reports if r.get("name") == config.REPORT_NAME), None
            )
            if report is None:
                raise HTTPException(404, f"Report {config.REPORT_NAME!r} not found")
    except requests.HTTPError as exc:  # pragma: no cover - network error path
        raise HTTPException(502, f"Power BI API error: {exc}") from exc
    return {
        "accessToken": token,
        "embedUrl": report.get("embedUrl", ""),
        "reportId": report.get("id", ""),
        "tokenType": "Aad",
    }


@functools.lru_cache(maxsize=1)
def _data_agent_tool() -> tuple[str, str]:
    """Discover the Data Agent tool name and its question argument name."""
    tools = mcp_client.list_tools(config.DATA_AGENT_MCP_URL)
    if not tools:
        raise HTTPException(502, "Data Agent exposed no tools")
    tool = next((t for t in tools if "DataAgent" in t.get("name", "")), tools[0])
    schema = tool.get("inputSchema", {}) or {}
    props = list((schema.get("properties") or {}).keys())
    required = schema.get("required") or props
    arg = "userQuestion"
    if arg not in props and required:
        arg = required[0]
    elif arg not in props and props:
        arg = props[0]
    return tool["name"], arg


# Phrases that signal a relationship / graph-traversal question the ontology
# answers best (entity-to-entity hops). Everything else — metrics, KPIs,
# rankings, ML, time windows — goes to the Data Agent first.
_ONTOLOGY_HINTS = (
    "deliver", "supplied by", "supplies", "supply", "which trucks",
    "which stores", "which distribution", "distribution center", "route",
    "connected to", "related to", "relationship", "linked", "feeds",
    "serviced by", "serves", "belongs to", "assigned to", "associated with",
    "path between", "upstream", "downstream", "where does", "who supplies",
)

# Strong Data-Agent signals (aggregation / analytics / ML) that should win even
# if a relationship-ish word also appears.
_DATA_AGENT_HINTS = (
    "total", "sum", "average", "avg", "count", "how many", "how much",
    "revenue", "sales", "margin", "profit", "top ", "bottom ", "rank",
    "trend", "forecast", "predict", "churn", "elasticity", "segment",
    "stockout", "recommend", "basket", "percent", "%", "growth", "kpi",
    "last 15 minutes", "per store", "by category", "by department",
)


def _route(message: str) -> list[str]:
    """Decide which backend to try first; the other is the fallback."""
    return _route_with_reason(message)[0]


def _route_with_reason(message: str) -> tuple[list[str], str]:
    """Return (order, human-readable routing reason)."""
    m = message.lower()
    hit = next((k for k in _DATA_AGENT_HINTS if k in m), None)
    if hit:
        return (["data-agent", "ontology"],
                f"Detected an analytic/metric cue ('{hit.strip()}') → semantic model first.")
    hit = next((k for k in _ONTOLOGY_HINTS if k in m), None)
    if hit:
        return (["ontology", "data-agent"],
                f"Detected a relationship cue ('{hit.strip()}') → ontology graph first.")
    return (["data-agent", "ontology"],
            "No strong cue; defaulted to the semantic model first.")


# Phrases that signal the user wants a *recommendation / action*, not just a
# number. These promote a question from "answer" to "answer + agent action".
_ACTION_CUES = (
    "what can we do", "what should we do", "what do we do", "do about",
    "should we", "recommend", "suggest", "next best", "how do we",
    "how can we", "address", "mitigate", "prevent", "respond to",
    "take action", "act on", "what now",
)
# Domain anchors.
_INVENTORY_DOMAIN = (
    "stock", "stockout", "stock out", "inventory", "on hand", "out of stock",
    "running low", "running out", "low on", "sku", "shelf",
)
# Words that trigger the inventory agent on their own (already action-oriented).
_INVENTORY_DIRECT = ("replenish", "reorder", "re-order", "restock", "draft reorder")
_RETENTION_DOMAIN = (
    "churn", "retention", "loyal", "leaving", "losing customers", "at risk",
    "at-risk customer", "attrition",
)
_RETENTION_DIRECT = (
    "win back", "win-back", "winback", "retain", "retention campaign",
    "start a campaign", "launch a campaign", "save customers", "keep customers",
)


def _action_route(message: str) -> str | None:
    """Return 'inventory' / 'retention' if the question warrants an action agent."""
    m = message.lower()
    cue = any(c in m for c in _ACTION_CUES)
    if (any(d in m for d in _INVENTORY_DIRECT)
            or (cue and any(d in m for d in _INVENTORY_DOMAIN))):
        return "inventory"
    if (any(d in m for d in _RETENTION_DIRECT)
            or (cue and any(d in m for d in _RETENTION_DOMAIN))):
        return "retention"
    return None


def _ask_data_agent(message: str) -> tuple[str, str, dict[str, Any]]:
    tool_name, arg = _data_agent_tool()
    answer = mcp_client.call_tool(
        config.DATA_AGENT_MCP_URL, tool_name, {arg: message}, timeout=240
    )
    if not answer or not answer.strip():
        raise ValueError("Data Agent returned no answer")
    meta = {
        "call": {
            "endpoint": "Fabric Data Agent (MCP)",
            "tool": tool_name,
            "arguments": {arg: message},
        },
        "note": (
            "The Fabric Data Agent generates and executes the DAX/SQL query "
            "server-side over the semantic model; the generated query text is "
            "not returned by the MCP endpoint."
        ),
    }
    return answer, tool_name, meta


def _render_ontology_rows(raw: dict[str, Any], limit: int = 25) -> str:
    """Render raw ontology query rows as a compact text table when the service
    returns results but no natural-language summary."""
    fields = raw.get("Fields") or []
    values = raw.get("Value") or []
    # Drop opaque ``*_json`` graph-blob columns the service sometimes includes.
    keep = [i for i, f in enumerate(fields) if not str(f).endswith("_json")]
    cols = [fields[i] for i in keep] or fields
    lines = [" | ".join(str(c) for c in cols)]
    for row in values[:limit]:
        cells = [row[i] for i in keep] if keep else row
        lines.append(" | ".join("" if c is None else str(c) for c in cells))
    if len(values) > limit:
        lines.append(f"… and {len(values) - limit} more rows.")
    return "\n".join(lines)


@functools.lru_cache(maxsize=1)
def _ontology_entity_names() -> tuple[str, ...]:
    """Entity type names from the ontology graph (for parsing result columns)."""
    try:
        edges = ontology_graph.get_edges()
        names = {e["source"] for e in edges} | {e["target"] for e in edges}
        return tuple(sorted(names, key=len, reverse=True))
    except Exception:
        return ()


def _derive_ontology_path(raw: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct which entities + relationships the graph query traversed from
    the result column names (e.g. ``CustomerSegment_customer_id`` → entity
    ``CustomerSegment``) — the closest visible proxy for the generated query,
    since Fabric does not return the graph query text itself."""
    fields = [str(f) for f in (raw.get("Fields") or [])]
    known = _ontology_entity_names()
    hit: list[str] = []
    for f in fields:
        prefix = f.split("_", 1)[0]
        # Entity-qualified columns are PascalCase-prefixed (Customer_id); plain
        # business columns are lower_snake (loyalty_card) and are skipped.
        for ent in known:
            if (f == ent or f.startswith(ent + "_")) and ent not in hit:
                hit.append(ent)
                break
        else:
            if prefix[:1].isupper() and prefix not in hit and prefix not in known:
                hit.append(prefix)
    edges = []
    try:
        for e in ontology_graph.get_edges():
            if e["source"] in hit and e["target"] in hit:
                edge = f"{e['source']} → {e['target']} ({e['label']})"
                if edge not in edges:
                    edges.append(edge)
    except Exception:
        pass
    return {"entities": hit, "relationships": edges}


class _OntologyTransientError(RuntimeError):
    """A transient ontology graph-query error (nondeterministic translator syntax
    failure) that is worth retrying — as opposed to a permanent 'no results' /
    'could not translate' outcome."""


def _is_transient_ontology_error(text: str) -> bool:
    """The translator nondeterministically emits a graph-query syntax error for a
    question it *can* answer (it built the right relationship but generated bad
    query text). These clear on a re-ask, so they are worth retrying. Genuine
    "couldn't translate" / "no results" responses are handled separately and are
    not retried (re-asking won't change them)."""
    low = text.lower()
    markers = (
        "badrequest",
        "syntax error",
        "is not defined",
        "does not exist in type",
        "internalcode",
        "access rule violation",
    )
    return any(m in low for m in markers)


def _ask_ontology_once(
    message: str, args: dict[str, Any], url: str | None = None,
) -> tuple[str, str, dict[str, Any]]:
    """Single Ontology MCP round-trip. Raises on every failure shape so the
    retry/fallback logic in :func:`_ask_ontology` can decide what to do. Never
    returns a translator error string as if it were a real answer.

    ``url`` selects which ontology endpoint to hit; defaults to the full
    ontology. The lite endpoint is used for bounded live event questions."""
    raw_text = mcp_client.call_tool(
        url or config.ONTOLOGY_MCP_URL, "search_ontology", args,
        timeout=config.ONTOLOGY_TIMEOUT,
    )
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("Ontology returned no content")
    if text.lower().startswith("failed to translate"):
        raise ValueError("Ontology could not translate the question to a graph query")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        # A non-JSON plain-text response. This is either a genuine prose answer or
        # a leaked translator error (e.g. "BadRequest: ... syntax error ...").
        # Treat the latter as a transient failure instead of surfacing garbage.
        if _is_transient_ontology_error(text):
            raise _OntologyTransientError(text[:160])
        meta = {"call": {"endpoint": "Ontology MCP", "tool": "search_ontology",
                         "arguments": args}}
        return text, "ontology", meta
    nlr = (data.get("naturalLanguageResponse") or "").strip()
    raw = data.get("raw") or {}
    rows = raw.get("Value") or []
    if _is_transient_ontology_error(nlr):
        raise _OntologyTransientError(nlr[:160])
    if not rows or nlr.lower().startswith("no results"):
        raise ValueError("Ontology found no matching rows")
    path = _derive_ontology_path(raw)
    meta = {
        "call": {"endpoint": "Ontology MCP", "tool": "search_ontology",
                 "arguments": args},
        "entities": path["entities"],
        "relationships": path["relationships"],
        "rawPreview": _render_ontology_rows(raw, limit=5),
        "note": (
            "The Ontology MCP translates this question into a graph query and "
            "runs it server-side; the generated query text is not returned. The "
            "entities/relationships above are reconstructed from the result "
            "columns to show the path that was traversed."
        ),
    }
    # Stash structured records (distinct store rows + product ids) so the
    # synthesis step can render names + locations instead of bare ids.
    meta["ids"] = _extract_id_columns(raw)
    meta["storeRecords"] = _extract_store_records(raw)
    answer = nlr if nlr else _render_ontology_rows(raw)
    return answer, "ontology", meta


def _extract_id_columns(raw: dict[str, Any]) -> dict[str, list[int]]:
    """From the ontology raw result, return ``{field: [distinct id values]}`` for
    any ``*_id`` column (e.g. ``store_id``, ``product_id``), preserving first-seen
    order. Used to enrich a bare-id answer with names/locations."""
    fields = [str(f) for f in (raw.get("Fields") or [])]
    rows = raw.get("Value") or []
    out: dict[str, list[int]] = {}
    for idx, field in enumerate(fields):
        if not field.endswith("_id"):
            continue
        seen: list[int] = []
        for row in rows:
            try:
                val = row[idx]
            except (IndexError, TypeError, KeyError):
                continue
            if isinstance(val, int) and val not in seen:
                seen.append(val)
        if seen:
            out[field] = seen
    return out


def _extract_store_records(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one record per distinct ``store_id`` in the ontology result,
    carrying whichever human-readable attribute columns the graph returned
    (``store_number``, ``store_address``, ``store_format``). When the graph only
    returned the id, those fields are left blank and resolved downstream from the
    semantic model."""
    fields = [str(f) for f in (raw.get("Fields") or [])]
    rows = raw.get("Value") or []
    if "store_id" not in fields:
        return []
    col = {name: fields.index(name) for name in
           ("store_id", "store_number", "store_address", "store_format")
           if name in fields}
    seen: dict[int, dict[str, Any]] = {}
    order: list[int] = []
    for row in rows:
        try:
            sid = row[col["store_id"]]
        except (IndexError, TypeError, KeyError):
            continue
        if not isinstance(sid, int) or sid in seen:
            continue
        order.append(sid)
        rec: dict[str, Any] = {"store_id": sid}
        for attr in ("store_number", "store_address", "store_format"):
            if attr in col:
                val = row[col[attr]]
                rec[attr] = str(val) if val is not None else ""
        seen[sid] = rec
    return [seen[s] for s in order]


# When the user asks "which stores are at risk…" we ask the graph to also return
# each store's human-readable attributes, so the answer comes back as names +
# locations instead of bare ids. The Store entity exposes these via its binding.
_STORE_LOOKUP_HINT_RE = re.compile(r"\bstores?\b", re.IGNORECASE)
_STORE_ATTRS_CLAUSE = (
    " Include each store's store number, address, and store format.")


def _ask_ontology(message: str, url: str | None = None) -> tuple[str, str, dict[str, Any]]:
    """Ask the Ontology MCP, retrying the *transient* graph-query syntax errors
    the translator emits nondeterministically before giving up.

    The translator can answer a question on one attempt and emit a bad-query
    ``BadRequest`` on the next for the *same* input, so a couple of re-asks make
    the demo reliable. Permanent failures (couldn't translate, no rows) are not
    retried — re-asking won't change them — and bubble up so the orchestrator
    transparently falls back to the Data Agent. A leaked translator error is
    never returned as if it were a real answer.

    For store lookups the question is augmented so the graph returns store
    number/address/format columns (resolved to a readable answer downstream).

    ``url`` selects the ontology endpoint (defaults to the full ontology).
    """
    query = message
    low = message.lower()
    if _STORE_LOOKUP_HINT_RE.search(low) and not (
            "address" in low or "store number" in low or "located" in low):
        query = message.rstrip() + _STORE_ATTRS_CLAUSE
    args = {"naturalLanguageQuery": query, "naturalLanguageResponse": True}
    attempts = max(1, config.ONTOLOGY_RETRIES + 1)
    last_transient: Exception | None = None
    for _ in range(attempts):
        try:
            return _ask_ontology_once(query, args, url=url)
        except _OntologyTransientError as exc:
            last_transient = exc
            continue
    # Exhausted retries on transient translator errors → let the orchestrator
    # fall back to the Data Agent rather than surface a syntax error.
    raise ValueError(
        f"Ontology translator failed after {attempts} attempts "
        f"({last_transient})")


# "Which products/stores sold the most in the last N minutes" — a live,
# real-time sales-velocity question. The historical Data Agent (semantic model,
# Direct Lake batch) has no last-15-minutes data, and the full ontology 500s on
# unbounded Eventhouse scans. The lite ontology (Store + receipt_created) answers
# this reliably at the STORE level straight from the live event stream.
_LIVE_SALES_RE = re.compile(
    r"(?:last|past|recent)\s+\d*\s*(?:min|minute|minutes|hour|hours)"
    r"|in\s+the\s+last\s+\d+\s*(?:min|minute|minutes)"
    r"|real[\s-]?time\s+sales|live\s+sales|right\s+now|sales\s+velocity",
    re.IGNORECASE,
)
# The lite ontology only has a Store entity, so product-level live questions are
# answered as store-level sales velocity (with a note). This is the proven
# phrasing that returns store number + location + total sales from the stream.
_LIVE_SALES_QUERY = (
    "Which stores had the highest total sales in the last 15 minutes? "
    "List the top 10 with store number, location, and total sales amount."
)


def _is_live_sales_question(message: str) -> bool:
    low = message.lower()
    if not _LIVE_SALES_RE.search(low):
        return False
    return any(w in low for w in ("sold", "sell", "selling", "sales", "sale",
                                  "revenue", "top product", "top store",
                                  "best sell", "moving"))


def _ask_live_sales() -> tuple[str, str, dict[str, Any]]:
    """Answer a real-time sales-velocity question from the lite ontology, which
    is bounded to the live event stream and never 500s the way the full ontology
    does on an unbounded Eventhouse scan."""
    answer, _source, meta = _ask_ontology(
        _LIVE_SALES_QUERY, url=config.ONTOLOGY_LITE_MCP_URL)
    meta = dict(meta)
    call = dict(meta.get("call") or {})
    call["endpoint"] = "Ontology MCP (lite — live event stream)"
    meta["call"] = call
    meta["note"] = (
        "Answered live from the streaming Eventhouse via the lite ontology "
        "(Store + receipt_created), which is bounded to the event tables. The "
        "historical Data Agent (semantic model) has no last-15-minutes data, and "
        "the full ontology times out on an unbounded event scan, so this "
        "real-time question is routed to the lite ontology at the store level."
    )
    return answer, "ontology", meta


# Phrasings that mean "which stores are linked to a product's stockout risk" —
# a graph LOOKUP. When the ontology answers one of these, we offer a one-click
# hand-off to the Inventory action agent ("now draft the reorders").
_STOCKOUT_LOOKUP_RE = re.compile(
    r"(?:at\s+risk\s+of\s+selling\s+out|selling\s+out|run(?:ning)?\s+out\s+of|"
    r"stockout\s+risk\s+for|at\s+stockout\s+risk\s+for|out\s+of\s+stock\s+(?:of|for))"
    r"\s+(?P<product>.+?)\s*[?.!]*$",
    re.IGNORECASE,
)


def _stockout_followup(message: str, surface: str) -> dict[str, str] | None:
    """If the ontology just answered a 'which stores are at risk of selling out
    <Product>' lookup, return a suggested follow-up that hands the user off to the
    Inventory action agent to draft replenishment for those at-risk stores.

    Returns ``{"label", "message"}`` (the chip text + the question to send) or
    ``None`` when the question isn't a product-stockout lookup."""
    if surface != "ontology":
        return None
    m = _STOCKOUT_LOOKUP_RE.search(message.strip())
    if not m:
        return None
    product = m.group("product").strip().strip("\"'\u201c\u201d")
    # Guard against absurdly long captures (the regex is greedy-anchored to EOL).
    if not product or len(product) > 60:
        return None
    return {
        "label": f"Draft replenishment for these at-risk stores \u2192",
        "message": (
            f"Draft replenishment reorders for {product} at the stores at risk "
            f"of selling out."
        ),
    }


@functools.lru_cache(maxsize=1)
def _store_index() -> dict[int, dict[str, str]]:
    """Cached ``{store_id: {number, address, format}}`` from the semantic model,
    so the synthesis step can turn bare ``store_id`` values into a readable
    'Store S000012 - 5301 Liberty Ave (neighborhood)'."""
    out: dict[int, dict[str, str]] = {}
    try:
        rows = dax.query_rows(
            "EVALUATE SELECTCOLUMNS('dim_stores', "
            "\"id\", 'dim_stores'[ID], \"number\", 'dim_stores'[Store Number], "
            "\"address\", 'dim_stores'[Address], \"format\", 'dim_stores'[Store Format])"
        )
        for r in rows:
            sid = r.get("id")
            if isinstance(sid, int):
                out[sid] = {
                    "number": str(r.get("number") or f"store {sid}"),
                    "address": str(r.get("address") or ""),
                    "format": str(r.get("format") or ""),
                }
    except Exception:
        pass
    return out


@functools.lru_cache(maxsize=1)
def _product_index() -> dict[int, str]:
    """Cached ``{product_id: name}`` from the semantic model."""
    out: dict[int, str] = {}
    try:
        rows = dax.query_rows(
            "EVALUATE SELECTCOLUMNS('dim_products', "
            "\"id\", 'dim_products'[ID], \"name\", 'dim_products'[Product Name])"
        )
        for r in rows:
            pid = r.get("id")
            if isinstance(pid, int):
                out[pid] = str(r.get("name") or f"product {pid}")
    except Exception:
        pass
    return out


# Cap how many enriched store lines we render so the answer stays readable.
_SYNTH_STORE_LIMIT = 12


# The ontology translator likes to echo raw ``product_id`` / ``store_id`` values
# even when the question asks for names only. For CIO-facing answers we strip
# those id artifacts from the prose, keeping the human-readable names, counts,
# quantities and timestamps intact.
_ID_LINE_RE = re.compile(
    r"(?im)^[ \t]*[-*][ \t]*(?:product|store)[ _]?id:?[ \t]*\d+[ \t]*\r?\n")
_ID_TOKEN_RE = re.compile(
    r"(?i)\b(?:for[ \t]+)?(?:product|store)[ _]?id:?[ \t]*\d+[,:]?[ \t]*")
_ID_PAREN_RE = re.compile(r"(?i)[ \t]*\((?:product[ _]?|store[ _]?)?id:?[ \t]*\d+\)")


def _strip_ids_from_text(text: str) -> str:
    """Remove bare product/store *id* references from ontology prose so the
    answer reads in business terms (names, not integer keys). Leaves store
    numbers (e.g. ``S000028``), quantities, counts and timestamps untouched."""
    if not text:
        return text
    text = _ID_PAREN_RE.sub("", text)
    text = _ID_LINE_RE.sub("", text)
    text = _ID_TOKEN_RE.sub("", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\(\s*\)", "", text)
    return text.strip()


def _synthesize_answer(message: str, surface: str, answer: str,
                       meta: dict[str, Any]) -> str:
    """Interpret a raw backend answer before returning it to the user.

    The ontology often replies with bare ``store_id``/``product_id`` integers.
    Rather than pass those through, we resolve them to store **numbers +
    locations** (and product **names**), sanity-check that we actually got
    rows, and return a clean, readable summary with the entities spelled out.
    Falls back to the original answer if there's nothing to enrich.
    """
    if surface != "ontology":
        return answer
    records = meta.get("storeRecords") or []
    is_stockout_lookup = bool(_STOCKOUT_LOOKUP_RE.search(message.strip()))
    if not records or not is_stockout_lookup:
        # Only the "which stores are at risk of selling out <product>" lookup is
        # rebuilt into the at-risk store list below. Every other ontology answer
        # (e.g. a single store's live event telemetry) keeps its own prose — we
        # just scrub raw product/store ids so it reads in business terms.
        return _strip_ids_from_text(answer)

    ids = meta.get("ids") or {}
    product_ids = ids.get("product_id") or []
    products = _product_index()
    prod_names = [products.get(p) for p in product_ids if products.get(p)]
    # Collapse the SKU-level product list to its distinct display name(s).
    prod_label = ""
    if prod_names:
        uniq = list(dict.fromkeys(prod_names))
        prod_label = uniq[0] if len(uniq) == 1 else ", ".join(uniq[:2])
    if not prod_label:
        # Fall back to the product the user named in the question.
        m = _STOCKOUT_LOOKUP_RE.search(message.strip())
        if m:
            cand = m.group("product").strip().strip("\"'\u201c\u201d")
            if cand and len(cand) <= 60:
                prod_label = cand

    stores = _store_index()  # fallback when the graph returned only the id
    shown = records[:_SYNTH_STORE_LIMIT]
    lines = []
    for rec in shown:
        sid = rec["store_id"]
        number = rec.get("store_number") or ""
        address = rec.get("store_address") or ""
        fmt = rec.get("store_format") or ""
        if not (number and address):  # graph gave only the id — resolve it
            fb = stores.get(sid)
            if fb:
                number = number or fb["number"]
                address = address or fb["address"]
                fmt = fmt or fb["format"]
        label = number or f"store_id {sid}"
        loc = f" — {address}" if address else ""
        fmt_s = f" ({fmt})" if fmt else ""
        lines.append(f"- **{label}** (store_id {sid}){loc}{fmt_s}")
    more = len(records) - len(shown)
    more_line = f"\n- …and {more} more" if more > 0 else ""

    headline = (
        f"**{len(records)} stores** are trending toward a stockout"
        + (f" on **{prod_label}**" if prod_label else "")
        + ":"
    )
    rec_line = (
        "\n\n_These stores need replenishment before they run dry. "
        "Use the **suggested next step** below to have the Inventory agent draft "
        "the reorders._"
    )
    return f"{headline}\n" + "\n".join(lines) + more_line + rec_line


_FAILED_ALL = (
    "I couldn't answer that from either the semantic model or the ontology. "
    "Try rephrasing — for metrics and ML ask things like \"total net sales by "
    "category\" or \"which products are most at risk of stockout\"; for "
    "relationships ask \"which trucks deliver to which stores\"."
)


_ACTION_REASON = {
    "inventory": "Action request about stock / replenishment → Inventory agent.",
    "retention": "Action request about churn / retention → Retention agent.",
}
_ROUTER_LABEL = {
    "keyword": "keyword fallback router",
    "manual": "manual override",
}


def _llm_router_label(model: str | None) -> str:
    return f"LLM intent router ({model or config.AOAI_DEPLOYMENT})"


def _orchestrate(message: str, use_llm: bool = True) -> tuple[str, str, str, list[str]]:
    """Pick a route. Returns (chosen, reason, router_label, order).

    With ``use_llm`` the LLM intent router is tried first; on any failure (or when
    ``use_llm`` is False) it falls back to the deterministic keyword router so
    routing never breaks. ``router_label`` names the actual decider (the LLM model
    that classified, or the keyword fallback) for the transparency trace.
    """
    decision = llm_router.classify(message) if use_llm else None
    if decision:
        route, reason = decision["route"], decision["reason"]
        label = _llm_router_label(decision.get("model"))
        if route == "data-agent":
            return route, reason, label, ["data-agent", "ontology"]
        if route == "ontology":
            return route, reason, label, ["ontology", "data-agent"]
        return route, reason, label, []  # inventory / retention (action)

    # --- deterministic fallback ---
    action = _action_route(message)
    if action:
        return action, _ACTION_REASON[action], _ROUTER_LABEL["keyword"], []
    order, reason = _route_with_reason(message)
    return order[0], reason, _ROUTER_LABEL["keyword"], order


@app.post("/api/chat")
def chat(req: ChatRequest) -> dict[str, Any]:
    message = (req.message or "").strip()
    if not message:
        raise HTTPException(400, "Empty message")
    if config.REPLAY_MODE:
        return replay.chat(message)

    # Real-time sales-velocity questions ("which sold the most in the last 15
    # minutes") have no answer in the historical semantic model and 500 the full
    # ontology. Route them to the lite ontology, which is bounded to the live
    # event stream and answers reliably at the store level.
    if req.surface not in ("data-agent", "ontology") and _is_live_sales_question(message):
        try:
            answer, tool, meta = _ask_live_sales()
            trace = {
                "source": "Ontology graph (MCP — lite, live event stream)",
                "basis": "Ontology graph (MCP — lite, live event stream)",
                "router": "live-sales router",
                "decision": (
                    "Real-time sales-velocity question → lite ontology over the "
                    "streaming Eventhouse (semantic model has no live data)."),
                "steps": [
                    "Detected a real-time sales-velocity question (last N minutes).",
                    "Sent it to the lite Ontology MCP, bounded to the live event "
                    "stream (Store + receipt_created).",
                    "Ontology aggregated live receipt events per store and returned "
                    "the top stores by sales in the last 15 minutes.",
                ],
                "fellBack": False,
            }
            for key in ("call", "rawPreview", "note"):
                if meta.get(key):
                    trace[key] = meta[key]
            return {"answer": answer, "surface": "ontology", "tool": tool,
                    "routedTo": "ontology-lite", "trace": trace}
        except (requests.HTTPError, requests.Timeout, ValueError, RuntimeError):
            pass  # fall through to the normal orchestrator

    # Decide the route. Explicit surface overrides keep the raw single-backend
    # behaviour for debugging; otherwise the orchestrator (LLM → keyword) chooses.
    if req.surface in ("data-agent", "ontology"):
        chosen, reason = req.surface, f"Manual override → {req.surface}."
        router, order = _ROUTER_LABEL["manual"], [req.surface]
    else:
        chosen, reason, router, order = _orchestrate(message, use_llm=req.router != "keyword")

    # Action agents (inventory / retention) own their own answer + trace.
    if chosen in ("inventory", "retention"):
        agent = {"inventory": agents.inventory_agent,
                 "retention": agents.retention_agent}[chosen]
        try:
            res = agent(message)
            if isinstance(res, dict) and res.get("trace"):
                res["trace"]["router"] = router
                res["trace"]["decision"] = reason
            return res
        except (requests.HTTPError, requests.Timeout, ValueError,
                KeyError, RuntimeError):
            # Agent couldn't read its data — degrade to a plain metric answer.
            order = ["data-agent", "ontology"]
            reason = f"{reason} (agent data unavailable; answered as a metric instead.)"

    _SOURCE_LABEL = {
        "data-agent": "Semantic model (Fabric Data Agent)",
        "ontology": "Ontology graph (MCP)",
    }
    _STEPS = {
        "data-agent": [
            "Sent the question to the Fabric Data Agent over the semantic model.",
            "Data Agent generated a query, executed it on Direct Lake, and summarized the result.",
        ],
        "ontology": [
            "Sent the question to the Ontology MCP (graph of entities + relationships).",
            "Ontology resolved the relevant entities/relationships and returned a natural-language answer.",
        ],
    }

    askers = {"data-agent": _ask_data_agent, "ontology": _ask_ontology}
    errors: list[str] = []
    for surface in order:
        try:
            answer, tool, meta = askers[surface](message)
            # Interpret the raw backend answer (resolve bare ids → store
            # numbers + locations and product names) before returning it.
            answer = _synthesize_answer(message, surface, answer, meta)
            steps = list(_STEPS[surface])
            # Expand the generic steps with the concrete entities/relationships
            # the ontology graph query traversed (reconstructed from columns).
            if surface == "ontology" and meta.get("relationships"):
                steps[1] = (
                    "Ontology translated the question to a graph query that "
                    f"traversed: {'; '.join(meta['relationships'])}.")
            elif surface == "ontology" and meta.get("entities"):
                steps[1] = (
                    "Ontology translated the question to a graph query over "
                    f"entities: {', '.join(meta['entities'])}.")
            if surface == "ontology" and (meta.get("ids") or {}).get("store_id"):
                steps.append(
                    "Interpreted the result: resolved the returned store/product "
                    "ids to store numbers + locations and added a recommended "
                    "next step.")
            trace = {
                "source": _SOURCE_LABEL[surface],
                "basis": _SOURCE_LABEL[surface],
                "router": router,
                "decision": reason,
                "steps": steps,
                "fellBack": len(errors) > 0,
            }
            # Surface the actual call we made + reconstructed query path so the
            # trace shows *how* the answer was reached, not just "used the MCP".
            if meta.get("call"):
                trace["call"] = meta["call"]
            if meta.get("entities"):
                trace["entities"] = meta["entities"]
            if meta.get("relationships"):
                trace["relationships"] = meta["relationships"]
            if meta.get("rawPreview"):
                trace["rawPreview"] = meta["rawPreview"]
            if meta.get("note"):
                trace["note"] = meta["note"]
            if errors:
                trace["steps"].insert(
                    0, f"First choice unavailable ({', '.join(errors)}); fell back to {surface}.")
            result = {
                "answer": answer,
                "surface": surface,
                "tool": tool,
                "routedTo": surface,
                "trace": trace,
            }
            followup = _stockout_followup(message, surface)
            if followup:
                result["followUp"] = followup
            return result
        except (requests.HTTPError, requests.Timeout, ValueError,
                RuntimeError) as exc:
            errors.append(f"{surface}: {type(exc).__name__}")
            continue
        except Exception as exc:  # unexpected — surface it
            raise HTTPException(502, f"{type(exc).__name__}: {exc}") from exc

    # Both backends failed/empty — degrade gracefully rather than 502.
    return {"answer": _FAILED_ALL, "surface": "none", "tool": None,
            "routedTo": None, "errors": errors}


class ProposalAction(BaseModel):
    status: str  # "approved" | "dismissed"


@app.get("/api/proposals")
def list_proposals() -> dict[str, Any]:
    """Return all drafted/approved/dismissed action proposals."""
    return {"proposals": proposals.list_all()}


@app.post("/api/proposals/{pid}")
def update_proposal(pid: str, action: ProposalAction) -> dict[str, Any]:
    """Approve or dismiss a drafted proposal (the human-in-the-loop step)."""
    try:
        updated = proposals.set_status(pid, action.status)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if updated is None:
        raise HTTPException(404, f"Proposal {pid} not found")
    # Close the loop: log the human decision back into the Eventhouse.
    try:
        from . import eventhouse
        status_event = {"approved": "approved",
                        "dismissed": "dismissed"}.get(action.status, action.status)
        eventhouse.log_action(updated, status_event)
    except Exception:
        pass
    return updated


@app.get("/api/actions/dashboard")
def actions_dashboard() -> dict[str, Any]:
    """Live approval-funnel snapshot from the Eventhouse agent_actions table."""
    if config.REPLAY_MODE:
        return replay.actions_dashboard()
    try:
        from . import eventhouse
        return eventhouse.dashboard()
    except Exception as exc:  # best-effort: never break the dashboard tab
        return {"enabled": False, "error": f"{type(exc).__name__}: {exc}",
                "kpis": {}, "byAgent": [], "recent": [], "timeline": []}


@app.get("/api/ontology/graph")
def ontology_graph_endpoint() -> dict[str, Any]:
    if config.REPLAY_MODE:
        return replay.GRAPH
    try:
        return ontology_graph.build_graph()
    except Exception as exc:  # pragma: no cover - network error path
        raise HTTPException(502, f"Ontology graph error: {exc}") from exc


@app.get("/api/ontology/entity")
def ontology_entity(name: str) -> JSONResponse:
    """Return properties + telemetry for a single entity (for the detail panel)."""
    if config.REPLAY_MODE:
        return JSONResponse(replay.ontology_entity(name))
    import json as _json

    try:
        text = mcp_client.call_tool(
            config.ONTOLOGY_MCP_URL,
            "list_ontology_entity_types",
            {"entityName": name, "includeProperties": True},
            timeout=60,
        )
        data = _json.loads(text)
    except Exception as exc:
        raise HTTPException(502, f"Entity lookup error: {exc}") from exc
    values = data.get("values", [])
    return JSONResponse(values[0] if values else {})


# Serve the single-page frontend at the root. Mounted last so /api/* wins.
@app.middleware("http")
async def _no_cache_static(request: Request, call_next):
    response = await call_next(request)
    # Always revalidate frontend assets so edits show up on a plain refresh.
    path = request.url.path
    if not path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
