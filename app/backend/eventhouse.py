"""Best-effort logging of agent actions back into the Fabric Eventhouse.

Every state transition of a proposal — *drafted* by an agent, then *approved* or
*dismissed* by a human — is appended as an event to the ``agent_actions`` KQL
table. This closes the loop: the multi-agent system's decisions and the
human-in-the-loop approvals become first-class, queryable telemetry in the same
Eventhouse that powers the real-time dashboards.

Design notes:
* **Never blocks or breaks the app.** Ingestion runs on a daemon thread and all
  errors are swallowed — losing a log row must never fail a chat turn or an
  approval click.
* Uses ``.ingest inline`` against the streaming-ingestion-enabled table, so rows
  are queryable within ~1-2 seconds (good for a live streaming dashboard).
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any

from . import config

_client = None
_client_lock = threading.Lock()


def _get_client():
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is None:
            from azure.kusto.data import (
                KustoClient,
                KustoConnectionStringBuilder,
            )

            from . import mcp_client

            cluster = config.EVENTHOUSE_CLUSTER
            # Use the shared, cached token helper instead of calling the Azure CLI
            # credential on every query. The KustoClient invokes this provider for
            # each request, and a fresh ``az`` subprocess spawn per query storms the
            # CLI under the dashboard's 5s polling (4 queries each), hanging the
            # endpoint. ``mcp_client.get_token`` caches the token until ~5 min
            # before expiry, so ``az`` runs at most once per refresh cycle.
            kcsb = KustoConnectionStringBuilder.with_token_provider(
                cluster, lambda: mcp_client.get_token(cluster + "/.default")
            )
            _client = KustoClient(kcsb)
    return _client


def _csv_str(value) -> str:
    s = "" if value is None else str(value)
    s = s.replace('"', '""').replace("\n", " ").replace("\r", " ")
    return f'"{s}"'


def _int(value) -> str:
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return "0"


def _real(value) -> str:
    try:
        return repr(float(value))
    except (TypeError, ValueError):
        return "0.0"


def _row(proposal: dict, status: str) -> str:
    payload = proposal.get("payload", {}) or {}
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    agent = "inventory" if proposal.get("kind") == "reorder" else "retention"
    cols = [
        _csv_str(proposal.get("id")),
        ts,                                  # ISO8601 -> datetime
        _csv_str(agent),
        _csv_str(proposal.get("kind")),
        _csv_str(status),
        _csv_str(proposal.get("title")),
        _csv_str(proposal.get("impact")),
        _int(payload.get("store_id", 0)),
        _int(payload.get("product_id", 0)),
        _int(payload.get("reorder_quantity", 0)),
        _int(payload.get("cohort_count", 0)),
        _real(payload.get("total_ltv_at_risk", 0.0)),
        _csv_str(proposal.get("detail")),
    ]
    return ",".join(cols)


def _ingest(line: str) -> None:
    try:
        client = _get_client()
        cmd = (
            f".ingest inline into table {config.EVENTHOUSE_TABLE} <|\n{line}"
        )
        client.execute_mgmt(config.EVENTHOUSE_DB, cmd)
    except Exception:
        # Best-effort telemetry: never propagate.
        pass


def log_action(proposal: dict, status: str) -> None:
    """Append an agent-action event to the Eventhouse (async, best-effort)."""
    if not config.EVENTHOUSE_LOG:
        return
    try:
        line = _row(proposal, status)
    except Exception:
        return
    threading.Thread(target=_ingest, args=(line,), daemon=True).start()


# --------------------------------------------------------------------------- #
# Read side — powers the live "Agent Operations" dashboard.
# --------------------------------------------------------------------------- #

def query(kql: str) -> list[dict]:
    """Run a KQL query and return rows as a list of dicts (JSON-safe)."""
    client = _get_client()
    resp = client.execute(config.EVENTHOUSE_DB, kql)
    table = resp.primary_results[0]
    cols = [c.column_name for c in table.columns]
    rows: list[dict] = []
    for r in table.rows:
        row = {}
        for c, v in zip(cols, r):
            row[c] = v.isoformat() if hasattr(v, "isoformat") else v
        rows.append(row)
    return rows


_KPI_KQL = f"""
{config.EVENTHOUSE_TABLE}
| summarize arg_max(action_ts, action_status, agent, reorder_quantity, ltv_at_risk) by action_id
| summarize
    total = count(),
    approved = countif(action_status == "approved"),
    dismissed = countif(action_status == "dismissed"),
    pending = countif(action_status == "drafted"),
    approved_reorder_units = sumif(reorder_quantity, action_status == "approved" and agent == "inventory"),
    approved_reorders = countif(action_status == "approved" and agent == "inventory"),
    approved_campaigns = countif(action_status == "approved" and agent == "retention"),
    addressed_ltv = sumif(ltv_at_risk, action_status == "approved" and agent == "retention")
"""

_BY_AGENT_KQL = f"""
{config.EVENTHOUSE_TABLE}
| summarize arg_max(action_ts, action_status, agent) by action_id
| summarize n = count() by agent, action_status
| order by agent asc, action_status asc
"""

_RECENT_KQL = f"""
{config.EVENTHOUSE_TABLE}
| order by action_ts desc
| take 25
| project action_ts, agent, action_kind, action_status, action_title, action_impact
"""

_TIMELINE_KQL = f"""
{config.EVENTHOUSE_TABLE}
| where action_ts > ago(1h)
| summarize events = count() by bin(action_ts, 1m), action_status
| order by action_ts asc
"""


_DASH_CACHE: dict[str, Any] = {"at": 0.0, "data": None}
_DASH_TTL = 4.0  # seconds — the frontend polls every 5s
_DASH_LOCK = threading.Lock()


def dashboard() -> dict:
    """Return the live approval-funnel snapshot for the dashboard tab.

    Results are cached for a few seconds and computed under a lock so the
    frontend's 5s polling (and any overlapping requests) reuse a single snapshot
    instead of each firing four serial Kusto queries — which otherwise stack up
    and hang the endpoint under concurrency."""
    if not config.EVENTHOUSE_LOG:
        return {"enabled": False, "kpis": {}, "byAgent": [],
                "recent": [], "timeline": []}
    now = time.time()
    cached = _DASH_CACHE.get("data")
    if cached is not None and (now - _DASH_CACHE["at"]) < _DASH_TTL:
        return cached
    with _DASH_LOCK:
        now = time.time()
        cached = _DASH_CACHE.get("data")
        if cached is not None and (now - _DASH_CACHE["at"]) < _DASH_TTL:
            return cached
        kpis = query(_KPI_KQL)
        data = {
            "enabled": True,
            "kpis": kpis[0] if kpis else {},
            "byAgent": query(_BY_AGENT_KQL),
            "recent": query(_RECENT_KQL),
            "timeline": query(_TIMELINE_KQL),
        }
        _DASH_CACHE["data"] = data
        _DASH_CACHE["at"] = time.time()
        return data
