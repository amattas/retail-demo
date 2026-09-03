"""A tiny file-backed store for *action proposals* drafted by the agents.

Every recommendation the agents make is written here as a ``draft`` — the agent
never silently mutates a system of record. A human promotes a draft to
``approved`` (or ``dismissed``) from the UI. In production these tool calls would
target an ERP/CRM/pricing API; for the demo we persist them to a local JSON file
so the full perceive -> recommend -> approve loop is visible and auditable.
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_STORE = Path(__file__).resolve().parent / "proposals_store.json"
_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load() -> list[dict[str, Any]]:
    if not _STORE.exists():
        return []
    try:
        return json.loads(_STORE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def _save(items: list[dict[str, Any]]) -> None:
    _STORE.write_text(json.dumps(items, indent=2), encoding="utf-8")


def add(kind: str, title: str, detail: str, payload: dict[str, Any],
        impact: str = "") -> dict[str, Any]:
    """Create a new draft proposal and return it."""
    proposal = {
        "id": uuid.uuid4().hex[:12],
        "kind": kind,                # "reorder" | "campaign"
        "title": title,
        "detail": detail,
        "impact": impact,
        "payload": payload,
        "status": "draft",           # draft | approved | dismissed
        "created_at": _now(),
        "updated_at": _now(),
    }
    with _lock:
        items = _load()
        items.append(proposal)
        _save(items)
    try:
        from . import eventhouse
        eventhouse.log_action(proposal, "drafted")
    except Exception:
        pass
    return proposal


def list_all() -> list[dict[str, Any]]:
    with _lock:
        return sorted(_load(), key=lambda p: p["created_at"], reverse=True)


def set_status(pid: str, status: str) -> dict[str, Any] | None:
    if status not in ("approved", "dismissed", "draft"):
        raise ValueError(f"invalid status: {status}")
    with _lock:
        items = _load()
        for p in items:
            if p["id"] == pid:
                p["status"] = status
                p["updated_at"] = _now()
                _save(items)
                return p
    return None
