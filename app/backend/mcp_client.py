"""Thin MCP (Model Context Protocol) JSON-RPC client and Azure token helper.

The Fabric Data Agent and Ontology endpoints are stateless MCP servers: each
call is a single JSON-RPC ``POST``. Responses come back either as plain JSON or
as a Server-Sent-Events stream (``text/event-stream``); this module normalises
both into a parsed dict.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any

import requests
from azure.identity import AzureCliCredential

from . import config

_credential = AzureCliCredential()
_token_cache: dict[str, tuple[str, float]] = {}
_token_lock = threading.Lock()


def get_token(scope: str) -> str:
    """Return a cached bearer token for ``scope`` (refreshed ~5 min before expiry)."""
    with _token_lock:
        cached = _token_cache.get(scope)
        now = time.time()
        if cached and cached[1] - 300 > now:
            return cached[0]
        token = _credential.get_token(scope)
        _token_cache[scope] = (token.token, token.expires_on)
        return token.token


def _parse_response(resp: requests.Response) -> dict[str, Any]:
    """Parse a JSON-RPC response that may be JSON or an SSE stream."""
    ctype = resp.headers.get("content-type", "")
    text = resp.text
    if "text/event-stream" in ctype:
        # Concatenate the JSON carried on ``data:`` lines.
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("data:"):
                payload = line[len("data:"):].strip()
                if payload and payload != "[DONE]":
                    try:
                        return json.loads(payload)
                    except json.JSONDecodeError:
                        continue
        raise ValueError(f"No JSON payload in SSE response: {text[:200]}")
    return resp.json()


def rpc(url: str, method: str, params: dict[str, Any] | None = None,
        timeout: int = 120) -> dict[str, Any]:
    """Make a single JSON-RPC call against an MCP endpoint."""
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    headers = {
        "Authorization": f"Bearer {get_token(config.FABRIC_SCOPE)}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    resp = requests.post(url, json=body, headers=headers, timeout=timeout)
    resp.raise_for_status()
    data = _parse_response(resp)
    if "error" in data:
        raise RuntimeError(data["error"].get("message", str(data["error"])))
    return data.get("result", {})


def list_tools(url: str) -> list[dict[str, Any]]:
    """Return the tool definitions advertised by an MCP endpoint."""
    return rpc(url, "tools/list").get("tools", [])


def call_tool(url: str, name: str, arguments: dict[str, Any],
              timeout: int = 180) -> str:
    """Call an MCP tool and return the concatenated text content."""
    result = rpc(url, "tools/call", {"name": name, "arguments": arguments},
                 timeout=timeout)
    parts = []
    for item in result.get("content", []):
        if item.get("type") == "text":
            parts.append(item.get("text", ""))
    return "\n".join(parts).strip()
