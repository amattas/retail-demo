"""Read real rows from the semantic model via the Power BI ``executeQueries`` API.

The action agents need *structured data* (not prose) so they can apply numeric
trigger rules — e.g. "stockout probability >= 0.7 AND days of cover <= 3". The
Data Agent returns natural language, so for the agents we query the underlying
``retail_model`` dataset directly with DAX and get back JSON rows.

Tokens are minted from the developer's ``az login`` session (Power BI scope),
exactly like the embed endpoint.
"""

from __future__ import annotations

import functools

import requests

from . import config, mcp_client


@functools.lru_cache(maxsize=1)
def dataset_id() -> str:
    """Return the dataset id, resolving it from the report if not configured."""
    if config.DATASET_ID:
        return config.DATASET_ID
    token = mcp_client.get_token(config.POWERBI_SCOPE)
    base = f"{config.POWERBI_HOST}/v1.0/myorg/groups/{config.WORKSPACE_ID}"
    resp = requests.get(
        f"{base}/reports/{config.REPORT_ID}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("datasetId", "")


def _clean_key(key: str) -> str:
    """Turn ``'table'[Column]`` or ``[alias]`` into a bare name."""
    if "[" in key and "]" in key:
        return key[key.index("[") + 1 : key.rindex("]")]
    return key


def query_rows(dax: str, timeout: int = 120) -> list[dict]:
    """Execute a DAX query and return a list of cleaned-key row dicts."""
    token = mcp_client.get_token(config.POWERBI_SCOPE)
    ds = dataset_id()
    url = (
        f"{config.POWERBI_HOST}/v1.0/myorg/groups/{config.WORKSPACE_ID}"
        f"/datasets/{ds}/executeQueries"
    )
    body = {
        "queries": [{"query": dax}],
        "serializerSettings": {"includeNulls": True},
    }
    resp = requests.post(
        url,
        json=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    tables = resp.json()["results"][0]["tables"]
    rows = tables[0].get("rows", []) if tables else []
    return [{_clean_key(k): v for k, v in row.items()} for row in rows]
