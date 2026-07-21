"""Reusable Fabric REST definition, pagination, and schedule helpers."""

from __future__ import annotations

import base64
import binascii
import json
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import requests

_MAX_PAGES = 1000
_MAX_DEFINITION_BYTES = 16 * 1024 * 1024


class FabricPaginationError(RuntimeError):
    """Raised when a paginated Fabric response is incomplete or loops."""


class FabricDefinitionError(ValueError):
    """Raised when a Fabric item definition is malformed or unsafe to inspect."""


def paginated_get(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    max_pages: int = _MAX_PAGES,
) -> list[dict[str, Any]]:
    """Exhaust a Fabric ``value`` collection, failing closed on malformed loops."""

    if max_pages < 1:
        raise ValueError("max_pages must be positive")

    base_url = url
    base_params = dict(params or {})
    next_url = base_url
    next_params: dict[str, Any] | None = base_params or None
    seen_markers: set[str] = set()
    values: list[dict[str, Any]] = []

    for _ in range(max_pages):
        if next_params is None:
            response = session.get(next_url)
        else:
            response = session.get(next_url, params=next_params)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
            raise FabricPaginationError(
                "Fabric collection response did not contain an array named 'value'."
            )
        page = payload["value"]
        if any(not isinstance(item, dict) for item in page):
            raise FabricPaginationError(
                "Fabric collection response contained a non-object item."
            )
        values.extend(page)

        continuation_uri = payload.get("continuationUri")
        continuation_token = payload.get("continuationToken")
        if continuation_uri:
            marker = f"uri:{continuation_uri}"
            next_url = str(continuation_uri)
            next_params = None
        elif continuation_token:
            marker = f"token:{continuation_token}"
            next_url = base_url
            next_params = {
                **base_params,
                "continuationToken": str(continuation_token),
            }
        else:
            return values

        if marker in seen_markers:
            raise FabricPaginationError(
                "Fabric collection returned a repeated pagination token/URI."
            )
        seen_markers.add(marker)

    raise FabricPaginationError(
        f"Fabric collection exceeded the {max_pages}-page safety bound."
    )


def decode_definition_parts(
    definition: dict[str, Any],
    *,
    max_total_bytes: int = _MAX_DEFINITION_BYTES,
) -> dict[str, bytes]:
    """Decode definition parts in memory after validating paths and size."""

    parts = definition.get("parts")
    if not isinstance(parts, list):
        raise FabricDefinitionError("Fabric definition has no parts array.")

    decoded: dict[str, bytes] = {}
    total = 0
    for part in parts:
        if not isinstance(part, dict):
            raise FabricDefinitionError("Fabric definition contains a non-object part.")
        path = part.get("path")
        payload = part.get("payload")
        if not isinstance(path, str) or not path:
            raise FabricDefinitionError("Fabric definition part has no path.")
        pure_path = PurePosixPath(path)
        if pure_path.is_absolute() or "\\" in path or ".." in pure_path.parts:
            raise FabricDefinitionError(f"Unsafe Fabric definition part path: {path!r}")
        if path in decoded:
            raise FabricDefinitionError(
                f"Fabric definition contains duplicate part path {path!r}."
            )
        if not isinstance(payload, str):
            raise FabricDefinitionError(
                f"Fabric definition part {path!r} has no base64 payload."
            )
        try:
            content = base64.b64decode(payload, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise FabricDefinitionError(
                f"Fabric definition part {path!r} has invalid base64."
            ) from exc
        total += len(content)
        if total > max_total_bytes:
            raise FabricDefinitionError(
                "Fabric definition exceeds the in-memory inspection size bound."
            )
        decoded[path] = content
    return decoded


def json_definition_part(parts: dict[str, bytes], suffix: str) -> dict[str, Any]:
    """Return the unique JSON definition part whose path ends with ``suffix``."""

    matches = [
        (path, content)
        for path, content in parts.items()
        if path.casefold().endswith(suffix.casefold())
    ]
    if len(matches) != 1:
        raise FabricDefinitionError(
            f"Expected exactly one definition part ending in {suffix!r}; "
            f"found {len(matches)}."
        )
    path, content = matches[0]
    try:
        value = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FabricDefinitionError(
            f"Fabric definition part {path!r} is not valid UTF-8 JSON."
        ) from exc
    if not isinstance(value, dict):
        raise FabricDefinitionError(
            f"Fabric definition part {path!r} must contain a JSON object."
        )
    return value


def schedule_document(parts: dict[str, bytes]) -> dict[str, Any] | None:
    """Read and normalize a live ``.schedules`` definition part."""

    matches = [
        content for path, content in parts.items() if PurePosixPath(path).name == ".schedules"
    ]
    if not matches:
        return None
    if len(matches) != 1:
        raise FabricDefinitionError("Fabric definition has duplicate .schedules parts.")
    try:
        document = json.loads(matches[0].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FabricDefinitionError("Fabric .schedules part is not valid JSON.") from exc
    return normalize_schedule_document(document)


def normalize_schedule_document(document: Any) -> dict[str, Any]:
    """Return the comparable schedule contract, excluding only its schema URI."""

    if not isinstance(document, dict) or not isinstance(document.get("schedules"), list):
        raise FabricDefinitionError("Schedule document must contain a schedules array.")
    if any(not isinstance(item, dict) for item in document["schedules"]):
        raise FabricDefinitionError("Schedule document contains a non-object schedule.")
    return {"schedules": document["schedules"]}
