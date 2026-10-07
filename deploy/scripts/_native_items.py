"""Idempotent Fabric item publishing for dedicated opt-in deployment steps."""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import UUID

from deploy.scripts.fabric_runtime import decode_definition_parts, paginated_get

if TYPE_CHECKING:
    import requests

API = "https://api.fabric.microsoft.com/v1"
COLLECTIONS = {
    "OperationsAgent": "operationsAgents",
    "Plan": "plans",
    "Notebook": "notebooks",
    "Report": "reports",
}


class NativeItemError(RuntimeError):
    """A native item could not be published or read back safely."""


def _guid(value: str) -> str:
    try:
        return str(UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise NativeItemError(
            "Fabric item/workspace references must be UUIDs."
        ) from exc


def json_part(path: str, payload: Any) -> dict[str, str]:
    return {
        "path": path,
        "payload": base64.b64encode(json.dumps(payload).encode("utf-8")).decode(
            "ascii"
        ),
        "payloadType": "InlineBase64",
    }


def _operation_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "api.fabric.microsoft.com"
        or not parsed.path.startswith("/v1/operations/")
        or parsed.fragment
    ):
        raise NativeItemError("Fabric returned an untrusted operation URL.")
    return value


def _error(response: requests.Response, operation: str) -> NativeItemError:
    code = ""
    try:
        body = response.json()
        if isinstance(body, dict):
            code = str(body.get("errorCode", ""))[:100]
    except ValueError:
        pass
    return NativeItemError(
        f"{operation} failed: HTTP {response.status_code} {code}".strip()
    )


def complete_operation(
    session: requests.Session,
    response: requests.Response,
    *,
    timeout_seconds: float = 600,
    operation_name: str = "Native Fabric operation",
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> None:
    if response.status_code != 202:
        if response.status_code not in (200, 201, 204):
            raise _error(response, operation_name)
        return
    operation_id = response.headers.get("x-ms-operation-id")
    location = (
        f"{API}/operations/{_guid(operation_id)}"
        if operation_id else response.headers.get("Location")
    )
    if not location:
        raise NativeItemError("Accepted native operation has no polling reference.")
    location = _operation_url(location)
    deadline = monotonic() + timeout_seconds
    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise NativeItemError(
                "Native Fabric operation timed out; inspect before retrying."
            )
        delay = float(response.headers.get("Retry-After", 2))
        if delay < 0 or not delay < float("inf"):
            raise NativeItemError("Invalid native operation Retry-After.")
        sleep(min(max(delay, 0.1), remaining))
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise NativeItemError(
                "Native Fabric operation timed out; inspect before retrying."
            )
        response = session.get(
            location, timeout=min(30, remaining), allow_redirects=False
        )
        if response.status_code == 429:
            continue
        if response.status_code != 200:
            raise _error(response, "Native Fabric operation polling")
        body = response.json()
        state = str(body.get("status", "")).casefold()
        if state == "succeeded":
            return
        if state in {"failed", "canceled", "cancelled"}:
            raise NativeItemError(f"Native Fabric operation {state}.")
        if state not in {"notstarted", "running"}:
            raise NativeItemError("Native Fabric operation returned an unknown state.")


def get_definition(
    session: requests.Session,
    workspace_id: str,
    item_type: str,
    item_id: str,
    format_name: str | None = None,
) -> dict[str, Any]:
    collection = "semanticModels" if item_type == "SemanticModel" else COLLECTIONS[item_type]
    url = f"{API}/workspaces/{_guid(workspace_id)}/{collection}/{_guid(item_id)}/getDefinition"
    options = {"params": {"format": format_name}} if format_name else {}
    for attempt in range(5):
        response = session.post(url, timeout=30, allow_redirects=False, **options)
        if response.status_code != 404 or attempt == 4:
            break
        time.sleep(2)
    if response.status_code == 202:
        complete_operation(session, response, operation_name=f"Read {item_type} definition")
        operation_id = response.headers.get("x-ms-operation-id")
        if operation_id:
            result_url = f"{API}/operations/{_guid(operation_id)}/result"
        else:
            parsed = urlsplit(_operation_url(response.headers["Location"]))
            result_url = (
                f"https://api.fabric.microsoft.com{parsed.path.rstrip('/')}/result"
            )
        response = session.get(result_url, timeout=30, allow_redirects=False)
    if response.status_code != 200:
        raise _error(response, f"Read {item_type} definition")
    body = response.json()
    definition = body.get("definition", body)
    decode_definition_parts(definition)
    return definition


def _contains(expected: Any, actual: Any, path: tuple[str, ...] = ()) -> bool:
    if path == ("datasetReference", "byConnection", "connectionString"):
        if not isinstance(expected, str) or not isinstance(actual, str):
            return False

        def parameters(value: str) -> dict[str, str]:
            return {
                key.strip().casefold(): item.strip()
                for segment in value.split(";") if "=" in segment
                for key, item in [segment.split("=", 1)]
            }

        wanted, observed = parameters(expected), parameters(actual)
        return bool(wanted.get("semanticmodelid")) and all(
            observed.get(key) == value for key, value in wanted.items()
        )
    if isinstance(expected, dict):
        if path in {("configuration", "actions"), ("configuration", "dataSources")}:
            if not isinstance(actual, dict) or set(actual) != set(expected):
                return False
        return isinstance(actual, dict) and all(
            key in actual and _contains(value, actual[key], (*path, key))
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(expected)
            and all(
                _contains(left, right, path)
                for left, right in zip(expected, actual, strict=True)
            )
        )
    if type(expected) in (int, float) and type(actual) in (int, float):
        return expected == actual
    return type(expected) is type(actual) and expected == actual


def definition_matches(expected: dict[str, Any], actual: dict[str, Any]) -> bool:
    wanted = decode_definition_parts(expected)
    observed = decode_definition_parts(actual)
    if "definition.pbir" in wanted:
        service_parts = {".platform", "semanticModelDiagramLayout.json"}
        if set(wanted) - service_parts != set(observed) - service_parts:
            return False
    for path, content in wanted.items():
        if path == ".platform":
            continue
        actual_path = path
        if actual_path not in observed and path.endswith(".ipynb"):
            notebooks = [name for name in observed if name.endswith(".ipynb")]
            if len(notebooks) == 1:
                actual_path = notebooks[0]
        if actual_path not in observed:
            return False
        if path.endswith((".json", ".ipynb", ".pbir")):
            if not _contains(json.loads(content), json.loads(observed[actual_path])):
                return False
        elif observed[actual_path] != content:
            return False
    return True


def ensure_native_item(
    session: requests.Session,
    *,
    workspace_id: str,
    item_type: str,
    display_name: str,
    description: str,
    definition: dict[str, Any],
) -> str:
    if item_type not in COLLECTIONS or not display_name.strip():
        raise NativeItemError(
            "A supported native item type and nonempty name are required."
        )
    workspace_id = _guid(workspace_id)
    collection_url = f"{API}/workspaces/{workspace_id}/{COLLECTIONS[item_type]}"
    items = paginated_get(session, collection_url, timeout=30)
    matches = [
        item
        for item in items
        if item.get("displayName", "").casefold() == display_name.casefold()
    ]
    if len(matches) > 1:
        raise NativeItemError(f"Multiple {item_type} items match {display_name!r}.")
    decode_definition_parts(definition)
    if matches:
        item_id = _guid(matches[0]["id"])
        actual = get_definition(
            session, workspace_id, item_type, item_id, definition.get("format")
        )
        if definition_matches(definition, actual):
            return item_id
        response = session.post(
            f"{collection_url}/{item_id}/updateDefinition",
            json={"definition": definition},
            timeout=30,
            allow_redirects=False,
        )
        complete_operation(session, response, operation_name=f"Update {item_type}")
    else:
        response = session.post(
            collection_url,
            json={
                "displayName": display_name,
                "description": description,
                "definition": definition,
            },
            timeout=30,
            allow_redirects=False,
        )
        complete_operation(session, response, operation_name=f"Create {item_type}")
        body = response.json() if response.status_code == 201 else {}
        item_id = body.get("id")
        if item_id:
            item_id = _guid(item_id)
        else:
            for attempt in range(5):
                matches = [
                    item
                    for item in paginated_get(session, collection_url, timeout=30)
                    if item.get("displayName", "").casefold() == display_name.casefold()
                ]
                if len(matches) == 1:
                    item_id = _guid(matches[0]["id"])
                    break
                if len(matches) > 1:
                    raise NativeItemError(
                        f"Created {item_type} has ambiguous identity."
                    )
                if attempt < 4:
                    time.sleep(2)
            if not item_id:
                raise NativeItemError(
                    f"Created {item_type} is not visible; inspect before retrying."
                )
    for attempt in range(5):
        actual = get_definition(
            session, workspace_id, item_type, item_id, definition.get("format")
        )
        if definition_matches(definition, actual):
            return item_id
        if attempt < 4:
            time.sleep(2)
    raise NativeItemError(
        f"{item_type} readback does not match the requested definition."
    )
