"""Configure Operations Agents through the workload endpoint used by Fabric UI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

import requests

from deploy.scripts._native_items import NativeItemError
from deploy.scripts.fabric_runtime import retry_read
from deploy.scripts.taskflow import PBI_SCOPE, _token

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

_ACTION_ID = str(uuid5(NAMESPACE_URL, "retail-demo:operations:prepare-case"))
_GOALS = (
    "Detect current high stockout risk or low inventory cases from the latest "
    "physical ops_availability_signals row per case_id, notify operators with "
    "complete evidence, and require human approval before preparing a draft."
)


@dataclass(frozen=True)
class OperationsUiContext:
    """Authenticated Operations Agent workload endpoint."""

    base_url: str
    token: str


def _guid(value: str) -> str:
    try:
        return str(UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise NativeItemError("Operations Agent UI references must be UUIDs.") from exc


def _trusted_url(value: str, suffixes: tuple[str, ...]) -> str:
    parsed = urlsplit(value)
    host = (parsed.hostname or "").casefold()
    if (
        parsed.scheme != "https"
        or not host
        or not any(host.endswith(suffix) for suffix in suffixes)
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise NativeItemError("Fabric returned an untrusted workload URL.")
    return value.rstrip("/")


def _raise_ui_error(response: requests.Response, operation: str) -> None:
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    code = str(payload.get("code", ""))[:100] if isinstance(payload, dict) else ""
    message = (
        " ".join(str(payload.get("message", "")).split())[:300]
        if isinstance(payload, dict)
        else ""
    )
    request_id = response.headers.get("x-ms-root-activity-id", "")[:100]
    details = ": ".join(value for value in (code, message) if value)
    request = f" (request {request_id})" if request_id else ""
    raise NativeItemError(
        f"{operation} failed: HTTP {response.status_code}"
        f"{' ' + details if details else ''}{request}"
    )


def _pbi_session(credential: TokenCredential) -> requests.Session:
    session = requests.Session()
    session.headers["Authorization"] = "Bear" + "er " + _token(PBI_SCOPE, credential)
    session.headers["Accept"] = "application/json"
    session.headers["Content-Type"] = "application/json"
    return session


def get_operations_ui_context(
    credential: TokenCredential,
    *,
    workspace_id: str,
    agent_id: str,
) -> OperationsUiContext:
    """Exchange the operator token for the workload token used by Fabric UI."""

    workspace_id, agent_id = _guid(workspace_id), _guid(agent_id)
    session = _pbi_session(credential)
    workspace_url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
    )
    response = retry_read(
        lambda: session.get(workspace_url, timeout=30, allow_redirects=False)
    )
    if response.status_code != 200:
        _raise_ui_error(response, "Read Power BI workspace routing")
    home_cluster = response.headers.get("home-cluster-uri")
    if not home_cluster:
        raise NativeItemError("Power BI workspace routing omitted home-cluster-uri.")
    home_cluster = _trusted_url(
        home_cluster,
        (".analysis.windows.net",),
    )
    response = session.post(
        f"{home_cluster}/metadata/v201606/generatemwctokenv2",
        json={
            "workloadType": "OA",
            "workspaceObjectId": workspace_id,
            "artifacts": [
                {
                    "artifactType": "OperationalAgents",
                    "artifactObjectId": agent_id,
                }
            ],
        },
        timeout=30,
        allow_redirects=False,
    )
    if response.status_code != 200:
        _raise_ui_error(response, "Acquire Operations Agent workload token")
    payload = response.json()
    target_host = payload.get("TargetUriHost") or payload.get("targetUriHost")
    capacity_id = payload.get("CapacityObjectId") or payload.get("capacityObjectId")
    token = payload.get("Token") or payload.get("token")
    if not target_host or not capacity_id or not token:
        raise NativeItemError("Operations Agent workload-token response is incomplete.")
    target_url = str(target_host)
    if not target_url.startswith("https://"):
        target_url = f"https://{target_url}"
    target_url = _trusted_url(
        target_url,
        (".pbidedicated.windows.net", ".analysis.windows.net"),
    )
    return OperationsUiContext(
        base_url=(
            f"{target_url}/webapi/capacities/{_guid(capacity_id)}/workloads/OA/"
            "OAgentsService/automatic/v1/"
            f"workspaces/{workspace_id}/operationalagents/{agent_id}"
        ),
        token=str(token),
    )


def render_ui_configuration(
    *,
    workspace_id: str,
    kql_database_id: str,
    kusto_uri: str,
    notebook_id: str,
    instructions: str,
) -> dict[str, Any]:
    """Render the configuration contract emitted by the current Fabric UI."""

    workspace_id, kql_database_id, notebook_id = (
        _guid(value) for value in (workspace_id, kql_database_id, notebook_id)
    )
    endpoint = _trusted_url(
        kusto_uri,
        (".kusto.fabric.microsoft.com", ".kusto.windows.net"),
    )
    return {
        "dataSources": {
            kql_database_id: {
                "id": kql_database_id,
                "type": "KustoDatabase",
                "displayName": "retail_eventhouse",
                "description": None,
                "workspaceId": workspace_id,
                "connection": {
                    "endpoint": endpoint,
                    "tableName": kql_database_id,
                },
            }
        },
        "actions": {
            "prepareCase": {
                "id": _ACTION_ID,
                "displayName": "Prepare replenishment case",
                "description": (
                    "Create a draft planning case after human approval; "
                    "never place an order."
                ),
                "kind": "FabricJobAction",
                "connection": {
                    "jobType": "RunNotebook",
                    "itemType": "SynapseNotebook",
                    "itemArtifactId": notebook_id,
                    "itemWorkspaceId": workspace_id,
                    "subItemId": "",
                },
                "parameters": [
                    {
                        "name": "CASE_ID",
                        "description": (
                            "case_id of the selected availability signal"
                        ),
                    }
                ],
            }
        },
        "goals": _GOALS,
        "instructions": instructions,
    }


def _configuration_matches(
    expected: dict[str, Any],
    actual: dict[str, Any],
) -> bool:
    if not isinstance(actual, dict):
        return False
    return all(actual.get(key) == value for key, value in expected.items())


def ensure_operations_ui_configuration(
    credential: TokenCredential,
    *,
    workspace_id: str,
    agent_id: str,
    kql_database_id: str,
    kusto_uri: str,
    notebook_id: str,
    instructions: str,
) -> dict[str, Any]:
    """Save and read back the source/action contract through the UI service."""

    context = get_operations_ui_context(
        credential,
        workspace_id=workspace_id,
        agent_id=agent_id,
    )
    session = requests.Session()
    session.headers["Authorization"] = "Mwc" + "Token " + context.token
    session.headers["Accept"] = "application/json"
    session.headers["Content-Type"] = "application/json"
    expected = render_ui_configuration(
        workspace_id=workspace_id,
        kql_database_id=kql_database_id,
        kusto_uri=kusto_uri,
        notebook_id=notebook_id,
        instructions=instructions,
    )
    response = session.post(
        f"{context.base_url}/configuration",
        json=expected,
        timeout=90,
        allow_redirects=False,
    )
    if response.status_code != 200:
        _raise_ui_error(response, "Save Operations Agent UI configuration")
    response = retry_read(
        lambda: session.get(
            f"{context.base_url}/configuration",
            timeout=30,
            allow_redirects=False,
        )
    )
    if response.status_code != 200:
        _raise_ui_error(response, "Read Operations Agent UI configuration")
    actual = response.json()
    if not _configuration_matches(expected, actual):
        raise NativeItemError(
            "Operations Agent UI configuration readback does not match."
        )
    return actual
