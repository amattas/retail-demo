"""Export and deploy Fabric workspace Task Flows.

Task Flows are not exposed by the public Fabric REST API. They live on the
Power BI **metadata cluster** and are read/written with these undocumented
endpoints (discovered by capturing the Fabric UI):

    cluster = PUT https://api.powerbi.com/spglobalservice/GetOrInsertClusterUrisByTenantLocation
              -> {"FixedClusterUri": "https://wabi-<region>.analysis.windows.net/"}

    read    = GET  {cluster}/metadata/workspaces/{workspaceId}/taskflow202602
              -> [{"etag", "resourceId", "taskFlow": {tasks, edges, id, name, description}}]

    save    = PUT  {cluster}/metadata/workspaces/{workspaceId}/taskflow202512/{resourceId}
              body = {"tasks": [...], "edges": [...]}

A task references workspace artifacts by ``artifactUniqueId = "<artifactType>:<guid>"``.
Because those GUIDs are workspace-specific, ``export_taskflow`` resolves each GUID
to the artifact's display name (a portable form), and ``deploy_taskflow`` resolves
the names back to the **target** workspace's GUIDs before saving.

Auth uses the Azure CLI login: the metadata cluster needs a Power BI token and
item listing needs a Fabric token.
"""

from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts import _output as console

if TYPE_CHECKING:
    import requests
    from azure.identity import AzureCliCredential

PBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_API = "https://api.fabric.microsoft.com/v1"
CLUSTER_DISCOVERY = (
    "https://api.powerbi.com/spglobalservice/GetOrInsertClusterUrisByTenantLocation"
)
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TASKFLOW_PATH = REPO_ROOT / "fabric" / "taskflow" / "taskflow.json"

# Task-flow artifactType -> Fabric REST item type (for name resolution).
ARTIFACT_TO_ITEM_TYPE = {
    "SynapseNotebook": "Notebook",
    "Pipeline": "DataPipeline",
    "Lakehouse": "Lakehouse",
    "SqlAnalyticsEndpoint": "SQLEndpoint",
    "KustoEventHouse": "Eventhouse",
    "KustoDatabase": "KQLDatabase",
    "LLMPlugin": "DataAgent",
    "Ontology": "Ontology",
    "GraphIndex": "GraphModel",
    "MLExperiment": "MLExperiment",
    "KQLQueryset": "KQLQueryset",
    "Report": "Report",
    "dataset": "SemanticModel",
}


def _credential(credential: AzureCliCredential | None = None) -> AzureCliCredential:
    """Azure CLI credential with a generous process timeout.

    On Windows ``az.cmd`` is slow to start, and a *cold* token for the Power BI /
    Fabric audience can take ~90s (the deploy fetches both back to back; warm
    calls are ~1s). 120s absorbs the cold-start without making a genuinely-broken
    ``az`` hang excessively. One credential is reused for both token requests.
    """

    from azure.identity import AzureCliCredential

    return credential or AzureCliCredential(process_timeout=120)


def _token(scope: str, credential: AzureCliCredential) -> str:
    return credential.get_token(scope).token


def _session(token: str) -> requests.Session:
    import requests

    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"
    return session


def resolve_cluster(pbi_session: requests.Session) -> str:
    """Resolve the tenant's metadata cluster URI (no trailing slash)."""

    response = pbi_session.put(CLUSTER_DISCOVERY, headers={"Content-Length": "0"})
    response.raise_for_status()
    return str(response.json()["FixedClusterUri"]).rstrip("/")


def find_workspace_id(fabric_session: requests.Session, workspace_name: str) -> str:
    """Resolve a workspace display name to its id (case-insensitive)."""

    response = fabric_session.get(f"{FABRIC_API}/workspaces")
    response.raise_for_status()
    target = workspace_name.casefold()
    for workspace in response.json().get("value", []):
        if str(workspace.get("displayName", "")).casefold() == target:
            return str(workspace["id"])
    raise ValueError(f"Workspace not found: {workspace_name!r}")


def list_workspace_items(
    fabric_session: requests.Session, workspace_id: str
) -> list[dict[str, Any]]:
    """List all items in a workspace."""

    response = fabric_session.get(f"{FABRIC_API}/workspaces/{workspace_id}/items")
    response.raise_for_status()
    return response.json().get("value", [])


def get_taskflow(
    pbi_session: requests.Session, cluster: str, workspace_id: str
) -> dict[str, Any] | None:
    """Read the raw task flow record (``{etag, resourceId, taskFlow}``) or ``None``.

    A workspace that has never had a task flow returns an empty list; this
    returns ``None`` in that case so callers can create one.
    """

    url = f"{cluster}/metadata/workspaces/{workspace_id}/taskflow202602"
    response = pbi_session.get(url)
    response.raise_for_status()
    records = response.json()
    return records[0] if records else None


def put_taskflow(
    pbi_session: requests.Session,
    cluster: str,
    workspace_id: str,
    record: dict[str, Any],
    task_flow: dict[str, Any],
) -> int:
    """Update an existing task flow in the workspace.

    The body must carry the existing task-flow identity (``id``/``name``/
    ``description``) — the server rejects an empty ``taskflow.Id`` — plus the new
    ``tasks`` and ``edges``.
    """

    resource_id = record["resourceId"]
    existing = record.get("taskFlow", {})
    url = f"{cluster}/metadata/workspaces/{workspace_id}/taskflow202512/{resource_id}"
    headers = {"Content-Type": "application/json"}
    if record.get("etag"):
        headers["If-Match"] = record["etag"]
    body = {
        "id": existing.get("id") or resource_id,
        "name": existing.get("name") or "Retail Demo",
        "description": existing.get("description", ""),
        "tasks": task_flow.get("tasks", []),
        "edges": task_flow.get("edges", []),
    }
    response = pbi_session.put(url, json=body, headers=headers)
    response.raise_for_status()
    return response.status_code


def create_taskflow(
    pbi_session: requests.Session,
    cluster: str,
    workspace_id: str,
    task_flow: dict[str, Any],
    name: str = "Retail Demo",
) -> int:
    """Create a workspace's first task flow via POST to the collection.

    The body is the full task-flow object (``id``, ``name``, ``description``,
    ``tasks``, ``edges``); the server rejects an empty ``id``.
    """

    url = f"{cluster}/metadata/workspaces/{workspace_id}/taskflow202512"
    body = {
        "id": task_flow.get("id") or str(uuid.uuid4()),
        "name": task_flow.get("name") or name,
        "description": task_flow.get("description", ""),
        "tasks": task_flow.get("tasks", []),
        "edges": task_flow.get("edges", []),
    }
    response = pbi_session.post(url, json=body, headers={"Content-Type": "application/json"})
    response.raise_for_status()
    return response.status_code


def _guid_name_map(items: list[dict[str, Any]]) -> dict[str, str]:
    return {str(i["id"]): str(i.get("displayName", "")) for i in items}


def to_portable(task_flow: dict[str, Any], guid_to_name: dict[str, str]) -> dict[str, Any]:
    """Replace each item's GUID with its display name so the flow is workspace-agnostic."""

    portable = json.loads(json.dumps(task_flow))  # deep copy
    for task in portable.get("tasks", []):
        for item in task.get("items", []):
            artifact_type, _, guid = str(item.get("artifactUniqueId", "")).partition(":")
            name = guid_to_name.get(item.get("artifactObjectId") or guid)
            item["artifactName"] = name  # None when unresolved (e.g. legacy dataset id)
            item["artifactType"] = item.get("artifactType", artifact_type)
    return portable


def to_workspace(
    portable: dict[str, Any], name_type_to_guid: dict[tuple[str, str], str]
) -> tuple[dict[str, Any], list[str]]:
    """Resolve portable item names to target GUIDs. Returns (task_flow, unresolved).

    Items that can't be resolved to a target-workspace GUID are dropped from
    their task (the server rejects references to non-existent artifacts) and
    reported in the returned ``unresolved`` list.
    """

    resolved = json.loads(json.dumps(portable))
    unresolved: list[str] = []
    for task in resolved.get("tasks", []):
        kept_items: list[dict[str, Any]] = []
        for item in task.get("items", []):
            artifact_type = str(item.get("artifactType", ""))
            name = item.get("artifactName")
            item_type = ARTIFACT_TO_ITEM_TYPE.get(artifact_type, artifact_type)
            guid = name_type_to_guid.get((item_type, name)) if name else None
            if guid:
                item["artifactUniqueId"] = f"{artifact_type}:{guid}"
                item["artifactObjectId"] = guid
                item.pop("artifactName", None)
                kept_items.append(item)
            else:
                unresolved.append(f"{artifact_type}:{name}")
        task["items"] = kept_items
    return resolved, unresolved


def export_taskflow(
    workspace: str,
    output_path: Path = DEFAULT_TASKFLOW_PATH,
    credential: AzureCliCredential | None = None,
) -> Path:
    """Export a workspace task flow to a portable JSON file (artifacts by name)."""

    credential = _credential(credential)
    pbi = _session(_token(PBI_SCOPE, credential))
    fabric = _session(_token(FABRIC_SCOPE, credential))
    workspace_id = workspace if _looks_like_guid(workspace) else find_workspace_id(
        fabric, workspace
    )
    cluster = resolve_cluster(pbi)
    record = get_taskflow(pbi, cluster, workspace_id)
    if record is None:
        raise ValueError(f"No task flow found in workspace {workspace}")
    guid_to_name = _guid_name_map(list_workspace_items(fabric, workspace_id))
    portable = to_portable(record["taskFlow"], guid_to_name)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(portable, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return output_path


def deploy_taskflow(
    workspace: str,
    input_path: Path = DEFAULT_TASKFLOW_PATH,
    credential: AzureCliCredential | None = None,
) -> list[str]:
    """Deploy a portable task flow to a workspace. Returns unresolved references."""

    portable = json.loads(input_path.read_text(encoding="utf-8"))
    credential = _credential(credential)
    pbi = _session(_token(PBI_SCOPE, credential))
    fabric = _session(_token(FABRIC_SCOPE, credential))
    workspace_id = workspace if _looks_like_guid(workspace) else find_workspace_id(
        fabric, workspace
    )
    items = list_workspace_items(fabric, workspace_id)
    name_type_to_guid = {
        (str(i["type"]), str(i.get("displayName", ""))): str(i["id"]) for i in items
    }
    task_flow, unresolved = to_workspace(portable, name_type_to_guid)
    cluster = resolve_cluster(pbi)
    record = get_taskflow(pbi, cluster, workspace_id)
    if record is None:
        # Fresh workspace with no task flow yet — create one.
        create_taskflow(pbi, cluster, workspace_id, {**portable, **task_flow})
    else:
        put_taskflow(pbi, cluster, workspace_id, record, task_flow)
    return unresolved


def _looks_like_guid(value: str) -> bool:
    return len(value) == 36 and value.count("-") == 4


def main() -> int:
    parser = argparse.ArgumentParser(description="Export or deploy a Fabric Task Flow")
    parser.add_argument("action", choices=["export", "deploy"])
    parser.add_argument("--workspace", required=True, help="Workspace name or id.")
    parser.add_argument("--path", type=Path, default=DEFAULT_TASKFLOW_PATH)
    args = parser.parse_args()

    if args.action == "export":
        out = export_taskflow(args.workspace, args.path)
        console.info(f"Exported task flow to {out}")
    else:
        unresolved = deploy_taskflow(args.workspace, args.path)
        console.info("Deployed task flow.")
        if unresolved:
            console.warn("Unresolved references (left unbound):")
            for ref in unresolved:
                console.detail(ref)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
