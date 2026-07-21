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

Auth uses the configured operator login: the metadata cluster needs a Power BI
token and item listing needs a Fabric token.
"""

from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES, build_credential
from deploy.scripts.fabric_runtime import paginated_get

if TYPE_CHECKING:
    import requests
    from azure.core.credentials import TokenCredential

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
TERRAFORM_ITEM_OUTPUTS = {
    "Lakehouse": "lakehouse_id",
    "Eventhouse": "eventhouse_id",
    "KQLDatabase": "kql_database_id",
}


def _credential(
    credential: TokenCredential | None = None,
    *,
    auth_mode: str = "azure_cli",
    tenant_id: str | None = None,
) -> TokenCredential:
    """Return an injected credential or construct the configured operator login."""

    return credential or build_credential(auth_mode, tenant_id=tenant_id)


def _token(scope: str, credential: TokenCredential) -> str:
    """Acquire an access token, retrying transient operator-login failures."""

    from azure.core.exceptions import ClientAuthenticationError

    from deploy.scripts._retry import retry_call

    return retry_call(
        lambda: credential.get_token(scope).token,
        retry_on=(ClientAuthenticationError,),
        on_retry=lambda n, exc: console.warn(
            f"Operator token attempt {n} failed ({type(exc).__name__}); retrying..."
        ),
    )


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

    target = workspace_name.casefold()
    for workspace in paginated_get(fabric_session, f"{FABRIC_API}/workspaces"):
        if str(workspace.get("displayName", "")).casefold() == target:
            return str(workspace["id"])
    raise ValueError(f"Workspace not found: {workspace_name!r}")


def list_workspace_items(
    fabric_session: requests.Session, workspace_id: str
) -> list[dict[str, Any]]:
    """List all items in a workspace."""

    return paginated_get(
        fabric_session,
        f"{FABRIC_API}/workspaces/{workspace_id}/items",
    )


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
    response = pbi_session.post(
        url, json=body, headers={"Content-Type": "application/json"}
    )
    response.raise_for_status()
    return response.status_code


def _guid_name_map(items: list[dict[str, Any]]) -> dict[str, str]:
    return {str(i["id"]): str(i.get("displayName", "")) for i in items}


def to_portable(
    task_flow: dict[str, Any], guid_to_name: dict[str, str]
) -> dict[str, Any]:
    """Replace each item's GUID with its display name so the flow is workspace-agnostic.

    Items whose GUID can't be resolved to a name (deleted/stale references, or ids
    not present in the workspace item listing) are dropped rather than emitted with
    ``artifactName: null`` — a null name can never bind on deploy and only adds
    noise to the unresolved-reference report.
    """

    portable = json.loads(json.dumps(task_flow))  # deep copy
    for task in portable.get("tasks", []):
        kept: list[dict[str, Any]] = []
        for item in task.get("items", []):
            artifact_type, _, guid = str(item.get("artifactUniqueId", "")).partition(
                ":"
            )
            name = guid_to_name.get(item.get("artifactObjectId") or guid)
            if name is None:
                continue
            item["artifactName"] = name
            item["artifactType"] = item.get("artifactType", artifact_type)
            kept.append(item)
        task["items"] = kept
    return portable


def to_workspace(
    portable: dict[str, Any],
    name_type_to_guid: dict[tuple[str, str], str],
    item_type_to_guid: dict[str, str] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Resolve portable items to target GUIDs. Returns (task_flow, unresolved).

    Terraform-owned item types use their supplied IDs without a name lookup.
    Other items resolve by type and display name. Unresolved items are dropped
    because the server rejects references to non-existent artifacts.
    """

    resolved = json.loads(json.dumps(portable))
    item_type_to_guid = item_type_to_guid or {}
    unresolved: list[str] = []
    for task in resolved.get("tasks", []):
        kept_items: list[dict[str, Any]] = []
        for item in task.get("items", []):
            artifact_type = str(item.get("artifactType", ""))
            name = item.get("artifactName")
            item_type = ARTIFACT_TO_ITEM_TYPE.get(artifact_type, artifact_type)
            guid = item_type_to_guid.get(item_type)
            if not guid and name:
                guid = name_type_to_guid.get((item_type, name))
            if guid:
                item["artifactUniqueId"] = f"{artifact_type}:{guid}"
                item["artifactObjectId"] = guid
                item.pop("artifactName", None)
                kept_items.append(item)
            else:
                unresolved.append(f"{artifact_type}:{name}")
        task["items"] = kept_items
    return resolved, unresolved


def filter_portable_items(
    portable: dict[str, Any],
    allowed_artifacts: set[tuple[str, str]],
) -> dict[str, Any]:
    """Keep only task-flow item references selected by the deployment profile."""

    filtered = json.loads(json.dumps(portable))
    for task in filtered.get("tasks", []):
        task["items"] = [
            item
            for item in task.get("items", [])
            if (
                str(item.get("artifactType", "")),
                str(item.get("artifactName", "")),
            )
            in allowed_artifacts
        ]
    return filtered


def profile_taskflow_artifacts(
    repo_root: Path,
    config: Any,
) -> set[tuple[str, str]]:
    """Return exact portable task-flow references selected by a profile."""

    from deploy.scripts.build_artifacts import ML_EXPERIMENT_GROUPS
    from deploy.scripts.deploy_config import ONTOLOGY_ITEM_NAME
    from deploy.scripts.profile_preflight import selected_notebook_names

    profile = config.profile
    allowed = {
        ("SynapseNotebook", name)
        for name in selected_notebook_names(profile)
    }
    allowed.update(
        ("Pipeline", Path(pipeline_ref).stem)
        for pipeline_ref in profile.pipeline_refs
    )
    if profile.selects("asset.lakehouse"):
        allowed.update(
            {
                ("Lakehouse", config.lakehouse.name),
                ("SqlAnalyticsEndpoint", config.lakehouse.name),
            }
        )
    if profile.provisions_eventhouse:
        allowed.update(
            {
                ("KustoEventHouse", config.eventhouse.name),
                ("KustoDatabase", config.eventhouse.kql_database_name),
            }
        )
    if profile.selects("asset.semantic-model"):
        allowed.add(("dataset", config.powerbi.semantic_model_name))
    if profile.selects("asset.report"):
        allowed.add(("Report", config.powerbi.report_name))
    if profile.selects("asset.ml-notebooks"):
        allowed.update(
            ("MLExperiment", name)
            for group in profile.notebook_groups
            for name in ML_EXPERIMENT_GROUPS.get(group, ())
        )
    if profile.selects("asset.ontology"):
        allowed.add(("Ontology", ONTOLOGY_ITEM_NAME))
    if profile.selects("asset.data-agents"):
        allowed.update(
            ("LLMPlugin", path.stem)
            for path in (repo_root / "fabric" / "data-agents").glob("*.DataAgent")
        )
    if profile.selects("asset.kql-queryset"):
        allowed.add(("KQLQueryset", "retail_querysets"))
    return allowed


def export_taskflow(
    workspace: str,
    output_path: Path = DEFAULT_TASKFLOW_PATH,
    credential: TokenCredential | None = None,
    *,
    auth_mode: str = "azure_cli",
    tenant_id: str | None = None,
) -> Path:
    """Export a workspace task flow to a portable JSON file (artifacts by name)."""

    credential = _credential(
        credential,
        auth_mode=auth_mode,
        tenant_id=tenant_id,
    )
    pbi = _session(_token(PBI_SCOPE, credential))
    fabric = _session(_token(FABRIC_SCOPE, credential))
    workspace_id = (
        workspace
        if _looks_like_guid(workspace)
        else find_workspace_id(fabric, workspace)
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
    credential: TokenCredential | None = None,
    *,
    auth_mode: str = "azure_cli",
    tenant_id: str | None = None,
    item_type_to_guid: dict[str, str] | None = None,
    allowed_artifacts: set[tuple[str, str]] | None = None,
) -> list[str]:
    """Deploy a portable task flow to a workspace. Returns unresolved references."""

    portable = json.loads(input_path.read_text(encoding="utf-8"))
    if allowed_artifacts is not None:
        portable = filter_portable_items(portable, allowed_artifacts)
    credential = _credential(
        credential,
        auth_mode=auth_mode,
        tenant_id=tenant_id,
    )
    pbi = _session(_token(PBI_SCOPE, credential))
    fabric = _session(_token(FABRIC_SCOPE, credential))
    workspace_id = (
        workspace
        if _looks_like_guid(workspace)
        else find_workspace_id(fabric, workspace)
    )
    items = list_workspace_items(fabric, workspace_id)
    name_type_to_guid = {
        (str(i["type"]), str(i.get("displayName", ""))): str(i["id"]) for i in items
    }
    task_flow, unresolved = to_workspace(
        portable,
        name_type_to_guid,
        item_type_to_guid,
    )
    if unresolved:
        raise ValueError(
            "Selected task-flow references are unresolved: "
            + ", ".join(sorted(unresolved))
        )
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


def terraform_taskflow_targets(
    output_path: Path,
    *,
    config: Any | None = None,
) -> tuple[str, dict[str, str]]:
    """Load the workspace and Terraform-owned item IDs for task-flow deployment."""

    from deploy.scripts.deploy_config import (
        load_terraform_outputs,
        validate_terraform_outputs,
    )

    outputs = load_terraform_outputs(output_path)
    if config is not None:
        validate_terraform_outputs(config, outputs)
    workspace_id = outputs.get("workspace_id")
    if not workspace_id:
        raise ValueError(f"workspace_id missing from {output_path}")

    item_ids: dict[str, str] = {}
    for item_type, output_name in TERRAFORM_ITEM_OUTPUTS.items():
        item_id = outputs.get(output_name)
        if not item_id:
            raise ValueError(f"{output_name} missing from {output_path}")
        item_ids[item_type] = str(item_id)
    return str(workspace_id), item_ids


def main() -> int:
    parser = argparse.ArgumentParser(description="Export or deploy a Fabric Task Flow")
    parser.add_argument("action", choices=["export", "deploy"])
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--workspace", help="Workspace name or id (manual mode).")
    target.add_argument(
        "--terraform-output",
        type=Path,
        help=(
            "Terraform output JSON providing the workspace and Terraform-owned "
            "item IDs."
        ),
    )
    parser.add_argument("--path", type=Path, default=DEFAULT_TASKFLOW_PATH)
    parser.add_argument(
        "--environment",
        help="Configured environment required for profile-aware task-flow deploy.",
    )
    parser.add_argument(
        "--profile",
        help="Expected executable deployment profile.",
    )
    parser.add_argument(
        "--auth-mode",
        choices=AUTH_MODES,
        default=None,
        help="Operator credential used for Power BI and Fabric requests.",
    )
    parser.add_argument(
        "--tenant-id",
        help="Entra tenant passed to the selected operator credential.",
    )
    args = parser.parse_args()

    if args.action == "export":
        workspace = args.workspace
        if args.terraform_output:
            workspace, _item_ids = terraform_taskflow_targets(
                args.terraform_output
            )
        if workspace is None:
            raise ValueError("A workspace name, ID, or Terraform output is required")
        out = export_taskflow(
            workspace,
            args.path,
            auth_mode=args.auth_mode or "azure_cli",
            tenant_id=args.tenant_id,
        )
        console.info(f"Exported task flow to {out}")
    else:
        if not args.environment or not args.profile:
            raise ValueError(
                "task-flow deploy requires --environment and --profile; "
                "unscoped legacy deployment is unsupported"
            )
        from deploy.scripts.deploy_config import load_environment

        config = load_environment(args.environment)
        if config.deployment.profile != args.profile:
            raise ValueError(
                f"--profile {args.profile!r} does not match configured profile "
                f"{config.deployment.profile!r}"
            )
        if not config.profile.deploys_task_flow:
            raise ValueError(
                f"profile {args.profile!r} does not select task-flow deployment"
            )
        configured_tenant = getattr(config, "tenant_id", None)
        if (
            args.tenant_id
            and configured_tenant
            and args.tenant_id.casefold() != configured_tenant.casefold()
        ):
            raise ValueError("--tenant-id does not match the configured tenant")
        auth_mode = args.auth_mode or getattr(config, "auth_mode", "azure_cli")
        tenant_id = args.tenant_id or configured_tenant
        workspace = args.workspace
        item_type_to_guid = None
        if args.terraform_output:
            workspace, item_type_to_guid = terraform_taskflow_targets(
                args.terraform_output,
                config=config,
            )
        if workspace is None:
            raise ValueError("A workspace name, ID, or Terraform output is required")
        deploy_taskflow(
            workspace,
            args.path,
            auth_mode=auth_mode,
            tenant_id=tenant_id,
            item_type_to_guid=item_type_to_guid,
            allowed_artifacts=profile_taskflow_artifacts(REPO_ROOT, config),
        )
        console.info("Deployed task flow.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
