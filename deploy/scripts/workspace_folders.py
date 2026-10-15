"""Organize Fabric demo items by object type without changing item identities."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from deploy.scripts import _output as console
from deploy.scripts._native_items import NativeItemError, _guid
from deploy.scripts.fabric_runtime import paginated_get, retry_read

ROOT = Path(__file__).resolve().parents[2]
API = "https://api.fabric.microsoft.com/v1"
ITEM_FOLDERS = {
    "Notebook": "Notebooks",
    "DataPipeline": "Pipelines",
    "Report": "Reports",
    "SemanticModel": "Semantic Models",
    "MLExperiment": "ML Experiments",
    "DataAgent": "Data Agents",
    "Lakehouse": "Lakehouses",
    "Eventhouse": "Eventhouses",
    "KQLQueryset": "KQL Querysets",
    "Ontology": "Ontologies",
    "GraphModel": "Graph Models",
    "AppBackend": "Fabric Apps",
    "SQLDatabase": "SQL Databases",
    "OperationsAgent": "Operations Agents",
    "Plan": "Plans",
    "Dashboard": "Dashboards",
    "KQLDashboard": "KQL Dashboards",
    "Reflex": "Activators",
    "MLModel": "ML Models",
}
MANAGED_CHILD_FOLDERS = {
    "SQLEndpoint": {"Lakehouses", "SQL Databases", "Fabric Apps"},
    "KQLDatabase": {"Eventhouses", "Ontologies"},
}
LEGACY_FOLDERS = frozenset({"Setup", "Streaming", "Reporting", "ML"})
_PARENT_ORDER = ("Ontology", "AppBackend", "Lakehouse", "Eventhouse", "SQLDatabase")


class WorkspaceFolderError(RuntimeError):
    """A workspace cannot be organized or its placements cannot be verified."""


def _inventory(
    session: requests.Session, base: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        paginated_get(session, f"{base}/folders", timeout=30),
        paginated_get(
            session, f"{base}/items", params={"recursive": "true"}, timeout=30
        ),
    )


def organize_workspace(
    session: requests.Session,
    workspace_id: str,
    *,
    cleanup_legacy: bool = True,
) -> dict[str, Any]:
    """Move parents once, retain service-managed children, and verify all IDs."""

    base = f"{API}/workspaces/{_guid(workspace_id)}"
    folders, items = _inventory(session, base)
    folder_ids: dict[str, str] = {}
    for folder in folders:
        if folder.get("parentFolderId"):
            continue
        name = str(folder["displayName"])
        if name.casefold() in {value.casefold() for value in folder_ids}:
            raise WorkspaceFolderError(f"Ambiguous top-level workspace folder: {name}")
        folder_ids[name] = _guid(folder["id"])
    unknown = sorted(
        {str(item["type"]) for item in items}
        - ITEM_FOLDERS.keys()
        - MANAGED_CHILD_FOLDERS.keys()
    )
    if unknown:
        raise WorkspaceFolderError(f"No object-type folder policy for: {unknown}")
    original_ids = {_guid(item["id"]) for item in items}
    if len(original_ids) != len(items):
        raise WorkspaceFolderError("Workspace inventory contains duplicate item IDs.")
    inherited: dict[str, str] = {}
    moved: list[dict[str, Any]] = []

    def target_folder(name: str) -> str:
        existing = next(
            (value for key, value in folder_ids.items() if key.casefold() == name.casefold()),
            None,
        )
        if existing:
            return existing
        response = session.post(
            f"{base}/folders",
            json={"displayName": name},
            timeout=30,
            allow_redirects=False,
        )
        response.raise_for_status()
        if response.status_code != 201:
            raise WorkspaceFolderError(f"Unexpected folder-create response for {name}.")
        folder_id = _guid(response.json()["id"])
        folder_ids[name] = folder_id
        return folder_id

    def ordering(item: dict[str, Any]) -> tuple[int, str, str]:
        item_type = str(item["type"])
        priority = (
            _PARENT_ORDER.index(item_type)
            if item_type in _PARENT_ORDER
            else len(_PARENT_ORDER)
        )
        return priority, item_type, str(item["displayName"])

    for item in sorted(items, key=ordering):
        item_id = _guid(item["id"])
        item_type = str(item["type"])
        if item_id in inherited or item_type in MANAGED_CHILD_FOLDERS:
            continue
        destination = target_folder(ITEM_FOLDERS[item_type])
        if item.get("folderId") == destination and item_type not in _PARENT_ORDER:
            continue
        response = session.post(
            f"{base}/items/{item_id}/move",
            json={"targetFolderId": destination},
            timeout=30,
            allow_redirects=False,
        )
        if (
            response.status_code == 400
            and response.json().get("errorCode") == "CannotMoveChildOnly"
        ):
            current = item.get("folderId")
            type_folder_ids = {
                value for name, value in folder_ids.items() if name in ITEM_FOLDERS.values()
            }
            if current not in type_folder_ids:
                raise WorkspaceFolderError(
                    f"Managed child {item['displayName']} is not in a type folder."
                )
            inherited[item_id] = str(current)
            console.warn(
                f"{item_type}:{item['displayName']} must remain with its parent."
            )
            continue
        response.raise_for_status()
        if response.status_code != 200:
            raise WorkspaceFolderError("Fabric returned an unexpected item-move status.")
        changed = response.json().get("value")
        if not isinstance(changed, list) or not changed:
            raise WorkspaceFolderError("Fabric item move returned no item identities.")
        changed_ids = set()
        for child in changed:
            child_id = _guid(child["id"])
            if child_id not in original_ids or child.get("folderId") != destination:
                raise WorkspaceFolderError("Fabric item move changed an unexpected target.")
            changed_ids.add(child_id)
            if child_id != item_id:
                inherited[child_id] = destination
            for observed in items:
                if observed["id"] == child_id:
                    observed["folderId"] = destination
        if item_id not in changed_ids:
            raise WorkspaceFolderError("Fabric item move omitted the requested parent.")
        moved.append(
            {
                "id": item_id,
                "name": item["displayName"],
                "type": item_type,
                "folder": ITEM_FOLDERS[item_type],
                "child_ids": sorted(changed_ids - {item_id}),
            }
        )

    final_folders, final_items = _inventory(session, base)
    names = {folder["id"]: str(folder["displayName"]) for folder in final_folders}
    if {_guid(item["id"]) for item in final_items} != original_ids:
        raise WorkspaceFolderError("Workspace item identities changed during organization.")
    errors = []
    for item in final_items:
        item_id, item_type = str(item["id"]), str(item["type"])
        actual = names.get(item.get("folderId"), "[workspace root]")
        if item_id in inherited:
            valid = item.get("folderId") == inherited[item_id]
        elif item_type in MANAGED_CHILD_FOLDERS:
            valid = actual in MANAGED_CHILD_FOLDERS[item_type]
        else:
            valid = actual == ITEM_FOLDERS[item_type]
        if not valid:
            errors.append(f"{item_type}:{item['displayName']} is in {actual}")
    if errors:
        raise WorkspaceFolderError("; ".join(errors))

    deleted, retained = [], []
    if cleanup_legacy:
        for folder in final_folders:
            if folder["displayName"] not in LEGACY_FOLDERS or folder.get("parentFolderId"):
                continue
            occupied = any(
                item.get("folderId") == folder["id"] for item in final_items
            ) or any(
                child.get("parentFolderId") == folder["id"] for child in final_folders
            )
            if occupied:
                retained.append(folder["displayName"])
                console.warn(f"Retained nonempty legacy folder {folder['displayName']}.")
                continue
            response = session.delete(
                f"{base}/folders/{_guid(folder['id'])}",
                timeout=30,
                allow_redirects=False,
            )
            response.raise_for_status()
            if response.status_code != 200:
                raise WorkspaceFolderError("Unexpected empty-folder deletion response.")
            deleted.append(folder["displayName"])
    return {
        "workspace_id": workspace_id,
        "observed_at": datetime.now(UTC).isoformat(),
        "status": "SUCCEEDED",
        "item_count": len(final_items),
        "moved": moved,
        "managed_children": [
            {"id": item["id"], "name": item["displayName"], "folder": names[item["folderId"]]}
            for item in final_items
            if item["id"] in inherited or item["type"] in MANAGED_CHILD_FOLDERS
        ],
        "folder_counts": dict(Counter(names[item["folderId"]] for item in final_items)),
        "deleted_empty_legacy_folders": deleted,
        "retained_legacy_folders": retained,
    }


def organize_environment(repo_root: Path, environment: str) -> dict[str, Any]:
    """Resolve and validate the configured target before organizing its metadata."""

    from deploy.scripts.deploy_config import (
        load_environment,
        load_terraform_outputs,
        validate_terraform_outputs,
    )
    from deploy.scripts.export_items import build_session

    config = load_environment(
        environment,
        config_path=repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=repo_root / "deploy" / "config" / "environments",
    )
    generated = repo_root / "deploy" / ".generated" / environment
    outputs = load_terraform_outputs(generated / "terraform-output.json")
    validate_terraform_outputs(config, outputs)
    session = build_session(auth_mode=config.auth_mode, tenant_id=config.tenant_id)
    workspace_id = _guid(outputs["workspace_id"])
    response = retry_read(
        lambda: session.get(f"{API}/workspaces/{workspace_id}", timeout=30)
    )
    response.raise_for_status()
    if response.json().get("displayName") != config.workspace.name:
        raise WorkspaceFolderError("Live workspace differs from the configured target.")
    report = organize_workspace(session, workspace_id)
    (generated / "workspace-folder-report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--environment", required=True)
    args = parser.parse_args()
    try:
        report = organize_environment(args.repo_root, args.environment)
    except (
        OSError, ValueError, requests.RequestException,
        WorkspaceFolderError, NativeItemError,
    ) as exc:
        console.error(f"Workspace folder organization failed: {exc}")
        return 1
    console.info(f"Organized {report['item_count']} items into object-type folders.")
    return 0


def organize_after_publish(repo_root: Path, environment: str) -> bool:
    """Report publication separately from a failed folder reconciliation."""

    try:
        report = organize_environment(repo_root, environment)
    except (
        OSError, ValueError, requests.RequestException,
        WorkspaceFolderError, NativeItemError,
    ) as exc:
        console.error(f"Items published, but workspace folder organization failed: {exc}")
        return False
    console.info(f"Verified object-type folders for {report['item_count']} items.")
    return True


if __name__ == "__main__":
    raise SystemExit(main())
