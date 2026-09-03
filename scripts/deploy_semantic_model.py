#!/usr/bin/env python3
"""Deploy the repo's semantic model (and optionally the report) to Fabric.

Replaces the workspace item definition wholesale via the Fabric REST API
(updateDefinition). Use this instead of Power BI Desktop's live-edit save when
the server-side model has drifted from the repo TMDL (e.g. stale columns that
no longer exist in the Delta tables) — Desktop's incremental save validates the
whole transaction against the stale server model and rolls back.

Usage:
    # Deploy the semantic model definition (browser login)
    python scripts/deploy_semantic_model.py

    # Also deploy the report definition
    python scripts/deploy_semantic_model.py --include-report

    # Deploy and trigger a model refresh afterwards
    python scripts/deploy_semantic_model.py --refresh

    # Preview the parts that would be uploaded
    python scripts/deploy_semantic_model.py --dry-run

Requirements:
    pip install --require-hashes -r utility/requirements-deploy.txt
"""

from __future__ import annotations

import argparse
import base64
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
POWERBI_API = "https://api.powerbi.com/v1.0/myorg"
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

MODEL_DIR = REPO_ROOT / "fabric" / "powerbi" / "retail_model.SemanticModel"
REPORT_DIR = REPO_ROOT / "fabric" / "powerbi" / "retail_model.Report"

# Local-only files that are not part of the Fabric item definition
EXCLUDED_NAMES = {".platform"}
EXCLUDED_DIRS = {".pbi"}
EXCLUDED_MODEL_FILES = {"diagramLayout.json"}


def get_credential():
    """Interactive browser credential with persistent token cache."""
    try:
        from azure.identity import (
            InteractiveBrowserCredential,
            TokenCachePersistenceOptions,
        )
    except ImportError:
        print(
            "ERROR: deployment dependencies are not installed. Run: "
            "pip install --require-hashes -r utility/requirements-deploy.txt"
        )
        sys.exit(1)

    import tempfile

    _ = Path(tempfile.gettempdir())  # parity with deploy_notebooks.py cache location
    return InteractiveBrowserCredential(
        additionally_allowed_tenants=["*"],
        cache_persistence_options=TokenCachePersistenceOptions(
            name="fabric_deploy_cache",
            allow_unencrypted_storage=True,
        ),
    )


def api_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def read_workspace_id_from_tmdl() -> str:
    import re

    content = (MODEL_DIR / "definition" / "expressions.tmdl").read_text(encoding="utf-8")
    match = re.search(
        r"onelake\.dfs\.fabric\.microsoft\.com/([0-9a-f-]{36})/[0-9a-f-]{36}", content
    )
    if not match:
        raise ValueError("Could not extract workspace ID from expressions.tmdl")
    return match.group(1)


def list_items(token: str, workspace_id: str, item_type: str) -> dict[str, str]:
    import requests

    items: dict[str, str] = {}
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items?type={item_type}"
    while url:
        resp = requests.get(url, headers=api_headers(token))
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("value", []):
            items[item["displayName"]] = item["id"]
        url = data.get("continuationUri")
    return items


def find_item(token: str, workspace_id: str, item_type: str, display_name: str) -> str:
    items = list_items(token, workspace_id, item_type)
    if display_name in items:
        return items[display_name]
    raise RuntimeError(
        f"{item_type} named {display_name!r} not found in workspace. "
        f"Available: {sorted(items) or 'none'}"
    )


def rebind_pbir_to_model(parts: list[dict], model_id: str) -> None:
    """Rewrite definition.pbir to reference the deployed model by id.

    The repo uses a byPath reference (resolved by Desktop/Git integration);
    REST-deployed definitions must use byConnection instead. The shape below
    mirrors microsoft/fabric-cicd: the legacy byConnection properties are only
    valid against the 1.0.0 definitionProperties schema, so the $schema is
    pinned to 1.0.0 here regardless of what the repo file declares.
    """
    import json as _json

    for part in parts:
        if part["path"] == "definition.pbir":
            pbir = _json.loads(base64.b64decode(part["payload"]))
            pbir["$schema"] = (
                "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/1.0.0/schema.json"
            )
            pbir["datasetReference"] = {
                "byConnection": {
                    "connectionString": None,
                    "pbiServiceModelId": None,
                    "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                    "pbiModelDatabaseName": f"{model_id}",
                    "name": "EntityDataSource",
                    "connectionType": "pbiServiceXmlaStyleLive",
                }
            }
            part["payload"] = base64.b64encode(
                _json.dumps(pbir, indent=4).encode("utf-8")
            ).decode("ascii")
            return
    raise RuntimeError("definition.pbir part not found")


def create_report(
    token: str, workspace_id: str, display_name: str, parts: list[dict]
) -> str:
    import requests

    body = {"displayName": display_name, "definition": {"parts": parts}}
    url = f"{FABRIC_API}/workspaces/{workspace_id}/reports"
    resp = requests.post(url, headers=api_headers(token), json=body)
    if resp.status_code == 202:
        op_url = resp.headers.get("Location") or resp.headers.get("Operation-Location")
        if op_url:
            poll_operation(token, op_url)
    elif resp.status_code not in (200, 201):
        raise RuntimeError(f"status {resp.status_code}: {resp.text[:500]}")
    return find_item(token, workspace_id, "Report", display_name)


def collect_parts(item_dir: Path, extra_excluded: set[str]) -> list[dict]:
    """Collect definition parts (path + base64 payload) for an item folder."""
    parts: list[dict] = []
    for path in sorted(item_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(item_dir)
        if rel.parts[0] in EXCLUDED_DIRS or path.name in EXCLUDED_NAMES:
            continue
        if str(rel) in extra_excluded:
            continue
        parts.append(
            {
                "path": rel.as_posix(),
                "payload": base64.b64encode(path.read_bytes()).decode("ascii"),
                "payloadType": "InlineBase64",
            }
        )
    return parts


def poll_operation(token: str, url: str, timeout: int = 300) -> None:
    import requests

    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(3)
        resp = requests.get(url, headers=api_headers(token))
        if resp.status_code == 200:
            data = resp.json()
            status = data.get("status", "Unknown")
            if status in ("Succeeded", "Completed"):
                return
            if status in ("Failed", "Cancelled"):
                raise RuntimeError(f"Operation {status}: {data.get('error', data)}")
        elif resp.status_code == 202:
            continue
    raise RuntimeError("Operation timed out")


def update_definition(
    token: str,
    workspace_id: str,
    item_id: str,
    parts: list[dict],
    endpoint: str,
) -> None:
    import requests

    url = f"{FABRIC_API}/workspaces/{workspace_id}/{endpoint}/{item_id}/updateDefinition"
    resp = requests.post(url, headers=api_headers(token), json={"definition": {"parts": parts}})
    if resp.status_code == 202:
        op_url = resp.headers.get("Location") or resp.headers.get("Operation-Location")
        if op_url:
            poll_operation(token, op_url)
    elif resp.status_code not in (200, 204):
        raise RuntimeError(f"status {resp.status_code}: {resp.text[:500]}")


def trigger_refresh(credential, workspace_id: str, model_id: str) -> None:
    import requests

    token = credential.get_token(POWERBI_SCOPE).token
    url = f"{POWERBI_API}/groups/{workspace_id}/datasets/{model_id}/refreshes"
    resp = requests.post(url, headers=api_headers(token), json={"type": "full"})
    if resp.status_code in (200, 202):
        print("  [OK] refresh requested (watch progress in the workspace)")
    else:
        raise RuntimeError(f"refresh failed, status {resp.status_code}: {resp.text[:300]}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deploy the semantic model (and optionally the report) to Fabric",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--workspace-id", help="Workspace GUID (default: from expressions.tmdl)")
    parser.add_argument("--model-name", default="retail_model")
    parser.add_argument("--report-name", default="retail_model")
    parser.add_argument("--include-report", action="store_true", help="Also deploy the report")
    parser.add_argument("--refresh", action="store_true", help="Trigger a model refresh after deploy")
    parser.add_argument("--dry-run", action="store_true", help="List parts without uploading")
    args = parser.parse_args()

    workspace_id = args.workspace_id or read_workspace_id_from_tmdl()
    print(f"Workspace: {workspace_id}")

    model_parts = collect_parts(MODEL_DIR, EXCLUDED_MODEL_FILES)
    print(f"\nSemantic model parts ({len(model_parts)}):")
    for p in model_parts:
        print(f"  {p['path']}")

    report_parts: list[dict] = []
    if args.include_report:
        report_parts = collect_parts(REPORT_DIR, set())
        print(f"\nReport parts ({len(report_parts)}):")
        for p in report_parts:
            print(f"  {p['path']}")

    if args.dry_run:
        print("\n[DRY RUN] Nothing uploaded.")
        return 0

    print("\nOpening browser for authentication...")
    credential = get_credential()
    token = credential.get_token(FABRIC_SCOPE).token
    print("[OK] Authenticated")

    model_id = find_item(token, workspace_id, "SemanticModel", args.model_name)
    print(f"\nDeploying semantic model {args.model_name!r} ({model_id})...")
    update_definition(token, workspace_id, model_id, model_parts, "semanticModels")
    print("  [OK] semantic model definition replaced")

    if args.include_report:
        rebind_pbir_to_model(report_parts, model_id)
        reports = list_items(token, workspace_id, "Report")
        if args.report_name in reports:
            report_id = reports[args.report_name]
            print(f"\nDeploying report {args.report_name!r} ({report_id})...")
            update_definition(token, workspace_id, report_id, report_parts, "reports")
            print("  [OK] report definition replaced")
        else:
            print(
                f"\nReport {args.report_name!r} not found "
                f"(existing reports: {sorted(reports) or 'none'}) - creating it..."
            )
            report_id = create_report(token, workspace_id, args.report_name, report_parts)
            print(f"  [OK] report created ({report_id})")

    if args.refresh:
        print(f"\nTriggering refresh of {args.model_name!r}...")
        trigger_refresh(credential, workspace_id, model_id)

    print("\nDone!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
