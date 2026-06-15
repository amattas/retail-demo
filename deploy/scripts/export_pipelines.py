"""Export Fabric DataPipeline items from a workspace into fabric-cicd item folders.

Fetches each Data Pipeline definition from a live Fabric workspace via the
``getDefinition`` REST API and writes it as a source-control item folder
(``<name>.DataPipeline/`` containing ``.platform``, ``pipeline-content.json`` and
any other returned parts). Authentication uses the Azure CLI login
(``AzureCliCredential``), matching the ``azure_cli`` auth mode used by the rest
of the deployment framework.

Example:
    python -m deploy.scripts.export_pipelines \
        --workspace-name "Retail Demo" --output-dir fabric/pipelines
"""

from __future__ import annotations

import argparse
import base64
import json
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import requests
    from azure.identity import AzureCliCredential

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "fabric" / "pipelines"


def build_session(credential: AzureCliCredential | None = None) -> requests.Session:
    """Create an authenticated requests session for the Fabric REST API."""

    import requests
    from azure.identity import AzureCliCredential

    credential = credential or AzureCliCredential()
    token = credential.get_token(FABRIC_SCOPE).token
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"
    return session


def find_workspace_id(session: requests.Session, workspace_name: str) -> str:
    """Resolve a workspace display name to its id (case-insensitive)."""

    response = session.get(f"{FABRIC_API}/workspaces")
    response.raise_for_status()
    target = workspace_name.casefold()
    for workspace in response.json().get("value", []):
        if str(workspace.get("displayName", "")).casefold() == target:
            return str(workspace["id"])
    raise ValueError(f"Workspace not found: {workspace_name!r}")


def list_pipelines(session: requests.Session, workspace_id: str) -> list[dict[str, Any]]:
    """List DataPipeline items in a workspace."""

    response = session.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/items", params={"type": "DataPipeline"}
    )
    response.raise_for_status()
    return sorted(
        response.json().get("value", []),
        key=lambda item: str(item.get("displayName", "")),
    )


def get_definition(
    session: requests.Session,
    workspace_id: str,
    item_id: str,
    poll_seconds: int = 2,
    max_polls: int = 60,
) -> dict[str, Any]:
    """Fetch an item definition, transparently handling the long-running operation."""

    response = session.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/getDefinition"
    )
    if response.status_code == 200:
        return response.json()["definition"]
    if response.status_code != 202:
        response.raise_for_status()

    operation_url = response.headers["Location"]
    retry_after = int(response.headers.get("Retry-After", poll_seconds))
    for _ in range(max_polls):
        time.sleep(retry_after)
        poll = session.get(operation_url)
        poll.raise_for_status()
        status = poll.json().get("status")
        if status == "Succeeded":
            result = session.get(f"{operation_url}/result")
            result.raise_for_status()
            return result.json()["definition"]
        if status in {"Failed", "Cancelled"}:
            raise RuntimeError(f"getDefinition {status}: {poll.text}")
        retry_after = int(poll.headers.get("Retry-After", poll_seconds))
    raise TimeoutError(f"getDefinition did not complete for item {item_id}")


def write_item(output_dir: Path, display_name: str, definition: dict[str, Any]) -> Path:
    """Write a fetched definition as a ``<name>.DataPipeline`` source item folder."""

    item_dir = output_dir / f"{display_name}.DataPipeline"
    item_dir.mkdir(parents=True, exist_ok=True)
    for part in definition["parts"]:
        decoded = base64.b64decode(part["payload"])
        part_path = item_dir / part["path"]
        part_path.parent.mkdir(parents=True, exist_ok=True)
        # Re-serialize JSON parts with stable indentation; keep others verbatim.
        if part["path"].endswith(".json") or part["path"] in {".platform", ".schedules"}:
            try:
                parsed = json.loads(decoded.decode("utf-8"))
                part_path.write_text(
                    json.dumps(parsed, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8",
                )
                continue
            except (UnicodeDecodeError, json.JSONDecodeError):
                pass
        part_path.write_bytes(decoded)
    return item_dir


def export_pipelines(
    workspace_name: str,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    credential: AzureCliCredential | None = None,
) -> list[Path]:
    """Export all DataPipeline items from a workspace into item folders."""

    session = build_session(credential)
    workspace_id = find_workspace_id(session, workspace_name)
    written: list[Path] = []
    for pipeline in list_pipelines(session, workspace_id):
        definition = get_definition(session, workspace_id, str(pipeline["id"]))
        written.append(write_item(output_dir, str(pipeline["displayName"]), definition))
    return written


def main() -> int:
    """Export Fabric pipelines into source-control item folders."""

    parser = argparse.ArgumentParser(
        description="Export Fabric DataPipeline items into fabric-cicd item folders"
    )
    parser.add_argument(
        "--workspace-name",
        required=True,
        help='Source workspace display name, e.g. "Retail Demo".',
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    written = export_pipelines(args.workspace_name, args.output_dir)
    print(f"Exported {len(written)} pipeline(s) to {args.output_dir}")
    for item in written:
        print(f"  {item.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
