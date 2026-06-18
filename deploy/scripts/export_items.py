"""Export Fabric items of a given type from a workspace into item folders.

Fetches each item definition from a live Fabric workspace via the
``getDefinition`` REST API and writes it as a source-control item folder
(``<name>.<ItemType>/`` containing ``.platform`` and the returned definition
parts, e.g. ``pipeline-content.json`` for pipelines or ``Files/Config/...`` for
data agents). Authentication uses the Azure CLI login (``AzureCliCredential``),
matching the ``azure_cli`` auth mode used by the rest of the deployment
framework.

Example:
    python -m deploy.scripts.export_items \
        --workspace-name "Retail Demo" --item-type DataAgent \
        --output-dir fabric/data-agents
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
# Parts that should be pretty-printed when they contain JSON.
_JSON_PART_SUFFIXES = (".json",)
_JSON_PART_NAMES = {".platform", ".schedules"}


def build_session(credential: AzureCliCredential | None = None) -> requests.Session:
    """Create an authenticated requests session for the Fabric REST API.

    Uses a generous credential process timeout and retries transient cold-``az``
    token failures (the deploy's pipeline trigger runs this right after a long
    Terraform/publish step, when the token cache may have lapsed).
    """

    import requests
    from azure.core.exceptions import ClientAuthenticationError
    from azure.identity import AzureCliCredential

    from deploy.scripts._retry import retry_call

    credential = credential or AzureCliCredential(process_timeout=120)
    token = retry_call(
        lambda: credential.get_token(FABRIC_SCOPE).token,
        retry_on=(ClientAuthenticationError,),
    )
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


def list_items(
    session: requests.Session, workspace_id: str, item_type: str
) -> list[dict[str, Any]]:
    """List items of a given type in a workspace, sorted by display name."""

    response = session.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/items", params={"type": item_type}
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


def write_item(
    output_dir: Path, display_name: str, item_type: str, definition: dict[str, Any]
) -> Path:
    """Write a fetched definition as a ``<name>.<ItemType>`` source item folder."""

    item_dir = output_dir / f"{display_name}.{item_type}"
    item_dir.mkdir(parents=True, exist_ok=True)
    for part in definition["parts"]:
        decoded = base64.b64decode(part["payload"])
        part_path = item_dir / part["path"]
        part_path.parent.mkdir(parents=True, exist_ok=True)
        # Re-serialize JSON parts with stable indentation; keep others verbatim.
        name = Path(part["path"]).name
        if part["path"].endswith(_JSON_PART_SUFFIXES) or name in _JSON_PART_NAMES:
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


def export_items(
    workspace_name: str,
    item_type: str,
    output_dir: Path,
    credential: AzureCliCredential | None = None,
) -> list[Path]:
    """Export all items of ``item_type`` from a workspace into item folders."""

    session = build_session(credential)
    workspace_id = find_workspace_id(session, workspace_name)
    written: list[Path] = []
    for item in list_items(session, workspace_id, item_type):
        definition = get_definition(session, workspace_id, str(item["id"]))
        written.append(
            write_item(output_dir, str(item["displayName"]), item_type, definition)
        )
    return written


def main() -> int:
    """Export Fabric items of a type into source-control item folders."""

    parser = argparse.ArgumentParser(
        description="Export Fabric items of a type into fabric-cicd item folders"
    )
    parser.add_argument(
        "--workspace-name",
        required=True,
        help='Source workspace display name, e.g. "Retail Demo".',
    )
    parser.add_argument(
        "--item-type",
        required=True,
        help="Fabric item type to export, e.g. DataPipeline or DataAgent.",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    written = export_items(args.workspace_name, args.item_type, args.output_dir)
    print(f"Exported {len(written)} {args.item_type} item(s) to {args.output_dir}")
    for item in written:
        print(f"  {item.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
