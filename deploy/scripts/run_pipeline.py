"""Run a named Fabric Data Pipeline on demand via the Job Scheduler API.

Resolves the workspace from the captured Terraform outputs, finds the pipeline
item by display name, and starts an on-demand run:

    POST {FABRIC_API}/workspaces/{ws}/items/{pipelineId}/jobs/instances?jobType=Pipeline

Auth reuses the Azure CLI login (see ``export_items.build_session``).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TYPE_CHECKING

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES
from deploy.scripts.export_items import build_session, list_items

if TYPE_CHECKING:
    import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"
REPO_ROOT = Path(__file__).resolve().parents[2]


def workspace_id_from_outputs(environment: str, repo_root: Path = REPO_ROOT) -> str:
    """Read the workspace id from the captured Terraform outputs for an environment."""

    path = repo_root / "deploy" / ".generated" / environment / "terraform-output.json"
    if not path.is_file():
        raise FileNotFoundError(
            f"Terraform outputs not found: {path}. Run `retail-setup deploy` first."
        )
    outputs = json.loads(path.read_text(encoding="utf-8"))
    value = outputs.get("workspace_id")
    workspace = value.get("value") if isinstance(value, dict) else value
    if not workspace:
        raise ValueError(f"workspace_id missing from {path}")
    return str(workspace)


def find_pipeline_id(
    session: requests.Session, workspace_id: str, pipeline_name: str
) -> str:
    """Resolve a DataPipeline display name to its item id."""

    for item in list_items(session, workspace_id, "DataPipeline"):
        if str(item.get("displayName", "")) == pipeline_name:
            return str(item["id"])
    raise ValueError(
        f"Pipeline {pipeline_name!r} not found in workspace {workspace_id}. "
        "Deploy it first (it is staged with the `setup` notebook group)."
    )


def run_pipeline(
    session: requests.Session, workspace_id: str, pipeline_id: str
) -> str | None:
    """Start an on-demand pipeline run; returns the job-instance URL if provided."""

    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances"
    response = session.post(url, params={"jobType": "Pipeline"})
    response.raise_for_status()
    return response.headers.get("Location")


def main() -> int:
    """Start an on-demand run of a named Fabric pipeline."""

    parser = argparse.ArgumentParser(description="Run a Fabric Data Pipeline on demand")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--pipeline", required=True, help="Pipeline display name.")
    parser.add_argument(
        "--auth-mode",
        choices=AUTH_MODES,
        default="azure_cli",
        help="Operator credential used for Fabric REST requests.",
    )
    args = parser.parse_args()

    workspace_id = workspace_id_from_outputs(args.environment)
    session = build_session(auth_mode=args.auth_mode)
    pipeline_id = find_pipeline_id(session, workspace_id, args.pipeline)
    location = run_pipeline(session, workspace_id, pipeline_id)
    console.info(f"Started pipeline run for {args.pipeline!r} ({pipeline_id}).")
    if location:
        console.detail(f"Track the run at: {location}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
