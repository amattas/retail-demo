"""Bind the real-time Spark pool to its Fabric Environment and publish it.

Terraform creates the ``retail_realtime`` Environment item, but the
``fabric_environment`` resource cannot set the Environment's Spark compute (the
custom pool binding) or publish it. This script performs those two steps via the
Fabric REST API after ``terraform apply`` -- the same post-apply pattern used by
``apply_kql`` for KQL schema.

A Fabric notebook cannot attach to a bare custom Spark pool directly; it attaches
to the workspace default pool or to an Environment. Binding the secondary pool to
this Environment lets the clickstream-generator notebook run on the 6-node pool
(valid on small capacities such as F8) without changing the workspace default.

Endpoints (under the operator credential):

    PATCH /v1/workspaces/{ws}/environments/{env}/staging/sparkcompute
          body = {"instancePool": {"name": "<pool>", "type": "Workspace"}}
    POST  /v1/workspaces/{ws}/environments/{env}/staging/publish
          -> publishDetails.state in {Waiting, Running, Success, Failed, ...}
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES, build_credential

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

REPO_ROOT = Path(__file__).resolve().parents[2]
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_API = "https://api.fabric.microsoft.com/v1"

_TERMINAL_SUCCESS = {"success"}
_TERMINAL_FAILURE = {"failed", "cancelled"}


def _terraform_outputs(environment: str) -> dict[str, Any]:
    from deploy.scripts.deploy_config import load_terraform_outputs

    path = REPO_ROOT / "deploy" / ".generated" / environment / "terraform-output.json"
    if not path.exists():
        raise SystemExit(
            f"Terraform outputs not found: {path}\n"
            "Run a full deploy first (the Terraform steps write this file), or "
            "pass --workspace-id, --environment-id, and --pool-name."
        )
    return load_terraform_outputs(path)


def _headers(credential: TokenCredential) -> dict[str, str]:
    token = credential.get_token(FABRIC_SCOPE).token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def bind_pool(
    *,
    workspace_id: str,
    environment_id: str,
    pool_name: str,
    credential: TokenCredential,
) -> None:
    """Point the Environment's staging Spark compute at the custom pool."""

    import requests

    url = (
        f"{FABRIC_API}/workspaces/{workspace_id}"
        f"/environments/{environment_id}/staging/sparkcompute"
    )
    body = {"instancePool": {"name": pool_name, "type": "Workspace"}}
    resp = requests.patch(url, headers=_headers(credential), json=body, timeout=60)
    resp.raise_for_status()
    bound = resp.json().get("instancePool", {})
    console.info(
        f"Bound environment {environment_id} to pool "
        f"'{bound.get('name', pool_name)}' ({bound.get('id', 'id n/a')})."
    )


def publish(
    *,
    workspace_id: str,
    environment_id: str,
    credential: TokenCredential,
    poll_interval: float = 10.0,
    timeout: float = 900.0,
) -> str:
    """Publish the Environment's staging settings and wait for a terminal state."""

    import requests

    base = f"{FABRIC_API}/workspaces/{workspace_id}/environments/{environment_id}"
    resp = requests.post(
        f"{base}/staging/publish", headers=_headers(credential), timeout=60
    )
    resp.raise_for_status()
    state = _publish_state(resp.json())
    if state in _TERMINAL_SUCCESS:
        console.info("Environment publish succeeded.")
        return "Success"

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if state in _TERMINAL_FAILURE:
            raise RuntimeError(f"Environment publish ended in state '{state}'.")
        console.detail(f"Publish state: {state or 'unknown'}; polling...")
        time.sleep(poll_interval)
        poll = requests.get(base, headers=_headers(credential), timeout=60)
        poll.raise_for_status()
        state = _publish_state(poll.json().get("properties", {}))
        if state in _TERMINAL_SUCCESS:
            console.info("Environment publish succeeded.")
            return "Success"
    raise TimeoutError(
        f"Environment publish did not complete within {timeout:.0f}s "
        f"(last state: {state or 'unknown'})."
    )


def _publish_state(payload: dict[str, Any]) -> str:
    """Extract the lowercased publish state from a publish or item payload."""

    details = payload.get("publishDetails") or payload.get("publish_details") or {}
    return str(details.get("state", "")).lower()


def configure(
    *,
    workspace_id: str,
    environment_id: str,
    pool_name: str,
    auth_mode: str = "azure_cli",
    credential: TokenCredential | None = None,
) -> int:
    """Bind the pool and publish the Environment. Returns 0 on success."""

    credential = credential or build_credential(auth_mode)
    bind_pool(
        workspace_id=workspace_id,
        environment_id=environment_id,
        pool_name=pool_name,
        credential=credential,
    )
    publish(
        workspace_id=workspace_id,
        environment_id=environment_id,
        credential=credential,
    )
    return 0


def main() -> int:
    """Bind the real-time pool to its Environment and publish it."""

    parser = argparse.ArgumentParser(
        description="Bind the real-time Spark pool to its Fabric Environment"
    )
    parser.add_argument(
        "--environment",
        help="Read ids/names from deploy/.generated/<env>/terraform-output.json.",
    )
    parser.add_argument("--workspace-id", help="Fabric workspace id.")
    parser.add_argument("--environment-id", help="Fabric Environment item id.")
    parser.add_argument("--pool-name", help="Custom Spark pool display name.")
    parser.add_argument(
        "--auth-mode",
        choices=AUTH_MODES,
        default="azure_cli",
        help="Operator credential used for Fabric requests.",
    )
    args = parser.parse_args()

    workspace_id = args.workspace_id
    environment_id = args.environment_id
    pool_name = args.pool_name
    if (not workspace_id or not environment_id or not pool_name) and args.environment:
        outputs = _terraform_outputs(args.environment)
        workspace_id = workspace_id or outputs.get("workspace_id")
        environment_id = environment_id or outputs.get("spark_realtime_environment_id")
        pool_name = pool_name or outputs.get("spark_realtime_pool_name")

    # The real-time pool/environment is opt-in. When disabled, the Terraform
    # outputs are null; skip cleanly so the step is a no-op in that case.
    if not environment_id or not pool_name:
        console.info(
            "Real-time environment not enabled (no environment id / pool name); "
            "skipping pool binding."
        )
        return 0
    if not workspace_id:
        raise SystemExit(
            "Configuring the real-time environment requires --workspace-id, or "
            "--environment with generated Terraform outputs."
        )

    return configure(
        workspace_id=str(workspace_id),
        environment_id=str(environment_id),
        pool_name=str(pool_name),
        auth_mode=args.auth_mode,
    )


if __name__ == "__main__":
    raise SystemExit(main())
