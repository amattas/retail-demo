"""Validate captured Fabric target IDs and operator access before publication."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES
from deploy.scripts.deploy_config import (
    load_environment,
    load_terraform_outputs,
)
from deploy.scripts.export_items import FABRIC_API, build_session

if TYPE_CHECKING:
    import requests

    from deploy.scripts.deploy_config import DeployConfig

REPO_ROOT = Path(__file__).resolve().parents[2]


class TargetAccessError(RuntimeError):
    """Raised when captured IDs cannot be proven against the live target."""


@dataclass(frozen=True)
class TargetAccessReport:
    """Validated live workspace target."""

    workspace_id: str
    workspace_name: str


def validate_target_access(
    session: requests.Session,
    config: DeployConfig,
    outputs: dict[str, Any],
) -> TargetAccessReport:
    """Require captured workspace identity and operator read access to agree."""

    workspace_id = str(outputs["workspace_id"])
    response = session.get(f"{FABRIC_API}/workspaces/{workspace_id}")
    try:
        response.raise_for_status()
    except Exception as exc:
        raise TargetAccessError(
            f"Captured workspace_id {workspace_id} is not readable by the "
            "deployment operator. Recapture Terraform outputs from the matching "
            "environment state and ensure the operator has a workspace role."
        ) from exc

    payload = response.json()
    if not isinstance(payload, dict):
        raise TargetAccessError("Fabric returned a non-object workspace response.")
    live_id = str(payload.get("id", ""))
    live_name = str(payload.get("displayName", ""))
    if live_id.casefold() != workspace_id.casefold():
        raise TargetAccessError(
            f"Fabric returned workspace id {live_id!r}, expected {workspace_id!r}."
        )
    if live_name != config.workspace.name:
        raise TargetAccessError(
            f"Captured workspace {workspace_id} is named {live_name!r}, expected "
            f"{config.workspace.name!r}. Recapture outputs from the correct state."
        )
    return TargetAccessReport(
        workspace_id=workspace_id,
        workspace_name=live_name,
    )


def main() -> int:
    """Validate one environment's captured workspace target."""

    parser = argparse.ArgumentParser(
        description="Validate captured Fabric target access"
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--auth-mode", choices=AUTH_MODES, default="azure_cli")
    parser.add_argument("--tenant-id")
    args = parser.parse_args()

    config_root = args.repo_root / "deploy" / "config"
    config = load_environment(
        args.environment,
        config_path=config_root / "deploy.yml",
        environments_root=config_root / "environments",
    )
    output_path = (
        args.repo_root
        / "deploy"
        / ".generated"
        / args.environment
        / "terraform-output.json"
    )
    try:
        outputs = load_terraform_outputs(output_path)
        session = build_session(
            auth_mode=args.auth_mode,
            tenant_id=args.tenant_id,
        )
        report = validate_target_access(session, config, outputs)
    except (TargetAccessError, FileNotFoundError, KeyError, ValueError) as exc:
        console.error(str(exc))
        return 1

    console.info(
        f"Captured workspace access validated: {report.workspace_name} "
        f"({report.workspace_id})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
