"""Content identity for safely reusing successful deployment steps."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from retail_setup.cli._deploy_journal import DeployJournal


def recreate_mode(previous: DeployJournal) -> bool:
    """Preserve recreate identity only after every destructive step succeeded."""

    if previous.targets.get("recreate") != "True":
        return False
    for step in previous.steps:
        if step.status != "SUCCEEDED":
            raise ValueError(
                "Cannot resume an unfinished destructive recreate phase; "
                "review the workspace and explicitly restart with --recreate."
            )
        if step.step_id == "deletion-wait":
            return True
    raise ValueError("Recreate journal has no completed deletion-wait step")


def fingerprint(
    repo_root: Path,
    env: str,
    generation_path: Path,
    commands: list[list[str]],
) -> str:
    """Hash configuration, ordered commands and deployment source, not secrets."""

    inputs = {
        repo_root / "deploy" / "config" / "deploy.yml",
        repo_root / "deploy" / "config" / "environments" / f"{env}.yml",
        generation_path,
        repo_root / "utility" / "pyproject.toml",
        repo_root / "utility" / "requirements-ci.txt",
    }
    for relative in (
        "contracts",
        "deploy/scripts",
        "deploy/terraform",
        "fabric",
        "utility/src",
        "utility/notebooks",
        "utility/data/dictionaries",
    ):
        directory = repo_root.joinpath(*relative.split("/"))
        inputs.update(
            path
            for path in directory.rglob("*")
            if path.is_file()
            and not any(
                part in {"__pycache__", ".terraform", ".pbi"}
                for part in path.relative_to(directory).parts
            )
            and path.suffix not in {".pyc", ".tfstate", ".tfvars", ".tfplan"}
        )
    app_root = repo_root / "app" / "fabric-app"
    for current, directories, files in os.walk(app_root):
        directories[:] = [
            name for name in directories
            if name not in {"node_modules", "dist", ".temp", ".vite"}
        ]
        inputs.update(
            Path(current) / name
            for name in files
            if not name.startswith(".env") and name != ".deployments.json"
        )
    digest = hashlib.sha256(json.dumps(commands).encode("utf-8"))
    for path in sorted(inputs):
        digest.update(str(path.relative_to(repo_root)).encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest() if path.is_file() else b"missing")
    return digest.hexdigest()


def validate_plan_changes(payload: Any) -> list[str]:
    """Return data-bearing resources slated for deletion in a Terraform plan."""

    if not isinstance(payload, dict) or not payload.get("format_version"):
        raise ValueError("Terraform returned an invalid JSON plan")
    changes = payload.get("resource_changes", [])
    if not isinstance(changes, list):
        raise ValueError("Terraform resource_changes must be an array")
    protected = {"fabric_workspace", "fabric_lakehouse", "fabric_eventhouse"}
    destructive = []
    for change in changes:
        if not isinstance(change, dict) or not isinstance(change.get("change"), dict):
            raise ValueError("Invalid Terraform resource change")
        actions = change["change"].get("actions")
        if not isinstance(actions, list) or not actions:
            raise ValueError("Missing Terraform resource actions")
        resource_type = change.get("type")
        address = change.get("address")
        if not isinstance(resource_type, str) or not isinstance(address, str):
            raise ValueError("Missing Terraform resource identity")
        if "delete" in actions and resource_type in protected:
            destructive.append(address)
    return sorted(destructive)
