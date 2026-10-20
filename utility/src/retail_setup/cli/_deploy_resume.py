"""Resume identity and destructive Terraform plan guards."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from retail_setup.cli._deploy_journal import DeployJournal


def recreate_mode(previous: DeployJournal) -> bool:
    """Preserve recreate identity only after destruction and deletion wait succeed."""

    if previous.targets.get("recreate") != "True":
        return False

    steps = {step.step_id: step for step in previous.steps}
    for step_id in ("terraform-destroy-recreate", "deletion-wait"):
        step = steps.get(step_id)
        if step is None or step.status != "SUCCEEDED":
            raise ValueError(
                "Cannot resume an unfinished destructive recreate phase; "
                "review the workspace and explicitly restart with --recreate."
            )
    return True


def _fingerprint_file_hashes(
    repo_root: Path,
    env: str,
    generation_path: Path,
) -> dict[str, str]:
    """Hash the same non-secret file set used by deployment identity."""

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
            name
            for name in directories
            if name not in {"node_modules", "dist", ".temp", ".vite"}
        ]
        inputs.update(
            Path(current) / name
            for name in files
            if not name.startswith(".env") and name != ".deployments.json"
        )

    return {
        path.relative_to(repo_root).as_posix(): (
            hashlib.sha256(path.read_bytes()).hexdigest()
            if path.is_file()
            else hashlib.sha256(b"missing").hexdigest()
        )
        for path in sorted(inputs)
    }


def fingerprint_details(
    repo_root: Path,
    env: str,
    generation_path: Path,
    commands: list[list[str]],
) -> dict[str, Any]:
    """Capture per-file hashes so adopted-run recovery can bound source drift."""

    return fingerprint_record(repo_root, env, generation_path, commands)[1]


def fingerprint_record(
    repo_root: Path,
    env: str,
    generation_path: Path,
    commands: list[list[str]],
) -> tuple[str, dict[str, Any]]:
    """Compute aggregate and per-file identities from one source snapshot."""

    command_json = json.dumps(commands).encode("utf-8")
    files = _fingerprint_file_hashes(repo_root, env, generation_path)
    details = {
        "commands_sha256": hashlib.sha256(command_json).hexdigest(),
        "files": files,
    }
    digest = hashlib.sha256(command_json)
    for relative, content_hash in files.items():
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(content_hash))
    return digest.hexdigest(), details


def changed_fingerprint_inputs(previous: Any, current: Any) -> set[str]:
    """Identify exact changed paths; aggregate-only legacy journals fail closed."""

    if not isinstance(previous, dict) or not isinstance(current, dict):
        raise ValueError("The prior journal has no per-file fingerprints.")
    old_files, new_files = previous.get("files"), current.get("files")
    old_commands, new_commands = (
        previous.get("commands_sha256"),
        current.get("commands_sha256"),
    )
    if (
        not isinstance(old_files, dict)
        or not isinstance(new_files, dict)
        or not isinstance(old_commands, str)
        or not isinstance(new_commands, str)
    ):
        raise ValueError("The prior journal has no per-file fingerprints.")
    changed = {
        str(path)
        for path in set(old_files) | set(new_files)
        if old_files.get(path) != new_files.get(path)
    }
    if old_commands != new_commands:
        changed.add("<deployment commands>")
    return changed


def fingerprint(
    repo_root: Path,
    env: str,
    generation_path: Path,
    commands: list[list[str]],
) -> str:
    """Hash configuration, ordered commands and deployment source, not secrets."""

    return fingerprint_record(repo_root, env, generation_path, commands)[0]


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
