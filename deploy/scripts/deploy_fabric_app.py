"""Deploy the Fabric Apps approvals spike (``app/fabric-app``) with the Rayfin CLI.

Requires Node.js 20+ and the Fabric Apps (preview) tenant setting. The Rayfin
CLI keeps its own sign-in, so the first run can open a browser. Evidence goes
to ``deploy/.generated/<env>/fabric-app-report.json`` with only allow-listed
fields; publishable keys and tokens are never recorded.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from deploy.scripts import _output as console

ROOT = Path(__file__).resolve().parents[2]
APP_DIR = Path("app") / "fabric-app"
_SAFE_KEYS = ("url", "id", "status", "workspace", "name", "state")
_SECRET_KEYS = ("key", "token", "secret", "password")


def _safe(value: Any) -> Any:
    """Keep only identifying, non-secret fields from Rayfin's JSON output."""
    if isinstance(value, dict):
        kept = {}
        for key, item in value.items():
            lowered = str(key).casefold()
            if any(secret in lowered for secret in _SECRET_KEYS):
                continue
            nested = _safe(item)
            if isinstance(item, (dict, list)):
                if nested:
                    kept[key] = nested
            elif any(safe in lowered for safe in _SAFE_KEYS):
                kept[key] = nested
        return kept
    if isinstance(value, list):
        return [item for item in (_safe(entry) for entry in value) if item not in ({}, [])]
    return value


def _parse_json(text: str) -> Any:
    start = text.find("{")
    if start < 0:
        return None
    try:
        return json.loads(text[start:])
    except ValueError:
        return None


def deploy_app(
    repo_root: Path,
    *,
    workspace_id: str,
    tenant_id: str | None,
    dry_run: bool = False,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    which: Callable[[str], str | None] = shutil.which,
) -> dict[str, Any]:
    npm, npx = which("npm"), which("npx")
    if not npm or not npx:
        raise RuntimeError("Node.js 20+ (npm and npx) is required to deploy the Fabric App.")
    app_dir = repo_root / APP_DIR
    env = {**os.environ, "RAYFIN_TELEMETRY_OPTOUT": "1"}
    commands: list[Sequence[str]] = [[npm, "ci", "--no-audit", "--no-fund"]]
    up = [npx, "rayfin", "up", "--workspace-id", workspace_id, "--yes", "--json"]
    if tenant_id:
        up += ["--tenant", tenant_id]
    if dry_run:
        up.append("--dry-run")
    commands.append(up)
    result: subprocess.CompletedProcess[str] | None = None
    for command in commands:
        result = run(list(command), cwd=app_dir, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"{Path(command[0]).name} {' '.join(command[1:3])} failed "
                f"(exit {result.returncode}). Check the Fabric Apps tenant setting, "
                "region support, and Rayfin sign-in."
            )
    assert result is not None
    return {"dry_run": dry_run, "rayfin": _safe(_parse_json(result.stdout)) or {}}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    from deploy.scripts.deploy_config import (
        load_environment,
        load_terraform_outputs,
        validate_terraform_outputs,
    )

    config = load_environment(
        args.environment,
        config_path=args.repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=args.repo_root / "deploy" / "config" / "environments",
    )
    generated = args.repo_root / "deploy" / ".generated" / args.environment
    outputs = load_terraform_outputs(generated / "terraform-output.json")
    validate_terraform_outputs(config, outputs)
    report: dict[str, Any] = {
        "schema_version": 1,
        "environment": args.environment,
        "observed_at": datetime.now(UTC).isoformat(),
    }
    try:
        report |= deploy_app(
            args.repo_root,
            workspace_id=str(outputs["workspace_id"]),
            tenant_id=config.tenant_id,
            dry_run=args.dry_run,
        )
        report["status"] = "SUCCEEDED"
    except (OSError, RuntimeError) as exc:
        report |= {"status": "FAILED", "error": str(exc)}
        console.error(str(exc))
    (generated / "fabric-app-report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    if report["status"] != "SUCCEEDED":
        return 1
    console.info("Fabric App approvals spike deployed." if not args.dry_run
                 else "Fabric App deployment validated (dry run).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
