"""Deploy the three Retail Operations Fabric Apps with the Rayfin CLI.

``app/fabric-app`` is one codebase that ships as three standalone Fabric App
items for separate business functions: ``retail-merchandising``,
``retail-promotions``, and ``retail-approvals``. The apps do not link to or
depend on each other. Each app is staged under ``deploy/.generated/<env>/fabric-apps/<variant>`` with its
own ``rayfin.yml`` identity and Rayfin deployment registry, and is built with
``VITE_APP_VARIANT`` so the bundle contains only that workspace. Staged copies
share the source project's ``node_modules`` through a directory link.

A new app learns its hosting origin only after the first ``rayfin up``; it is
then deployed once more with that origin allowed so Fabric sign-in works
immediately.

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
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from deploy.scripts import _output as console

ROOT = Path(__file__).resolve().parents[2]
APP_DIR = Path("app") / "fabric-app"
_SAFE_KEYS = ("url", "id", "status", "workspace", "name", "state")
_SECRET_KEYS = ("key", "token", "secret", "password")
_LOCAL_ORIGINS = ("http://localhost", "http://127.0.0.1")
# Never copied into a staged app: build output, local env, and per-item state.
_SKIP_TOP = frozenset({"node_modules", "dist", ".vite", "coverage", "rayfin.config.json"})
_KEEP_RAYFIN = (".env", ".deployments.json")
_SKIP_RAYFIN = frozenset({".temp", ".deployments.json"})


@dataclass(frozen=True)
class FabricApp:
    variant: str
    item_name: str


FABRIC_APPS = (
    FabricApp("approvals", "retail-approvals"),
    FabricApp("merchandising", "retail-merchandising"),
    FabricApp("promotions", "retail-promotions"),
)


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


def _is_local(origin: str) -> bool:
    return origin.startswith(_LOCAL_ORIGINS)


def _ignore(directory: str, names: list[str]) -> set[str]:
    in_rayfin = Path(directory).name == "rayfin"
    skipped = set()
    for name in names:
        if (
            name in _SKIP_TOP and not in_rayfin
            or name.endswith(".tsbuildinfo")
            or name.endswith(".local")
            or in_rayfin and (name in _SKIP_RAYFIN or name.startswith(".env"))
        ):
            skipped.add(name)
    return skipped


def _link_directory(target: Path, link: Path) -> None:
    if link.exists() or link.is_symlink():
        return
    if sys.platform == "win32":
        import _winapi

        _winapi.CreateJunction(str(target), str(link))
    else:
        link.symlink_to(target, target_is_directory=True)


def stage_app(source: Path, staging: Path, app: FabricApp) -> Path:
    """Refresh one staged app from source, preserving its Rayfin deployment state."""

    staging.mkdir(parents=True, exist_ok=True)
    for entry in staging.iterdir():
        if entry.name in {"node_modules", "rayfin"}:
            continue
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        else:
            entry.unlink()
    rayfin = staging / "rayfin"
    if rayfin.is_dir():
        for entry in rayfin.iterdir():
            if entry.name.startswith(_KEEP_RAYFIN):
                continue
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
    if app.variant == "approvals" and not (rayfin / ".deployments.json").exists():
        # Adopt the item first deployed from the source project so its data stays.
        for entry in (source / "rayfin").glob(".*"):
            if entry.is_file() and entry.name.startswith(_KEEP_RAYFIN):
                rayfin.mkdir(exist_ok=True)
                shutil.copy2(entry, rayfin / entry.name)
    shutil.copytree(source, staging, ignore=_ignore, dirs_exist_ok=True)
    _link_directory(source / "node_modules", staging / "node_modules")
    return staging


def registry_urls(project: Path) -> list[str]:
    """Hosting URLs recorded in a project's Rayfin deployment registry."""

    path = project / "rayfin" / ".deployments.json"
    if not path.exists():
        return []
    deployments = json.loads(path.read_text(encoding="utf-8")).get("deployments", {})
    return [
        str(entry["hostingUrl"])
        for entry in deployments.values()
        if isinstance(entry, dict) and entry.get("hostingUrl")
    ]


def configure_identity(project: Path, app: FabricApp, source_origins: list[str]) -> list[str]:
    """Write the app's Rayfin identity and sign-in origins; return the origins."""

    path = project / "rayfin" / "rayfin.yml"
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    config["id"] = config["name"] = app.item_name
    inherited = [
        origin for origin in source_origins
        if _is_local(origin) or app.variant == "approvals"
    ]
    origins = list(dict.fromkeys(inherited + registry_urls(project)))
    config["services"]["auth"]["allowedRedirectUris"] = origins
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return origins


def _run_checked(
    run: Callable[..., subprocess.CompletedProcess[str]],
    command: Sequence[str],
    cwd: Path,
    env: Mapping[str, str],
) -> subprocess.CompletedProcess[str]:
    result = run(list(command), cwd=cwd, env=dict(env), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"{Path(command[0]).name} {' '.join(command[1:3])} failed "
            f"(exit {result.returncode}) in {cwd.name}. Check the Fabric Apps tenant "
            "setting, region support, and Rayfin sign-in."
        )
    return result


def deploy_app(
    repo_root: Path,
    *,
    workspace_id: str,
    tenant_id: str | None,
    environment: str = "default",
    dry_run: bool = False,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    which: Callable[[str], str | None] = shutil.which,
) -> dict[str, Any]:
    npm, npx = which("npm"), which("npx")
    if not npm or not npx:
        raise RuntimeError("Node.js 20+ (npm and npx) is required to deploy the Fabric App.")
    source = repo_root / APP_DIR
    base_env = {**os.environ, "RAYFIN_TELEMETRY_OPTOUT": "1"}
    _run_checked(run, [npm, "ci", "--no-audit", "--no-fund"], source, base_env)
    _run_checked(
        run,
        [npm, "run", "install", "--prefix", str(source / "node_modules" / "keytar")],
        source,
        base_env,
    )
    source_config = yaml.safe_load((source / "rayfin" / "rayfin.yml").read_text(encoding="utf-8"))
    source_origins = list(source_config["services"]["auth"].get("allowedRedirectUris") or [])
    staging_root = repo_root / "deploy" / ".generated" / environment / "fabric-apps"
    projects = {
        app.variant: stage_app(source, staging_root / app.variant, app) for app in FABRIC_APPS
    }
    up = [npx, "rayfin", "up", "--workspace-id", workspace_id, "--yes", "--json"]
    if tenant_id:
        up += ["--tenant", tenant_id]
    if dry_run:
        up.append("--dry-run")

    reports: dict[str, Any] = {}
    for app in FABRIC_APPS:
        project = projects[app.variant]
        env = {**base_env, "VITE_APP_VARIANT": app.variant}
        for attempt in range(2):
            origins = configure_identity(project, app, source_origins)
            result = _run_checked(run, up, project, env)
            payload = _parse_json(result.stdout) or {}
            hosting = (payload.get("deployment") or {}).get("hostingUrl")
            # A new item only learns its origin after deploying; allow it and redeploy.
            if dry_run or attempt or not hosting or hosting in origins:
                break
        reports[app.variant] = {"item_name": app.item_name, "rayfin": _safe(payload) or {}}
    return {"dry_run": dry_run, "apps": reports}


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
            environment=args.environment,
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
    if not args.dry_run:
        from deploy.scripts.workspace_folders import organize_after_publish

        if not organize_after_publish(args.repo_root, args.environment):
            return 1
    console.info("Fabric Apps deployed." if not args.dry_run
                 else "Fabric Apps deployment validated (dry run).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
