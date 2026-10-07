"""Fabric App deploy wrapper: commands, failure handling, and evidence redaction."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from deploy.scripts import deploy_fabric_app as wrapper

WS = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def _which(name: str) -> str:
    return f"/bin/{name}"


def test_runs_npm_ci_then_rayfin_up_and_redacts_secrets(tmp_path: Path):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        stdout = (
            'progress...\n{"appUrl": "https://x-app.rayfin.windows.net/", "itemId": "i1",'
            ' "publishableKey": "pk-secret", "auth": {"accessToken": "t"}, "status": "Succeeded"}'
            if command[1] == "rayfin" else ""
        )
        return subprocess.CompletedProcess(command, 0, stdout, "")

    report = wrapper.deploy_app(
        tmp_path, workspace_id=WS, tenant_id="tenant", run=run, which=_which
    )

    assert [command[:3] for command, _ in calls] == [
        ["/bin/npm", "ci", "--no-audit"],
        ["/bin/npx", "rayfin", "up"],
    ]
    up, options = calls[1]
    assert up[3:] == ["--workspace-id", WS, "--yes", "--json", "--tenant", "tenant"]
    assert options["cwd"] == tmp_path / "app" / "fabric-app"
    assert options["env"]["RAYFIN_TELEMETRY_OPTOUT"] == "1"
    assert report["rayfin"] == {
        "appUrl": "https://x-app.rayfin.windows.net/", "itemId": "i1", "status": "Succeeded",
    }
    assert "secret" not in str(report) and "accessToken" not in str(report)


def test_dry_run_flag_and_missing_node(tmp_path: Path):
    seen = []

    def run(command, **_):
        seen.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    wrapper.deploy_app(tmp_path, workspace_id=WS, tenant_id=None, dry_run=True,
                       run=run, which=_which)
    assert seen[-1][-1] == "--dry-run" and "--tenant" not in seen[-1]
    with pytest.raises(RuntimeError, match="Node.js"):
        wrapper.deploy_app(tmp_path, workspace_id=WS, tenant_id=None, which=lambda _: None)


def test_failed_command_stops_the_deploy(tmp_path: Path):
    def run(command, **_):
        return subprocess.CompletedProcess(command, 7 if command[1] == "ci" else 0, "", "")

    with pytest.raises(RuntimeError, match="exit 7"):
        wrapper.deploy_app(tmp_path, workspace_id=WS, tenant_id=None, run=run, which=_which)
