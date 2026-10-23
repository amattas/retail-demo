"""Fabric Apps deploy wrapper: staging, standalone identity, origin redeploys, redaction."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

from deploy.scripts import deploy_fabric_app as wrapper

WS = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def _which(name: str) -> str:
    return f"/bin/{name}"


def _source(tmp_path: Path) -> Path:
    source = tmp_path / "app" / "fabric-app"
    (source / "rayfin").mkdir(parents=True)
    (source / "node_modules").mkdir()
    (source / "src").mkdir()
    (source / "src" / "main.tsx").write_text("app", encoding="utf-8")
    (source / ".env.local").write_text("VITE_RAYFIN_API_URL=approvals", encoding="utf-8")
    (source / "rayfin" / ".env").write_text("RAYFIN_SECRET=x", encoding="utf-8")
    (source / "rayfin" / ".deployments.json").write_text(json.dumps({"deployments": {
        "dev": {"fabricWorkspaceId": WS, "hostingUrl": "https://approvals.example"},
    }}), encoding="utf-8")
    (source / "rayfin" / "rayfin.yml").write_text(
        "id: retail-approvals\nname: retail-approvals\nservices:\n  auth:\n"
        "    allowedRedirectUris:\n      - http://localhost:5173\n"
        "      - https://approvals.example\n",
        encoding="utf-8",
    )
    return source


class FakeRayfin:
    """Records commands and emulates ``rayfin up`` writing its deployment registry."""

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], dict]] = []

    def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        if command[1] != "rayfin":
            return subprocess.CompletedProcess(command, 0, "", "")
        cwd: Path = kwargs["cwd"]
        host = f"https://{cwd.name}.example"
        if "--dry-run" not in command:
            registry = cwd / "rayfin" / ".deployments.json"
            registry.write_text(json.dumps({"deployments": {
                "dev": {"fabricWorkspaceId": WS, "hostingUrl": host},
            }}), encoding="utf-8")
        stdout = "progress...\n" + json.dumps({
            "status": "success",
            "publishableKey": "pk-secret",
            "deployment": {"itemName": cwd.name, "hostingUrl": host},
            "auth": {"accessToken": "t"},
        })
        return subprocess.CompletedProcess(command, 0, stdout, "")

    def ups(self) -> list[tuple[str, dict]]:
        return [
            (kwargs["cwd"].name, kwargs["env"])
            for command, kwargs in self.calls
            if command[1] == "rayfin"
        ]


def test_deploys_three_staged_apps_with_their_own_identity(tmp_path: Path):
    source = _source(tmp_path)
    fake = FakeRayfin()

    report = wrapper.deploy_app(
        tmp_path, workspace_id=WS, tenant_id="tenant", environment="dev",
        run=fake, which=_which,
    )

    assert [command[:3] for command, _ in fake.calls[:2]] == [
        ["/bin/npm", "ci", "--no-audit"],
        ["/bin/npm", "run", "install"],
    ]
    assert fake.calls[0][1]["cwd"] == source
    up = next(command for command, _ in fake.calls if command[1] == "rayfin")
    assert up[3:] == ["--workspace-id", WS, "--yes", "--json", "--tenant", "tenant"]
    # Each app builds only its own variant and never receives another app's URL.
    assert [(cwd, env["VITE_APP_VARIANT"]) for cwd, env in fake.ups()] == [
        ("approvals", "approvals"),
        ("merchandising", "merchandising"),
        ("merchandising", "merchandising"),
        ("promotions", "promotions"),
        ("promotions", "promotions"),
    ]
    assert not any(key.endswith("_APP_URL") for _, env in fake.ups() for key in env)
    # Only the new apps redeploy, once, to allow their own sign-in origin.

    staging = tmp_path / "deploy" / ".generated" / "dev" / "fabric-apps"
    merch = staging / "merchandising"
    identity = yaml.safe_load((merch / "rayfin" / "rayfin.yml").read_text(encoding="utf-8"))
    assert identity["id"] == identity["name"] == "retail-merchandising"
    assert identity["services"]["auth"]["allowedRedirectUris"] == [
        "http://localhost:5173", "https://merchandising.example",
    ]
    assert (merch / "src" / "main.tsx").exists()
    assert (merch / "node_modules").exists()
    assert not (merch / ".env.local").exists()
    assert not (merch / "rayfin" / ".env").exists()
    # Approvals adopts the existing item's registry so its data is kept.
    assert (staging / "approvals" / "rayfin" / ".env").exists()
    assert set(report["apps"]) == {"approvals", "merchandising", "promotions"}
    assert report["apps"]["promotions"]["item_name"] == "retail-promotions"
    assert report["apps"]["promotions"]["rayfin"]["deployment"]["hostingUrl"] == (
        "https://promotions.example"
    )
    assert "secret" not in str(report) and "accessToken" not in str(report)

    rerun = FakeRayfin()
    wrapper.deploy_app(
        tmp_path, workspace_id=WS, tenant_id=None, environment="dev",
        run=rerun, which=_which,
    )
    assert [cwd for cwd, _ in rerun.ups()] == ["approvals", "merchandising", "promotions"]


def test_dry_run_flag_and_missing_node(tmp_path: Path):
    _source(tmp_path)
    fake = FakeRayfin()

    wrapper.deploy_app(tmp_path, workspace_id=WS, tenant_id=None, dry_run=True,
                       run=fake, which=_which)
    ups = [command for command, _ in fake.calls if command[1] == "rayfin"]
    assert len(ups) == 3
    assert all(command[-1] == "--dry-run" and "--tenant" not in command for command in ups)
    with pytest.raises(RuntimeError, match="Node.js"):
        wrapper.deploy_app(tmp_path, workspace_id=WS, tenant_id=None, which=lambda _: None)


def test_failed_command_stops_the_deploy(tmp_path: Path):
    _source(tmp_path)

    def run(command, **_):
        return subprocess.CompletedProcess(command, 7 if command[1] == "ci" else 0, "", "")

    with pytest.raises(RuntimeError, match="exit 7"):
        wrapper.deploy_app(tmp_path, workspace_id=WS, tenant_id=None, run=run, which=_which)


@pytest.mark.parametrize(
    ("dry_run", "folder_success", "exit_code"),
    [(False, True, 0), (False, False, 1), (True, False, 0)],
)
def test_published_app_reconciles_folders_but_dry_run_does_not(
    tmp_path, monkeypatch, dry_run, folder_success, exit_code
):
    generated = tmp_path / "deploy" / ".generated" / "dev"
    generated.mkdir(parents=True)
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_environment",
        lambda *_args, **_kwargs: SimpleNamespace(tenant_id="tenant"),
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_terraform_outputs",
        lambda *_args: {"workspace_id": WS},
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.validate_terraform_outputs", Mock()
    )
    monkeypatch.setattr(
        wrapper, "deploy_app",
        lambda *_args, **_kwargs: {"dry_run": dry_run, "rayfin": {}},
    )
    organize = Mock(return_value=folder_success)
    monkeypatch.setattr(
        "deploy.scripts.workspace_folders.organize_after_publish", organize
    )
    args = ["--repo-root", str(tmp_path), "--environment", "dev"]
    if dry_run:
        args.append("--dry-run")

    assert wrapper.main(args) == exit_code
    if dry_run:
        organize.assert_not_called()
    else:
        organize.assert_called_once_with(tmp_path, "dev")
