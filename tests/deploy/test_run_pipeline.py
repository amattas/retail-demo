"""Tests for the on-demand Fabric pipeline runner."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from deploy.scripts import run_pipeline


def test_workspace_id_from_outputs_reads_terraform_value(tmp_path: Path) -> None:
    out = tmp_path / "deploy" / ".generated" / "dev" / "terraform-output.json"
    out.parent.mkdir(parents=True)
    out.write_text(json.dumps({"workspace_id": {"value": "ws-123"}}), encoding="utf-8")

    assert run_pipeline.workspace_id_from_outputs("dev", repo_root=tmp_path) == "ws-123"


def test_workspace_id_from_outputs_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Terraform outputs not found"):
        run_pipeline.workspace_id_from_outputs("dev", repo_root=tmp_path)


def test_find_pipeline_id_matches_display_name() -> None:
    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "value": [
                    {"displayName": "daily-maintenance", "id": "1"},
                    {"displayName": "setup-pipeline", "id": "2"},
                ]
            }

    class _FakeSession:
        def get(self, url: str, params: dict | None = None) -> _FakeResponse:
            return _FakeResponse()

    assert run_pipeline.find_pipeline_id(_FakeSession(), "ws", "setup-pipeline") == "2"


def test_find_pipeline_id_raises_when_missing() -> None:
    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"value": [{"displayName": "other", "id": "1"}]}

    class _FakeSession:
        def get(self, url: str, params: dict | None = None) -> _FakeResponse:
            return _FakeResponse()

    with pytest.raises(ValueError, match="not found"):
        run_pipeline.find_pipeline_id(_FakeSession(), "ws", "setup-pipeline")


def test_main_passes_selected_auth_mode_to_session(monkeypatch) -> None:
    calls: dict[str, str] = {}

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_pipeline",
            "--environment",
            "dev",
            "--pipeline",
            "setup-pipeline",
            "--auth-mode",
            "azure_powershell",
        ],
    )
    monkeypatch.setattr(run_pipeline, "workspace_id_from_outputs", lambda _env: "ws")

    def fake_build_session(*, auth_mode: str):
        calls["auth_mode"] = auth_mode
        return object()

    monkeypatch.setattr(run_pipeline, "build_session", fake_build_session)
    monkeypatch.setattr(run_pipeline, "find_pipeline_id", lambda *_args: "pipeline-id")
    monkeypatch.setattr(run_pipeline, "run_pipeline", lambda *_args: None)

    try:
        result = run_pipeline.main()
    except SystemExit as exc:
        pytest.fail(f"--auth-mode was not accepted: {exc}")

    assert result == 0
    assert calls["auth_mode"] == "azure_powershell"
