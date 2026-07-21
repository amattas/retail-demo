"""Tests for the on-demand Fabric pipeline runner."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable
from pathlib import Path

import pytest

from deploy.scripts import run_pipeline

TENANT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


class _StatusResponse:
    def __init__(self, status: object) -> None:
        self._status = status

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return {"status": self._status}


class _StatusSession:
    def __init__(self, statuses: Iterable[object]) -> None:
        self._statuses = iter(statuses)
        self.urls: list[str] = []

    def get(self, url: str) -> _StatusResponse:
        self.urls.append(url)
        return _StatusResponse(next(self._statuses))


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
            "--tenant-id",
            TENANT_ID,
            "--wait",
        ],
    )
    monkeypatch.setattr(run_pipeline, "workspace_id_from_outputs", lambda _env: "ws")
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_environment",
        lambda _env: type(
            "Config",
            (),
            {"tenant_id": TENANT_ID, "auth_mode": "azure_cli"},
        )(),
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_terraform_outputs",
        lambda _path: {"workspace_id": "ws"},
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.validate_terraform_outputs",
        lambda _config, _outputs: calls.update(targets_validated="yes"),
    )

    def fake_build_session(*, auth_mode: str, tenant_id: str | None):
        calls["auth_mode"] = auth_mode
        calls["tenant_id"] = tenant_id
        return object()

    monkeypatch.setattr(run_pipeline, "build_session", fake_build_session)
    monkeypatch.setattr(run_pipeline, "find_pipeline_id", lambda *_args: "pipeline-id")
    monkeypatch.setattr(
        run_pipeline,
        "run_pipeline",
        lambda *_args: "https://api.fabric.microsoft.com/v1/jobs/instances/run-id",
    )

    def fake_wait(_session, _location, **kwargs):
        calls["pipeline_id"] = str(kwargs["pipeline_id"])
        return "Completed"

    monkeypatch.setattr(run_pipeline, "wait_for_pipeline_run", fake_wait)

    try:
        result = run_pipeline.main()
    except SystemExit as exc:
        pytest.fail(f"--auth-mode was not accepted: {exc}")

    assert result == 0
    assert calls["auth_mode"] == "azure_powershell"
    assert calls["tenant_id"] == TENANT_ID
    assert calls["targets_validated"] == "yes"
    assert calls["pipeline_id"] == "pipeline-id"


def test_wait_for_pipeline_run_polls_exact_location_until_completed() -> None:
    location = "https://api.fabric.microsoft.com/v1/jobs/instances/current-run"
    session = _StatusSession(["NotStarted", "InProgress", "Completed"])

    assert (
        run_pipeline.wait_for_pipeline_run(
            session,
            location,
            timeout_seconds=1,
            poll_interval_seconds=0,
        )
        == "Completed"
    )
    assert session.urls == [location, location, location]


@pytest.mark.parametrize(
    "status",
    ["Failed", "Cancelled", "Deduped", "Skipped", None, "Succeeded"],
)
def test_wait_for_pipeline_run_rejects_non_success_terminal_status(
    status: object,
) -> None:
    session = _StatusSession([status])

    with pytest.raises(run_pipeline.PipelineRunError, match="status"):
        run_pipeline.wait_for_pipeline_run(
            session,
            "https://api.fabric.microsoft.com/v1/jobs/instances/current-run",
            timeout_seconds=1,
            poll_interval_seconds=0,
        )


def test_wait_for_pipeline_run_rejects_missing_current_run_location() -> None:
    with pytest.raises(run_pipeline.PipelineRunError, match="status is unknown"):
        run_pipeline.wait_for_pipeline_run(_StatusSession([]), "")


def test_wait_for_pipeline_run_times_out_in_progress(monkeypatch) -> None:
    times = iter([10.0, 12.0])
    monkeypatch.setattr(run_pipeline.time, "monotonic", lambda: next(times))
    session = _StatusSession(["InProgress"])

    with pytest.raises(run_pipeline.PipelineRunError, match="did not complete"):
        run_pipeline.wait_for_pipeline_run(
            session,
            "https://api.fabric.microsoft.com/v1/jobs/instances/current-run",
            timeout_seconds=1,
            poll_interval_seconds=0,
        )


def test_latest_pipeline_run_rejects_unknown_state_and_missing_time() -> None:
    with pytest.raises(run_pipeline.PipelineRunError, match="unknown status"):
        run_pipeline.latest_pipeline_run(
            [{"status": "Mystery", "startTimeUtc": "2026-01-01T00:00:00Z"}]
        )
    with pytest.raises(run_pipeline.PipelineRunError, match="startTimeUtc"):
        run_pipeline.latest_pipeline_run([{"status": "Completed"}])


def test_wait_for_pipeline_job_rejects_mismatched_location_correlation() -> None:
    class _MismatchedResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "id": "different-run",
                "itemId": "pipeline-id",
                "status": "Completed",
            }

    class _MismatchedSession:
        def get(self, _url: str) -> _MismatchedResponse:
            return _MismatchedResponse()

    with pytest.raises(run_pipeline.PipelineRunError, match="Location correlation"):
        run_pipeline.wait_for_pipeline_job(
            _MismatchedSession(),
            "https://api.fabric.microsoft.com/v1/jobs/instances/exact-run",
            pipeline_id="pipeline-id",
            timeout_seconds=1,
            poll_interval_seconds=0,
        )
