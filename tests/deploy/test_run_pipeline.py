"""Tests for the on-demand Fabric pipeline runner."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from requests import ConnectionError, Timeout

from deploy.scripts import _retry, run_pipeline

TENANT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


class _StatusResponse:
    def __init__(self, status: object) -> None:
        self._status = status
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return {"status": self._status}


class _StatusSession:
    def __init__(self, statuses: Iterable[object]) -> None:
        self._statuses = iter(statuses)
        self.urls: list[str] = []

    def get(self, url: str, **_kwargs: object) -> _StatusResponse:
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
        lambda *_args: (
            "https://api.fabric.microsoft.com/v1/workspaces/ws/items/"
            "pipeline-id/jobs/instances/run-id"
        ),
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


def test_wait_for_pipeline_run_refreshes_expired_token() -> None:
    class _UnauthorizedResponse:
        status_code = 401

    class _ExpiredSession:
        def get(self, _url: str, **_kwargs: object) -> _UnauthorizedResponse:
            return _UnauthorizedResponse()

    replacement = _StatusSession(["Completed"])
    refreshes = 0

    def refresh():
        nonlocal refreshes
        refreshes += 1
        return replacement

    status = run_pipeline.wait_for_pipeline_run(
        _ExpiredSession(),
        "https://api.fabric.microsoft.com/v1/jobs/instances/current-run",
        timeout_seconds=1,
        poll_interval_seconds=0,
        refresh_session=refresh,
    )

    assert status == "Completed"
    assert refreshes == 1


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


def test_wait_for_pipeline_run_times_out_during_status_request(monkeypatch) -> None:
    now = [10.0]
    monkeypatch.setattr(run_pipeline.time, "monotonic", lambda: now[0])

    class _SlowSession:
        def get(self, _url: str, **kwargs: object) -> _StatusResponse:
            assert kwargs["timeout"] == 1.0
            now[0] += 1.0
            return _StatusResponse("InProgress")

    session = _SlowSession()

    with pytest.raises(run_pipeline.PipelineRunError, match="did not complete"):
        run_pipeline.wait_for_pipeline_run(
            session,
            "https://api.fabric.microsoft.com/v1/jobs/instances/current-run",
            timeout_seconds=1,
            poll_interval_seconds=0,
        )


def test_network_retry_stops_before_overshooting_deadline(monkeypatch) -> None:
    now = [0.0]
    calls = []
    monkeypatch.setattr(run_pipeline.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(_retry.time, "sleep", lambda _seconds: None)

    class _SlowFailureSession:
        def get(self, _url: str, **kwargs: object) -> _StatusResponse:
            calls.append(kwargs["timeout"])
            now[0] += float(kwargs["timeout"])
            raise Timeout("read consumed the wait deadline")

    with pytest.raises(run_pipeline.PipelineRunError, match="did not complete"):
        run_pipeline.wait_for_pipeline_run(
            _SlowFailureSession(),
            "https://api.fabric.microsoft.com/v1/jobs/instances/current-run",
            timeout_seconds=7,
            poll_interval_seconds=1,
        )
    assert calls == [7.0]


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
        def get(self, _url: str, **_kwargs: object) -> _MismatchedResponse:
            return _MismatchedResponse()

    with pytest.raises(run_pipeline.PipelineRunError, match="Location correlation"):
        run_pipeline.wait_for_pipeline_job(
            _MismatchedSession(),
            "https://api.fabric.microsoft.com/v1/jobs/instances/exact-run",
            pipeline_id="pipeline-id",
            timeout_seconds=1,
            poll_interval_seconds=0,
        )


@pytest.mark.parametrize("error_cls", [ConnectionError, Timeout])
def test_wait_for_pipeline_run_retries_transient_network_errors(
    monkeypatch: pytest.MonkeyPatch,
    error_cls: type[Exception],
) -> None:
    waits: list[float] = []
    monkeypatch.setattr(_retry.time, "sleep", waits.append)

    calls = 0

    class _FlakySession:
        def get(self, _url: str, **_kwargs: object) -> _StatusResponse:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise error_cls("transient failure")
            return _StatusResponse("Completed")

    status = run_pipeline.wait_for_pipeline_run(
        _FlakySession(),
        "https://api.fabric.microsoft.com/v1/jobs/instances/current-run",
        timeout_seconds=60,
        poll_interval_seconds=0,
    )
    assert status == "Completed"
    assert calls == 3
    assert len(waits) == 2


def test_wait_for_pipeline_run_fails_after_exceeding_max_network_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_retry.time, "sleep", lambda _s: None)

    class _BrokenSession:
        def get(self, _url: str, **_kwargs: object) -> _StatusResponse:
            raise Timeout("permanent network timeout")

    with pytest.raises(
        run_pipeline.PipelineRunError, match="consecutive network errors"
    ):
        run_pipeline.wait_for_pipeline_run(
            _BrokenSession(),
            "https://api.fabric.microsoft.com/v1/jobs/instances/current-run",
            timeout_seconds=60,
            poll_interval_seconds=0,
        )


def test_wait_for_pipeline_job_passes_timeout_to_session_get() -> None:
    captured_kwargs: dict[str, object] = {}

    class _CaptureSession:
        def get(self, _url: str, **kwargs: object) -> _StatusResponse:
            captured_kwargs.update(kwargs)
            return _StatusResponse("Completed")

    run_pipeline.wait_for_pipeline_job(
        _CaptureSession(),
        "https://api.fabric.microsoft.com/v1/jobs/instances/current-run",
        timeout_seconds=60,
        poll_interval_seconds=0,
    )
    assert captured_kwargs.get("timeout") == 30.0


def test_main_writes_exact_evidence_before_first_status_get(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "pipeline.json"
    location = (
        "https://api.fabric.microsoft.com/v1/workspaces/ws/items/"
        "pipeline-id/jobs/instances/11111111-1111-4111-8111-111111111111"
    )

    class _Session:
        def post(self, *_args: object, **_kwargs: object):
            return type(
                "Response",
                (),
                {
                    "headers": {"Location": location},
                    "raise_for_status": lambda self: None,
                },
            )()

        def get(self, _url: str, **_kwargs: object) -> _StatusResponse:
            payload = json.loads(evidence_path.read_text(encoding="utf-8"))
            assert payload["pipeline_id"] == "pipeline-id"
            assert payload["run_id"] == "11111111-1111-4111-8111-111111111111"
            assert payload["location"] == location
            assert payload["status"] == "Submitted"
            assert payload["submitted_at"]
            return _StatusResponse("Completed")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_pipeline",
            "--environment",
            "dev",
            "--pipeline",
            "setup-pipeline",
            "--wait",
            "--poll-interval-seconds",
            "0",
            "--evidence-output",
            str(evidence_path),
        ],
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_environment",
        lambda _env: type(
            "Config", (), {"tenant_id": TENANT_ID, "auth_mode": "azure_cli"}
        )(),
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_terraform_outputs",
        lambda _path: {"workspace_id": "ws"},
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.validate_terraform_outputs",
        lambda *_args: None,
    )
    session = _Session()
    monkeypatch.setattr(run_pipeline, "build_session", lambda **_kwargs: session)
    monkeypatch.setattr(run_pipeline, "find_pipeline_id", lambda *_args: "pipeline-id")

    assert run_pipeline.main() == 0
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["status"] == "Completed"
    assert evidence["status_observed_at"]


def test_main_reattaches_without_posting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_id = "22222222-2222-4222-8222-222222222222"
    location = (
        "https://api.fabric.microsoft.com/v1/workspaces/ws/items/"
        f"pipeline-id/jobs/instances/{run_id}"
    )
    evidence_path = tmp_path / "pipeline.json"
    evidence_path.write_text(
        json.dumps(
            {
                "workspace_id": "ws",
                "pipeline_name": "setup-pipeline",
                "pipeline_id": "pipeline-id",
                "run_id": run_id,
                "location": location,
                "submitted_at": "2026-10-15T12:00:00+00:00",
                "status": "InProgress",
                "status_observed_at": "2026-10-15T12:01:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    class _Session:
        def post(self, *_args: object, **_kwargs: object):
            raise AssertionError("reattachment must not POST")

        def get(self, _url: str, **_kwargs: object) -> _StatusResponse:
            return _StatusResponse("Completed")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_pipeline",
            "--environment",
            "dev",
            "--pipeline",
            "setup-pipeline",
            "--wait",
            "--reattach-location",
            location,
            "--evidence-output",
            str(evidence_path),
            "--poll-interval-seconds",
            "0",
        ],
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_environment",
        lambda _env: type(
            "Config", (), {"tenant_id": TENANT_ID, "auth_mode": "azure_cli"}
        )(),
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_terraform_outputs",
        lambda _path: {"workspace_id": "ws"},
    )
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.validate_terraform_outputs",
        lambda *_args: None,
    )
    session = _Session()
    monkeypatch.setattr(run_pipeline, "build_session", lambda **_kwargs: session)
    monkeypatch.setattr(run_pipeline, "find_pipeline_id", lambda *_args: "pipeline-id")

    assert run_pipeline.main() == 0
    assert json.loads(evidence_path.read_text(encoding="utf-8"))["submitted_at"] == (
        "2026-10-15T12:00:00+00:00"
    )


def test_ambiguous_submission_reconciles_one_candidate_without_second_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_at = datetime.now(UTC)

    class _Session:
        post_calls = 0

        def post(self, *_args: object, **_kwargs: object):
            self.post_calls += 1
            raise Timeout("response lost")

    session = _Session()
    candidate = {
        "id": "33333333-3333-4333-8333-333333333333",
        "itemId": "pipeline-id",
        "status": "InProgress",
        "startTimeUtc": requested_at.isoformat(),
    }
    monkeypatch.setattr(run_pipeline, "list_pipeline_runs", lambda *_args: [candidate])
    location, submission = run_pipeline.submit_pipeline_run(
        session,
        "ws",
        "pipeline-id",
        request_started_at=requested_at,
    )

    assert session.post_calls == 1
    assert location.endswith(candidate["id"])
    assert submission == "reconciled"


def test_ambiguous_submission_with_duplicate_candidates_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_at = datetime.now(UTC)

    class _Session:
        post_calls = 0

        def post(self, *_args: object, **_kwargs: object):
            self.post_calls += 1
            raise Timeout("response lost")

    candidates = [
        {
            "id": str(index),
            "itemId": "pipeline-id",
            "status": "InProgress",
            "startTimeUtc": requested_at.isoformat(),
        }
        for index in (1, 2)
    ]
    monkeypatch.setattr(run_pipeline, "list_pipeline_runs", lambda *_args: candidates)
    session = _Session()

    with pytest.raises(run_pipeline.PipelineRunError, match="found 2"):
        run_pipeline.submit_pipeline_run(
            session,
            "ws",
            "pipeline-id",
            request_started_at=requested_at,
        )
    assert session.post_calls == 1


def test_monitor_timeout_is_distinct_from_remote_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    monkeypatch.setattr(run_pipeline.time, "monotonic", lambda: now[0])

    class _ActiveSession:
        def get(self, _url: str, **_kwargs: object) -> _StatusResponse:
            now[0] += 2
            return _StatusResponse("InProgress")

    with pytest.raises(run_pipeline.PipelineMonitorTimeout):
        run_pipeline.wait_for_pipeline_job(
            _ActiveSession(),
            "https://api.fabric.microsoft.com/v1/jobs/instances/current-run",
            timeout_seconds=100,
            monitor_timeout_seconds=1,
            poll_interval_seconds=0,
        )
