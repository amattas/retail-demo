"""Start or monitor a named Fabric Data Pipeline.

Resolve the workspace from captured Terraform outputs and find the pipeline
by display name. Start an on-demand run through the Job Scheduler API:

    POST {FABRIC_API}/workspaces/{ws}/items/{pipelineId}/jobs/instances?jobType=Pipeline

Use ``--reattach-location`` or ``--reattach-run-id`` to monitor an existing run
without submitting another. Authentication uses the configured operator login
(see ``export_items.build_session``).
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from requests import ConnectionError, Timeout

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES
from deploy.scripts.export_items import build_session, list_items
from deploy.scripts.fabric_runtime import paginated_get, retry_read

if TYPE_CHECKING:
    import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"
REPO_ROOT = Path(__file__).resolve().parents[2]
_IN_PROGRESS_STATUSES = frozenset({"NotStarted", "InProgress"})
_FAILED_STATUSES = frozenset({"Failed", "Cancelled", "Deduped", "Skipped"})
_SUCCESS_STATUS = "Completed"
_KNOWN_STATUSES = _IN_PROGRESS_STATUSES | _FAILED_STATUSES | {_SUCCESS_STATUS}
_MAX_AUTH_REFRESHES = 8
_RECONCILIATION_SKEW = timedelta(seconds=5)
MONITOR_DETACHED_EXIT_CODE = 4


class PipelineRunError(RuntimeError):
    """Raised when the exact started pipeline run does not complete successfully."""


class PipelineMonitorTimeout(RuntimeError):
    """Raised when local monitoring ends while the exact Fabric run remains active."""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def run_id_from_location(location: str) -> str:
    """Extract the exact Fabric job-instance ID from a Location URL."""

    run_id = location.rstrip("/").rsplit("/", maxsplit=1)[-1]
    if not run_id or "?" in run_id or "#" in run_id:
        raise PipelineRunError("Fabric returned an invalid job-instance Location.")
    return run_id


def _write_evidence(path: Path, evidence: dict[str, object]) -> None:
    """Atomically write bounded, non-secret pipeline correlation evidence."""

    allowed = {
        "workspace_id",
        "pipeline_name",
        "pipeline_id",
        "run_id",
        "location",
        "submitted_at",
        "status",
        "status_observed_at",
        "started_at",
        "ended_at",
        "submission",
        "failure_code",
        "failure_message",
        "failure_request_id",
    }
    bounded = {
        key: value
        for key, value in evidence.items()
        if key in allowed and isinstance(value, (str, int, float, bool, type(None)))
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        dir=path.parent, prefix=".pipeline-run-", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(bounded, handle, indent=2)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.remove(temporary)


def _read_evidence(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise PipelineRunError(f"Pipeline evidence at {path} is not an object.")
    return payload


def _evidence_from_submission(
    *,
    workspace_id: str,
    pipeline_name: str,
    pipeline_id: str,
    location: str,
    submitted_at: str,
    submission: str,
) -> dict[str, object]:
    return {
        "workspace_id": workspace_id,
        "pipeline_name": pipeline_name,
        "pipeline_id": pipeline_id,
        "run_id": run_id_from_location(location),
        "location": location,
        "submitted_at": submitted_at,
        "status": "Submitted",
        "status_observed_at": submitted_at,
        "started_at": None,
        "ended_at": None,
        "submission": submission,
    }


def _update_evidence_status(
    evidence: dict[str, object],
    payload: dict[str, object],
) -> dict[str, object]:
    updated = dict(evidence)
    updated["status"] = str(payload.get("status", "Unknown"))
    observed_at = _utc_now()
    updated["status_observed_at"] = observed_at
    for evidence_key, payload_key in (
        ("started_at", "startTimeUtc"),
        ("ended_at", "endTimeUtc"),
    ):
        value = payload.get(payload_key)
        if isinstance(value, str) and value:
            updated[evidence_key] = value
    if not updated.get("started_at"):
        updated["started_at"] = updated.get("submitted_at")
    if updated["status"] in _FAILED_STATUSES | {_SUCCESS_STATUS} and not updated.get(
        "ended_at"
    ):
        updated["ended_at"] = observed_at
    failure = payload.get("failureReason")
    if isinstance(failure, dict):
        for evidence_key, payload_key, limit in (
            ("failure_code", "errorCode", 100),
            ("failure_message", "message", 500),
            ("failure_request_id", "requestId", 100),
        ):
            value = failure.get(payload_key)
            if value not in (None, ""):
                updated[evidence_key] = " ".join(str(value).split())[:limit]
    return updated


def workspace_id_from_outputs(environment: str, repo_root: Path = REPO_ROOT) -> str:
    """Read the workspace id from the captured Terraform outputs for an environment."""

    path = repo_root / "deploy" / ".generated" / environment / "terraform-output.json"
    if not path.is_file():
        raise FileNotFoundError(
            f"Terraform outputs not found: {path}. Run `retail-setup deploy` first."
        )
    outputs = json.loads(path.read_text(encoding="utf-8"))
    value = outputs.get("workspace_id")
    workspace = value.get("value") if isinstance(value, dict) else value
    if not workspace:
        raise ValueError(f"workspace_id missing from {path}")
    return str(workspace)


def find_pipeline_id(
    session: requests.Session, workspace_id: str, pipeline_name: str
) -> str:
    """Resolve a DataPipeline display name to its item id."""

    for item in list_items(session, workspace_id, "DataPipeline"):
        if str(item.get("displayName", "")) == pipeline_name:
            return str(item["id"])
    raise ValueError(
        f"Pipeline {pipeline_name!r} not found in workspace {workspace_id}. "
        "Deploy it first (it is staged with the `setup` notebook group)."
    )


def run_pipeline(
    session: requests.Session, workspace_id: str, pipeline_id: str
) -> str | None:
    """Start an on-demand pipeline run; returns the job-instance URL if provided."""

    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances"
    response = session.post(url, params={"jobType": "Pipeline"}, timeout=60)
    response.raise_for_status()
    return response.headers.get("Location")


def submit_pipeline_run(
    session: requests.Session,
    workspace_id: str,
    pipeline_id: str,
    *,
    request_started_at: datetime | None = None,
) -> tuple[str, str]:
    """Submit once, reconciling only one exact-window run after an ambiguous loss."""

    requested_at = (request_started_at or datetime.now(UTC)).astimezone(UTC)
    try:
        location = run_pipeline(session, workspace_id, pipeline_id)
    except (ConnectionError, Timeout) as exc:
        observed_at = datetime.now(UTC)
        candidates = recent_pipeline_runs(
            list_pipeline_runs(session, workspace_id, pipeline_id),
            pipeline_id=pipeline_id,
            requested_at=requested_at,
            observed_at=observed_at,
        )
        if len(candidates) != 1:
            raise PipelineRunError(
                "Pipeline submission outcome is ambiguous and reconciliation "
                f"found {len(candidates)} matching runs; refusing to POST again."
            ) from exc
        run_id = str(candidates[0]["id"])
        location = (
            f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}"
            f"/jobs/instances/{run_id}"
        )
        return location, "reconciled"
    if not location:
        raise PipelineRunError(
            "Fabric accepted the pipeline request without a job-instance Location; "
            "the submission cannot be safely correlated or retried."
        )
    return location, "posted"


def wait_for_pipeline_run(
    session: requests.Session,
    location: str,
    *,
    pipeline_id: str | None = None,
    timeout_seconds: float = 21600,
    monitor_timeout_seconds: float | None = None,
    poll_interval_seconds: float = 15,
    refresh_session: Callable[[], requests.Session] | None = None,
    on_observation: Callable[[dict[str, object]], None] | None = None,
) -> str:
    """Poll the returned job URL until the exact run reaches ``Completed``."""

    payload = wait_for_pipeline_job(
        session,
        location,
        pipeline_id=pipeline_id,
        timeout_seconds=timeout_seconds,
        monitor_timeout_seconds=monitor_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        refresh_session=refresh_session,
        on_observation=on_observation,
    )
    return str(payload["status"])


def wait_for_pipeline_job(
    session: requests.Session,
    location: str,
    *,
    pipeline_id: str | None = None,
    timeout_seconds: float = 21600,
    monitor_timeout_seconds: float | None = None,
    poll_interval_seconds: float = 15,
    refresh_session: Callable[[], requests.Session] | None = None,
    on_observation: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    """Poll one exact job URL and return its terminal-success payload."""

    if not location:
        raise PipelineRunError(
            "Fabric did not return a job-instance Location; run status is unknown."
        )
    if (
        timeout_seconds <= 0
        or (monitor_timeout_seconds is not None and monitor_timeout_seconds <= 0)
        or poll_interval_seconds < 0
    ):
        raise ValueError("pipeline wait timeout must be positive and interval non-negative")

    execution_deadline = time.monotonic() + timeout_seconds
    monitor_deadline = (
        time.monotonic() + monitor_timeout_seconds
        if monitor_timeout_seconds is not None
        else float("inf")
    )
    auth_refreshes = 0
    consecutive_network_errors = 0
    max_consecutive_network_errors = 5
    while True:
        execution_remaining = execution_deadline - time.monotonic()
        if execution_remaining <= 0:
            raise PipelineRunError(
                f"Pipeline run did not complete within {timeout_seconds:g} seconds."
            )
        monitor_remaining = monitor_deadline - time.monotonic()
        if monitor_remaining <= 0:
            raise PipelineMonitorTimeout(
                "Local pipeline monitoring timed out while the exact Fabric run "
                "remained active."
            )

        def read_status() -> requests.Response:
            request_budget = min(
                execution_deadline - time.monotonic(),
                monitor_deadline - time.monotonic(),
            )
            if request_budget <= 0:
                if monitor_timeout_seconds is not None:
                    raise PipelineMonitorTimeout(
                        "Local pipeline monitoring timed out while the exact Fabric "
                        "run remained active."
                    )
                raise PipelineRunError(
                    f"Pipeline run did not complete within {timeout_seconds:g} seconds."
                )
            return session.get(location, timeout=min(30.0, request_budget))

        try:
            response = retry_read(
                read_status,
                deadline=min(execution_deadline, monitor_deadline),
            )
            consecutive_network_errors = 0
        except (ConnectionError, Timeout) as exc:
            execution_remaining = execution_deadline - time.monotonic()
            monitor_remaining = monitor_deadline - time.monotonic()
            if monitor_remaining <= 0:
                raise PipelineMonitorTimeout(
                    "Local pipeline monitoring timed out while the exact Fabric run "
                    "remained active."
                ) from exc
            if execution_remaining <= 0:
                raise PipelineRunError(
                    f"Pipeline run did not complete within {timeout_seconds:g} seconds."
                ) from exc
            consecutive_network_errors += 1
            if consecutive_network_errors >= max_consecutive_network_errors:
                raise PipelineRunError(
                    f"Pipeline status polling failed after "
                    f"{consecutive_network_errors} consecutive network errors: "
                    f"{exc}"
                ) from exc
            console.warn(
                f"Pipeline status polling hit a network error "
                f"({consecutive_network_errors}/{max_consecutive_network_errors}); "
                "retrying at the next poll interval."
            )
            time.sleep(
                min(poll_interval_seconds, execution_remaining, monitor_remaining)
            )
            continue

        execution_remaining = execution_deadline - time.monotonic()
        monitor_remaining = monitor_deadline - time.monotonic()
        if execution_remaining <= 0:
            raise PipelineRunError(
                f"Pipeline run did not complete within {timeout_seconds:g} seconds."
            )
        if getattr(response, "status_code", None) == 401 and refresh_session:
            auth_refreshes += 1
            if auth_refreshes > _MAX_AUTH_REFRESHES:
                raise PipelineRunError(
                    "Pipeline status polling remained unauthorized after "
                    f"{_MAX_AUTH_REFRESHES} token refreshes."
                )
            session = refresh_session()
            continue
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise PipelineRunError("Pipeline run returned a non-object job payload.")
        status = payload.get("status")
        _validate_job_correlation(payload, location, pipeline_id)
        if on_observation is not None:
            on_observation(payload)
        if status == _SUCCESS_STATUS:
            return payload
        if status in _FAILED_STATUSES:
            raise PipelineRunError(
                f"Pipeline run reached terminal status {status!r}."
            )
        if status not in _IN_PROGRESS_STATUSES:
            raise PipelineRunError(
                f"Pipeline run returned unknown status {status!r}."
            )
        if monitor_remaining <= 0:
            raise PipelineMonitorTimeout(
                "Local pipeline monitoring timed out while the exact Fabric run "
                "remained active."
            )
        time.sleep(
            min(poll_interval_seconds, execution_remaining, monitor_remaining)
        )


def list_pipeline_runs(
    session: requests.Session,
    workspace_id: str,
    pipeline_id: str,
) -> list[dict[str, object]]:
    """List every visible run for one pipeline, exhausting Fabric pagination."""

    url = (
        f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances"
    )
    return paginated_get(session, url, params={"jobType": "Pipeline"})


def latest_pipeline_run(
    runs: list[dict[str, object]],
    *,
    pipeline_id: str | None = None,
) -> dict[str, object]:
    """Return the newest known-state run, failing closed on missing evidence."""

    if not runs:
        raise PipelineRunError("No pipeline run evidence was returned.")
    normalized: list[tuple[datetime, dict[str, object]]] = []
    for run in runs:
        status = run.get("status")
        if status not in _KNOWN_STATUSES:
            raise PipelineRunError(
                f"Pipeline run returned unknown status {status!r}."
            )
        if pipeline_id and run.get("itemId") not in (None, pipeline_id):
            raise PipelineRunError("Pipeline run item correlation did not match.")
        started = _parse_job_time(run.get("startTimeUtc"))
        if started is None:
            raise PipelineRunError("Pipeline run has no valid startTimeUtc.")
        normalized.append((started, run))
    return max(normalized, key=lambda pair: pair[0])[1]


def recent_pipeline_runs(
    runs: list[dict[str, object]],
    *,
    pipeline_id: str,
    requested_at: datetime,
    observed_at: datetime,
) -> list[dict[str, object]]:
    """Return exact-pipeline runs in the bounded ambiguous-submission window."""

    lower = requested_at.astimezone(UTC) - _RECONCILIATION_SKEW
    upper = observed_at.astimezone(UTC) + _RECONCILIATION_SKEW
    candidates = []
    for run in runs:
        if run.get("itemId") not in (None, pipeline_id):
            continue
        run_id = run.get("id")
        started = _parse_job_time(run.get("startTimeUtc"))
        if (
            isinstance(run_id, str)
            and run_id
            and started is not None
            and run.get("status") in _KNOWN_STATUSES
            and lower <= started <= upper
        ):
            candidates.append(run)
    return candidates


def _validate_job_correlation(
    payload: dict[str, object],
    location: str,
    pipeline_id: str | None,
) -> None:
    """Reject a status payload that does not describe the requested exact run."""

    if pipeline_id and payload.get("itemId") not in (None, pipeline_id):
        raise PipelineRunError("Pipeline run item correlation did not match.")
    location_id = location.rstrip("/").rsplit("/", maxsplit=1)[-1]
    payload_id = payload.get("id")
    if payload_id and str(payload_id) != location_id:
        raise PipelineRunError("Pipeline run Location correlation did not match.")


def _parse_job_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def main() -> int:
    """Start or monitor a named Fabric pipeline."""

    parser = argparse.ArgumentParser(description="Run a Fabric Data Pipeline on demand")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--pipeline", required=True, help="Pipeline display name.")
    parser.add_argument(
        "--auth-mode",
        choices=AUTH_MODES,
        default=None,
        help="Operator credential used for Fabric REST requests.",
    )
    parser.add_argument(
        "--tenant-id",
        help="Entra tenant passed to the selected operator credential.",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for this exact run to complete successfully.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=21600,
        help="Execution-time limit in seconds for the Fabric run.",
    )
    parser.add_argument(
        "--monitor-timeout-seconds",
        type=float,
        default=None,
        help="Stop waiting after this many seconds without cancelling the Fabric run.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=15,
        help="Seconds between run-status requests.",
    )
    parser.add_argument(
        "--evidence-output",
        type=Path,
        help="Save this run's identity, status, and timestamps as JSON at this path.",
    )
    reattach = parser.add_mutually_exclusive_group()
    reattach.add_argument(
        "--reattach-location",
        help="Monitor this exact Fabric job Location without starting a new run.",
    )
    reattach.add_argument(
        "--reattach-run-id",
        help="Monitor this exact Fabric run ID without starting a new run.",
    )
    args = parser.parse_args()

    from deploy.scripts.deploy_config import (
        load_environment,
        load_terraform_outputs,
        validate_terraform_outputs,
    )

    config = load_environment(args.environment)
    output_path = (
        REPO_ROOT
        / "deploy"
        / ".generated"
        / args.environment
        / "terraform-output.json"
    )
    outputs = load_terraform_outputs(output_path)
    validate_terraform_outputs(config, outputs)
    workspace_id = str(outputs["workspace_id"])
    tenant_id = args.tenant_id or config.tenant_id
    if (
        args.tenant_id
        and config.tenant_id
        and args.tenant_id.casefold() != config.tenant_id.casefold()
    ):
        raise SystemExit("--tenant-id does not match the configured tenant")
    auth_mode = args.auth_mode or config.auth_mode

    def fresh_session():
        return build_session(
            auth_mode=auth_mode,
            tenant_id=tenant_id,
        )

    session = fresh_session()
    pipeline_id = find_pipeline_id(session, workspace_id, args.pipeline)
    submitted_at = _utc_now()
    if args.reattach_location:
        location = args.reattach_location
        submission = "reattached"
    elif args.reattach_run_id:
        location = (
            f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}"
            f"/jobs/instances/{args.reattach_run_id}"
        )
        submission = "reattached"
    else:
        try:
            location, submission = submit_pipeline_run(
                session,
                workspace_id,
                pipeline_id,
                request_started_at=datetime.fromisoformat(submitted_at),
            )
        except PipelineRunError as exc:
            console.error(str(exc))
            return 1
    expected_prefix = (
        f"{FABRIC_API}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/"
    )
    if not location.startswith(expected_prefix):
        console.error(
            "The exact job Location does not match the selected workspace and pipeline."
        )
        return 1
    if (
        submission == "reattached"
        and args.evidence_output
        and args.evidence_output.is_file()
    ):
        try:
            evidence = _read_evidence(args.evidence_output)
        except (json.JSONDecodeError, PipelineRunError) as exc:
            console.error(str(exc))
            return 1
        expected = {
            "workspace_id": workspace_id,
            "pipeline_name": args.pipeline,
            "pipeline_id": pipeline_id,
            "run_id": run_id_from_location(location),
            "location": location,
        }
        if any(evidence.get(key) != value for key, value in expected.items()):
            console.error("Existing pipeline evidence is incompatible with reattachment.")
            return 1
        evidence["submission"] = "reattached"
    else:
        evidence = _evidence_from_submission(
            workspace_id=workspace_id,
            pipeline_name=args.pipeline,
            pipeline_id=pipeline_id,
            location=location,
            submitted_at=submitted_at,
            submission=submission,
        )
    if args.evidence_output:
        _write_evidence(args.evidence_output, evidence)
    action = "Reattached to" if submission == "reattached" else "Started"
    console.info(f"{action} pipeline run for {args.pipeline!r} ({pipeline_id}).")
    console.detail(f"Track the run at: {location}")
    if args.wait:
        def record_observation(payload: dict[str, object]) -> None:
            nonlocal evidence
            evidence = _update_evidence_status(evidence, payload)
            if args.evidence_output:
                _write_evidence(args.evidence_output, evidence)

        try:
            status = wait_for_pipeline_run(
                session,
                location,
                pipeline_id=pipeline_id,
                timeout_seconds=args.timeout_seconds,
                monitor_timeout_seconds=args.monitor_timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
                refresh_session=fresh_session,
                on_observation=record_observation,
            )
        except PipelineMonitorTimeout as exc:
            console.warn(str(exc))
            return MONITOR_DETACHED_EXIT_CODE
        except PipelineRunError as exc:
            console.error(str(exc))
            return 1
        console.info(
            f"Pipeline run for {args.pipeline!r} completed successfully "
            f"({status})."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
