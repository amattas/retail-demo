"""Run a named Fabric Data Pipeline on demand via the Job Scheduler API.

Resolves the workspace from the captured Terraform outputs, finds the pipeline
item by display name, and starts an on-demand run:

    POST {FABRIC_API}/workspaces/{ws}/items/{pipelineId}/jobs/instances?jobType=Pipeline

Auth reuses the configured operator login (see ``export_items.build_session``).
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES
from deploy.scripts.export_items import build_session, list_items
from deploy.scripts.fabric_runtime import paginated_get

if TYPE_CHECKING:
    import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"
REPO_ROOT = Path(__file__).resolve().parents[2]
_IN_PROGRESS_STATUSES = frozenset({"NotStarted", "InProgress"})
_FAILED_STATUSES = frozenset({"Failed", "Cancelled", "Deduped", "Skipped"})
_SUCCESS_STATUS = "Completed"
_KNOWN_STATUSES = _IN_PROGRESS_STATUSES | _FAILED_STATUSES | {_SUCCESS_STATUS}


class PipelineRunError(RuntimeError):
    """Raised when the exact started pipeline run does not complete successfully."""


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
    response = session.post(url, params={"jobType": "Pipeline"})
    response.raise_for_status()
    return response.headers.get("Location")


def wait_for_pipeline_run(
    session: requests.Session,
    location: str,
    *,
    pipeline_id: str | None = None,
    timeout_seconds: float = 21600,
    poll_interval_seconds: float = 15,
) -> str:
    """Poll the returned job URL until the exact run reaches ``Completed``."""

    payload = wait_for_pipeline_job(
        session,
        location,
        pipeline_id=pipeline_id,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
    return str(payload["status"])


def wait_for_pipeline_job(
    session: requests.Session,
    location: str,
    *,
    pipeline_id: str | None = None,
    timeout_seconds: float = 21600,
    poll_interval_seconds: float = 15,
) -> dict[str, object]:
    """Poll one exact job URL and return its terminal-success payload."""

    if not location:
        raise PipelineRunError(
            "Fabric did not return a job-instance Location; run status is unknown."
        )
    if timeout_seconds <= 0 or poll_interval_seconds < 0:
        raise ValueError("pipeline wait timeout must be positive and interval non-negative")

    deadline = time.monotonic() + timeout_seconds
    while True:
        response = session.get(location)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise PipelineRunError("Pipeline run returned a non-object job payload.")
        status = payload.get("status")
        _validate_job_correlation(payload, location, pipeline_id)
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
        if time.monotonic() >= deadline:
            raise PipelineRunError(
                f"Pipeline run did not complete within {timeout_seconds:g} seconds."
            )
        time.sleep(poll_interval_seconds)


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
    """Start an on-demand run of a named Fabric pipeline."""

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
        help="Wait for the exact started run to reach terminal success.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=21600,
        help="Maximum wait for a terminal pipeline state.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=15,
        help="Delay between job-instance status requests.",
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
    session = build_session(
        auth_mode=args.auth_mode or config.auth_mode,
        tenant_id=tenant_id,
    )
    pipeline_id = find_pipeline_id(session, workspace_id, args.pipeline)
    location = run_pipeline(session, workspace_id, pipeline_id)
    console.info(f"Started pipeline run for {args.pipeline!r} ({pipeline_id}).")
    if location:
        console.detail(f"Track the run at: {location}")
    if args.wait:
        try:
            status = wait_for_pipeline_run(
                session,
                location or "",
                pipeline_id=pipeline_id,
                timeout_seconds=args.timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
            )
        except PipelineRunError as exc:
            console.error(str(exc))
            return 1
        console.info(
            f"Pipeline run for {args.pipeline!r} reached terminal success "
            f"({status})."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
