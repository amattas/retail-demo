"""Start the live ``stream-events`` notebook after deployment.

The stream is optional (``stream.start_after_deploy``). It writes synthetic
events to Eventhouse until ``run_seconds`` elapse or, when ``run_seconds`` is
0, until the notebook job is cancelled in Fabric. Submission is idempotent: an
already queued or running stream is reported, never duplicated.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from deploy.scripts import _output as console
from deploy.scripts._auth import build_credential
from deploy.scripts.deploy_config import (
    DeployConfig,
    load_environment,
    load_terraform_outputs,
    validate_terraform_outputs,
)
from deploy.scripts.export_items import build_session, list_items
from deploy.scripts.verify_readiness import write_report_atomic

ROOT = Path(__file__).resolve().parents[2]
API = "https://api.fabric.microsoft.com/v1"
STREAM_NOTEBOOK = "stream-events"
_ACTIVE = {"NotStarted", "InProgress"}


class StreamStartError(RuntimeError):
    """The stream could not be started or its submission was not accepted."""


def _stream_notebook_id(session: requests.Session, workspace_id: str) -> str:
    matches = [
        item
        for item in list_items(session, workspace_id, "Notebook")
        if item.get("displayName") == STREAM_NOTEBOOK
    ]
    if len(matches) != 1:
        raise StreamStartError(
            f"Expected one deployed {STREAM_NOTEBOOK!r} notebook; found {len(matches)}."
        )
    return str(matches[0]["id"])


def _active_run(
    session: requests.Session, workspace_id: str, notebook_id: str
) -> dict[str, Any] | None:
    response = session.get(
        f"{API}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances",
        timeout=30,
    )
    response.raise_for_status()
    runs = [run for run in response.json().get("value", []) if run.get("status") in _ACTIVE]
    return max(runs, key=lambda run: str(run.get("startTimeUtc") or ""), default=None)


def start_stream(
    session: requests.Session,
    *,
    workspace_id: str,
    rows_per_second: int,
    run_seconds: int,
) -> dict[str, Any]:
    """Submit one stream run unless one is already queued or running."""

    notebook_id = _stream_notebook_id(session, workspace_id)
    parameters = {"source_rows_per_second": rows_per_second, "run_seconds": run_seconds}
    active = _active_run(session, workspace_id, notebook_id)
    if active is not None:
        return {
            "state": "ALREADY_RUNNING",
            "notebook_id": notebook_id,
            "job_instance_id": active.get("id"),
        }
    response = session.post(
        f"{API}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances",
        params={"jobType": "RunNotebook"},
        json={
            "executionData": {
                "parameters": {
                    name: {"value": value, "type": "int"} for name, value in parameters.items()
                }
            }
        },
        timeout=60,
    )
    if response.status_code != 202 or not response.headers.get("Location"):
        raise StreamStartError(
            f"Fabric did not accept the {STREAM_NOTEBOOK} run: HTTP {response.status_code}."
        )
    location = response.headers["Location"]
    status = session.get(location, timeout=30)
    status.raise_for_status()
    run = status.json()
    if run.get("status") not in _ACTIVE | {"Completed"}:
        reason = (run.get("failureReason") or {}).get("message", "")
        raise StreamStartError(
            f"The {STREAM_NOTEBOOK} run reported {run.get('status')!r}. {reason}".strip()
        )
    return {
        "state": "STARTED",
        "notebook_id": notebook_id,
        "job_instance_id": run.get("id") or location.rstrip("/").rsplit("/", 1)[-1],
        "parameters": parameters,
    }


def run(repo_root: Path, config: DeployConfig) -> dict[str, Any]:
    generated = repo_root / "deploy" / ".generated" / config.environment
    outputs = load_terraform_outputs(generated / "terraform-output.json")
    validate_terraform_outputs(config, outputs)
    credential = build_credential(config.auth_mode, tenant_id=config.tenant_id)
    result = start_stream(
        build_session(credential),
        workspace_id=str(outputs["workspace_id"]),
        rows_per_second=config.stream.rows_per_second,
        run_seconds=config.stream.run_seconds,
    )
    report = {
        "environment": config.environment,
        "observed_at": datetime.now(UTC).isoformat(),
        **result,
    }
    write_report_atomic(generated / "stream-report.json", report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    try:
        report = run(args.repo_root, load_environment(args.environment))
    except (OSError, ValueError, requests.RequestException, StreamStartError) as exc:
        console.error(str(exc))
        return 1
    if report["state"] == "ALREADY_RUNNING":
        console.info(f"{STREAM_NOTEBOOK} is already running; no new run was submitted.")
    else:
        duration = (
            f"for {report['parameters']['run_seconds']} seconds"
            if report["parameters"]["run_seconds"]
            else "until it is cancelled in Fabric"
        )
        console.info(f"Started {STREAM_NOTEBOOK} {duration}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
