"""Refresh Lakehouse SQL endpoint metadata after setup and ML table writes."""

from __future__ import annotations

import argparse
import math
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES
from deploy.scripts.deploy_config import (
    load_environment,
    load_terraform_outputs,
    validate_terraform_outputs,
)
from deploy.scripts.export_items import FABRIC_API, build_session

if TYPE_CHECKING:
    import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
_IN_PROGRESS_STATUSES = frozenset({"NotStarted", "Running"})
_FAILED_STATUSES = frozenset({"Failed", "Cancelled"})
_MAX_AUTH_REFRESHES = 3
_REQUEST_TIMEOUT_SECONDS = 60.0


class SqlEndpointRefreshError(RuntimeError):
    """Raised when SQL endpoint metadata cannot be proven synchronized."""


def resolve_sql_endpoint_id(
    session: requests.Session,
    workspace_id: str,
    lakehouse_id: str,
) -> str:
    """Resolve the generated SQL endpoint ID from the live Lakehouse."""

    response = session.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise SqlEndpointRefreshError(
            "Fabric returned a non-object Lakehouse response."
        )
    properties = payload.get("properties")
    endpoint_properties = (
        properties.get("sqlEndpointProperties")
        if isinstance(properties, dict)
        else None
    )
    endpoint_id = (
        endpoint_properties.get("id")
        if isinstance(endpoint_properties, dict)
        else None
    )
    if not isinstance(endpoint_id, str) or not endpoint_id.strip():
        raise SqlEndpointRefreshError(
            "The Lakehouse SQL endpoint ID is unavailable. Wait for Lakehouse "
            "provisioning to finish, then rerun deployment."
        )
    return endpoint_id.strip()


def validate_sync_statuses(payload: Any) -> list[dict[str, Any]]:
    """Require every table to synchronize or prove a prior successful sync."""

    if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
        raise SqlEndpointRefreshError(
            "SQL endpoint metadata refresh returned an invalid result."
        )
    statuses = payload["value"]
    if not statuses:
        raise SqlEndpointRefreshError(
            "SQL endpoint metadata refresh returned no table statuses."
        )
    failures: list[str] = []
    for item in statuses:
        if not isinstance(item, dict):
            raise SqlEndpointRefreshError(
                "SQL endpoint metadata refresh returned an invalid table status."
            )
        status = item.get("status")
        already_current = (
            status == "NotRun"
            and isinstance(item.get("lastSuccessfulSyncDateTime"), str)
            and bool(item["lastSuccessfulSyncDateTime"].strip())
        )
        if status != "Success" and not already_current:
            table = str(item.get("tableName") or "<unknown>")
            error = item.get("error")
            message = (
                str(error.get("message", "")).strip()
                if isinstance(error, dict)
                else ""
            )
            failures.append(
                f"{table} ({status or 'missing status'})"
                + (f": {message}" if message else "")
            )
    if failures:
        raise SqlEndpointRefreshError(
            "SQL endpoint metadata did not synchronize every table: "
            + "; ".join(failures)
        )
    return statuses


def refresh_sql_endpoint_metadata(
    session: requests.Session,
    workspace_id: str,
    sql_endpoint_id: str,
    *,
    timeout_seconds: float = 1800,
    poll_interval_seconds: float = 10,
    refresh_session: Callable[[], requests.Session] | None = None,
) -> list[dict[str, Any]]:
    """Start a metadata refresh and wait for its exact LRO result."""

    if timeout_seconds <= 0 or poll_interval_seconds < 0:
        raise ValueError(
            "metadata refresh timeout must be positive and interval non-negative"
        )
    deadline = time.monotonic() + timeout_seconds
    response = session.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/sqlEndpoints/"
        f"{sql_endpoint_id}/refreshMetadata",
        json={
            "timeout": {
                "value": max(1, math.ceil(timeout_seconds)),
                "timeUnit": "Seconds",
            }
        },
        timeout=_request_timeout(deadline),
    )
    response.raise_for_status()
    if response.status_code == 200:
        return validate_sync_statuses(response.json())
    if response.status_code != 202:
        raise SqlEndpointRefreshError(
            f"SQL endpoint metadata refresh returned unexpected HTTP "
            f"{response.status_code}."
        )

    operation_url = response.headers.get("Location")
    if not operation_url:
        raise SqlEndpointRefreshError(
            "Fabric accepted the metadata refresh without an operation Location."
        )
    retry_after = _retry_after(response, poll_interval_seconds)
    auth_refreshes = 0

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SqlEndpointRefreshError(
                f"SQL endpoint metadata refresh did not complete within "
                f"{timeout_seconds:g} seconds."
            )
        time.sleep(min(retry_after, remaining))
        poll = session.get(
            operation_url,
            timeout=_request_timeout(deadline),
        )
        if getattr(poll, "status_code", None) == 401 and refresh_session:
            auth_refreshes += 1
            if auth_refreshes > _MAX_AUTH_REFRESHES:
                raise SqlEndpointRefreshError(
                    "Metadata refresh polling remained unauthorized after "
                    f"{_MAX_AUTH_REFRESHES} token refreshes."
                )
            session = refresh_session()
            continue
        poll.raise_for_status()
        payload = poll.json()
        if not isinstance(payload, dict):
            raise SqlEndpointRefreshError(
                "Fabric returned a non-object metadata refresh operation."
            )
        status = payload.get("status")
        if status == "Succeeded":
            result_url = poll.headers.get("Location")
            if not result_url or result_url == operation_url:
                result_url = f"{operation_url.rstrip('/')}/result"
            result = session.get(
                result_url,
                timeout=_request_timeout(deadline),
            )
            result.raise_for_status()
            return validate_sync_statuses(result.json())
        if status in _FAILED_STATUSES:
            error = payload.get("error")
            detail = (
                str(error.get("message", "")).strip()
                if isinstance(error, dict)
                else ""
            )
            raise SqlEndpointRefreshError(
                f"SQL endpoint metadata refresh reached terminal status "
                f"{status!r}" + (f": {detail}" if detail else ".")
            )
        if status not in _IN_PROGRESS_STATUSES:
            raise SqlEndpointRefreshError(
                f"SQL endpoint metadata refresh returned unknown status "
                f"{status!r}."
            )
        retry_after = _retry_after(poll, poll_interval_seconds)


def _retry_after(response: Any, default: float) -> float:
    value = response.headers.get("Retry-After")
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _request_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SqlEndpointRefreshError(
            "SQL endpoint metadata refresh exceeded its request deadline."
        )
    return min(_REQUEST_TIMEOUT_SECONDS, remaining)


def main() -> int:
    """Refresh one environment's Lakehouse SQL endpoint metadata."""

    parser = argparse.ArgumentParser(
        description="Refresh Lakehouse SQL endpoint metadata"
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--auth-mode", choices=AUTH_MODES, default=None)
    parser.add_argument("--tenant-id")
    parser.add_argument("--timeout-seconds", type=float, default=1800)
    parser.add_argument("--poll-interval-seconds", type=float, default=10)
    args = parser.parse_args()

    config_root = args.repo_root / "deploy" / "config"
    config = load_environment(
        args.environment,
        config_path=config_root / "deploy.yml",
        environments_root=config_root / "environments",
    )
    output_path = (
        args.repo_root
        / "deploy"
        / ".generated"
        / args.environment
        / "terraform-output.json"
    )
    outputs = load_terraform_outputs(output_path)
    validate_terraform_outputs(config, outputs)
    tenant_id = args.tenant_id or config.tenant_id
    if (
        args.tenant_id
        and config.tenant_id
        and args.tenant_id.casefold() != config.tenant_id.casefold()
    ):
        console.error("--tenant-id does not match the configured tenant")
        return 1
    auth_mode = args.auth_mode or config.auth_mode

    def fresh_session() -> requests.Session:
        return build_session(
            auth_mode=auth_mode,
            tenant_id=tenant_id,
        )

    try:
        session = fresh_session()
        workspace_id = str(outputs["workspace_id"])
        endpoint_id = resolve_sql_endpoint_id(
            session,
            workspace_id,
            str(outputs["lakehouse_id"]),
        )
        statuses = refresh_sql_endpoint_metadata(
            session,
            workspace_id,
            endpoint_id,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
            refresh_session=fresh_session,
        )
    except (KeyError, SqlEndpointRefreshError, ValueError) as exc:
        console.error(str(exc))
        return 1

    console.info(
        f"SQL endpoint metadata is current for {len(statuses)} tables."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
