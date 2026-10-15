"""Bounded Direct Lake refresh after additive semantic-model metadata changes."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from requests import Response

from deploy.scripts._native_items import NativeItemError, _guid
from deploy.scripts.fabric_runtime import retry_read
from deploy.scripts.taskflow import PBI_SCOPE, _session, _token


def wait_for_refresh(
    session: Any, workspace_id: str, model_id: str, request_id: str,
    *, timeout_seconds: float = 600,
) -> None:
    url = (
        "https://api.powerbi.com/v1.0/myorg/groups/"
        f"{_guid(workspace_id)}/datasets/{_guid(model_id)}/refreshes/{_guid(request_id)}"
    )
    deadline = time.monotonic() + timeout_seconds

    def read_status() -> Response:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise NativeItemError(
                f"Direct Lake refresh timed out; inspect {request_id} before retrying."
            )
        return session.get(
            url, timeout=min(30, remaining), allow_redirects=False
        )

    while time.monotonic() < deadline:
        response = retry_read(read_status, deadline=deadline)
        if response.status_code not in (200, 202):
            response.raise_for_status()
            raise NativeItemError("Unexpected model refresh response.")
        body = response.json()
        status = str(body.get("status", "")).casefold()
        if status == "completed":
            return
        if status in {"failed", "disabled", "cancelled", "canceled"}:
            raise NativeItemError(
                f"Direct Lake refresh {status}; inspect refresh {request_id}. Credentials were not changed."
            )
        if status not in {"unknown", "notstarted", "inprogress"}:
            raise NativeItemError(f"Unexpected Direct Lake refresh state: {status}")
        time.sleep(min(5, max(0, deadline - time.monotonic())))
    raise NativeItemError(f"Direct Lake refresh timed out; inspect {request_id} before retrying.")


def refresh_model(
    credential: Any, workspace_id: str, model_id: str,
    *, on_started: Callable[[str], None] | None = None,
) -> str:
    session = _session(_token(PBI_SCOPE, credential))
    url = (
        "https://api.powerbi.com/v1.0/myorg/groups/"
        f"{_guid(workspace_id)}/datasets/{_guid(model_id)}/refreshes"
    )
    response = session.post(
        url, json={"type": "full", "commitMode": "transactional"},
        timeout=30, allow_redirects=False,
    )
    response.raise_for_status()
    if response.status_code != 202:
        raise NativeItemError("Model refresh did not return an accepted request.")
    identifier = response.headers.get("x-ms-request-id")
    if not identifier:
        # Only extract the operation UUID; never forward credentials to a supplied host.
        identifier = urlsplit(response.headers.get("Location", "")).path.rsplit("/", 1)[-1]
    request_id = str(UUID(identifier))
    if on_started is not None:
        on_started(request_id)
    wait_for_refresh(session, workspace_id, model_id, request_id)
    return request_id
