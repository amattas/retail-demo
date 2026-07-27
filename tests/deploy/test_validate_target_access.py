"""Captured Fabric target-access tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from deploy.scripts.validate_target_access import (
    TargetAccessError,
    validate_target_access,
)


class _Response:
    def __init__(self, status: int, payload: dict[str, object]):
        self.status_code = status
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict[str, object]:
        return self._payload


class _Session:
    def __init__(self, response: _Response):
        self.response = response
        self.urls: list[str] = []

    def get(self, url: str) -> _Response:
        self.urls.append(url)
        return self.response


def _config(name: str = "retail-demo"):
    return SimpleNamespace(workspace=SimpleNamespace(name=name))


def test_captured_workspace_must_be_live_and_readable() -> None:
    session = _Session(
        _Response(
            200,
            {"id": "workspace-id", "displayName": "retail-demo"},
        )
    )

    report = validate_target_access(
        session,
        _config(),
        {"workspace_id": "workspace-id"},
    )

    assert report.workspace_name == "retail-demo"
    assert session.urls[0].endswith("/workspaces/workspace-id")


def test_unreadable_captured_workspace_fails_with_recovery_action() -> None:
    session = _Session(_Response(403, {}))

    with pytest.raises(TargetAccessError, match="Recapture Terraform outputs"):
        validate_target_access(
            session,
            _config(),
            {"workspace_id": "stale-id"},
        )


def test_workspace_name_mismatch_fails_closed() -> None:
    session = _Session(
        _Response(
            200,
            {"id": "workspace-id", "displayName": "other-workspace"},
        )
    )

    with pytest.raises(TargetAccessError, match="expected 'retail-demo'"):
        validate_target_access(
            session,
            _config(),
            {"workspace_id": "workspace-id"},
        )
