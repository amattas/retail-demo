"""Tests for the post-destroy Fabric workspace-deletion poll.

These exercise `deploy.scripts._workspace_wait.wait_for_workspace_absence`
directly: success once the workspace disappears, a bounded timeout when it
never does, and that the selected `auth_mode` is always honored via the
shared `build_credential` helper (never silently substituting a different
login).
"""

from __future__ import annotations

import pytest

from deploy.scripts import _workspace_wait


class _FakeToken:
    token = "unused-in-tests"  # noqa: S105 - not a real credential


class _FakeCredential:
    def get_token(self, *_scopes: str) -> _FakeToken:
        return _FakeToken()


class _FakeResponse:
    def __init__(
        self,
        workspaces: list[str],
        *,
        continuation_uri: str | None = None,
        continuation_token: str | None = None,
    ) -> None:
        self._workspaces = workspaces
        self._continuation_uri = continuation_uri
        self._continuation_token = continuation_token

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        payload: dict = {"value": [{"displayName": name} for name in self._workspaces]}
        if self._continuation_uri:
            payload["continuationUri"] = self._continuation_uri
        if self._continuation_token:
            payload["continuationToken"] = self._continuation_token
        return payload


def test_wait_for_workspace_absence_returns_once_workspace_disappears(
    monkeypatch,
) -> None:
    responses = [
        _FakeResponse(["retail-demo-dev", "other"]),
        _FakeResponse(["retail-demo-dev", "other"]),
        _FakeResponse(["other"]),
    ]
    calls = {"n": 0}

    def fake_get(*_args, **_kwargs):
        response = responses[calls["n"]]
        calls["n"] += 1
        return response

    monkeypatch.setattr(_workspace_wait, "_default_http_get", fake_get)
    sleeps: list[float] = []

    _workspace_wait.wait_for_workspace_absence(
        "retail-demo-dev",
        credential=_FakeCredential(),
        timeout_seconds=60,
        poll_interval_seconds=5,
        sleep=sleeps.append,
        clock=iter([0.0, 0.0, 5.0, 5.0, 10.0, 10.0]).__next__,
    )

    assert calls["n"] == 3
    assert sleeps == [5, 5]


def test_wait_for_workspace_absence_times_out_when_still_present(monkeypatch) -> None:
    monkeypatch.setattr(
        _workspace_wait,
        "_default_http_get",
        lambda *_a, **_k: _FakeResponse(["retail-demo-dev"]),
    )
    clock_values = iter([0.0, 0.0, 5.0, 5.0, 10.0, 10.0])

    with pytest.raises(
        _workspace_wait.WorkspaceDeletionTimeout, match="retail-demo-dev"
    ):
        _workspace_wait.wait_for_workspace_absence(
            "retail-demo-dev",
            credential=_FakeCredential(),
            timeout_seconds=10,
            poll_interval_seconds=5,
            sleep=lambda _s: None,
            clock=lambda: next(clock_values),
        )


def test_wait_for_workspace_absence_is_case_insensitive(monkeypatch) -> None:
    monkeypatch.setattr(
        _workspace_wait,
        "_default_http_get",
        lambda *_a, **_k: _FakeResponse(["Retail-Demo-Dev"]),
    )

    with pytest.raises(_workspace_wait.WorkspaceDeletionTimeout):
        _workspace_wait.wait_for_workspace_absence(
            "retail-demo-dev",
            credential=_FakeCredential(),
            timeout_seconds=0,
            poll_interval_seconds=1,
            sleep=lambda _s: None,
            clock=lambda: 0.0,
        )


def test_wait_for_workspace_absence_builds_credential_for_selected_auth_mode(
    monkeypatch,
) -> None:
    """The polling helper must honor `auth_mode`, never silently using a
    different login (e.g. Azure CLI when `azure_powershell` is configured)."""

    seen_modes = []

    def fake_build_credential(auth_mode: str, **_kwargs):
        seen_modes.append(auth_mode)
        return _FakeCredential()

    monkeypatch.setattr(_workspace_wait, "build_credential", fake_build_credential)
    monkeypatch.setattr(
        _workspace_wait, "_default_http_get", lambda *_a, **_k: _FakeResponse([])
    )

    _workspace_wait.wait_for_workspace_absence(
        "retail-demo-dev",
        auth_mode="azure_powershell",
        timeout_seconds=5,
        poll_interval_seconds=1,
        sleep=lambda _s: None,
        clock=lambda: 0.0,
    )

    assert seen_modes == ["azure_powershell"]


def test_wait_for_workspace_absence_rejects_unsupported_auth_mode() -> None:
    with pytest.raises(ValueError, match="Unsupported auth mode"):
        _workspace_wait.wait_for_workspace_absence(
            "retail-demo-dev",
            auth_mode="managed_identity",
            timeout_seconds=1,
            poll_interval_seconds=1,
        )


def test_wait_for_workspace_absence_finds_workspace_only_present_on_second_page(
    monkeypatch,
) -> None:
    """The target only appears on page 2 via `continuationToken`; the poll
    must not declare absence after reading page 1 alone."""

    def fake_get(url: str, headers=None, params=None, timeout=None):
        if params and params.get("continuationToken") == "tok-1":
            return _FakeResponse(["retail-demo-dev"])
        return _FakeResponse(["other"], continuation_token="tok-1")

    monkeypatch.setattr(_workspace_wait, "_default_http_get", fake_get)

    with pytest.raises(
        _workspace_wait.WorkspaceDeletionTimeout, match="retail-demo-dev"
    ):
        _workspace_wait.wait_for_workspace_absence(
            "retail-demo-dev",
            credential=_FakeCredential(),
            timeout_seconds=0,
            poll_interval_seconds=1,
            sleep=lambda _s: None,
            clock=lambda: 0.0,
        )


def test_wait_for_workspace_absence_follows_continuation_uri_across_pages(
    monkeypatch,
) -> None:
    """A `continuationUri` (rather than a bare token) must also be followed
    to the next page before the workspace is declared absent."""

    next_page_url = "https://api.fabric.microsoft.com/v1/workspaces?page=2"

    def fake_get(url: str, headers=None, params=None, timeout=None):
        if url == next_page_url:
            return _FakeResponse(["retail-demo-dev"])
        return _FakeResponse(["other"], continuation_uri=next_page_url)

    monkeypatch.setattr(_workspace_wait, "_default_http_get", fake_get)

    with pytest.raises(
        _workspace_wait.WorkspaceDeletionTimeout, match="retail-demo-dev"
    ):
        _workspace_wait.wait_for_workspace_absence(
            "retail-demo-dev",
            credential=_FakeCredential(),
            timeout_seconds=0,
            poll_interval_seconds=1,
            sleep=lambda _s: None,
            clock=lambda: 0.0,
        )


def test_wait_for_workspace_absence_fails_closed_on_repeated_continuation_token(
    monkeypatch,
) -> None:
    """A malformed/looping API response (same continuation token forever)
    must not hang the poll, but it must also never be treated as evidence
    of absence: a repeated token/URI has to raise `WorkspacePaginationError`
    rather than silently returning an incomplete workspace set."""

    call_count = {"n": 0}

    def fake_get(url: str, headers=None, params=None, timeout=None):
        call_count["n"] += 1
        # Always advertises the *same* continuation token, simulating a
        # buggy or looping API response. The target workspace is never
        # actually listed, so a fail-open implementation would wrongly
        # report it absent.
        return _FakeResponse(["other"], continuation_token="tok-loop")

    monkeypatch.setattr(_workspace_wait, "_default_http_get", fake_get)

    with pytest.raises(
        _workspace_wait.WorkspacePaginationError, match="repeated pagination"
    ):
        _workspace_wait.wait_for_workspace_absence(
            "retail-demo-dev",
            credential=_FakeCredential(),
            timeout_seconds=5,
            poll_interval_seconds=1,
            sleep=lambda _s: None,
            clock=lambda: 0.0,
        )

    # First page + one repeat of the same token before the fail-closed guard
    # raises -- it stops paging rather than looping forever.
    assert call_count["n"] == 2
