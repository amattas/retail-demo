"""Tests for the real-time Environment pool-binding helper."""

from __future__ import annotations

from deploy.scripts import configure_environment as ce


def test_publish_state_reads_publish_details() -> None:
    payload = {"publishDetails": {"state": "Success"}}
    assert ce._publish_state(payload) == "success"


def test_publish_state_reads_snake_case_and_lowercases() -> None:
    payload = {"publish_details": {"state": "Running"}}
    assert ce._publish_state(payload) == "running"


def test_publish_state_missing_details_returns_empty() -> None:
    assert ce._publish_state({}) == ""


def test_configure_binds_then_publishes(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        ce, "bind_pool", lambda **kwargs: calls.append(f"bind:{kwargs['pool_name']}")
    )
    monkeypatch.setattr(
        ce, "publish", lambda **kwargs: calls.append(f"publish:{kwargs['environment_id']}")
    )

    rc = ce.configure(
        workspace_id="ws",
        environment_id="env",
        pool_name="retail_realtime_pool",
        credential=object(),
    )

    assert rc == 0
    assert calls == ["bind:retail_realtime_pool", "publish:env"]
