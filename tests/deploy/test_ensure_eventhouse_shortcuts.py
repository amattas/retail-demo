"""Tests for safe Eventhouse-to-Lakehouse shortcut provisioning."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from deploy.scripts import deploy_config
from deploy.scripts import ensure_eventhouse_shortcuts as shortcuts

REPO_ROOT = Path(__file__).resolve().parents[2]


def _config() -> deploy_config.DeployConfig:
    config = deploy_config.load_environment("retail-demo")
    return replace(
        config,
        streaming_shortcuts=deploy_config.StreamingShortcutsConfig(enabled=True),
    )


def _outputs() -> dict[str, str]:
    return {
        "workspace_id": "workspace",
        "lakehouse_id": "lakehouse",
        "kql_database_id": "database",
    }


def _valid_shortcut() -> dict[str, object]:
    return {
        "name": "receipt_created",
        "path": "Tables/cusn",
        "target": {
            "type": "OneLake",
            "oneLake": {
                "workspaceId": "workspace",
                "itemId": "database",
                "path": "Tables/receipt_created",
            },
        },
    }


def _prepare_common_reads(monkeypatch, *, live_tables, policies, existing_shortcut):
    monkeypatch.setattr(shortcuts, "validate_terraform_outputs", lambda *_: None)
    monkeypatch.setattr(shortcuts, "expected_event_tables", lambda *_: ("receipt_created",))
    monkeypatch.setattr(shortcuts, "build_credential", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        shortcuts, "resolve_kql_database", lambda *_: ("https://kusto.invalid", "db")
    )
    monkeypatch.setattr(shortcuts, "build_session", lambda *_: MagicMock())
    monkeypatch.setattr(
        shortcuts, "_read_live_event_tables", lambda *_: set(live_tables)
    )
    monkeypatch.setattr(
        shortcuts, "_read_shortcut", lambda *_: existing_shortcut
    )
    monkeypatch.setattr(
        shortcuts,
        "_read_mirror_policy",
        lambda _uri, _db, _credential, table: policies[table],
    )


def test_existing_shortcut_must_match_the_eventhouse_source():
    shortcut = _valid_shortcut()
    shortcut["target"]["oneLake"]["workspaceId"] = "another-workspace"

    with pytest.raises(shortcuts.ShortcutProvisionError, match="expected Eventhouse"):
        shortcuts._validate_shortcut(
            shortcut, "workspace", "database", "receipt_created"
        )


def test_malformed_shortcut_response_fails_validation():
    with pytest.raises(shortcuts.ShortcutProvisionError, match="malformed"):
        shortcuts._validate_shortcut(
            [], "workspace", "database", "receipt_created"
        )


def test_missing_eventhouse_table_stops_before_shortcut_or_mirror_writes(
    monkeypatch,
):
    _prepare_common_reads(
        monkeypatch,
        live_tables=set(),
        policies={},
        existing_shortcut=None,
    )
    ensure_mirroring = MagicMock()
    monkeypatch.setattr(shortcuts, "_ensure_mirroring", ensure_mirroring)

    with pytest.raises(shortcuts.ShortcutProvisionError, match="receipt_created"):
        shortcuts.ensure_shortcuts(REPO_ROOT, _config(), _outputs())

    ensure_mirroring.assert_not_called()


def test_disabled_mirroring_policy_is_enabled_even_when_policy_has_other_values(
    monkeypatch,
):
    policy = {"IsEnabled": False, "Backfill": True}
    _prepare_common_reads(
        monkeypatch,
        live_tables={"receipt_created"},
        policies={"receipt_created": policy},
        existing_shortcut={"name": "receipt_created"},
    )
    enabled_policy = {"IsEnabled": True, "Backfill": True}
    ensure_mirroring = MagicMock(return_value=enabled_policy)
    monkeypatch.setattr(shortcuts, "_ensure_mirroring", ensure_mirroring)

    report = shortcuts.ensure_shortcuts(REPO_ROOT, _config(), _outputs())

    ensure_mirroring.assert_called_once()
    assert report["status"] == "SUCCEEDED"
    assert report["tables"][0]["mirroring_enabled"] is True


def test_enabled_mirroring_without_backfill_fails_before_mutation(monkeypatch):
    _prepare_common_reads(
        monkeypatch,
        live_tables={"receipt_created"},
        policies={"receipt_created": {"IsEnabled": True, "Backfill": False}},
        existing_shortcut=None,
    )
    ensure_mirroring = MagicMock()
    monkeypatch.setattr(shortcuts, "_ensure_mirroring", ensure_mirroring)

    with pytest.raises(shortcuts.ShortcutProvisionError, match="Backfill"):
        shortcuts.ensure_shortcuts(REPO_ROOT, _config(), _outputs())

    ensure_mirroring.assert_not_called()
