"""Live tenant and capacity preflight tests."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from retail_setup.contracts import load_repository_manifest, resolve_profile

from deploy.scripts import live_profile_preflight
from deploy.scripts.deploy_config import _to_deploy_config
from deploy.scripts.live_profile_preflight import (
    LiveProfilePreflightError,
    validate_live_profile_preflight,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "deploy" / "config" / "deploy.yml"


def _config():
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    data["environment"] = "dev"
    data["tenant_id"] = "11111111-1111-4111-8111-111111111111"
    data["workspace"].update({"name": "dev", "capacity_name": "demo-capacity"})
    base = _to_deploy_config(data)
    manifest, validation = load_repository_manifest(REPO_ROOT)
    profile = resolve_profile(
        manifest,
        validation,
        "full-demo",
        available_item_types=base.deployment.available_item_types,
        configured_kql_scripts=data["eventhouse"]["kql_scripts"],
    )
    return replace(
        base,
        profile=profile,
        deployment=replace(
            base.deployment,
            profile="full-demo",
            item_types_in_scope=list(profile.item_types_in_scope),
        ),
        eventhouse=replace(
            base.eventhouse,
            enabled=True,
            kql_scripts=list(profile.kql_scripts),
        ),
        spark=replace(base.spark, use_custom_pool=True),
    )


def _capacity(*, sku: str = "F64", region: str = "West US 3"):
    return {
        "id": "capacity-id",
        "displayName": "demo-capacity",
        "sku": sku,
        "state": "Active",
        "region": region,
    }


def _setting(name: str, *, enabled: bool = True):
    return {
        "settingName": name,
        "title": name,
        "enabled": enabled,
        "canSpecifySecurityGroups": True,
    }


def _settings(*, ontology_enabled: bool = True):
    return [
        _setting("OntologyPreview", enabled=ontology_enabled),
        _setting("EnableAOAI"),
        _setting("CopilotCapacitySetupPermissionSwitch"),
        _setting("AllowSendAOAIDataToOtherRegions"),
        _setting("AllowStoreAOAIDataInOtherRegions"),
    ]


def _stub_collections(monkeypatch, capacities, settings):
    def get(_session, url):
        if url == live_profile_preflight.CAPACITIES_URL:
            return capacities
        if url == live_profile_preflight.TENANT_SETTINGS_URL:
            return settings
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(live_profile_preflight, "paginated_get", get)


def test_full_demo_live_preflight_passes_without_operator_acknowledgements(
    monkeypatch,
) -> None:
    _stub_collections(monkeypatch, [_capacity()], _settings())

    report = validate_live_profile_preflight(object(), _config())

    assert report.capacity_sku == "F64"
    assert report.checked_tenant_settings == (
        "OntologyPreview",
        "EnableAOAI",
        "CopilotCapacitySetupPermissionSwitch",
    )


def test_disabled_tenant_setting_reports_exact_admin_remediation(
    monkeypatch,
) -> None:
    _stub_collections(
        monkeypatch,
        [_capacity()],
        _settings(ontology_enabled=False),
    )

    with pytest.raises(LiveProfilePreflightError) as exc_info:
        validate_live_profile_preflight(object(), _config())

    assert exc_info.value.tenant_settings_fixable
    message = str(exc_info.value)
    assert "Enable tenant setting" in message
    assert "Users can create Ontology (preview) items" in message
    assert live_profile_preflight.TENANT_SETTINGS_PORTAL in message


def test_cross_boundary_capacity_requires_cross_region_ai_settings(
    monkeypatch,
) -> None:
    settings = [
        setting
        for setting in _settings()
        if setting["settingName"] != "AllowStoreAOAIDataInOtherRegions"
    ]
    _stub_collections(
        monkeypatch,
        [_capacity(region="Australia East")],
        settings,
    )

    with pytest.raises(LiveProfilePreflightError, match="stored outside"):
        validate_live_profile_preflight(object(), _config())


def test_full_demo_rejects_capacity_smaller_than_f64(monkeypatch) -> None:
    _stub_collections(monkeypatch, [_capacity(sku="F32")], _settings())

    with pytest.raises(LiveProfilePreflightError) as exc_info:
        validate_live_profile_preflight(object(), _config())

    assert not exc_info.value.tenant_settings_fixable
    assert "require F64 or larger" in str(exc_info.value)


def test_security_group_scoped_setting_fails_closed(monkeypatch) -> None:
    settings = _settings()
    settings[0]["enabledSecurityGroups"] = [
        {"graphId": "group-id", "name": "Fabric Preview Users"}
    ]
    _stub_collections(monkeypatch, [_capacity()], settings)

    with pytest.raises(LiveProfilePreflightError, match="security-group scoped"):
        validate_live_profile_preflight(object(), _config())
