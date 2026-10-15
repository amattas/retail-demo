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
    def get(_session, url, **_kwargs):
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
    assert report.capacity_sizing is not None
    assert report.capacity_sizing.max_node_count == 2
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
    assert "driver and one Medium executor" in str(exc_info.value)


def test_security_group_scoped_setting_fails_closed(monkeypatch) -> None:
    settings = _settings()
    settings[0]["enabledSecurityGroups"] = [
        {"graphId": "group-id", "name": "Fabric Preview Users"}
    ]
    _stub_collections(monkeypatch, [_capacity()], settings)

    with pytest.raises(LiveProfilePreflightError, match="security-group scoped"):
        validate_live_profile_preflight(object(), _config())


def test_read_only_preflight_does_not_resume_inactive_capacity(monkeypatch):
    capacity = {**_capacity(), "state": "Inactive"}
    _stub_collections(monkeypatch, [capacity], _settings())
    with pytest.raises(LiveProfilePreflightError, match="not Active"):
        validate_live_profile_preflight(object(), _config())


def test_missing_capacity_may_wait_but_ambiguous_capacity_never_resumes():
    config = _config()
    assert live_profile_preflight._resolve_capacity([], config, allow_missing=True) is None
    with pytest.raises(LiveProfilePreflightError, match="found 2"):
        live_profile_preflight._resolve_capacity(
            [_capacity(), _capacity()], config, allow_missing=True,
        )


@pytest.mark.parametrize("resume", [False, True])
@pytest.mark.parametrize("explicit_resource", [False, True])
def test_cli_resumes_only_when_requested_and_uses_configured_target(
    monkeypatch, tmp_path, resume, explicit_resource,
):
    from deploy.scripts import capacity_resume, deploy_config

    config = _config()
    resource = (
        "/subscriptions/11111111-1111-4111-8111-111111111111/resourceGroups/demo"
        "/providers/Microsoft.Fabric/capacities/demo-capacity"
    )
    resource = resource if explicit_resource else None
    config = replace(config, workspace=replace(config.workspace, capacity_resource_id=resource))
    monkeypatch.setattr(deploy_config, "load_environment", lambda *args, **kwargs: config)
    _stub_collections(monkeypatch, [_capacity()], _settings())
    sessions = []
    monkeypatch.setattr(
        live_profile_preflight, "build_session",
        lambda **kwargs: sessions.append(kwargs) or object(),
    )
    requests = []

    def ensure(get_capacity, **kwargs):
        assert get_capacity()["id"] == "capacity-id"
        requests.append(kwargs)

    monkeypatch.setattr(capacity_resume, "ensure_capacity_active", ensure)
    monkeypatch.setattr(
        live_profile_preflight.sys, "argv",
        ["live_profile_preflight", "--environment", "dev",
         "--auth-mode", "azure_powershell",
         "--capacity-sizing-output", str(tmp_path / "capacity-sizing.json"),
         *(["--resume-capacity"] if resume else [])],
    )
    assert live_profile_preflight.main() == 0
    evidence_path = tmp_path / "capacity-sizing.json"
    assert evidence_path.is_file()
    evidence = yaml.safe_load(evidence_path.read_text(encoding="utf-8"))
    assert evidence["environment"] == "dev"
    assert evidence["capacity_id"] == "capacity-id"
    assert evidence["producer_concurrency"] == 6
    assert evidence["max_node_count"] == 2
    assert sessions == [{"auth_mode": "azure_powershell", "tenant_id": config.tenant_id}]
    assert len(requests) == int(resume)
    if resume:
        assert requests[0]["resource_id"] == resource
        assert requests[0]["expected_name"] == config.workspace.capacity_name
        assert requests[0]["tenant_id"] == config.tenant_id


def test_cli_stops_when_resume_fails(monkeypatch):
    from deploy.scripts import capacity_resume, deploy_config

    monkeypatch.setattr(deploy_config, "load_environment", lambda *args, **kwargs: _config())
    monkeypatch.setattr(live_profile_preflight, "build_session", lambda **kwargs: object())
    monkeypatch.setattr(live_profile_preflight, "validate_live_profile_preflight",
                        lambda *args: pytest.fail("must not proceed after failed resume"))

    def fail(*args, **kwargs):
        raise capacity_resume.CapacityResumeError("permission denied")

    monkeypatch.setattr(capacity_resume, "ensure_capacity_active", fail)
    monkeypatch.setattr(
        live_profile_preflight.sys, "argv",
        ["live_profile_preflight", "--environment", "dev", "--resume-capacity"],
    )
    assert live_profile_preflight.main() == 1


def test_standalone_preflight_defaults_to_configured_authentication(
    monkeypatch,
    tmp_path,
):
    from deploy.scripts import deploy_config

    config = replace(_config(), auth_mode="azure_powershell")
    monkeypatch.setattr(deploy_config, "load_environment", lambda *args, **kwargs: config)
    _stub_collections(monkeypatch, [_capacity()], _settings())
    sessions = []
    monkeypatch.setattr(
        live_profile_preflight, "build_session",
        lambda **kwargs: sessions.append(kwargs) or object(),
    )
    monkeypatch.setattr(
        live_profile_preflight.sys,
        "argv",
        [
            "live_profile_preflight",
            "--environment",
            "dev",
            "--capacity-sizing-output",
            str(tmp_path / "capacity-sizing.json"),
        ],
    )
    assert live_profile_preflight.main() == 0
    assert sessions == [{"auth_mode": "azure_powershell", "tenant_id": config.tenant_id}]
