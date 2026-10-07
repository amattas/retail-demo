import json
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from deploy.scripts import deploy_operations_planning as deploy

ID = "11111111-1111-4111-8111-111111111111"


def config(**changes):
    return SimpleNamespace(
        environment="dev",
        auth_mode="azure_cli",
        tenant_id=ID,
        operations_planning=SimpleNamespace(
            **{
                "enabled": True,
                "include_plan": True,
                "plan_connection_id": None,
                "plan_billing_acknowledged": False,
                "recipient": None,
                **changes,
            },
        ),
        powerbi=SimpleNamespace(semantic_model_name="retail_model"),
        workspace=SimpleNamespace(name="retail-demo"),
        lakehouse=SimpleNamespace(name="retail_lakehouse"),
    )


@pytest.mark.parametrize(
    "settings,expected",
    [
        ({"enabled": False}, "Enable"),
        ({}, "shared Power BI Semantic Model"),
        ({"plan_connection_id": ID}, "30-day"),
    ],
)
def test_missing_prerequisites_fail_before_authentication(
    monkeypatch, tmp_path, settings, expected
):
    monkeypatch.setattr(deploy, "validate_terraform_outputs", Mock())
    credential = Mock()
    monkeypatch.setattr(deploy, "build_credential", credential)
    with pytest.raises(deploy.NativeItemError, match=expected):
        deploy.publish(repo_root=tmp_path, config=config(**settings), outputs={})
    credential.assert_not_called()


def connection(**changes):
    return {
        "connectivityType": "ShareableCloud",
        "connectionDetails": {
            "type": "PowerBIDatasets",
            "path": "",
        },
        "credentialDetails": {"singleSignOnType": "None", "credentialType": "OAuth2"},
        **changes,
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"connectivityType": "PersonalCloud"},
        {
            "credentialDetails": {
                "singleSignOnType": "None",
                "credentialType": "WorkspaceIdentity",
            }
        },
        {
            "credentialDetails": {
                "singleSignOnType": "MicrosoftEntraID",
                "credentialType": "OAuth2",
            }
        },
        {"connectionDetails": {"type": "SQL", "path": "wrong"}},
        {"connectionDetails": {"type": "AnalysisServices", "path": "other;model"}},
    ],
)
def test_invalid_planning_connections_are_rejected(changes):
    session = Mock()
    session.get.return_value.json.return_value = connection(**changes)
    with pytest.raises(deploy.NativeItemError):
        deploy.validate_plan_connection(session, ID)
    session.post.assert_not_called()


def test_shared_fixed_correct_model_connection_passes():
    session = Mock()
    session.get.return_value.json.return_value = connection()
    deploy.validate_plan_connection(session, ID)
    session.post.assert_not_called()


def test_cli_reports_prerequisite_failure_without_success(monkeypatch, capsys):
    monkeypatch.setattr(deploy, "load_environment", lambda *a, **k: config())
    monkeypatch.setattr(deploy, "load_terraform_outputs", lambda *a: {})
    monkeypatch.setattr(deploy, "publish", Mock(side_effect=deploy.NativeItemError("Missing connection")))
    assert deploy.main(["--environment", "dev"]) == 1
    output = capsys.readouterr()
    assert "Missing connection" in output.err
    assert "Published" not in output.out


def test_cli_dry_run_never_calls_publisher(monkeypatch):
    monkeypatch.setattr(deploy, "load_environment", lambda *a, **k: config())
    publisher = Mock()
    monkeypatch.setattr(deploy, "publish", publisher)
    assert deploy.main(["--environment", "dev", "--dry-run"]) == 0
    publisher.assert_not_called()


def test_disabled_dry_run_does_not_promise_publication(monkeypatch, capsys):
    monkeypatch.setattr(deploy, "load_environment", lambda *a, **k: config(enabled=False))
    publisher = Mock()
    monkeypatch.setattr(deploy, "publish", publisher)
    assert deploy.main(["--environment", "dev", "--dry-run"]) == 0
    output = capsys.readouterr().out
    assert "disabled" in output
    assert "Would publish" not in output
    publisher.assert_not_called()


@pytest.fixture
def publication(monkeypatch):
    monkeypatch.setattr(deploy, "validate_terraform_outputs", Mock())
    monkeypatch.setattr(deploy, "build_credential", Mock())
    session = Mock()
    monkeypatch.setattr(deploy, "build_session", lambda *a: session)
    monkeypatch.setattr(
        deploy, "list_items", lambda *a: [{"id": ID, "displayName": "retail_model"}]
    )
    monkeypatch.setattr(deploy, "_source_rows", lambda *a: [])
    monkeypatch.setattr(deploy, "_ensure_native_capacity", Mock())
    monkeypatch.setattr(deploy, "scenario_signals", lambda *a: [{"case_id": ID}])
    monkeypatch.setattr(
        deploy, "resolve_kql_database", lambda *a: ("https://kusto.test", "retail")
    )
    monkeypatch.setattr(deploy, "build_database_script", lambda *a: "DDL")
    monkeypatch.setattr(deploy, "signal_ingestion_query", lambda *a: "SEED")
    monkeypatch.setattr(deploy, "execute_database_script", Mock())
    monkeypatch.setattr(deploy, "_verify_signals", Mock())
    publish_item = Mock(return_value=ID)
    monkeypatch.setattr(deploy, "ensure_native_item", publish_item)
    return session, publish_item


@pytest.mark.parametrize("configured_operations_only", [False, True])
def test_operations_only_persists_stopped_items_without_plan_or_action_execution(
    publication, monkeypatch, tmp_path, configured_operations_only,
):
    session, publish_item = publication
    plan_checks = [Mock(), Mock(), Mock()]
    for name, check in zip(
        ("validate_plan_connection", "validate_model_connections", "validate_planning_measures"),
        plan_checks, strict=True,
    ):
        monkeypatch.setattr(deploy, name, check)
    result = deploy.publish(
        repo_root=tmp_path,
        config=config(include_plan=not configured_operations_only),
        operations_only=not configured_operations_only,
        outputs={"workspace_id": ID, "lakehouse_id": ID, "kql_database_id": ID},
    )
    assert [call.kwargs["item_type"] for call in publish_item.call_args_list] == [
        "Notebook",
        "OperationsAgent",
    ]
    assert result["operations_agent"]["state"] == "STOPPED"
    assert result["plan"]["state"] == (
        "NOT_SELECTED" if configured_operations_only else "NOT_REQUESTED"
    )
    assert "SSO" in result["acceptance"]
    for check in plan_checks:
        check.assert_not_called()
    persisted = (
        tmp_path / "deploy" / ".generated" / "dev" / "operations-planning-report.json"
    )
    assert json.loads(persisted.read_text()) == result
    session.post.assert_not_called()


def test_plan_failure_retains_real_operations_evidence_and_records_failure(
    publication, monkeypatch, tmp_path
):
    _session, publish_item = publication
    publish_item.side_effect = [ID, ID, deploy.NativeItemError("Rejected")]
    module = ModuleType("deploy.scripts._plan_definition")
    module.render_plan_definition = Mock(return_value={"parts": []})
    monkeypatch.setitem(sys.modules, "deploy.scripts._plan_definition", module)
    monkeypatch.setattr(deploy, "validate_plan_connection", Mock())
    monkeypatch.setattr(deploy, "validate_model_connections", Mock())
    monkeypatch.setattr(deploy, "validate_planning_measures", Mock())
    with pytest.raises(deploy.NativeItemError, match="Rejected"):
        deploy.publish(
            repo_root=tmp_path,
            config=config(plan_connection_id=ID, plan_billing_acknowledged=True),
            outputs={"workspace_id": ID, "lakehouse_id": ID, "kql_database_id": ID},
        )
    path = tmp_path / "deploy" / ".generated" / "dev" / "operations-planning-report.json"
    report = json.loads(path.read_text())
    assert report["operations_agent"]["id"] == ID
    assert report["plan"]["state"] == "PUBLICATION_FAILED"


def test_agent_rejection_preserves_supporting_asset_evidence(publication, tmp_path):
    _session, publish_item = publication
    publish_item.side_effect = [ID, deploy.NativeItemError("Create OperationsAgent failed: HTTP 400")]
    with pytest.raises(deploy.NativeItemError, match="Create OperationsAgent"):
        deploy.publish(
            repo_root=tmp_path, config=config(include_plan=False),
            outputs={"workspace_id": ID, "lakehouse_id": ID, "kql_database_id": ID},
        )
    path = tmp_path / "deploy" / ".generated" / "dev" / "operations-planning-report.json"
    report = json.loads(path.read_text())
    assert report["action_notebook"]["id"] == ID
    assert report["operations_agent"]["state"] == "PUBLICATION_FAILED"
    assert report["plan"]["state"] == "NOT_SELECTED"


@pytest.mark.parametrize("bindings", [
    [], [{"connectivityType": "Automatic"}], [{"connectivityType": "None"}],
])
def test_missing_or_sso_backing_connections_fail_closed(monkeypatch, bindings):
    monkeypatch.setattr(deploy, "paginated_get", lambda *a, **k: bindings)
    with pytest.raises(deploy.NativeItemError):
        deploy.validate_model_connections(Mock(), ID, ID)


@pytest.mark.parametrize("complete", [False, True])
def test_model_requires_real_seven_day_measures(monkeypatch, complete):
    from deploy.scripts._native_items import json_part

    monkeypatch.setattr(deploy, "get_definition", lambda *args: {
        "parts": [json_part("unused.json", {})],
    })
    text = "\n".join(f"\tmeasure '{name}' = 1" for name in deploy.PLAN_MEASURES)
    monkeypatch.setattr(deploy, "decode_definition_parts", lambda *args: {
        "definition/tables/demand_forecast.tmdl": text.encode() if complete else b"table demand_forecast",
    })
    if complete:
        deploy.validate_planning_measures(Mock(), ID, ID)
    else:
        with pytest.raises(deploy.NativeItemError, match="updated Reporting"):
            deploy.validate_planning_measures(Mock(), ID, ID)


@pytest.mark.parametrize("sso", ["None", "MicrosoftEntraID"])
def test_backing_workspace_identity_is_allowed_only_without_sso(monkeypatch, sso):
    monkeypatch.setattr(deploy, "paginated_get", lambda *a, **k: [
        {"id": ID, "connectivityType": "ShareableCloud"},
    ])
    session = Mock()
    session.get.return_value.json.return_value = {
        "credentialDetails": {"credentialType": "WorkspaceIdentity", "singleSignOnType": sso},
    }
    if sso == "None":
        deploy.validate_model_connections(session, ID, ID)
    else:
        with pytest.raises(deploy.NativeItemError, match="fixed-credential"):
            deploy.validate_model_connections(session, ID, ID)


@pytest.mark.parametrize("sku,region", [("FT1", "West US 3"), ("F64", "East US")])
def test_unsupported_capacity_fails_before_resume(monkeypatch, sku, region):
    settings = config()
    settings.subscription_id = None
    settings.workspace.capacity_name = "retail"
    settings.workspace.capacity_resource_id = None
    session = Mock()
    workspace, capacity = Mock(), Mock()
    workspace.json.return_value = {"displayName": "retail-demo", "capacityId": ID}
    capacity.json.return_value = {"sku": sku, "region": region}
    session.get.side_effect = [workspace, capacity]
    monkeypatch.setattr(
        deploy, "ensure_capacity_active", lambda read, **kwargs: read(),
    )
    arm = Mock()
    monkeypatch.setattr(deploy, "build_arm_session", arm)
    with pytest.raises(deploy.NativeItemError, match="paid capacity outside East US"):
        deploy._ensure_native_capacity(session, settings, ID)
    arm.assert_not_called()
