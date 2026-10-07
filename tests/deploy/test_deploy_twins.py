import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from deploy.scripts import deploy_twins as deploy
from deploy.scripts._twins_runtime import require_sso

ID = "11111111-1111-4111-8111-111111111111"


def config(enabled=True):
    return SimpleNamespace(
        twins=SimpleNamespace(enabled=enabled), auth_mode="azure_cli", tenant_id=ID,
        environment="dev", powerbi=SimpleNamespace(semantic_model_name="retail_model", report_name="retail_model"),
    )


def cohort():
    return {
        "dc_id": 9, "truck_ids": [10, 20], "store_ids": [50, 60],
        "product_ids": [70], "shipment_ids": ["actual-shipment"],
        "source_as_of": "2026-09-08T10:00:00+00:00", "window_start": "2026-08-25T10:00:00+00:00",
    }


def test_sso_guard_never_substitutes_a_fixed_connection(monkeypatch):
    from deploy.scripts import _twins_runtime

    for bindings in ([], [{"connectivityType": "ShareableCloud"}]):
        monkeypatch.setattr(_twins_runtime, "paginated_get", lambda *a, **k: bindings)
        with pytest.raises(deploy.NativeItemError, match="SSO"):
            require_sso(Mock(), ID, ID)


def test_disabled_twins_do_not_authenticate(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy, "validate_terraform_outputs", Mock())
    auth = Mock()
    monkeypatch.setattr(deploy, "build_credential", auth)
    with pytest.raises(deploy.NativeItemError, match="Enable"):
        deploy.publish_twins(tmp_path, config(False), {})
    auth.assert_not_called()


def test_primary_report_name_collision_fails_before_authentication(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy, "validate_terraform_outputs", Mock())
    auth = Mock()
    monkeypatch.setattr(deploy, "build_credential", auth)
    settings = config()
    settings.powerbi.report_name = "RETAIL-TWINS"
    with pytest.raises(deploy.NativeItemError, match="primary report"):
        deploy.publish_twins(tmp_path, settings, {})
    auth.assert_not_called()


def test_cli_handles_http_failure_without_a_success_message(monkeypatch, capsys):
    from requests import HTTPError

    monkeypatch.setattr(deploy, "load_environment", lambda *a, **k: config())
    monkeypatch.setattr(deploy, "load_terraform_outputs", lambda *a: {})
    monkeypatch.setattr(deploy, "publish_twins", Mock(side_effect=HTTPError("HTTP 403")))
    assert deploy.main(["--environment", "dev"]) == 1
    output = capsys.readouterr()
    assert "HTTP 403" in output.err
    assert "Published" not in output.out


def test_entity_queries_are_bounded_and_do_not_use_operator_web_app():
    metrics = [{"table": table, "name": f"Twin {index}"}
               for index, table in enumerate(("dim_stores", "dim_trucks", "dim_distribution_centers"))]
    queries = deploy.entity_queries(cohort(), metrics)
    assert len(queries) == 3
    assert all("TREATAS({10, 20}, 'dim_trucks'[ID])" in query for query in queries.values())
    bad = {**cohort(), "truck_ids": ["10); DROP TABLE"]}
    with pytest.raises(ValueError):
        deploy.entity_queries(bad, metrics)


def test_publication_retains_ssobinding_and_full_cohort_evidence(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy, "validate_terraform_outputs", Mock())
    monkeypatch.setattr(deploy, "build_credential", Mock())
    session = Mock()
    monkeypatch.setattr(deploy, "build_session", lambda *a: session)
    monkeypatch.setattr(deploy, "list_items", lambda *a: [{"id": ID, "displayName": "retail_model"}])
    monkeypatch.setattr(deploy, "require_sso", lambda *a: [{"connectivityType": "Automatic"}])
    monkeypatch.setattr(deploy, "ensure_workspace_active", Mock())
    network = {**cohort(), "shipment_ids": [f"shipment-{i}" for i in range(50)]}
    monkeypatch.setattr(deploy, "discover_cohort", lambda *a: network)
    renderer = ModuleType("deploy.scripts._twins_report")
    renderer.render_twins_report = Mock(return_value={"parts": []})
    monkeypatch.setitem(sys.modules, "deploy.scripts._twins_report", renderer)
    monkeypatch.setattr(deploy, "get_definition", lambda *a: {"parts": []})
    monkeypatch.setattr(deploy, "merge_twin_model", lambda *a: ({"parts": []}, []))
    monkeypatch.setattr(deploy, "load_metrics", lambda *a: [])
    refresh = Mock(return_value=ID)
    monkeypatch.setattr(deploy, "refresh_model", refresh)
    monkeypatch.setattr(deploy, "validate_report_model_fields", Mock())
    monkeypatch.setattr(deploy, "verify_entity_queries", lambda *a: {"verified": True})
    publish = Mock(return_value=ID)
    monkeypatch.setattr(deploy, "ensure_native_item", publish)
    result = deploy.publish_twins(tmp_path, config(), {"workspace_id": ID})
    assert result["status"] == "PUBLISHED"
    assert result["sso_preserved"] is True
    assert publish.call_args.kwargs["item_type"] == "Report"
    session.post.assert_not_called()
    import json
    saved = json.loads((tmp_path / "deploy" / ".generated" / "dev" / "twins-report.json").read_text())
    assert saved["cohort"]["shipment_ids"] == network["shipment_ids"]
    refresh.assert_called_once()
