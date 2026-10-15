"""Fabric UI-native Operations Agent configuration stays bounded and exact."""

from unittest.mock import Mock

import pytest

from deploy.scripts import _operations_ui as ui

ID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"


def response(status: int, body: dict):
    result = Mock(status_code=status, headers={})
    result.json.return_value = body
    return result


def test_ui_configuration_uses_fabric_native_source_and_action_fields():
    configuration = ui.render_ui_configuration(
        workspace_id=ID,
        kql_database_id=OTHER,
        kusto_uri="https://cluster.z0.kusto.fabric.microsoft.com",
        notebook_id=ID,
        instructions="Monitor bounded availability.",
    )

    source = configuration["dataSources"][OTHER]
    assert source["connection"]["tableName"] == OTHER
    assert source["connection"]["endpoint"].endswith(
        ".kusto.fabric.microsoft.com"
    )
    action = configuration["actions"]["prepareCase"]
    assert action["kind"] == "FabricJobAction"
    assert action["connection"] == {
        "jobType": "RunNotebook",
        "itemType": "SynapseNotebook",
        "itemArtifactId": ID,
        "itemWorkspaceId": ID,
        "subItemId": "",
    }
    assert "jobArtifactId" not in action["connection"]


@pytest.mark.parametrize(
    "url",
    [
        "http://cluster.z0.kusto.fabric.microsoft.com",
        "https://evil.example",
        "https://cluster.z0.kusto.fabric.microsoft.com?token=x",
    ],
)
def test_ui_configuration_rejects_untrusted_kusto_urls(url):
    with pytest.raises(ui.NativeItemError, match="untrusted"):
        ui.render_ui_configuration(
            workspace_id=ID,
            kql_database_id=OTHER,
            kusto_uri=url,
            notebook_id=ID,
            instructions="Monitor bounded availability.",
        )


def test_ui_configuration_posts_and_requires_exact_readback(monkeypatch):
    expected = ui.render_ui_configuration(
        workspace_id=ID,
        kql_database_id=OTHER,
        kusto_uri="https://cluster.z0.kusto.fabric.microsoft.com",
        notebook_id=ID,
        instructions="Monitor bounded availability.",
    )
    session = Mock()
    session.headers = {}
    session.post.return_value = response(200, expected)
    session.get.return_value = response(200, expected)
    monkeypatch.setattr(
        ui,
        "get_operations_ui_context",
        lambda *_args, **_kwargs: ui.OperationsUiContext(
            "https://capacity.pbidedicated.windows.net/ui", "token"
        ),
    )
    monkeypatch.setattr(ui.requests, "Session", lambda: session)

    actual = ui.ensure_operations_ui_configuration(
        Mock(),
        workspace_id=ID,
        agent_id=OTHER,
        kql_database_id=OTHER,
        kusto_uri="https://cluster.z0.kusto.fabric.microsoft.com",
        notebook_id=ID,
        instructions="Monitor bounded availability.",
    )

    assert actual == expected
    assert session.post.call_args.kwargs["json"] == expected
    assert session.headers["Authorization"] == "MwcToken token"


def test_ui_configuration_rejects_drifted_readback(monkeypatch):
    expected = ui.render_ui_configuration(
        workspace_id=ID,
        kql_database_id=OTHER,
        kusto_uri="https://cluster.z0.kusto.fabric.microsoft.com",
        notebook_id=ID,
        instructions="Monitor bounded availability.",
    )
    session = Mock()
    session.headers = {}
    session.post.return_value = response(200, expected)
    session.get.return_value = response(200, {**expected, "actions": {}})
    monkeypatch.setattr(
        ui,
        "get_operations_ui_context",
        lambda *_args, **_kwargs: ui.OperationsUiContext(
            "https://capacity.pbidedicated.windows.net/ui", "token"
        ),
    )
    monkeypatch.setattr(ui.requests, "Session", lambda: session)

    with pytest.raises(ui.NativeItemError, match="readback"):
        ui.ensure_operations_ui_configuration(
            Mock(),
            workspace_id=ID,
            agent_id=OTHER,
            kql_database_id=OTHER,
            kusto_uri="https://cluster.z0.kusto.fabric.microsoft.com",
            notebook_id=ID,
            instructions="Monitor bounded availability.",
        )
