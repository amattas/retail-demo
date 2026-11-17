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


def _workload(monkeypatch, behavior_model, settings_state="Active"):
    session = Mock()
    session.get.side_effect = [*behavior_model, response(200, {"agentState": settings_state})]
    session.patch.return_value = response(200, {"agentState": "Active"})
    monkeypatch.setattr(
        ui,
        "get_operations_ui_context",
        lambda *_args, **_kwargs: ui.OperationsUiContext(
            "https://capacity.pbidedicated.windows.net/ui", "token"
        ),
    )
    monkeypatch.setattr(ui, "_workload_session", lambda _context: session)
    return session


PLAYBOOK = {
    "ontology": "...",
    "rules": "<details><summary>High risk</summary></details>"
    "<details><summary>Low inventory</summary></details>",
}
GENERATING = response(409, {"code": "BehaviorModelGenerationInProgress"})


def test_agent_starts_after_fabric_finishes_generating_its_playbook(monkeypatch):
    session = _workload(monkeypatch, [GENERATING, GENERATING, response(200, PLAYBOOK)])
    waits = []

    result = ui.activate_operations_agent(
        Mock(), workspace_id=ID, agent_id=OTHER, sleep=waits.append, clock=lambda: 0
    )

    assert result == {"state": "ACTIVE", "rule_count": 2}
    assert len(waits) == 2
    assert session.patch.call_args.kwargs["json"] == {"agentState": "Active"}


@pytest.mark.parametrize(
    "behavior_model,match",
    [
        ([response(200, {"ontology": "...", "rules": ""})], "no rules"),
        ([response(400, {"code": "InvalidConfiguration"})], "InvalidConfiguration"),
        ([GENERATING], "did not finish"),
    ],
    ids=["empty-playbook", "generation-error", "timeout"],
)
def test_agent_is_never_started_without_a_generated_playbook(monkeypatch, behavior_model, match):
    session = _workload(monkeypatch, behavior_model)
    clock = iter([0, 1000])

    with pytest.raises(ui.NativeItemError, match=match):
        ui.activate_operations_agent(
            Mock(), workspace_id=ID, agent_id=OTHER, sleep=Mock(), clock=lambda: next(clock)
        )

    session.patch.assert_not_called()


def test_agent_start_requires_active_readback(monkeypatch):
    _workload(monkeypatch, [response(200, PLAYBOOK)], settings_state="Inactive")

    with pytest.raises(ui.NativeItemError, match="Inactive"):
        ui.activate_operations_agent(
            Mock(), workspace_id=ID, agent_id=OTHER, sleep=Mock(), clock=lambda: 0
        )
