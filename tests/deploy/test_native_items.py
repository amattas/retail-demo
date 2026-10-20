"""Native item publishing must remain idempotent, bounded, and stopped."""

from unittest.mock import Mock

import pytest

from deploy.scripts import _native_items as native

WS, ITEM, OP = (f"{value:08d}-1111-4111-8111-111111111111" for value in (1, 2, 3))


def response(status=200, body=None, headers=None):
    return Mock(
        status_code=status, headers=headers or {}, json=Mock(return_value=body or {})
    )


def definition(value=False):
    return {"parts": [native.json_part("Configurations.json", {"shouldRun": value})]}


def test_existing_definition_is_read_without_update(monkeypatch):
    session = Mock()
    monkeypatch.setattr(
        native, "paginated_get", lambda *a, **k: [{"id": ITEM, "displayName": "Agent"}]
    )
    session.post.return_value = response(body={"definition": definition()})
    assert (
        native.ensure_native_item(
            session,
            workspace_id=WS,
            item_type="OperationsAgent",
            display_name="Agent",
            description="Stopped",
            definition=definition(),
        )
        == ITEM
    )
    assert session.post.call_count == 1
    assert session.post.call_args.args[0].endswith("/getDefinition")


def test_duplicate_names_reject_without_mutation(monkeypatch):
    session = Mock()
    monkeypatch.setattr(
        native,
        "paginated_get",
        lambda *a, **k: [
            {"id": ITEM, "displayName": "Agent"},
            {"id": OP, "displayName": "agent"},
        ],
    )
    with pytest.raises(native.NativeItemError, match="Multiple"):
        native.ensure_native_item(
            session,
            workspace_id=WS,
            item_type="OperationsAgent",
            display_name="Agent",
            description="Stopped",
            definition=definition(),
        )
    session.post.assert_not_called()


def test_create_uses_returned_identity_and_reads_definition(monkeypatch):
    session = Mock()
    monkeypatch.setattr(native, "paginated_get", lambda *a, **k: [])
    session.post.side_effect = [
        response(201, {"id": ITEM}),
        response(200),
        response(body={"definition": definition()}),
    ]
    assert (
        native.ensure_native_item(
            session,
            workspace_id=WS,
            item_type="OperationsAgent",
            display_name="Agent",
            description="Stopped",
            definition=definition(),
        )
        == ITEM
    )
    assert session.post.call_count == 3
    assert session.post.call_args_list[0].kwargs["json"] == {
        "displayName": "Agent",
        "description": "Stopped",
    }
    assert session.post.call_args_list[1].kwargs["json"] == {
        "definition": definition()
    }


def test_agent_definition_failure_removes_only_new_blank_shell(monkeypatch):
    session = Mock()
    monkeypatch.setattr(native, "paginated_get", lambda *a, **k: [])
    session.post.side_effect = [
        response(201, {"id": ITEM}),
        response(
            400,
            {
                "errorCode": "InvalidInput",
                "message": "Rejected",
                "requestId": OP,
            },
        ),
    ]
    session.delete.return_value = response(204)

    with pytest.raises(native.NativeItemError, match="Rejected") as exc_info:
        native.ensure_native_item(
            session,
            workspace_id=WS,
            item_type="OperationsAgent",
            display_name="Agent",
            description="Stopped",
            definition=definition(),
        )
    assert OP in str(exc_info.value)

    session.delete.assert_called_once_with(
        f"{native.API}/workspaces/{WS}/operationsAgents/{ITEM}",
        timeout=30,
        allow_redirects=False,
    )


def test_agent_readback_mismatch_removes_new_shell(monkeypatch):
    session = Mock()
    monkeypatch.setattr(native, "paginated_get", lambda *a, **k: [])
    monkeypatch.setattr(native.time, "sleep", lambda _: None)
    session.post.side_effect = [
        response(201, {"id": ITEM}),
        response(200),
        *[response(body={"definition": definition(True)}) for _ in range(5)],
    ]
    session.delete.return_value = response(204)

    with pytest.raises(native.NativeItemError, match="readback does not match"):
        native.ensure_native_item(
            session,
            workspace_id=WS,
            item_type="OperationsAgent",
            display_name="Agent",
            description="Stopped",
            definition=definition(),
        )

    session.delete.assert_called_once()


def test_live_agent_definition_is_stopped_on_publication(monkeypatch):
    session = Mock()
    monkeypatch.setattr(
        native, "paginated_get", lambda *a, **k: [{"id": ITEM, "displayName": "Agent"}]
    )
    session.post.side_effect = [
        response(body={"definition": definition(True)}),
        response(200),
        response(body={"definition": definition()}),
    ]
    native.ensure_native_item(
        session,
        workspace_id=WS,
        item_type="OperationsAgent",
        display_name="Agent",
        description="Stopped",
        definition=definition(),
    )
    assert session.post.call_args_list[1].kwargs["json"] == {"definition": definition()}


def test_agent_update_uses_documented_platform_metadata_contract(monkeypatch):
    expected = {
        "format": "OperationsAgentV1",
        "parts": [
            native.json_part("Configurations.json", {"shouldRun": False}),
            native.json_part(".platform", {"metadata": {"displayName": "Agent"}}),
        ],
    }
    stale = {
        "parts": [
            native.json_part("Configurations.json", {"shouldRun": True}),
            native.json_part(".platform", {"metadata": {"displayName": "Agent"}}),
        ],
    }
    session = Mock()
    monkeypatch.setattr(
        native, "paginated_get", lambda *a, **k: [{"id": ITEM, "displayName": "Agent"}]
    )
    session.post.side_effect = [
        response(body={"definition": stale}),
        response(200),
        response(body={"definition": expected}),
    ]

    assert native.ensure_native_item(
        session,
        workspace_id=WS,
        item_type="OperationsAgent",
        display_name="Agent",
        description="Stopped",
        definition=expected,
    ) == ITEM

    update = session.post.call_args_list[1]
    assert update.kwargs["params"] == {"updateMetadata": "true"}
    assert update.kwargs["json"] == {
        "definition": {"parts": expected["parts"]}
    }


def test_notebook_readback_requests_ipynb_format():
    session = Mock()
    session.post.return_value = response(body={"definition": definition()})
    native.get_definition(session, WS, "Notebook", ITEM, "ipynb")
    assert session.post.call_args.kwargs["params"] == {"format": "ipynb"}


def test_read_after_create_retries_not_yet_visible_definition(monkeypatch):
    monkeypatch.setattr(native.time, "sleep", lambda _: None)
    session = Mock()
    session.post.side_effect = [
        response(404),
        response(body={"definition": definition()}),
    ]
    assert native.get_definition(session, WS, "OperationsAgent", ITEM) == definition()
    assert session.post.call_count == 2


def test_permission_denied_is_not_retried():
    session = Mock()
    session.post.return_value = response(403)
    with pytest.raises(native.NativeItemError, match="403"):
        native.get_definition(session, WS, "OperationsAgent", ITEM)
    session.post.assert_called_once()


def test_lro_polling_and_result_query_handling(monkeypatch):
    session = Mock()
    initial = response(
        202, headers={"Location": f"{native.API}/operations/{OP}?foo=bar"}
    )
    session.post.return_value = initial
    session.get.side_effect = [
        response(body={"status": "Succeeded"}),
        response(body={"definition": definition()}),
    ]
    monkeypatch.setattr(
        native, "complete_operation", lambda *a, **k: session.get("poll")
    )
    assert native.get_definition(session, WS, "OperationsAgent", ITEM) == definition()
    assert session.get.call_args.args[0] == f"{native.API}/operations/{OP}/result"


@pytest.mark.parametrize(
    "location",
    [
        "https://evil.test/v1/operations/a",
        "http://api.fabric.microsoft.com/v1/operations/a",
    ],
)
def test_lro_rejects_untrusted_locations(location):
    with pytest.raises(native.NativeItemError, match="untrusted"):
        native.complete_operation(Mock(), response(202, headers={"Location": location}))


@pytest.mark.parametrize("state", ["Failed", "Cancelled", "Surprise"])
def test_lro_rejects_failed_or_unknown_states(state):
    session = Mock()
    session.get.return_value = response(body={"status": state})
    with pytest.raises(native.NativeItemError):
        native.complete_operation(
            session,
            response(202, headers={"Location": f"{native.API}/operations/{OP}"}),
            sleep=lambda _: None,
        )


def test_lro_timeout_is_bounded():
    with pytest.raises(native.NativeItemError, match="timed out"):
        native.complete_operation(
            Mock(),
            response(202, headers={"Location": f"{native.API}/operations/{OP}"}),
            timeout_seconds=1,
            monotonic=iter([0, 2]).__next__,
            sleep=lambda _: None,
        )


def test_operation_id_uses_canonical_api_without_following_regional_location():
    session = Mock()
    session.get.return_value = response(body={"status": "Succeeded"})
    native.complete_operation(
        session,
        response(202, headers={
            "Location": f"https://regional.example/v1/operations/{OP}",
            "x-ms-operation-id": OP,
        }),
        sleep=lambda _: None,
    )
    assert session.get.call_args.args[0] == f"{native.API}/operations/{OP}"


def test_definition_numeric_normalization_never_confuses_boolean():
    assert native.definition_matches(
        {"parts": [native.json_part("x.json", {"value": 0})]},
        {"parts": [native.json_part("x.json", {"value": 0.0, "extra": True})]},
    )
    assert not native.definition_matches(definition(), definition(0))


@pytest.mark.parametrize("collection", ["actions", "dataSources"])
def test_operations_readback_rejects_extra_actions_and_sources(collection):
    expected = {"configuration": {collection: {"allowed": {"id": ITEM}}}}
    actual = {"configuration": {collection: {"allowed": {"id": ITEM}, "extra": {"id": OP}}}}
    assert not native.definition_matches(
        {"parts": [native.json_part("Configurations.json", expected)]},
        {"parts": [native.json_part("Configurations.json", actual)]},
    )


@pytest.mark.parametrize(
    "service_id",
    [None, "00000000-0000-0000-0000-000000000000"],
)
def test_operations_readback_accepts_service_normalized_source_id(service_id):
    expected = {
        "configuration": {
            "dataSources": {
                "availability": {
                    "id": ITEM,
                    "workspaceId": WS,
                    "type": "KustoDatabase",
                }
            }
        }
    }
    actual = {
        "configuration": {
            "dataSources": {
                "availability": {
                    "id": service_id,
                    "workspaceId": WS,
                    "type": "KustoDatabase",
                }
            }
        }
    }

    assert native.definition_matches(
        {"parts": [native.json_part("Configurations.json", expected)]},
        {"parts": [native.json_part("Configurations.json", actual)]},
    )


def test_notebook_server_filename_can_differ():
    assert native.definition_matches(
        {"parts": [native.json_part("notebook-content.ipynb", {"cells": []})]},
        {"parts": [native.json_part("artifact.content.ipynb", {"cells": []})]},
    )


def test_report_binding_allows_service_expansion_but_not_another_model():
    def report(connection):
        return {"parts": [native.json_part("definition.pbir", {
            "datasetReference": {"byConnection": {"connectionString": connection}},
        })]}

    expected = report(f"semanticmodelid={ITEM}")
    assert native.definition_matches(
        expected, report(f"Data Source=powerbi://example;semanticmodelid={ITEM};integrated security=ClaimsToken"),
    )
    assert not native.definition_matches(
        expected, report(f"initial catalog={ITEM};semanticmodelid={OP}"),
    )


def test_report_readback_rejects_extra_pages_but_allows_platform_metadata():
    expected = {"parts": [native.json_part("definition.pbir", {"version": "4.0"})]}
    stale = {"parts": [*expected["parts"], native.json_part("definition/pages/old/page.json", {})]}
    assert not native.definition_matches(expected, stale)
    normalized = {"parts": [*expected["parts"], native.json_part(".platform", {})]}
    assert native.definition_matches(expected, normalized)
