"""Tests for the generic Fabric item exporter."""

from __future__ import annotations

import base64
import inspect
import json
import sys
import time
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests
from azure.core.credentials import AccessToken
from azure.core.exceptions import ClientAuthenticationError
from requests import ConnectionError, Timeout

from deploy.scripts import _retry, export_items, export_pipelines

TENANT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
ACCESS_TOKEN = "request-contract-token"  # noqa: S105 - synthetic test token


def _b64(payload: object) -> str:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def test_export_clients_accept_selected_auth_mode() -> None:
    assert "auth_mode" in inspect.signature(export_items.build_session).parameters
    assert "auth_mode" in inspect.signature(export_items.export_items).parameters
    assert "tenant_id" in inspect.signature(export_items.build_session).parameters
    assert "tenant_id" in inspect.signature(export_items.export_items).parameters
    assert (
        "auth_mode" in inspect.signature(export_pipelines.export_pipelines).parameters
    )
    assert (
        "tenant_id" in inspect.signature(export_pipelines.export_pipelines).parameters
    )


def test_build_session_uses_bearer_header_and_configured_tenant(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}
    credential = object()

    def fake_build_credential(
        auth_mode: str,
        *,
        tenant_id: str | None,
    ) -> object:
        calls["auth_mode"] = auth_mode
        calls["tenant_id"] = tenant_id
        return credential

    monkeypatch.setattr(export_items, "build_credential", fake_build_credential)
    monkeypatch.setattr(
        export_items,
        "_access_token",
        lambda scope, provided: (
            calls.update(scope=scope, credential=provided)
            or AccessToken(ACCESS_TOKEN, int(time.time()) + 3600)
        ),
    )

    session = export_items.build_session(
        auth_mode="azure_powershell",
        tenant_id=TENANT_ID,
    )

    assert session.headers["Authorization"] == f"Bearer {ACCESS_TOKEN}"
    assert calls == {
        "auth_mode": "azure_powershell",
        "tenant_id": TENANT_ID,
        "scope": export_items.FABRIC_SCOPE,
        "credential": credential,
    }


@pytest.mark.parametrize("seconds_remaining", [0, 299])
def test_session_renews_expiring_tokens_using_the_same_credential(
    monkeypatch, seconds_remaining
):
    clock = [1000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    first = AccessToken(ACCESS_TOKEN, 2000)
    renewed = AccessToken(ACCESS_TOKEN + "-renewed", 5000)
    credential = Mock()
    credential.get_token.side_effect = [first, renewed]
    session = export_items.build_session(credential)
    url = f"{export_items.FABRIC_API}/workspaces"

    initial = session.prepare_request(requests.Request("GET", url))
    assert initial.headers["Authorization"] == f"Bearer {first.token}"
    credential.get_token.assert_called_once_with(export_items.FABRIC_SCOPE)

    clock[0] = first.expires_on - seconds_remaining
    next_request = session.prepare_request(
        requests.Request("POST", url, json={"displayName": "test-workspace"})
    )
    assert next_request.headers["Authorization"] == f"Bearer {renewed.token}"
    assert next_request.body == b'{"displayName": "test-workspace"}'
    assert credential.get_token.call_count == 2
    assert all(
        call.args == (export_items.FABRIC_SCOPE,)
        for call in credential.get_token.call_args_list
    )

    later = session.prepare_request(requests.Request("GET", url))
    assert later.headers["Authorization"] == f"Bearer {renewed.token}"
    assert credential.get_token.call_count == 2


def test_token_renewal_failure_does_not_send_the_expired_token(monkeypatch):
    clock = [1000.0]
    waits = []
    monkeypatch.setattr(time, "time", lambda: clock[0])
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    credential = Mock()
    credential.get_token.side_effect = [
        AccessToken(ACCESS_TOKEN, 2000),
        ClientAuthenticationError("operator authentication unavailable"),
        ClientAuthenticationError("operator authentication unavailable"),
        ClientAuthenticationError("operator authentication unavailable"),
    ]
    session = export_items.build_session(credential)
    session.send = Mock()
    clock[0] = 2000

    with pytest.raises(ClientAuthenticationError):
        session.get(f"{export_items.FABRIC_API}/workspaces")

    session.send.assert_not_called()
    assert credential.get_token.call_count == 4
    assert waits == [5.0, 10.0]


def test_token_accessor_preserves_the_string_contract_for_arm_clients():
    scope = "https://management.azure.com/.default"
    credential = Mock()
    credential.get_token.return_value = AccessToken(
        ACCESS_TOKEN, int(time.time()) + 3600
    )

    assert export_items._token(scope, credential) == ACCESS_TOKEN
    credential.get_token.assert_called_once_with(scope)


@pytest.mark.parametrize(
    ("module", "function_name"),
    [
        (export_items, "export_items"),
        (export_pipelines, "export_pipelines"),
    ],
)
def test_export_main_passes_selected_auth_mode(
    monkeypatch,
    tmp_path: Path,
    module,
    function_name: str,
) -> None:
    calls: dict[str, object] = {}
    argv = [
        module.__name__,
        "--workspace-name",
        "retail-demo",
        "--output-dir",
        str(tmp_path),
        "--auth-mode",
        "azure_powershell",
        "--tenant-id",
        TENANT_ID,
    ]
    if module is export_items:
        argv.extend(["--item-type", "DataAgent"])
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(
        module,
        function_name,
        lambda *args, **kwargs: calls.update(kwargs) or [],
    )

    try:
        result = module.main()
    except SystemExit as exc:
        pytest.fail(f"--auth-mode was not accepted: {exc}")

    assert result == 0
    assert calls["auth_mode"] == "azure_powershell"
    assert calls["tenant_id"] == TENANT_ID


def test_write_item_writes_nested_parts_for_data_agent(tmp_path: Path) -> None:
    definition = {
        "parts": [
            {"path": ".platform", "payload": _b64({"metadata": {"type": "DataAgent"}})},
            {
                "path": "Files/Config/data_agent.json",
                "payload": _b64({"$schema": "dataAgent/2.1.0"}),
            },
            {
                "path": "Files/Config/draft/semantic-model-retail_model/datasource.json",
                "payload": _b64({"artifactId": "abc", "type": "semantic_model"}),
            },
        ]
    }

    item_dir = export_items.write_item(
        tmp_path, "retail-semantic-model-agent", "DataAgent", definition
    )

    assert item_dir == tmp_path / "retail-semantic-model-agent.DataAgent"
    platform = json.loads((item_dir / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"]["type"] == "DataAgent"
    # Nested part directories are created.
    datasource = (
        item_dir / "Files/Config/draft/semantic-model-retail_model/datasource.json"
    )
    assert json.loads(datasource.read_text(encoding="utf-8"))["artifactId"] == "abc"


def test_list_items_filters_by_type_and_sorts() -> None:
    class _FakeResponse:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._payload

    class _FakeSession:
        def __init__(self) -> None:
            self.params: dict | None = None

        def get(self, url: str, params: dict | None = None) -> _FakeResponse:
            self.params = params
            return _FakeResponse(
                {
                    "value": [
                        {"displayName": "b-agent", "id": "2"},
                        {"displayName": "a-agent", "id": "1"},
                    ]
                }
            )

    session = _FakeSession()
    items = export_items.list_items(session, "ws-id", "DataAgent")

    assert session.params == {"type": "DataAgent"}
    assert [item["displayName"] for item in items] == ["a-agent", "b-agent"]


@pytest.mark.parametrize("error_type", [ConnectionError, Timeout])
def test_definition_read_retries_are_bounded_and_only_repeat_the_read_api(
    monkeypatch, error_type
):
    waits = []
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    definition = {"parts": []}
    response = Mock(status_code=200)
    response.json.return_value = {"definition": definition}
    session = Mock()
    session.post.side_effect = [
        error_type("connection reset"),
        error_type("connection reset"),
        response,
    ]

    assert export_items.get_definition(session, "workspace", "item") == definition

    assert waits == [5.0, 10.0]
    assert session.post.call_count == 3
    assert all(
        call.args[0].endswith("/items/item/getDefinition")
        and call.kwargs == {"timeout": 30}
        for call in session.post.call_args_list
    )
    session.get.assert_not_called()


def test_definition_operation_status_and_result_reads_have_request_timeouts(
    monkeypatch,
):
    monkeypatch.setattr(export_items.time, "sleep", lambda _seconds: None)
    definition = {"parts": []}
    operation_url = "https://api.fabric.microsoft.com/v1/operations/operation"
    accepted = Mock(status_code=202, headers={"Location": operation_url})
    poll = Mock(headers={})
    poll.json.return_value = {"status": "Succeeded"}
    result = Mock()
    result.json.return_value = {"definition": definition}
    session = Mock()
    session.post.return_value = accepted
    session.get.side_effect = [poll, result]

    assert export_items.get_definition(session, "workspace", "item") == definition

    session.post.assert_called_once_with(
        f"{export_items.FABRIC_API}/workspaces/workspace/items/item/getDefinition",
        timeout=30,
    )
    assert [call.args[0] for call in session.get.call_args_list] == [
        operation_url,
        f"{operation_url}/result",
    ]
    assert all(
        call.kwargs == {"timeout": 30}
        for call in session.get.call_args_list
    )


def test_definition_poll_connection_failure_never_starts_another_export(monkeypatch):
    waits = []
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    operation_url = "https://api.fabric.microsoft.com/v1/operations/operation"
    session = Mock()
    session.post.return_value = Mock(
        status_code=202, headers={"Location": operation_url}
    )
    session.get.side_effect = ConnectionError("connection reset")

    with pytest.raises(ConnectionError):
        export_items.get_definition(session, "workspace", "item")

    session.post.assert_called_once()
    assert session.get.call_count == 3
    assert waits == [2, 5.0, 10.0]
    assert all(
        call.args[0] == operation_url and call.kwargs == {"timeout": 30}
        for call in session.get.call_args_list
    )
