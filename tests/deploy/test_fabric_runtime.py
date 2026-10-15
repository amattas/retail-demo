"""Contract tests for shared Fabric pagination and definition helpers."""

from __future__ import annotations

import base64
import json
from unittest.mock import Mock

import pytest
from requests import ConnectionError, HTTPError, Timeout

from deploy.scripts import _retry, fabric_runtime
from deploy.scripts.fabric_runtime import (
    FabricDefinitionError,
    FabricPaginationError,
    decode_definition_parts,
    normalize_schedule_document,
    paginated_get,
    schedule_document,
)


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self.payload


class _Session:
    def __init__(self, responses: list[object]) -> None:
        self.responses = iter(responses)
        self.calls: list[tuple[str, dict | None]] = []

    def get(self, url: str, params: dict | None = None) -> _Response:
        self.calls.append((url, params))
        return _Response(next(self.responses))


def _part(path: str, value: object) -> dict[str, str]:
    payload = value if isinstance(value, str) else json.dumps(value)
    return {
        "path": path,
        "payload": base64.b64encode(payload.encode()).decode(),
    }


def test_paginated_get_exhausts_token_and_uri_pages() -> None:
    next_uri = "https://api.fabric.microsoft.com/v1/workspaces?page=3"
    session = _Session(
        [
            {"value": [{"id": "1"}], "continuationToken": "token-2"},
            {"value": [{"id": "2"}], "continuationUri": next_uri},
            {"value": [{"id": "3"}]},
        ]
    )

    values = paginated_get(
        session,
        "https://api.fabric.microsoft.com/v1/workspaces",
        params={"type": "Notebook"},
    )

    assert [item["id"] for item in values] == ["1", "2", "3"]
    assert session.calls[1][1] == {
        "type": "Notebook",
        "continuationToken": "token-2",
    }
    assert session.calls[2] == (next_uri, None)


@pytest.mark.parametrize("error_type", [ConnectionError, Timeout])
def test_paginated_get_retries_only_the_failed_page(monkeypatch, error_type):
    waits = []
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    session = Mock()
    session.get.side_effect = [
        _Response({"value": [{"id": "one"}], "continuationToken": "next"}),
        error_type("connection reset"),
        error_type("connection reset"),
        _Response({"value": [{"id": "two"}]}),
    ]
    url = "https://api.fabric.microsoft.com/v1/items"

    result = paginated_get(session, url, params={"type": "Notebook"})

    assert result == [{"id": "one"}, {"id": "two"}]
    assert session.get.call_count == 4
    assert waits == [5.0, 10.0]
    assert all(
        call.args == (url,)
        and call.kwargs == {"params": {"type": "Notebook", "continuationToken": "next"}}
        for call in session.get.call_args_list[1:]
    )
    session.post.assert_not_called()


def test_paginated_get_exhausted_connection_retries_propagate(monkeypatch):
    waits = []
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    session = Mock()
    session.get.side_effect = ConnectionError("connection reset")

    with pytest.raises(ConnectionError):
        paginated_get(session, "https://api.fabric.microsoft.com/v1/items")

    assert session.get.call_count == 3
    assert waits == [5.0, 10.0]


def test_paginated_get_does_not_retry_authorization_failure(monkeypatch):
    waits = []
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    response = Mock()
    response.raise_for_status.side_effect = HTTPError("403 Forbidden")
    session = Mock()
    session.get.return_value = response

    with pytest.raises(HTTPError):
        paginated_get(session, "https://api.fabric.microsoft.com/v1/items")

    session.get.assert_called_once()
    assert waits == []


@pytest.mark.parametrize("deadline", [3, 6])
def test_paginated_get_network_retries_keep_the_collection_deadline(
    monkeypatch, deadline
):
    clock = [0.0]
    waits = []
    monkeypatch.setattr(fabric_runtime.time, "monotonic", lambda: clock[0])

    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(_retry.time, "sleep", sleep)
    session = Mock()
    session.get.side_effect = [
        ConnectionError("connection reset"),
        _Response({"value": [{"id": "one"}]}),
    ]
    url = "https://api.fabric.microsoft.com/v1/items"

    if deadline == 3:
        with pytest.raises(FabricPaginationError, match="deadline expired"):
            paginated_get(session, url, timeout=30, deadline=deadline)
        session.get.assert_called_once_with(url, timeout=3)
        assert waits == [3]
    else:
        assert paginated_get(session, url, timeout=30, deadline=deadline) == [
            {"id": "one"}
        ]
        assert [call.kwargs["timeout"] for call in session.get.call_args_list] == [6, 1]
        assert waits == [5]


def test_paginated_get_fails_closed_on_repeated_token() -> None:
    session = _Session(
        [
            {"value": [], "continuationToken": "loop"},
            {"value": [], "continuationToken": "loop"},
        ]
    )

    with pytest.raises(FabricPaginationError, match="repeated"):
        paginated_get(session, "https://api.fabric.microsoft.com/v1/items")


@pytest.mark.parametrize("elapsed", [6, 11])
def test_paginated_get_applies_one_deadline_across_all_pages(monkeypatch, elapsed):
    from deploy.scripts import fabric_runtime

    clock = [0]
    timeouts = []
    monkeypatch.setattr(fabric_runtime.time, "monotonic", lambda: clock[0])

    class TimedSession:
        def get(self, _url, **kwargs):
            timeouts.append(kwargs["timeout"])
            clock[0] += elapsed
            return _Response(
                {"value": [{"id": "one"}], "continuationToken": "next"}
                if len(timeouts) == 1 else {"value": [{"id": "two"}]}
            )

    if elapsed == 11:
        with pytest.raises(FabricPaginationError, match="deadline expired"):
            paginated_get(TimedSession(), "https://api.fabric.microsoft.com/v1/capacities",
                          timeout=30, deadline=10)
        assert timeouts == [10]
    else:
        result = paginated_get(
            TimedSession(), "https://api.fabric.microsoft.com/v1/capacities",
            timeout=30, deadline=10,
        )
        assert len(result) == 2
        assert timeouts == [10, 4]


@pytest.mark.parametrize("payload", [{}, {"value": {}}, {"value": [1]}])
def test_paginated_get_rejects_malformed_collection(payload: object) -> None:
    with pytest.raises(FabricPaginationError):
        paginated_get(
            _Session([payload]),
            "https://api.fabric.microsoft.com/v1/items",
        )


def test_decode_definition_parts_rejects_duplicates_and_traversal() -> None:
    with pytest.raises(FabricDefinitionError, match="duplicate"):
        decode_definition_parts(
            {"parts": [_part("one.json", {}), _part("one.json", {})]}
        )
    with pytest.raises(FabricDefinitionError, match="Unsafe"):
        decode_definition_parts({"parts": [_part("../one.json", {})]})


def test_schedule_document_preserves_enabled_state_and_configuration() -> None:
    expected = {
        "schedules": [
            {
                "enabled": False,
                "jobType": "Execute",
                "configuration": {"type": "Cron", "interval": 5},
            }
        ]
    }
    parts = decode_definition_parts(
        {
            "parts": [
                _part(
                    ".schedules",
                    {
                        "$schema": "ignored-for-comparison",
                        **expected,
                    },
                )
            ]
        }
    )

    assert schedule_document(parts) == expected
    assert normalize_schedule_document(expected) == expected
