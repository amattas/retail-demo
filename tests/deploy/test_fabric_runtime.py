"""Contract tests for shared Fabric pagination and definition helpers."""

from __future__ import annotations

import base64
import json

import pytest

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


def test_paginated_get_fails_closed_on_repeated_token() -> None:
    session = _Session(
        [
            {"value": [], "continuationToken": "loop"},
            {"value": [], "continuationToken": "loop"},
        ]
    )

    with pytest.raises(FabricPaginationError, match="repeated"):
        paginated_get(session, "https://api.fabric.microsoft.com/v1/items")


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
