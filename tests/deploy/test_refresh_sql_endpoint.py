"""Tests for Lakehouse SQL endpoint metadata synchronization."""

from __future__ import annotations

from collections import deque

import pytest

from deploy.scripts.refresh_sql_endpoint import (
    SqlEndpointRefreshError,
    refresh_sql_endpoint_metadata,
    resolve_sql_endpoint_id,
    validate_sync_statuses,
)


class _Response:
    def __init__(
        self,
        status_code: int,
        payload: object,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> object:
        return self._payload


class _Session:
    def __init__(
        self,
        *,
        gets: list[_Response] | None = None,
        posts: list[_Response] | None = None,
    ) -> None:
        self.gets = deque(gets or [])
        self.posts = deque(posts or [])
        self.get_urls: list[str] = []
        self.post_urls: list[str] = []
        self.post_kwargs: list[dict[str, object]] = []

    def get(self, url: str, **_kwargs: object) -> _Response:
        self.get_urls.append(url)
        return self.gets.popleft()

    def post(self, url: str, **_kwargs: object) -> _Response:
        self.post_urls.append(url)
        self.post_kwargs.append(_kwargs)
        return self.posts.popleft()


def test_resolve_sql_endpoint_id_reads_live_lakehouse_properties() -> None:
    session = _Session(
        gets=[
            _Response(
                200,
                {
                    "properties": {
                        "sqlEndpointProperties": {"id": "sql-endpoint-id"}
                    }
                },
            )
        ]
    )

    endpoint_id = resolve_sql_endpoint_id(
        session,  # type: ignore[arg-type]
        "workspace-id",
        "lakehouse-id",
    )

    assert endpoint_id == "sql-endpoint-id"
    assert session.get_urls == [
        "https://api.fabric.microsoft.com/v1/workspaces/workspace-id/"
        "lakehouses/lakehouse-id"
    ]


def test_refresh_accepts_immediate_success() -> None:
    statuses = [{"tableName": "ag.dim_customer", "status": "Success"}]
    session = _Session(
        posts=[_Response(200, {"value": statuses})],
    )

    result = refresh_sql_endpoint_metadata(
        session,  # type: ignore[arg-type]
        "workspace-id",
        "sql-endpoint-id",
    )

    assert result == statuses
    assert session.post_kwargs[0]["json"] == {
        "timeout": {"value": 1800, "timeUnit": "Seconds"}
    }


def test_refresh_polls_lro_and_reads_exact_result(monkeypatch) -> None:
    statuses = [{"tableName": "au.fact_sales", "status": "Success"}]
    operation_url = "https://api.fabric.microsoft.com/v1/operations/operation-id"
    result_url = f"{operation_url}/result"
    session = _Session(
        posts=[
            _Response(
                202,
                {},
                headers={"Location": operation_url, "Retry-After": "0"},
            )
        ],
        gets=[
            _Response(200, {"status": "Running"}, headers={"Retry-After": "0"}),
            _Response(200, {"status": "Succeeded"}),
            _Response(200, {"value": statuses}),
        ],
    )
    monkeypatch.setattr(
        "deploy.scripts.refresh_sql_endpoint.time.sleep",
        lambda _seconds: None,
    )

    result = refresh_sql_endpoint_metadata(
        session,  # type: ignore[arg-type]
        "workspace-id",
        "sql-endpoint-id",
    )

    assert result == statuses
    assert session.get_urls == [operation_url, operation_url, result_url]


def test_refresh_accepts_already_current_table() -> None:
    statuses = validate_sync_statuses(
        {
            "value": [
                {
                    "tableName": "ag.fact_sales",
                    "status": "NotRun",
                    "lastSuccessfulSyncDateTime": "2026-07-28T01:50:08Z",
                }
            ]
        }
    )

    assert statuses[0]["status"] == "NotRun"


@pytest.mark.parametrize("status", ["Failure", "NotRun", "Unknown"])
def test_refresh_rejects_any_unsynchronized_table(status: str) -> None:
    with pytest.raises(SqlEndpointRefreshError, match="did not synchronize"):
        validate_sync_statuses(
            {
                "value": [
                    {
                        "tableName": "ag.fact_sales",
                        "status": status,
                        "error": {"message": "sync failed"},
                    }
                ]
            }
        )


def test_refresh_rejects_failed_lro(monkeypatch) -> None:
    operation_url = "https://api.fabric.microsoft.com/v1/operations/operation-id"
    session = _Session(
        posts=[
            _Response(
                202,
                {},
                headers={"Location": operation_url, "Retry-After": "0"},
            )
        ],
        gets=[
            _Response(
                200,
                {"status": "Failed", "error": {"message": "capacity unavailable"}},
            )
        ],
    )
    monkeypatch.setattr(
        "deploy.scripts.refresh_sql_endpoint.time.sleep",
        lambda _seconds: None,
    )

    with pytest.raises(SqlEndpointRefreshError, match="capacity unavailable"):
        refresh_sql_endpoint_metadata(
            session,  # type: ignore[arg-type]
            "workspace-id",
            "sql-endpoint-id",
        )
