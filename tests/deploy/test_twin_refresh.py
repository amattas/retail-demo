from unittest.mock import Mock

import pytest
from requests import ConnectionError, HTTPError, Timeout

from deploy.scripts import _retry
from deploy.scripts import _twin_refresh as refresh
from deploy.scripts._native_items import NativeItemError

ID = "11111111-1111-4111-8111-111111111111"


def response(status, body, headers=None):
    result = Mock(status_code=status, headers=headers or {})
    result.json.return_value = body
    return result


def test_refresh_polls_canonical_url_and_records_identity_before_waiting(monkeypatch):
    session = Mock()
    session.post.return_value = response(202, {}, {
        "Location": f"https://regional.example/refreshes/{ID}", "x-ms-request-id": ID,
    })
    session.get.return_value = response(200, {"status": "Completed"})
    monkeypatch.setattr(refresh, "_token", lambda *args: "test")
    monkeypatch.setattr(refresh, "_session", lambda *args: session)
    identities = []
    assert refresh.refresh_model(Mock(), ID, ID, on_started=identities.append) == ID
    assert identities == [ID]
    assert session.get.call_args.args[0].startswith("https://api.powerbi.com/")
    assert session.post.call_count == 1


@pytest.mark.parametrize("error_type", [ConnectionError, Timeout])
def test_refresh_status_retries_keep_the_exact_accepted_request(
    monkeypatch, error_type
):
    waits = []
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    session = Mock()
    session.post.return_value = response(202, {}, {"x-ms-request-id": ID})
    session.get.side_effect = [
        error_type("connection reset"),
        error_type("connection reset"),
        response(200, {"status": "Completed"}),
    ]
    monkeypatch.setattr(refresh, "_token", lambda *args: "test")
    monkeypatch.setattr(refresh, "_session", lambda *args: session)
    identities = []

    assert refresh.refresh_model(Mock(), ID, ID, on_started=identities.append) == ID

    assert identities == [ID]
    session.post.assert_called_once()
    assert waits == [5.0, 10.0]
    assert all(
        call.args[0].endswith(f"/refreshes/{ID}")
        for call in session.get.call_args_list
    )


def test_refresh_status_retries_do_not_exceed_the_refresh_deadline(monkeypatch):
    clock = [0.0]
    waits = []
    monkeypatch.setattr(refresh.time, "monotonic", lambda: clock[0])

    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(_retry.time, "sleep", sleep)
    session = Mock()
    session.get.side_effect = ConnectionError("connection reset")

    with pytest.raises(NativeItemError, match="timed out"):
        refresh.wait_for_refresh(session, ID, ID, ID, timeout_seconds=3)

    assert clock[0] == 3
    assert waits == [3]
    session.get.assert_called_once()
    assert session.get.call_args.kwargs["timeout"] == 3
    session.post.assert_not_called()


def test_failed_refresh_is_not_retried_or_hidden():
    session = Mock()
    session.get.return_value = response(200, {"status": "Failed"})
    with pytest.raises(NativeItemError, match="refresh failed"):
        refresh.wait_for_refresh(session, ID, ID, ID)
    session.post.assert_not_called()


def test_permission_failure_is_terminal():
    session = Mock()
    result = response(403, {})
    result.raise_for_status.side_effect = HTTPError("denied")
    session.get.return_value = result
    with pytest.raises(HTTPError):
        refresh.wait_for_refresh(session, ID, ID, ID)
    session.get.assert_called_once()
