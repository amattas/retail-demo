from unittest.mock import Mock

import pytest
from requests import HTTPError

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
