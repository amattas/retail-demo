"""Starting the live stream is parameterized, verified, and never duplicated."""

from unittest.mock import Mock

import pytest

from deploy.scripts import start_stream as stream

WS = "11111111-1111-4111-8111-111111111111"
NB = "22222222-2222-4222-8222-222222222222"
LOCATION = f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/items/{NB}/jobs/instances/run-1"


def response(status=200, body=None, headers=None):
    result = Mock(status_code=status, headers=headers or {})
    result.json.return_value = body or {}
    return result


def _session(monkeypatch, runs, accepted=None, submitted_status="NotStarted"):
    monkeypatch.setattr(
        stream, "list_items", lambda *_args: [{"id": NB, "displayName": "stream-events"}]
    )
    session = Mock()
    session.get.side_effect = [
        response(body={"value": runs}),
        response(body={"id": "run-1", "status": submitted_status}),
    ]
    session.post.return_value = accepted or response(202, headers={"Location": LOCATION})
    return session


def test_stream_run_is_submitted_with_the_configured_rate_and_duration(monkeypatch):
    session = _session(monkeypatch, runs=[{"id": "old", "status": "Completed"}])

    result = stream.start_stream(session, workspace_id=WS, rows_per_second=20, run_seconds=600)

    assert result["state"] == "STARTED"
    assert result["job_instance_id"] == "run-1"
    assert session.post.call_args.kwargs["params"] == {"jobType": "RunNotebook"}
    assert session.post.call_args.kwargs["json"]["executionData"]["parameters"] == {
        "source_rows_per_second": {"value": 20, "type": "int"},
        "run_seconds": {"value": 600, "type": "int"},
    }


def test_a_running_stream_is_reported_not_duplicated(monkeypatch):
    session = _session(monkeypatch, runs=[{"id": "live", "status": "InProgress"}])

    result = stream.start_stream(session, workspace_id=WS, rows_per_second=5, run_seconds=0)

    assert result == {"state": "ALREADY_RUNNING", "notebook_id": NB, "job_instance_id": "live"}
    session.post.assert_not_called()


@pytest.mark.parametrize(
    "accepted,submitted_status",
    [(response(400), "NotStarted"), (None, "Failed")],
    ids=["rejected", "failed-immediately"],
)
def test_unaccepted_stream_runs_fail(monkeypatch, accepted, submitted_status):
    session = _session(monkeypatch, runs=[], accepted=accepted, submitted_status=submitted_status)

    with pytest.raises(stream.StreamStartError):
        stream.start_stream(session, workspace_id=WS, rows_per_second=5, run_seconds=0)
