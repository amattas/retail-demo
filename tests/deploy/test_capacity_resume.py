"""Capacity resume uses explicit ARM identity, bounded waits and one mutation."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests

from deploy.scripts.capacity_resume import CapacityResumeError, ensure_capacity_active

SUBSCRIPTION = "11111111-1111-4111-8111-111111111111"
TENANT = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
OTHER_SUBSCRIPTION = "22222222-2222-4222-8222-222222222222"
RESOURCE = (
    f"/subscriptions/{SUBSCRIPTION}/resourceGroups/demo/providers/"
    "Microsoft.Fabric/capacities/demo-capacity"
)
OPERATION = "https://management.azure.com/providers/Microsoft.Fabric/operations/one"


def _fabric(state="Inactive", sku="F64"):
    return {
        "id": "fabric-guid",
        "displayName": "demo-capacity",
        "state": state,
        "sku": sku,
    }


def _resource(state, *, sku="F64", provisioning="Succeeded"):
    return {
        "id": RESOURCE,
        "name": "demo-capacity",
        "type": "Microsoft.Fabric/capacities",
        "sku": {"name": sku},
        "properties": {"state": state, "provisioningState": provisioning},
    }


class Response:
    def __init__(self, body=None, status=200, headers=None):
        self.body = body
        self.status_code = status
        self.headers = headers or {}

    def json(self):
        if self.body is None:
            raise ValueError("No response body")
        return self.body


class Session:
    def __init__(self, script):
        self.script = list(script)
        self.calls = []
        self.closed = False

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        assert kwargs["allow_redirects"] is False
        assert 0 < kwargs["timeout"] <= 30
        expected_method, expected_path, response = self.script.pop(0)
        assert method == expected_method
        assert expected_path in url
        if isinstance(response, Exception):
            raise response
        return response

    def close(self):
        self.closed = True


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.now += delay


def _run(
    session, *, get_capacity=None, resource_id=RESOURCE, timeout=60, subscription=None
):
    clock = Clock()
    supplier = get_capacity or Mock(side_effect=[_fabric(), _fabric("Active")])
    ensure_capacity_active(
        supplier,
        resource_id=resource_id,
        expected_name="demo-capacity",
        subscription_id=subscription,
        arm_session_factory=lambda: session,
        tenant_id=TENANT,
        timeout_seconds=timeout,
        poll_seconds=5,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    assert not session.script
    assert session.closed
    return clock


@pytest.mark.parametrize("sku", ["F64", "FT1", "P1"])
def test_active_capacity_is_noop_without_any_arm_credentials(sku):
    factory = Mock(side_effect=AssertionError("ARM must not be consulted"))
    ensure_capacity_active(
        lambda: _fabric("Active", sku),
        resource_id=None,
        expected_name="demo-capacity",
        subscription_id=None,
        arm_session_factory=factory,
    )
    factory.assert_not_called()


@pytest.mark.parametrize("paused", ["Paused", "Suspended"])
def test_paused_capacity_resumes_once_then_requires_fabric_active(paused):
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource(paused))),
            ("POST", "/resume?api-version=2023-11-01", Response()),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    supplier = Mock(side_effect=[_fabric(), _fabric(), _fabric("Active")])
    _run(session, get_capacity=supplier)
    assert supplier.call_count == 3
    assert sum(method == "POST" for method, _, _ in session.calls) == 1
    assert "json" not in session.calls[1][2] and "data" not in session.calls[1][2]


def test_async_resume_honors_retry_after_and_terminal_operation_state():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Paused"))),
            (
                "POST",
                "/resume?",
                Response(
                    status=202,
                    headers={
                        "Azure-AsyncOperation": OPERATION,
                        "Retry-After": "2",
                    },
                ),
            ),
            (
                "GET",
                OPERATION,
                Response({"status": "InProgress"}, headers={"Retry-After": "3"}),
            ),
            ("GET", OPERATION, Response({"status": "Succeeded"})),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    clock = _run(session)
    assert clock.sleeps[:2] == [2, 3]


def test_location_polling_handles_empty_success_body():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Suspended"))),
            ("POST", "/resume?", Response(status=202, headers={"Location": OPERATION})),
            ("GET", OPERATION, Response(status=202)),
            ("GET", OPERATION, Response(status=200)),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    _run(session)


def test_existing_resume_operation_is_waited_on_not_reissued():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Resuming"))),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    _run(session)
    assert all(method == "GET" for method, _, _ in session.calls)


def test_hidden_paused_capacity_can_use_explicit_arm_target():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Paused"))),
            ("POST", "/resume?", Response()),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    _run(session, get_capacity=Mock(side_effect=[None, _fabric("Active")]))


@pytest.mark.parametrize("state", ["Failed", "Deleting", "Mystery"])
def test_unexpected_capacity_states_fail_without_mutation(state):
    session = Session([("GET", RESOURCE, Response(_resource(state)))])
    with pytest.raises(CapacityResumeError, match="not resumable"):
        _run(session)
    assert session.closed
    assert len(session.calls) == 1


@pytest.mark.parametrize("status", [401, 403])
def test_arm_permission_failures_are_actionable_and_not_ignored(status):
    session = Session([("GET", RESOURCE, Response(status=status))])
    with pytest.raises(CapacityResumeError, match="Azure RBAC"):
        _run(session)
    assert session.closed


def test_failed_async_operation_does_not_let_deployment_continue():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Paused"))),
            (
                "POST",
                "/resume?",
                Response(status=202, headers={"Azure-AsyncOperation": OPERATION}),
            ),
            ("GET", OPERATION, Response({"status": "Failed"})),
        ]
    )
    with pytest.raises(CapacityResumeError, match="operation failed"):
        _run(session)
    assert session.closed


@pytest.mark.parametrize(
    "url",
    [
        "https://untrusted.invalid/token",
        "http://management.azure.com/operation",
        "https://management.azure.com:bad/operation",
    ],
)
def test_untrusted_polling_url_never_receives_arm_credentials(url):
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Paused"))),
            (
                "POST",
                "/resume?",
                Response(
                    status=202,
                    headers={
                        "Azure-AsyncOperation": url,
                    },
                ),
            ),
        ]
    )
    with pytest.raises(CapacityResumeError, match="polling URL"):
        _run(session)
    assert len(session.calls) == 2
    assert session.closed


def test_lost_post_response_is_followed_by_reads_not_another_post():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Paused"))),
            ("POST", "/resume?", requests.Timeout("response lost")),
            ("GET", RESOURCE, Response(_resource("Resuming"))),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    _run(session)
    assert sum(method == "POST" for method, _, _ in session.calls) == 1


def test_capacity_polling_stops_at_deadline():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Resuming"))),
            ("GET", RESOURCE, Response(_resource("Resuming"))),
        ]
    )
    with pytest.raises(CapacityResumeError, match="Timed out"):
        _run(session, timeout=6)
    assert session.closed
    assert len(session.calls) == 2


@pytest.mark.parametrize(
    "resource_id", ["fabric-guid", RESOURCE.replace("demo-capacity", "other")]
)
def test_missing_or_mismatched_arm_identity_never_resumes(resource_id):
    session = Session([])
    with pytest.raises(CapacityResumeError):
        _run(session, resource_id=resource_id)
    assert not session.calls


def test_subscription_mismatch_never_resumes():
    with pytest.raises(CapacityResumeError, match="subscription_id"):
        _run(Session([]), subscription="22222222-2222-4222-8222-222222222222")


@pytest.mark.parametrize("sku", ["FT1", "FTL64", "P1"])
def test_inactive_trial_and_premium_capacities_are_not_resumed(sku):
    with pytest.raises(CapacityResumeError, match="F-SKU"):
        _run(Session([]), get_capacity=lambda: _fabric(sku=sku))


def test_failed_provisioning_prevents_resume_of_paused_resource():
    session = Session(
        [("GET", RESOURCE, Response(_resource("Paused", provisioning="Failed")))]
    )
    with pytest.raises(CapacityResumeError, match="provisioning is failed"):
        _run(session)
    assert session.closed


def test_resource_identity_is_rechecked_before_resuming():
    wrong = _resource("Paused")
    wrong["id"] = RESOURCE.replace("demo-capacity", "different")
    session = Session([("GET", RESOURCE, Response(wrong))])
    with pytest.raises(CapacityResumeError, match="unexpected identity"):
        _run(session)
    assert len(session.calls) == 1 and session.closed


def test_arm_session_uses_management_audience_and_selected_auth_mode(monkeypatch):
    from deploy.scripts import capacity_resume

    credential = object()
    build = Mock(return_value=credential)
    token = Mock(return_value="unit-test-token")
    monkeypatch.setattr(capacity_resume, "build_credential", build)
    monkeypatch.setattr(capacity_resume, "_token", token)
    session = capacity_resume.build_arm_session("azure_powershell", "tenant")
    try:
        build.assert_called_once_with("azure_powershell", tenant_id="tenant")
        token.assert_called_once_with(
            "https://management.azure.com/.default", credential
        )
        assert session.headers["Authorization"] == "Bearer unit-test-token"
    finally:
        session.close()


def test_fabric_must_be_active_after_arm_resume():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    with pytest.raises(CapacityResumeError, match="Timed out"):
        _run(session, get_capacity=lambda: _fabric(), timeout=6)
    assert session.closed


def test_conflicting_resume_does_not_trigger_duplicate_post():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Paused"))),
            ("POST", "/resume?", Response(status=409)),
            ("GET", RESOURCE, Response(_resource("Resuming"))),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    _run(session)
    assert sum(method == "POST" for method, _, _ in session.calls) == 1


def test_throttled_operation_and_resource_reads_retry_without_reposting_resume():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Paused"))),
            (
                "POST",
                "/resume?",
                Response(status=202, headers={"Azure-AsyncOperation": OPERATION}),
            ),
            ("GET", OPERATION, Response(status=429, headers={"Retry-After": "3"})),
            ("GET", OPERATION, Response({"status": "Succeeded"})),
            ("GET", RESOURCE, Response(status=429, headers={"Retry-After": "1"})),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    clock = _run(session)
    assert 3 in clock.sleeps and 1 in clock.sleeps
    assert sum(method == "POST" for method, _, _ in session.calls) == 1


def test_throttled_get_still_obeys_overall_deadline():
    session = Session(
        [
            ("GET", RESOURCE, Response(status=429, headers={"Retry-After": "20"})),
        ]
    )
    with pytest.raises(CapacityResumeError, match="Timed out"):
        _run(session, timeout=6)
    assert len(session.calls) == 1
    assert session.closed


def test_throttled_resume_post_is_not_retried():
    session = Session(
        [
            ("GET", RESOURCE, Response(_resource("Paused"))),
            ("POST", "/resume?", Response(status=429, headers={"Retry-After": "1"})),
        ]
    )
    with pytest.raises(CapacityResumeError, match="HTTP 429"):
        _run(session)
    assert len(session.calls) == 2
    assert session.closed


def _subscription(identifier=SUBSCRIPTION, tenant=TENANT):
    return {"subscriptionId": identifier, "tenantId": tenant}


def test_name_only_discovers_and_resumes_the_unique_tenant_capacity():
    session = Session(
        [
            (
                "GET",
                "/subscriptions?api-version=2022-12-01",
                Response(
                    {
                        "value": [
                            _subscription(),
                            _subscription(
                                OTHER_SUBSCRIPTION,
                                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                            ),
                        ]
                    }
                ),
            ),
            (
                "GET",
                f"/subscriptions/{SUBSCRIPTION}/providers/Microsoft.Fabric/capacities?",
                Response({"value": [_resource("Paused")]}),
            ),
            ("GET", RESOURCE, Response(_resource("Paused"))),
            ("POST", "/resume?", Response()),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    _run(
        session,
        resource_id=None,
        get_capacity=Mock(side_effect=[None, _fabric("Active")]),
    )
    assert not any(OTHER_SUBSCRIPTION in url for _, url, _ in session.calls)
    assert sum(method == "POST" for method, _, _ in session.calls) == 1


def test_name_discovery_honors_configured_subscription_without_listing_others():
    session = Session(
        [
            (
                "GET",
                f"/subscriptions/{SUBSCRIPTION}?api-version=2022-12-01",
                Response(_subscription()),
            ),
            (
                "GET",
                f"/subscriptions/{SUBSCRIPTION}/providers/Microsoft.Fabric/capacities?",
                Response({"value": [_resource("Active")]}),
            ),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    _run(session, resource_id=None, subscription=SUBSCRIPTION)
    assert not any("/subscriptions?" in url for _, url, _ in session.calls)
    assert all(method == "GET" for method, _, _ in session.calls)


def test_duplicate_capacity_names_across_subscriptions_are_not_guessed():
    duplicate = _resource("Paused")
    duplicate["id"] = RESOURCE.replace(SUBSCRIPTION, OTHER_SUBSCRIPTION)
    session = Session(
        [
            (
                "GET",
                "/subscriptions?",
                Response(
                    {
                        "value": [
                            _subscription(),
                            _subscription(OTHER_SUBSCRIPTION),
                        ]
                    }
                ),
            ),
            (
                "GET",
                f"/subscriptions/{SUBSCRIPTION}/providers/",
                Response({"value": [_resource("Paused")]}),
            ),
            (
                "GET",
                f"/subscriptions/{OTHER_SUBSCRIPTION}/providers/",
                Response({"value": [duplicate]}),
            ),
        ]
    )
    with pytest.raises(CapacityResumeError, match="found 2"):
        _run(session, resource_id=None)
    assert session.closed
    assert all(method == "GET" for method, _, _ in session.calls)


def test_name_discovery_follows_every_subscription_and_capacity_page():
    subscription_page = "https://management.azure.com/subscriptions?next=2"
    capacity_page = (
        f"https://management.azure.com/subscriptions/{SUBSCRIPTION}/providers/"
        "Microsoft.Fabric/capacities?next=2"
    )
    session = Session(
        [
            (
                "GET",
                "/subscriptions?",
                Response({"value": [], "nextLink": subscription_page}),
            ),
            ("GET", subscription_page, Response({"value": [_subscription()]})),
            (
                "GET",
                f"/subscriptions/{SUBSCRIPTION}/providers/",
                Response({"value": [], "nextLink": capacity_page}),
            ),
            ("GET", capacity_page, Response({"value": [_resource("Active")]})),
            ("GET", RESOURCE, Response(_resource("Active"))),
        ]
    )
    _run(session, resource_id=None)


def test_incomplete_azure_discovery_does_not_accept_an_earlier_name_match():
    session = Session(
        [
            (
                "GET",
                "/subscriptions?",
                Response(
                    {
                        "value": [
                            _subscription(),
                            _subscription(OTHER_SUBSCRIPTION),
                        ]
                    }
                ),
            ),
            (
                "GET",
                f"/subscriptions/{SUBSCRIPTION}/providers/",
                Response({"value": [_resource("Paused")]}),
            ),
            (
                "GET",
                f"/subscriptions/{OTHER_SUBSCRIPTION}/providers/",
                Response(status=403),
            ),
        ]
    )
    with pytest.raises(CapacityResumeError, match="Azure RBAC"):
        _run(session, resource_id=None)
    assert len(session.calls) == 3 and session.closed


def test_name_discovery_rejects_unknown_subscription_ownership():
    session = Session(
        [
            (
                "GET",
                "/subscriptions?",
                Response({"value": [{"subscriptionId": SUBSCRIPTION}]}),
            ),
        ]
    )
    with pytest.raises(CapacityResumeError, match="ownership is unavailable"):
        _run(session, resource_id=None)
    assert session.closed


def test_name_discovery_rejects_configured_subscription_in_another_tenant():
    session = Session(
        [
            (
                "GET",
                f"/subscriptions/{SUBSCRIPTION}?",
                Response(_subscription(tenant="other-tenant")),
            ),
        ]
    )
    with pytest.raises(CapacityResumeError, match="not in the configured tenant"):
        _run(session, resource_id=None, subscription=SUBSCRIPTION)
    assert session.closed


def test_name_discovery_reports_missing_capacity_without_mutation():
    session = Session(
        [
            ("GET", "/subscriptions?", Response({"value": [_subscription()]})),
            (
                "GET",
                f"/subscriptions/{SUBSCRIPTION}/providers/",
                Response({"value": []}),
            ),
        ]
    )
    with pytest.raises(CapacityResumeError, match="found 0"):
        _run(session, resource_id=None)
    assert session.closed


def test_discovery_rejects_untrusted_continuation_before_sending_credentials():
    session = Session(
        [
            (
                "GET",
                "/subscriptions?",
                Response(
                    {
                        "value": [_subscription()],
                        "nextLink": "https://untrusted.invalid/subscriptions",
                    }
                ),
            ),
        ]
    )
    with pytest.raises(CapacityResumeError, match="untrusted"):
        _run(session, resource_id=None)
    assert len(session.calls) == 1 and session.closed


def test_discovery_rejects_pagination_loops_instead_of_accepting_partial_results():
    url = "https://management.azure.com/subscriptions?api-version=2022-12-01"
    session = Session(
        [
            ("GET", url, Response({"value": [_subscription()], "nextLink": url})),
        ]
    )
    with pytest.raises(CapacityResumeError, match="pagination repeated"):
        _run(session, resource_id=None)
    assert len(session.calls) == 1 and session.closed


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_invalid_timeouts_fail_before_any_lookup(timeout):
    lookup = Mock(side_effect=AssertionError("must validate timeout first"))
    with pytest.raises(ValueError, match="must be positive"):
        _run(Session([]), get_capacity=lookup, timeout=timeout)
    lookup.assert_not_called()
