"""Resolve and resume the configured Azure Fabric capacity safely."""

from __future__ import annotations

import math
import re
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import requests

from deploy.scripts import _output as console
from deploy.scripts._auth import build_credential
from deploy.scripts.export_items import _token

ARM_ENDPOINT = "https://management.azure.com"
ARM_SCOPE = "https://management.azure.com/.default"
API_VERSION = "2023-11-01"
SUBSCRIPTIONS_VERSION = "2022-12-01"
_RESOURCE_ID = re.compile(
    r"/subscriptions/(?P<subscription>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    r"/resourceGroups/[^/?#%\\]+/providers/Microsoft\.Fabric"
    r"/capacities/(?P<name>[a-z0-9-]+)",
    re.IGNORECASE,
)
_TRANSITIONS = {
    "resuming",
    "pausing",
    "suspending",
    "provisioning",
    "updating",
    "scaling",
    "preparing",
}


class CapacityResumeError(RuntimeError):
    """The selected capacity could not be safely resumed or confirmed active."""


def build_arm_session(auth_mode: str, tenant_id: str) -> requests.Session:
    from azure.core.exceptions import ClientAuthenticationError

    credential = build_credential(auth_mode, tenant_id=tenant_id)
    try:
        token = _token(ARM_SCOPE, credential)
    except ClientAuthenticationError as exc:
        raise CapacityResumeError(
            "Could not authenticate to Azure Resource Manager. Sign in using the "
            "configured auth.mode and tenant before resuming capacity."
        ) from exc
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"
    return session


def _trusted_url(url: str) -> str:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise CapacityResumeError(
            "Azure returned an invalid capacity polling URL."
        ) from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname != "management.azure.com"
        or port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise CapacityResumeError("Azure returned an untrusted capacity polling URL.")
    return url


def _resource_match(
    resource_id: str,
    names: list[str],
    subscription_id: str | None,
) -> re.Match[str]:
    match = _RESOURCE_ID.fullmatch(resource_id)
    if match is None:
        raise CapacityResumeError("The capacity ARM resource ID is invalid.")
    if (
        subscription_id
        and match["subscription"].casefold() != subscription_id.casefold()
    ):
        raise CapacityResumeError(
            "Capacity resource ID does not belong to configured subscription_id."
        )
    if any(name.casefold() != match["name"].casefold() for name in names):
        raise CapacityResumeError(
            "Azure capacity resource name differs from the configured Fabric target."
        )
    return match


def _arm_collection(
    request: Callable[[str, str], requests.Response],
    url: str,
) -> list[dict[str, Any]]:
    values = []
    seen = set()
    for _ in range(1000):
        if url in seen:
            raise CapacityResumeError(
                "Azure capacity discovery pagination repeated a URL."
            )
        seen.add(url)
        response = request("GET", url)
        if response.status_code != 200:
            raise CapacityResumeError(
                "Azure capacity discovery did not return a complete collection."
            )
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("value"), list)
            or any(not isinstance(item, dict) for item in payload["value"])
        ):
            raise CapacityResumeError(
                "Azure capacity discovery returned an invalid collection."
            )
        values.extend(payload["value"])
        next_link = payload.get("nextLink")
        if next_link is None or next_link == "":
            return values
        if not isinstance(next_link, str):
            raise CapacityResumeError(
                "Azure capacity discovery returned an invalid nextLink."
            )
        url = _trusted_url(next_link)
    raise CapacityResumeError("Azure capacity discovery exceeded its pagination limit.")


def _discover_resource_id(
    request: Callable[[str, str], requests.Response],
    name: str,
    tenant_id: str,
    subscription_id: str | None,
) -> str:
    if subscription_id:
        try:
            subscription_id = str(UUID(subscription_id))
        except ValueError as exc:
            raise CapacityResumeError(
                "Configured subscription_id is not a UUID."
            ) from exc
        response = request(
            "GET",
            f"{ARM_ENDPOINT}/subscriptions/{subscription_id}?api-version={SUBSCRIPTIONS_VERSION}",
        )
        subscription = response.json()
        if (
            response.status_code != 200
            or not isinstance(subscription, dict)
            or str(subscription.get("subscriptionId", "")).casefold() != subscription_id
            or str(subscription.get("tenantId", "")).casefold() != tenant_id.casefold()
        ):
            raise CapacityResumeError(
                "The configured Azure subscription is not in the configured tenant."
            )
        subscriptions = [subscription]
    else:
        subscriptions = _arm_collection(
            request,
            f"{ARM_ENDPOINT}/subscriptions?api-version={SUBSCRIPTIONS_VERSION}",
        )
    subscription_ids = set()
    for subscription in subscriptions:
        owner = subscription.get("tenantId")
        if not isinstance(owner, str) or not owner:
            raise CapacityResumeError(
                "Azure subscription ownership is unavailable; discovery is incomplete."
            )
        try:
            owner = str(UUID(owner))
        except ValueError as exc:
            raise CapacityResumeError(
                "Azure subscription ownership is invalid; discovery is incomplete."
            ) from exc
        if owner.casefold() != tenant_id.casefold():
            continue
        try:
            identifier = str(UUID(str(subscription.get("subscriptionId", ""))))
        except ValueError as exc:
            raise CapacityResumeError(
                "Azure returned an invalid subscription identity."
            ) from exc
        subscription_ids.add(identifier)
    matches: dict[str, str] = {}
    for identifier in sorted(subscription_ids):
        capacities = _arm_collection(
            request,
            f"{ARM_ENDPOINT}/subscriptions/{identifier}/providers/"
            f"Microsoft.Fabric/capacities?api-version={API_VERSION}",
        )
        for capacity in capacities:
            if not isinstance(capacity.get("name"), str) or not capacity["name"]:
                raise CapacityResumeError(
                    "Azure capacity discovery returned an unnamed resource."
                )
            if str(capacity.get("name", "")).casefold() != name.casefold():
                continue
            if (
                str(capacity.get("type", "")).casefold()
                != "microsoft.fabric/capacities"
            ):
                raise CapacityResumeError(
                    "Azure returned an unexpected capacity resource type."
                )
            resource_id = str(capacity.get("id", ""))
            _resource_match(resource_id, [name], identifier)
            matches[resource_id.casefold()] = resource_id
    if len(matches) != 1:
        raise CapacityResumeError(
            f"Expected one accessible Azure Fabric capacity named {name!r} in the "
            f"configured tenant; found {len(matches)}. Set subscription_id to narrow "
            "discovery or workspace.capacity_resource_id to select an exact resource."
        )
    return next(iter(matches.values()))


def ensure_capacity_active(
    get_fabric_capacity: Callable[[], dict[str, Any] | None],
    *,
    resource_id: str | None,
    expected_name: str | None,
    subscription_id: str | None,
    arm_session_factory: Callable[[], requests.Session],
    tenant_id: str | None = None,
    timeout_seconds: float = 600,
    poll_seconds: float = 5,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Leave active capacities alone; resume only paused F-SKU ARM resources.

    Resolve an explicit ARM ID or a unique Azure resource name within the
    configured tenant. No scaling, creation or workspace reassignment occurs.
    """

    if (
        not math.isfinite(timeout_seconds)
        or not math.isfinite(poll_seconds)
        or timeout_seconds <= 0
        or poll_seconds <= 0
    ):
        raise ValueError("Capacity timeout and poll interval must be positive.")
    deadline = monotonic() + timeout_seconds

    def remaining() -> float:
        value = deadline - monotonic()
        if value <= 0:
            raise CapacityResumeError(
                "Timed out waiting for the configured capacity to become Active. "
                "Check its Azure operation status before retrying deployment."
            )
        return value

    def wait(response: requests.Response | None = None) -> None:
        delay = poll_seconds
        header = response.headers.get("Retry-After") if response is not None else None
        if header is not None:
            try:
                requested = float(header)
                if not math.isfinite(requested) or requested < 0:
                    raise ValueError("invalid retry interval")
                delay = max(0.1, requested)
            except (TypeError, ValueError) as exc:
                raise CapacityResumeError(
                    "Azure returned an invalid Retry-After header."
                ) from exc
        sleep(min(delay, remaining()))

    capacity = get_fabric_capacity()
    remaining()
    if capacity is not None and capacity.get("state") == "Active":
        console.info(f"Capacity {capacity.get('displayName')!r} is already Active.")
        return
    if capacity is not None and not re.fullmatch(
        r"F[1-9]\d*", str(capacity.get("sku", ""))
    ):
        raise CapacityResumeError(
            "Automatic resume supports Azure F-SKU capacities only. "
            "Resume this trial/Premium capacity manually or choose an active F capacity."
        )
    names = [
        str(name)
        for name in (expected_name, (capacity or {}).get("displayName"))
        if name
    ]
    if not names:
        raise CapacityResumeError(
            "The Fabric capacity is unavailable and only a GUID was configured. "
            "Also set workspace.capacity_name to identify the Azure resume target."
        )
    if len({name.casefold() for name in names}) != 1:
        raise CapacityResumeError(
            "Configured capacity name disagrees with its Fabric identity."
        )
    if resource_id:
        _resource_match(resource_id, names, subscription_id)
    elif not tenant_id:
        raise CapacityResumeError(
            "Configured tenant_id is required for Azure capacity discovery."
        )
    session = arm_session_factory()
    resume_requested = False

    def request(method: str, url: str) -> requests.Response:
        while True:
            response = session.request(
                method,
                _trusted_url(url),
                timeout=min(30, remaining()),
                allow_redirects=False,
            )
            if method != "GET" or response.status_code != 429:
                break
            wait(response)
        if response.status_code in (401, 403):
            raise CapacityResumeError(
                "Azure denied capacity access. The configured operator needs Azure RBAC "
                "capacity read/resume and operation-status permissions; Fabric workspace "
                "permissions alone do not grant these rights."
            )
        if response.status_code not in (200, 202, 204, 409):
            raise CapacityResumeError(
                f"Azure capacity {method} failed (HTTP {response.status_code})."
            )
        return response

    def wait_operation(response: requests.Response) -> None:
        operation = response.headers.get("Azure-AsyncOperation")
        location = response.headers.get("Location")
        polling_url = operation or location
        if not polling_url:
            return
        _trusted_url(polling_url)
        while True:
            wait(response)
            response = request("GET", polling_url)
            if not operation and response.status_code in (200, 204):
                return
            if not operation and response.status_code == 202:
                continue
            payload = response.json()
            status = (
                str(payload.get("status", "")).casefold()
                if isinstance(payload, dict)
                else ""
            )
            if status == "succeeded":
                return
            if status in {"failed", "canceled", "cancelled"}:
                raise CapacityResumeError(f"Azure capacity resume operation {status}.")
            if status not in {"inprogress", "running", "accepted", "notstarted"}:
                raise CapacityResumeError(
                    "Azure returned an unknown resume operation status."
                )

    try:
        if not resource_id:
            assert tenant_id is not None
            resource_id = _discover_resource_id(
                request, names[0], tenant_id, subscription_id
            )
        match = _resource_match(resource_id, names, subscription_id)
        resource_url = f"{ARM_ENDPOINT}{resource_id}?api-version={API_VERSION}"
        while True:
            response = request("GET", resource_url)
            if response.status_code != 200:
                raise CapacityResumeError(
                    "Azure did not return the configured capacity resource."
                )
            document = response.json()
            if (
                not isinstance(document, dict)
                or str(document.get("id", "")).casefold() != resource_id.casefold()
                or str(document.get("type", "")).casefold()
                != "microsoft.fabric/capacities"
                or str(document.get("name", "")).casefold() != match["name"].casefold()
            ):
                raise CapacityResumeError(
                    "Azure returned a capacity with an unexpected identity."
                )
            sku = document.get("sku", {})
            if not isinstance(sku, dict) or not re.fullmatch(
                r"F[1-9]\d*", str(sku.get("name", ""))
            ):
                raise CapacityResumeError(
                    "The Azure resume target is not an F-SKU Fabric capacity."
                )
            properties = document.get("properties")
            if not isinstance(properties, dict):
                raise CapacityResumeError("Azure capacity state is unavailable.")
            state = str(properties.get("state", "")).casefold()
            provisioning = str(properties.get("provisioningState", "")).casefold()
            if provisioning in {"failed", "canceled", "cancelled", "deleting"}:
                raise CapacityResumeError(
                    f"Azure capacity provisioning is {provisioning}."
                )
            if state == "active":
                break
            if state in {"paused", "suspended"}:
                if not resume_requested:
                    console.info(
                        f"Resuming capacity {match['name']!r}; Azure capacity billing will restart."
                    )
                    resume_requested = True
                    try:
                        resumed = request(
                            "POST",
                            f"{ARM_ENDPOINT}{resource_id}/resume?api-version={API_VERSION}",
                        )
                    except (requests.Timeout, requests.ConnectionError):
                        # The request may have reached Azure. Re-read state, never POST blindly.
                        console.warn(
                            "Resume response was not received; checking Azure state without "
                            "submitting another resume request."
                        )
                        wait()
                        continue
                    if resumed.status_code == 409:
                        wait(resumed)
                        continue
                    if resumed.status_code not in (200, 202):
                        raise CapacityResumeError(
                            "Azure returned an unexpected resume response."
                        )
                    wait_operation(resumed)
                    wait(resumed)
                    continue
            elif state not in _TRANSITIONS:
                raise CapacityResumeError(
                    f"Azure capacity is {state or 'unknown'}, not resumable."
                )
            wait(response)
        while True:
            remaining()
            capacity = get_fabric_capacity()
            remaining()
            if capacity is not None and capacity.get("state") == "Active":
                console.info(
                    f"Capacity {capacity.get('displayName')!r} is Active in Fabric."
                )
                return
            wait()
    except requests.RequestException as exc:
        raise CapacityResumeError(
            "Azure capacity resume/readiness could not be confirmed because a request failed."
        ) from exc
    finally:
        session.close()
