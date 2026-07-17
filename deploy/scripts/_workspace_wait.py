"""Poll Fabric for a workspace's absence after a destroy, before recreating it.

Fabric releases a deleted workspace's display name asynchronously. Recreating
immediately with the same name can race the still-draining old workspace, so
`retail-setup deploy --recreate` used to insert a blind fixed-duration sleep
between the destroy and the apply. This module replaces that sleep with a
bounded poll of the same Fabric REST API the rest of the deploy framework
uses, authenticated with the operator credential selected by ``auth_mode``
(never silently substituting a different login, e.g. Azure CLI when
``azure_powershell`` is configured).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from deploy.scripts._auth import build_credential

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Bounded wait for Fabric to release a deleted workspace's display name, and
# how often to re-check while waiting.
DEFAULT_TIMEOUT_SECONDS = 180
DEFAULT_POLL_INTERVAL_SECONDS = 10

# A callable with the subset of `requests.get`'s signature this module uses.
HttpGet = Callable[..., Any]


def _default_http_get(
    url: str, *, headers: dict[str, str], params: dict[str, str] | None, timeout: int
) -> Any:
    """Perform the workspace-listing GET with `requests`.

    `requests` is imported lazily (and injected in tests via `http_get`) so the
    deploy framework's lightweight contract tests can exercise the pagination
    logic without the `requests` dependency installed.
    """

    import requests

    return requests.get(url, headers=headers, params=params, timeout=timeout)


class WorkspaceDeletionTimeout(TimeoutError):
    """Raised when a workspace name is still in use after the bounded wait."""


class WorkspacePaginationError(RuntimeError):
    """Raised when Fabric's workspace listing API returns a looping/malformed
    pagination response (the same continuation token/URI repeats).

    This fails closed rather than silently returning a partial (and possibly
    incomplete) set of workspace names, which could otherwise cause the
    target workspace to be falsely reported absent.
    """


def _list_workspace_names(credential: TokenCredential, http_get: HttpGet) -> set[str]:
    """Return the case-folded display names of every workspace visible to the token.

    Fully exhausts Fabric's pagination before returning: a workspace that only
    appears on a later page must never be reported absent. Follows a returned
    `continuationUri` verbatim when present, otherwise re-requests the first
    page URL with a `continuationToken` query parameter. Raises
    `WorkspacePaginationError` (rather than silently stopping) if a
    token/URI repeats, since that would otherwise risk under-reporting
    workspaces and falsely declaring the target absent.
    """

    token = credential.get_token(FABRIC_SCOPE).token
    headers = {"Authorization": f"Bearer {token}"}
    base_url = f"{FABRIC_API}/workspaces"

    names: set[str] = set()
    seen_continuations: set[str] = set()
    url = base_url
    params: dict[str, str] | None = None
    while True:
        response = http_get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        names.update(
            str(item.get("displayName", "")).casefold()
            for item in payload.get("value", [])
        )

        continuation_uri = payload.get("continuationUri")
        continuation_token = payload.get("continuationToken")
        if continuation_uri:
            marker = continuation_uri
            next_url, next_params = continuation_uri, None
        elif continuation_token:
            marker = continuation_token
            next_url, next_params = base_url, {"continuationToken": continuation_token}
        else:
            break

        if not marker:
            break
        if marker in seen_continuations:
            raise WorkspacePaginationError(
                "Fabric workspace listing returned a repeated pagination "
                "token/URI; refusing to guess the workspace list is complete."
            )
        seen_continuations.add(marker)
        url, params = next_url, next_params

    return names


def wait_for_workspace_absence(
    workspace_name: str,
    *,
    auth_mode: str = "azure_cli",
    credential: TokenCredential | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    http_get: HttpGet | None = None,
) -> None:
    """Block until ``workspace_name`` no longer appears in the tenant's workspaces.

    Builds the operator credential via the shared `build_credential` helper
    (or uses an injected `credential`, e.g. for tests) so the selected
    ``auth_mode`` is always honored. Raises `WorkspaceDeletionTimeout` if the
    name is still present after `timeout_seconds`.
    """

    credential = credential or build_credential(auth_mode)
    http_get = http_get or _default_http_get
    target = workspace_name.casefold()
    deadline = clock() + timeout_seconds
    while True:
        if target not in _list_workspace_names(credential, http_get):
            return
        if clock() >= deadline:
            raise WorkspaceDeletionTimeout(
                f"Workspace {workspace_name!r} still present after {timeout_seconds}s; "
                "Fabric has not finished releasing the name."
            )
        sleep(poll_interval_seconds)
