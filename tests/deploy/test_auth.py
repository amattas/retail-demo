"""Tests for deployment credential selection."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from deploy.scripts import _auth


@pytest.mark.parametrize(
    ("auth_mode", "expected_kind"),
    [
        ("azure_cli", "cli"),
        ("azure_powershell", "powershell"),
    ],
)
def test_build_credential_uses_selected_operator_login(
    monkeypatch,
    auth_mode: str,
    expected_kind: str,
) -> None:
    identity = ModuleType("azure.identity")

    class _Credential:
        def __init__(self, kind: str, *, process_timeout: int) -> None:
            self.kind = kind
            self.process_timeout = process_timeout

    identity.AzureCliCredential = lambda **kwargs: _Credential("cli", **kwargs)
    identity.AzurePowerShellCredential = lambda **kwargs: _Credential(
        "powershell", **kwargs
    )
    azure = ModuleType("azure")
    azure.identity = identity
    monkeypatch.setitem(sys.modules, "azure", azure)
    monkeypatch.setitem(sys.modules, "azure.identity", identity)

    credential = _auth.build_credential(auth_mode)

    assert credential.kind == expected_kind
    assert credential.process_timeout == 120


def test_build_credential_rejects_unsupported_mode() -> None:
    with pytest.raises(ValueError, match="Unsupported auth mode"):
        _auth.build_credential("managed_identity")
