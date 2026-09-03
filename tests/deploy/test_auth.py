"""Tests for deployment credential selection."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from deploy.scripts import _auth, deploy_items

TENANT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


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
        def __init__(
            self,
            kind: str,
            *,
            tenant_id: str | None,
            process_timeout: int,
        ) -> None:
            self.kind = kind
            self.tenant_id = tenant_id
            self.process_timeout = process_timeout

    identity.AzureCliCredential = lambda **kwargs: _Credential("cli", **kwargs)
    identity.AzurePowerShellCredential = lambda **kwargs: _Credential(
        "powershell", **kwargs
    )
    azure = ModuleType("azure")
    azure.identity = identity
    monkeypatch.setitem(sys.modules, "azure", azure)
    monkeypatch.setitem(sys.modules, "azure.identity", identity)

    credential = _auth.build_credential(auth_mode, tenant_id=TENANT_ID)

    assert credential.kind == expected_kind
    assert credential.tenant_id == TENANT_ID
    assert credential.process_timeout == 120


def test_build_credential_rejects_unsupported_mode() -> None:
    with pytest.raises(ValueError, match="Unsupported auth mode"):
        _auth.build_credential("managed_identity")


def test_deploy_items_passes_configured_tenant_to_credential(
    monkeypatch,
    tmp_path,
) -> None:
    calls: dict[str, object] = {}
    credential = object()
    fabric_cicd = ModuleType("fabric_cicd")

    def fake_build_credential(
        auth_mode: str,
        *,
        tenant_id: str | None,
    ) -> object:
        calls.update(auth_mode=auth_mode, tenant_id=tenant_id)
        return credential

    def fake_deploy_with_config(**kwargs) -> None:
        calls.update(deploy_kwargs=kwargs)

    fabric_cicd.deploy_with_config = fake_deploy_with_config
    monkeypatch.setitem(sys.modules, "fabric_cicd", fabric_cicd)
    monkeypatch.setattr(deploy_items, "build_credential", fake_build_credential)

    deploy_items.deploy(
        tmp_path / "config.yml",
        "dev",
        "azure_powershell",
        tenant_id=TENANT_ID,
    )

    assert calls["auth_mode"] == "azure_powershell"
    assert calls["tenant_id"] == TENANT_ID
    deploy_kwargs = calls["deploy_kwargs"]
    assert deploy_kwargs["token_credential"] is credential
