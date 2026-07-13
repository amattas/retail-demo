"""Shared credential construction for deployment clients."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

AUTH_MODES = ("azure_cli", "azure_powershell")


def build_credential(
    auth_mode: str,
    *,
    process_timeout: int = 120,
) -> TokenCredential:
    """Build the operator credential selected by deployment configuration."""

    if auth_mode == "azure_cli":
        from azure.identity import AzureCliCredential

        return AzureCliCredential(process_timeout=process_timeout)
    if auth_mode == "azure_powershell":
        from azure.identity import AzurePowerShellCredential

        return AzurePowerShellCredential(process_timeout=process_timeout)
    raise ValueError(
        f"Unsupported auth mode {auth_mode!r}. Use 'azure_cli' or 'azure_powershell'."
    )
