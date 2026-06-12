"""Deploy staged Fabric item folders with fabric-cicd."""

from __future__ import annotations

import argparse
from pathlib import Path

from deploy.scripts.deploy_config import DEPLOY_ROOT


def build_credential(auth_mode: str):
    """Build an explicit Azure credential for fabric-cicd."""

    if auth_mode == "azure_cli":
        from azure.identity import AzureCliCredential

        return AzureCliCredential()
    if auth_mode == "azure_powershell":
        from azure.identity import AzurePowerShellCredential

        return AzurePowerShellCredential()
    raise ValueError(
        "Unsupported auth mode. Use 'azure_cli' or 'azure_powershell' for this wrapper."
    )


def deploy(config_path: Path, environment: str, auth_mode: str) -> None:
    """Run fabric-cicd configuration deployment."""

    from fabric_cicd import deploy_with_config

    deploy_with_config(
        config_file_path=str(config_path.resolve()),
        token_credential=build_credential(auth_mode),
        environment=environment,
    )


def main() -> int:
    """Deploy staged Fabric items with fabric-cicd."""

    parser = argparse.ArgumentParser(description="Deploy Fabric items with fabric-cicd")
    parser.add_argument("--environment", default="dev")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEPLOY_ROOT / "fabric-cicd" / "config.yml",
    )
    parser.add_argument(
        "--auth-mode",
        choices=["azure_cli", "azure_powershell"],
        default="azure_cli",
    )
    args = parser.parse_args()

    deploy(args.config, args.environment, args.auth_mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
