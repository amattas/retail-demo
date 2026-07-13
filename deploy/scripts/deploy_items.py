"""Deploy staged Fabric item folders with fabric-cicd."""

from __future__ import annotations

import argparse
from pathlib import Path

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES, build_credential
from deploy.scripts.deploy_config import DEPLOY_ROOT


def deploy(config_path: Path, environment: str, auth_mode: str) -> None:
    """Run fabric-cicd configuration deployment with quiet, consistent output.

    fabric-cicd logs verbose ``[info] HH:MM:SS - ####`` banners at INFO. Raise
    its package logger to WARNING so only warnings and errors surface, and print
    our own concise progress lines to match the other deploy steps.
    """

    import logging

    from fabric_cicd import deploy_with_config

    logging.getLogger("fabric_cicd").setLevel(logging.WARNING)

    console.info(f"Publishing Fabric items (environment '{environment}')...")
    deploy_with_config(
        config_file_path=str(config_path.resolve()),
        token_credential=build_credential(auth_mode),
        environment=environment,
    )
    console.info("Published Fabric items.")


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
        choices=AUTH_MODES,
        default="azure_cli",
    )
    args = parser.parse_args()

    deploy(args.config, args.environment, args.auth_mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
