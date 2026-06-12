"""Offline validation for generated deployment framework files."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from deploy.scripts.deploy_config import DEPLOY_ROOT, load_environment


def validate_generated_files(deploy_root: Path, environment: str) -> list[str]:
    """Return validation errors for generated deployment files."""

    errors: list[str] = []
    load_environment(environment)

    required_files = [
        deploy_root / "terraform" / "environments" / f"{environment}.tfvars",
        deploy_root / "fabric-cicd" / "config.yml",
        deploy_root / "fabric-cicd" / "parameter.yml",
    ]
    for path in required_files:
        if not path.exists():
            errors.append(f"Missing required file: {path}")

    for yaml_path in required_files[1:]:
        if yaml_path.exists():
            try:
                yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
            except yaml.YAMLError as exc:
                errors.append(f"Invalid YAML in {yaml_path}: {exc}")

    workspace_dir = deploy_root / "workspace"
    if workspace_dir.exists():
        unresolved = [
            path
            for path in workspace_dir.rglob("*")
            if path.is_file()
            and "FABRIC_KQL_DATABASE_RESOURCE_ID" in path.read_text(
                encoding="utf-8", errors="ignore"
            )
        ]
        errors.extend(f"Unresolved placeholder in {path}" for path in unresolved)

    return errors


def main() -> int:
    """Validate generated deployment files."""

    parser = argparse.ArgumentParser(description="Validate deployment framework output")
    parser.add_argument("--deploy-root", type=Path, default=DEPLOY_ROOT)
    parser.add_argument("--environment", default="dev")
    args = parser.parse_args()

    errors = validate_generated_files(args.deploy_root, args.environment)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Deployment framework validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
