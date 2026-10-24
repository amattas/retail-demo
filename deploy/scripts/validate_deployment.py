"""Check generated deployment files without contacting Fabric."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from deploy.scripts import _output as console
from deploy.scripts.deploy_config import DEPLOY_ROOT, load_environment

# Staged files keep these markers until fabric-cicd substitutes them at publish
# time. Each marker needs a find_replace rule for the selected environment.
KNOWN_PLACEHOLDERS = ("FABRIC_KQL_DATABASE_RESOURCE_ID",)


def _resolved_placeholders(parameter_path: Path, environment: str) -> set[str]:
    """Return markers that parameter.yml can resolve for this environment."""

    if not parameter_path.exists():
        return set()
    try:
        params = yaml.safe_load(parameter_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return set()
    resolved: set[str] = set()
    for rule in params.get("find_replace", []) or []:
        find_value = rule.get("find_value")
        replace_value = (rule.get("replace_value") or {}).get(environment)
        if find_value and replace_value:
            resolved.add(str(find_value))
    return resolved


def validate_generated_files(deploy_root: Path, environment: str) -> list[str]:
    """Return validation errors for generated deployment files."""

    errors: list[str] = []
    load_environment(
        environment,
        config_path=deploy_root / "config" / "deploy.yml",
        environments_root=deploy_root / "config" / "environments",
    )

    environment_root = deploy_root / ".generated" / environment
    parameter_path = environment_root / "fabric-cicd" / "parameter.yml"
    required_files = [
        environment_root / "terraform.tfvars",
        environment_root / "fabric-cicd" / "config.yml",
        parameter_path,
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

    workspace_dir = environment_root / "workspace"
    if workspace_dir.exists():
        resolved = _resolved_placeholders(parameter_path, environment)
        for path in workspace_dir.rglob("*"):
            if not path.is_file():
                continue
            content = path.read_text(encoding="utf-8", errors="ignore")
            for placeholder in KNOWN_PLACEHOLDERS:
                if placeholder in content and placeholder not in resolved:
                    errors.append(
                        f"Unresolved placeholder {placeholder} in {path} "
                        f"(no find_replace rule for environment '{environment}' "
                        f"in parameter.yml)"
                    )

    return errors


def main() -> int:
    """Validate generated deployment files."""

    parser = argparse.ArgumentParser(description="Check generated deployment files")
    parser.add_argument("--deploy-root", type=Path, default=DEPLOY_ROOT)
    parser.add_argument("--environment", required=True)
    args = parser.parse_args()

    errors = validate_generated_files(args.deploy_root, args.environment)
    if errors:
        for error in errors:
            console.error(error)
        return 1
    console.info("Generated deployment files passed validation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
