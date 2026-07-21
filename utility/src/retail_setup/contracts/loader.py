"""Load the canonical solution manifest."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from .manifest import SolutionManifest
from .validation import RepositoryValidation, validate_manifest_repository

MANIFEST_RELATIVE_PATH = Path("contracts") / "retail-demo.json"


class ManifestLoadError(ValueError):
    """Raised when the manifest cannot be decoded or validated."""


def load_solution_manifest(path: Path) -> SolutionManifest:
    """Load and type-check a solution manifest JSON file."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        return SolutionManifest.model_validate(document)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise ManifestLoadError(f"invalid solution manifest: {path}") from exc


def load_repository_manifest(
    repo_root: Path,
) -> tuple[SolutionManifest, RepositoryValidation]:
    """Load the canonical manifest and validate all repository references."""

    manifest = load_solution_manifest(repo_root / MANIFEST_RELATIVE_PATH)
    validation = validate_manifest_repository(manifest, repo_root)
    return manifest, validation
