"""Standard-library reader for bootstrap-safe solution manifest fields."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_PATH = REPO_ROOT / "contracts" / "retail-demo.json"


class ManifestFormatError(ValueError):
    """Raised when bootstrap-safe manifest fields are missing or malformed."""


@dataclass(frozen=True)
class BootstrapMetadata:
    """Solution identity needed by bootstrap callers."""

    id: str
    name: str
    description: str


@dataclass(frozen=True)
class BootstrapPrerequisite:
    """Prerequisite metadata safe to read before dependencies are installed."""

    id: str
    name: str
    description: str
    support_status: str
    kind: str
    requirement: str | None
    check_command: tuple[str, ...]
    bootstrap_required: bool


@dataclass(frozen=True)
class BootstrapCommand:
    """Command metadata safe to display during bootstrap."""

    id: str
    name: str
    description: str
    support_status: str
    argv: tuple[str, ...]
    examples: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class BootstrapProfile:
    """Deployment-profile projection safe to read before installation."""

    id: str
    name: str
    deployment_name: str
    default: bool
    description: str
    support_status: str
    infrastructure_item_count: int
    reporting_item_count: int
    infrastructure_folders: tuple[str, ...]
    reporting_folders: tuple[str, ...]

    @property
    def all_item_count(self) -> int:
        """Total expected items across both publication phases."""

        return self.infrastructure_item_count + self.reporting_item_count


@dataclass(frozen=True)
class BootstrapManifest:
    """Minimal dependency-free projection of the solution manifest."""

    version: str
    sha256: str
    metadata: BootstrapMetadata
    prerequisites: tuple[BootstrapPrerequisite, ...]
    commands: tuple[BootstrapCommand, ...]
    profiles: tuple[BootstrapProfile, ...]


def load_solution_manifest(
    path: Path = DEFAULT_MANIFEST_PATH,
) -> BootstrapManifest:
    """Load only fields that setup/bootstrap code may safely consume."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestFormatError(f"invalid solution manifest: {path}") from exc
    return parse_solution_manifest(document)


def parse_solution_manifest(document: Any) -> BootstrapManifest:
    """Validate a decoded manifest and return its bootstrap-safe projection."""

    root = _mapping(document, "manifest")
    metadata = _mapping(root.get("metadata"), "metadata")

    prerequisites = tuple(
        _read_prerequisite(item, index)
        for index, item in enumerate(
            _sequence(root.get("prerequisites"), "prerequisites")
        )
    )
    commands = tuple(
        _read_command(item, index)
        for index, item in enumerate(_sequence(root.get("commands"), "commands"))
    )
    profiles = tuple(
        _read_profile(item, index)
        for index, item in enumerate(_sequence(root.get("profiles"), "profiles"))
    )
    _require_unique_ids((*prerequisites, *commands, *profiles))
    defaults = [profile for profile in profiles if profile.default]
    if len(defaults) != 1:
        raise ManifestFormatError(
            "manifest must define exactly one default executable profile"
        )
    return BootstrapManifest(
        version=_string(root.get("version"), "version"),
        sha256=_manifest_sha256(root),
        metadata=BootstrapMetadata(
            id=_string(metadata.get("id"), "metadata.id"),
            name=_string(metadata.get("name"), "metadata.name"),
            description=_string(
                metadata.get("description"),
                "metadata.description",
            ),
        ),
        prerequisites=prerequisites,
        commands=commands,
        profiles=profiles,
    )


def _read_prerequisite(value: Any, index: int) -> BootstrapPrerequisite:
    item = _mapping(value, f"prerequisites[{index}]")
    prefix = f"prerequisites[{index}]"
    requirement = item.get("requirement")
    if requirement is not None:
        requirement = _string(requirement, f"{prefix}.requirement")
    return BootstrapPrerequisite(
        id=_string(item.get("id"), f"{prefix}.id"),
        name=_string(item.get("name"), f"{prefix}.name"),
        description=_string(item.get("description"), f"{prefix}.description"),
        support_status=_string(
            item.get("support_status"),
            f"{prefix}.support_status",
        ),
        kind=_string(item.get("kind"), f"{prefix}.kind"),
        requirement=requirement,
        check_command=_string_tuple(
            item.get("check_command", []),
            f"{prefix}.check_command",
        ),
        bootstrap_required=_boolean(
            item.get("bootstrap_required"),
            f"{prefix}.bootstrap_required",
        ),
    )


def _read_command(value: Any, index: int) -> BootstrapCommand:
    item = _mapping(value, f"commands[{index}]")
    prefix = f"commands[{index}]"
    argv = _string_tuple(item.get("argv"), f"{prefix}.argv", require_items=True)
    examples = tuple(
        _string_tuple(
            example,
            f"{prefix}.examples[{example_index}]",
            require_items=True,
        )
        for example_index, example in enumerate(
            _sequence(item.get("examples"), f"{prefix}.examples")
        )
    )
    if not examples:
        raise ManifestFormatError(f"{prefix}.examples must not be empty")
    if len(examples) != len(set(examples)):
        raise ManifestFormatError(f"{prefix}.examples must be unique")
    if any(example[: len(argv)] != argv for example in examples):
        raise ManifestFormatError(f"{prefix}.examples must begin with argv")
    return BootstrapCommand(
        id=_string(item.get("id"), f"{prefix}.id"),
        name=_string(item.get("name"), f"{prefix}.name"),
        description=_string(item.get("description"), f"{prefix}.description"),
        support_status=_string(
            item.get("support_status"),
            f"{prefix}.support_status",
        ),
        argv=argv,
        examples=examples,
    )


def _read_profile(value: Any, index: int) -> BootstrapProfile:
    item = _mapping(value, f"profiles[{index}]")
    prefix = f"profiles[{index}]"
    publication = _mapping(
        item.get("publication"),
        f"{prefix}.publication",
    )
    return BootstrapProfile(
        id=_string(item.get("id"), f"{prefix}.id"),
        name=_string(item.get("name"), f"{prefix}.name"),
        deployment_name=_string(
            item.get("deployment_name"),
            f"{prefix}.deployment_name",
        ),
        default=_boolean(item.get("default", False), f"{prefix}.default"),
        description=_string(item.get("description"), f"{prefix}.description"),
        support_status=_string(
            item.get("support_status"),
            f"{prefix}.support_status",
        ),
        infrastructure_item_count=_nonnegative_integer(
            publication.get("infrastructure_item_count"),
            f"{prefix}.publication.infrastructure_item_count",
        ),
        reporting_item_count=_nonnegative_integer(
            publication.get("reporting_item_count"),
            f"{prefix}.publication.reporting_item_count",
        ),
        infrastructure_folders=_string_tuple(
            publication.get("infrastructure_folders", []),
            f"{prefix}.publication.infrastructure_folders",
        ),
        reporting_folders=_string_tuple(
            publication.get("reporting_folders", []),
            f"{prefix}.publication.reporting_folders",
        ),
    )


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ManifestFormatError(f"{field} must be an object")
    return value


def _sequence(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ManifestFormatError(f"{field} must be an array")
    return value


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ManifestFormatError(f"{field} must be a non-empty string")
    return value


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ManifestFormatError(f"{field} must be a boolean")
    return value


def _nonnegative_integer(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ManifestFormatError(f"{field} must be a non-negative integer")
    return value


def _string_tuple(
    value: Any,
    field: str,
    *,
    require_items: bool = False,
) -> tuple[str, ...]:
    values = _sequence(value, field)
    if require_items and not values:
        raise ManifestFormatError(f"{field} must not be empty")
    return tuple(
        _string(item, f"{field}[{index}]") for index, item in enumerate(values)
    )


def _require_unique_ids(records: tuple[Any, ...]) -> None:
    ids = [record.id for record in records]
    duplicates = sorted({record_id for record_id in ids if ids.count(record_id) > 1})
    if duplicates:
        raise ManifestFormatError(f"bootstrap IDs must be unique: {duplicates}")


def _drop_nulls(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _drop_nulls(child)
            for key, child in value.items()
            if child is not None
        }
    if isinstance(value, list):
        return [_drop_nulls(child) for child in value]
    return value


def _manifest_sha256(document: dict[str, Any]) -> str:
    encoded = json.dumps(
        _drop_nulls(document),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
