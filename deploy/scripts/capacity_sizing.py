"""Capacity-aware custom Spark pool sizing and deployment evidence."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from deploy.scripts.deploy_config import DeployConfig

HEADROOM_POLICY = "reserve_25_percent_of_base_capacity"
HEADROOM_RATIO = 0.25
NODE_SIZE = "Medium"
NODE_VCORES = 8
MIN_NODES = 2
MAX_NODES = 16
EVIDENCE_MAX_AGE_SECONDS = 3600
EVIDENCE_MAX_BYTES = 16_384
REQUIRED_ML_PIPELINE = (
    Path("fabric")
    / "pipelines"
    / "ml-required.DataPipeline"
    / "pipeline-content.json"
)
_SKU_PATTERN = re.compile(r"(?:F|FTL)(\d+)", flags=re.IGNORECASE)


class CapacitySizingError(ValueError):
    """Raised when capacity sizing cannot be derived or proven."""


@dataclass(frozen=True)
class CapacitySizingEvidence:
    """Bounded, non-secret evidence used to render custom pool sizing."""

    schema_version: int
    environment: str
    capacity_id: str
    capacity_name: str
    capacity_sku: str
    capacity_units: int
    base_vcores: int
    usable_vcores: int
    producer_concurrency: int
    node_size: str
    node_vcores: int
    min_node_count: int
    max_node_count: int
    aggregate_max_vcores: int
    headroom_policy: str
    observed_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def capacity_units(sku: str) -> int | None:
    """Return capacity units for supported Fabric F/FTL SKUs."""

    match = _SKU_PATTERN.fullmatch(sku.strip())
    return int(match.group(1)) if match else None


def required_ml_producer_count(repo_root: Path) -> int:
    """Count root notebook producers in the required-ML pipeline."""

    path = repo_root / REQUIRED_ML_PIPELINE
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        activities = payload["properties"]["activities"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise CapacitySizingError(
            f"Cannot read required-ML pipeline topology from {path}."
        ) from exc
    if not isinstance(activities, list):
        raise CapacitySizingError(
            "Required-ML pipeline activities must be a JSON array."
        )
    producers = [
        activity
        for activity in activities
        if isinstance(activity, dict)
        and activity.get("type") == "TridentNotebook"
        and activity.get("dependsOn", []) == []
    ]
    if not producers:
        raise CapacitySizingError(
            "Required-ML pipeline must contain at least one root notebook producer."
        )
    return len(producers)


def derive_capacity_sizing(
    *,
    environment: str,
    capacity: dict[str, Any],
    repo_root: Path,
    observed_at: datetime | None = None,
) -> CapacitySizingEvidence:
    """Derive fail-closed custom pool sizing from base capacity only."""

    capacity_id = str(capacity.get("id", "")).strip()
    capacity_name = str(capacity.get("displayName", "")).strip()
    sku = str(capacity.get("sku", "")).strip()
    units = capacity_units(sku)
    if not capacity_id or not capacity_name:
        raise CapacitySizingError(
            "Fabric capacity identity and display name are required for sizing."
        )
    if units is None:
        raise CapacitySizingError(
            f"Capacity SKU {sku or 'unknown'!r} is unsupported; expected F<n> or FTL<n>."
        )

    producer_count = required_ml_producer_count(repo_root)
    base_vcores = units * 2
    usable_vcores = math.floor(base_vcores * (1 - HEADROOM_RATIO))
    per_session_max = math.floor(
        usable_vcores / (producer_count * NODE_VCORES)
    )
    if per_session_max < MIN_NODES:
        required_vcores = producer_count * MIN_NODES * NODE_VCORES
        raise CapacitySizingError(
            f"Capacity {capacity_name!r} ({sku}) has {usable_vcores} usable base "
            f"Spark vCores after 25% headroom, but {producer_count} concurrent "
            f"required-ML sessions need at least {required_vcores} vCores for "
            "one Medium driver and one Medium executor each. Select a larger "
            "capacity; burst capacity is not used."
        )

    max_nodes = min(max(per_session_max, MIN_NODES), MAX_NODES)
    min_nodes = min(MIN_NODES, max_nodes)
    aggregate_max_vcores = producer_count * max_nodes * NODE_VCORES
    timestamp = observed_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return CapacitySizingEvidence(
        schema_version=1,
        environment=environment,
        capacity_id=capacity_id,
        capacity_name=capacity_name,
        capacity_sku=sku.upper(),
        capacity_units=units,
        base_vcores=base_vcores,
        usable_vcores=usable_vcores,
        producer_concurrency=producer_count,
        node_size=NODE_SIZE,
        node_vcores=NODE_VCORES,
        min_node_count=min_nodes,
        max_node_count=max_nodes,
        aggregate_max_vcores=aggregate_max_vcores,
        headroom_policy=HEADROOM_POLICY,
        observed_at=timestamp.astimezone(UTC).isoformat(),
    )


def write_capacity_sizing_evidence(
    path: Path,
    evidence: CapacitySizingEvidence,
) -> None:
    """Persist sizing evidence atomically beside other generated deployment data."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(evidence.to_dict(), indent=2, sort_keys=True) + "\n"
    if len(payload.encode("utf-8")) > EVIDENCE_MAX_BYTES:
        raise CapacitySizingError("Capacity sizing evidence exceeds its size limit.")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(path)


def _integer_field(payload: dict[str, Any], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CapacitySizingError(f"Capacity sizing evidence field {name} must be an integer.")
    return value


def load_capacity_sizing_evidence(
    path: Path,
    *,
    config: DeployConfig,
    repo_root: Path,
    now: datetime | None = None,
    max_age_seconds: int = EVIDENCE_MAX_AGE_SECONDS,
) -> CapacitySizingEvidence:
    """Load and validate fresh evidence against config and current topology."""

    if not config.profile.uses_custom_pool:
        raise CapacitySizingError(
            "Starter-pool profiles must not use capacity sizing evidence."
        )
    try:
        if path.stat().st_size > EVIDENCE_MAX_BYTES:
            raise CapacitySizingError("Capacity sizing evidence exceeds its size limit.")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CapacitySizingError(
            f"Capacity sizing evidence not found: {path}. Run live preflight first."
        ) from exc
    except json.JSONDecodeError as exc:
        raise CapacitySizingError(
            f"Capacity sizing evidence is not valid JSON: {path}."
        ) from exc
    if not isinstance(payload, dict):
        raise CapacitySizingError("Capacity sizing evidence must be a JSON object.")

    string_fields = (
        "environment",
        "capacity_id",
        "capacity_name",
        "capacity_sku",
        "node_size",
        "headroom_policy",
        "observed_at",
    )
    for name in string_fields:
        if not isinstance(payload.get(name), str) or not payload[name].strip():
            raise CapacitySizingError(
                f"Capacity sizing evidence field {name} must be a non-empty string."
            )
    integer_fields = (
        "schema_version",
        "capacity_units",
        "base_vcores",
        "usable_vcores",
        "producer_concurrency",
        "node_vcores",
        "min_node_count",
        "max_node_count",
        "aggregate_max_vcores",
    )
    values = {name: _integer_field(payload, name) for name in integer_fields}
    try:
        evidence = CapacitySizingEvidence(
            **{name: payload[name] for name in string_fields},
            **values,
        )
    except TypeError as exc:
        raise CapacitySizingError("Capacity sizing evidence has an invalid schema.") from exc

    if evidence.schema_version != 1:
        raise CapacitySizingError("Unsupported capacity sizing evidence schema version.")
    if evidence.environment != config.environment:
        raise CapacitySizingError(
            "Capacity sizing evidence environment does not match deployment environment."
        )
    if (
        config.workspace.capacity_id
        and evidence.capacity_id.casefold() != config.workspace.capacity_id.casefold()
    ):
        raise CapacitySizingError(
            "Capacity sizing evidence capacity ID does not match configured capacity."
        )
    if (
        config.workspace.capacity_name
        and evidence.capacity_name.casefold()
        != config.workspace.capacity_name.casefold()
    ):
        raise CapacitySizingError(
            "Capacity sizing evidence capacity name does not match configured capacity."
        )

    units = capacity_units(evidence.capacity_sku)
    if units is None or units != evidence.capacity_units:
        raise CapacitySizingError(
            "Capacity sizing evidence contains an unsupported or inconsistent SKU."
        )
    producer_count = required_ml_producer_count(repo_root)
    if evidence.producer_concurrency != producer_count:
        raise CapacitySizingError(
            "Capacity sizing evidence producer count does not match the current "
            "required-ML pipeline root producer count; rerun live preflight."
        )
    expected = derive_capacity_sizing(
        environment=config.environment,
        capacity={
            "id": evidence.capacity_id,
            "displayName": evidence.capacity_name,
            "sku": evidence.capacity_sku,
        },
        repo_root=repo_root,
        observed_at=datetime.now(UTC),
    )
    for name in (
        "capacity_units",
        "base_vcores",
        "usable_vcores",
        "producer_concurrency",
        "node_size",
        "node_vcores",
        "min_node_count",
        "max_node_count",
        "aggregate_max_vcores",
        "headroom_policy",
    ):
        if getattr(evidence, name) != getattr(expected, name):
            raise CapacitySizingError(
                f"Capacity sizing evidence field {name} is inconsistent with "
                "the supported sizing formula."
            )
    try:
        observed = datetime.fromisoformat(evidence.observed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CapacitySizingError(
            "Capacity sizing evidence observed_at must be an ISO-8601 timestamp."
        ) from exc
    if observed.tzinfo is None:
        raise CapacitySizingError(
            "Capacity sizing evidence observed_at must include a time zone."
        )
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    age_seconds = (current.astimezone(UTC) - observed.astimezone(UTC)).total_seconds()
    if age_seconds < -300 or age_seconds > max_age_seconds:
        raise CapacitySizingError(
            "Capacity sizing evidence is stale or has a future timestamp; rerun "
            "live preflight for this deployment run."
        )
    return evidence


def sizing_identity(evidence: CapacitySizingEvidence) -> dict[str, object]:
    """Return the stable, timestamp-independent sizing identity for journals."""

    fields = {
        key: value
        for key, value in evidence.to_dict().items()
        if key not in {"schema_version", "observed_at"}
    }
    canonical = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()
    return {
        **fields,
        "sha256": hashlib.sha256(canonical).hexdigest(),
    }
