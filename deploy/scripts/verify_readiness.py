"""Profile-aware live Fabric readiness and freshness verification.

The verifier is read-only unless ``--run-pipeline`` is supplied. Reports contain
only bounded operational evidence and are written atomically to
``deploy/.generated/<environment>/readiness-report.json``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import struct
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

from deploy.scripts import _output as console
from deploy.scripts._auth import build_credential
from deploy.scripts.apply_kql import resolve_kql_database
from deploy.scripts.build_artifacts import ML_EXPERIMENT_GROUPS
from deploy.scripts.deploy_config import (
    DeployConfig,
    load_environment,
    load_terraform_outputs,
    validate_terraform_outputs,
)
from deploy.scripts.export_items import FABRIC_API, build_session, get_definition
from deploy.scripts.fabric_runtime import (
    FabricDefinitionError,
    decode_definition_parts,
    json_definition_part,
    normalize_schedule_document,
    paginated_get,
    schedule_document,
)
from deploy.scripts.profile_preflight import selected_notebook_names
from deploy.scripts.run_pipeline import (
    latest_pipeline_run,
    list_pipeline_runs,
    run_pipeline,
    wait_for_pipeline_job,
)

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
    from retail_setup.contracts import ResolvedProfile, SolutionManifest

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_SCHEMA_VERSION = "1.0.0"
REPORT_NAME = "readiness-report.json"
SQL_SCOPE = "https://database.windows.net/.default"

CheckStatus = Literal["PASS", "FAIL", "UNKNOWN", "SKIPPED"]
OverallStatus = Literal["SUCCEEDED", "FAILED", "DEGRADED"]

_SETUP_MAX_AGE = timedelta(days=7)
_MODEL_MAX_AGE = timedelta(days=7)
_STREAM_MAX_AGE = timedelta(minutes=30)
_FUTURE_SKEW = timedelta(minutes=5)
_CORRELATION_SKEW = timedelta(minutes=5)
_MAX_EVIDENCE_STRING = 300
_MAX_EVIDENCE_ITEMS = 25
_MAX_EVIDENCE_DEPTH = 5
_SECRET_KEY_RE = re.compile(
    r"(?i)(authorization|bearer|token|secret|password|payload|definition|"
    r"business_rows|raw_rows|diagnostic|tenant[-_]?id)"
)
_SECRET_VALUE_RE = re.compile(
    r"(?i)\b(Bearer\s+\S+|(?:access[-_]?token|client[-_]?secret|password|"
    r"api[-_]?key|tenant[-_]?id)\s*[:=]\s*\S+)"
)
_JWT_RE = re.compile(
    r"\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"
)
_POST_ONTOLOGY_CHECKS = frozenset(
    {
        "items.manual",
        "bindings.data_agents",
        "taskflow.bindings",
    }
)

_TABLE_RE = re.compile(
    r"(?im)^\s*\.create-merge\s+table\s+([A-Za-z_][A-Za-z0-9_]*)"
)
_FUNCTION_RE = re.compile(
    r"(?im)^\s*\.create-or-alter\s+function"
    r"(?:\s+with\s*\([^)]*\))?\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("
)
_MATERIALIZED_VIEW_RE = re.compile(
    r"(?im)^\s*\.create-or-alter\s+materialized-view\s+"
    r"([A-Za-z_][A-Za-z0-9_]*)"
)
_MAPPING_RE = re.compile(
    r"(?im)^\s*\.create-or-alter\s+table\s+([A-Za-z_][A-Za-z0-9_]*)"
    r"\s+ingestion\s+json\s+mapping\s+'([^']+)'"
)


class CheckFailed(RuntimeError):
    """A live fact was observed and did not satisfy its contract."""


class EvidenceUnknown(RuntimeError):
    """Required live evidence could not be obtained or correlated."""


class ReadinessUsageError(ValueError):
    """Operator input cannot be applied to the resolved profile."""


@dataclass(frozen=True)
class ExpectedItem:
    """One expected workspace item."""

    item_type: str
    display_name: str
    item_id: str | None = None
    required: bool = True

    @property
    def key(self) -> tuple[str, str]:
        return self.item_type, self.display_name


@dataclass(frozen=True)
class KqlInventory:
    """Expected or observed user-owned KQL objects."""

    tables: frozenset[str] = frozenset()
    functions: frozenset[str] = frozenset()
    materialized_views: frozenset[str] = frozenset()
    mappings: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Observation:
    """Safe, bounded check output before status aggregation."""

    summary: str
    evidence: dict[str, Any] = field(default_factory=dict)
    freshness: dict[str, Any] | None = None


@dataclass(frozen=True)
class CheckResult:
    """One normalized readiness check."""

    check_id: str
    category: str
    selected: bool
    required: bool
    status: CheckStatus
    summary: str
    evidence: dict[str, Any] = field(default_factory=dict)
    freshness: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.check_id,
            "category": self.category,
            "selected": self.selected,
            "required": self.required,
            "status": self.status,
            "summary": self.summary,
            "evidence": bound_evidence(self.evidence),
            "freshness": bound_evidence(self.freshness),
        }


@dataclass
class ReadinessContext:
    """Resolved local inputs and target identities for one verification."""

    repo_root: Path
    environment: str
    config: DeployConfig
    manifest: SolutionManifest
    outputs: dict[str, Any]
    manifest_hash: str
    profile_hash: str
    deploy_journal: dict[str, Any] | None
    observed_at: datetime
    observed_at_fixed: bool = True


class ReadinessAdapter(Protocol):
    """Injectable live boundary used by unit tests and the real adapter."""

    def list_items(self) -> list[dict[str, Any]]: ...

    def get_definition(self, item_id: str) -> dict[str, Any]: ...

    def get_taskflow(self) -> dict[str, Any] | None: ...

    def get_kql_inventory(self, expected_tables: frozenset[str]) -> KqlInventory: ...

    def list_pipeline_runs(self, pipeline_id: str) -> list[dict[str, object]]: ...

    def trigger_pipeline(
        self,
        pipeline_id: str,
        *,
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> dict[str, object]: ...

    def setup_signal(self) -> dict[str, Any] | None: ...

    def watermark_signal(self) -> dict[str, Any] | None: ...

    def eventhouse_signal(
        self, expected_tables: frozenset[str]
    ) -> dict[str, Any] | None: ...

    def checkpoint_signal(
        self, expected_tables: frozenset[str]
    ) -> dict[str, Any] | None: ...

    def model_signals(self, contracts: list[dict[str, Any]]) -> list[dict[str, Any]]: ...

    def alert_signal(self) -> dict[str, Any] | None: ...


def bound_evidence(value: Any, *, _depth: int = 0) -> Any:
    """Redact and bound report evidence without retaining raw live payloads."""

    if value is None or isinstance(value, (bool, int, float)):
        return value
    if _depth >= _MAX_EVIDENCE_DEPTH:
        return "[TRUNCATED]"
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, str):
        text = _SECRET_VALUE_RE.sub("[REDACTED]", value)
        text = _JWT_RE.sub("[REDACTED]", text)
        return text[:_MAX_EVIDENCE_STRING]
    if isinstance(value, dict):
        bounded: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= _MAX_EVIDENCE_ITEMS:
                bounded["_truncated"] = True
                break
            safe_key = str(key)[:80]
            bounded[safe_key] = (
                "[REDACTED]"
                if _SECRET_KEY_RE.search(safe_key)
                else bound_evidence(item, _depth=_depth + 1)
            )
        return bounded
    if isinstance(value, (list, tuple, set, frozenset)):
        items = list(value)
        bounded_items = [
            bound_evidence(item, _depth=_depth + 1)
            for item in items[:_MAX_EVIDENCE_ITEMS]
        ]
        if len(items) > _MAX_EVIDENCE_ITEMS:
            bounded_items.append("[TRUNCATED]")
        return bounded_items
    return str(value)[:_MAX_EVIDENCE_STRING]


def aggregate_status(checks: list[CheckResult]) -> OverallStatus:
    """Apply the required/optional fail-closed status contract."""

    required_failure = False
    optional_failure = False
    for check in checks:
        status = check.status
        if status == "SKIPPED" and check.selected:
            status = "UNKNOWN"
        if not check.selected:
            if status != "SKIPPED":
                optional_failure = True
            continue
        if status in {"FAIL", "UNKNOWN"}:
            if check.required:
                required_failure = True
            else:
                optional_failure = True
    if required_failure:
        return "FAILED"
    if optional_failure:
        return "DEGRADED"
    return "SUCCEEDED"


def validate_readiness_contract(
    manifest: SolutionManifest,
    profile: ResolvedProfile,
    checks: list[CheckResult],
    *,
    deferred_check_ids: frozenset[str] = frozenset(),
) -> None:
    """Require runtime checks to agree with manifest-owned readiness metadata."""

    expected = {
        expectation.id: expectation
        for expectation in manifest.readiness_expectations
    }
    actual = {check.check_id: check for check in checks}
    if set(actual) != set(expected):
        missing = sorted(set(expected) - set(actual))
        unexpected = sorted(set(actual) - set(expected))
        raise RuntimeError(
            "readiness taxonomy differs from the solution manifest: "
            f"missing={missing}, unexpected={unexpected}"
        )
    errors: list[str] = []
    for check_id, check in actual.items():
        expectation = expected[check_id]
        selected = profile.id in expectation.profile_refs
        if check_id in deferred_check_ids:
            if not selected:
                errors.append(
                    f"{check_id} cannot be deferred because the manifest "
                    "does not select it"
                )
            selected = False
        if check.category != expectation.category:
            errors.append(
                f"{check_id} category {check.category!r} != "
                f"{expectation.category!r}"
            )
        if check.selected != selected:
            errors.append(
                f"{check_id} selected={check.selected} != manifest {selected}"
            )
        required = expectation.required_when_selected if selected else False
        if check.required != required:
            errors.append(
                f"{check_id} required={check.required} != manifest {required}"
            )
    if errors:
        raise RuntimeError(
            "readiness behavior differs from manifest metadata: "
            + "; ".join(errors)
        )


def exit_code_for_status(status: OverallStatus) -> int:
    """Map report status to the public verifier process contract."""

    return {"SUCCEEDED": 0, "FAILED": 1, "DEGRADED": 3}[status]


def readiness_report_path(repo_root: Path, environment: str) -> Path:
    return (
        repo_root / "deploy" / ".generated" / environment / REPORT_NAME
    )


def write_report_atomic(path: Path, report: dict[str, Any]) -> None:
    """Atomically write a redacted readiness report beside environment state."""

    safe_report = bound_evidence(report)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=".readiness-report-",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(safe_report, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.remove(temp_name)


def parse_kql_inventory(
    repo_root: Path,
    script_names: tuple[str, ...] | list[str],
) -> KqlInventory:
    """Derive exact KQL object names from the profile-selected scripts."""

    tables: set[str] = set()
    functions: set[str] = set()
    materialized_views: set[str] = set()
    mappings: set[str] = set()
    for script_name in script_names:
        path = repo_root / "fabric" / "kql_database" / script_name
        text = path.read_text(encoding="utf-8")
        tables.update(_TABLE_RE.findall(text))
        functions.update(_FUNCTION_RE.findall(text))
        materialized_views.update(_MATERIALIZED_VIEW_RE.findall(text))
        mappings.update(
            f"{table}/{mapping}"
            for table, mapping in _MAPPING_RE.findall(text)
        )
    return KqlInventory(
        tables=frozenset(tables),
        functions=frozenset(functions),
        materialized_views=frozenset(materialized_views),
        mappings=frozenset(mappings),
    )


def compare_sets(expected: frozenset[str], observed: frozenset[str]) -> dict[str, Any]:
    """Return stable set-difference evidence used by KQL and task-flow checks."""

    return {
        "expected_count": len(expected),
        "observed_count": len(observed),
        "missing": sorted(expected - observed),
        "unexpected": sorted(observed - expected),
    }


def compare_item_inventory(
    expected: list[ExpectedItem],
    observed: list[dict[str, Any]],
) -> dict[str, Any]:
    """Find missing, duplicated, and Terraform-ID-mismatched live items."""

    counts = Counter(
        (str(item.get("type", "")), str(item.get("displayName", "")))
        for item in observed
    )
    ids: dict[tuple[str, str], list[str]] = {}
    for item in observed:
        key = (str(item.get("type", "")), str(item.get("displayName", "")))
        ids.setdefault(key, []).append(str(item.get("id", "")))

    missing = [f"{item.item_type}:{item.display_name}" for item in expected if not counts[item.key]]
    duplicates = [
        f"{item.item_type}:{item.display_name}"
        for item in expected
        if counts[item.key] > 1
    ]
    mismatched_ids = []
    missing_ids = []
    for item in expected:
        if counts[item.key] == 1 and not ids[item.key][0]:
            missing_ids.append(f"{item.item_type}:{item.display_name}")
        elif item.item_id and counts[item.key] == 1 and ids[item.key][0] != item.item_id:
            mismatched_ids.append(
                {
                    "item": f"{item.item_type}:{item.display_name}",
                    "expected_id": item.item_id,
                    "observed_id": ids[item.key][0],
                }
            )
    return {
        "expected_count": len(expected),
        "matched_count": sum(counts[item.key] == 1 for item in expected),
        "missing": missing,
        "duplicates": duplicates,
        "missing_ids": missing_ids,
        "mismatched_ids": mismatched_ids,
    }


def notebook_binding_errors(
    definition: dict[str, Any],
    *,
    lakehouse_id: str,
    lakehouse_name: str,
    workspace_id: str,
) -> list[str]:
    """Validate one live notebook's default-Lakehouse binding."""

    parts = decode_definition_parts(definition)
    notebook = json_definition_part(parts, ".ipynb")
    lakehouse = (
        notebook.get("metadata", {})
        .get("dependencies", {})
        .get("lakehouse", {})
    )
    errors: list[str] = []
    if lakehouse.get("default_lakehouse") != lakehouse_id:
        errors.append("default_lakehouse ID mismatch")
    if lakehouse.get("default_lakehouse_name") != lakehouse_name:
        errors.append("default_lakehouse_name mismatch")
    if lakehouse.get("default_lakehouse_workspace_id") != workspace_id:
        errors.append("default_lakehouse_workspace_id mismatch")
    return errors


def pipeline_binding_errors(
    definition: dict[str, Any],
    *,
    expected_notebook_ids: dict[str, str],
    workspace_id: str,
) -> list[str]:
    """Validate notebook and workspace bindings in one pipeline definition."""

    parts = decode_definition_parts(definition)
    pipeline = json_definition_part(parts, "pipeline-content.json")
    activities = pipeline.get("properties", {}).get("activities", [])
    if not isinstance(activities, list):
        return ["pipeline activities are missing"]
    live_refs: dict[str, tuple[str, str]] = {}
    errors: list[str] = []
    for activity in activities:
        if not isinstance(activity, dict) or activity.get("type") != "TridentNotebook":
            continue
        name = str(activity.get("name", ""))
        properties = activity.get("typeProperties", {})
        if not isinstance(properties, dict):
            errors.append(f"{name}: typeProperties are missing")
            continue
        if name in live_refs:
            errors.append(f"{name}: duplicate notebook activity")
        live_refs[name] = (
            str(properties.get("notebookId", "")),
            str(properties.get("workspaceId", "")),
        )
    if set(live_refs) != set(expected_notebook_ids):
        errors.append("pipeline notebook activity inventory mismatch")
    for name, notebook_id in expected_notebook_ids.items():
        live_id, live_workspace = live_refs.get(name, ("", ""))
        if live_id != notebook_id:
            errors.append(f"{name}: notebook ID mismatch")
        if live_workspace != workspace_id:
            errors.append(f"{name}: workspace ID mismatch")
    return errors


def semantic_model_binding_errors(
    definition: dict[str, Any],
    *,
    workspace_id: str,
    lakehouse_id: str,
) -> list[str]:
    """Validate the live Direct Lake target without persisting TMDL."""

    parts = decode_definition_parts(definition)
    text = "\n".join(
        content.decode("utf-8", errors="ignore")
        for path, content in parts.items()
        if path.casefold().endswith(".tmdl")
    )
    expected = f"/{workspace_id}/{lakehouse_id}"
    return [] if expected.casefold() in text.casefold() else ["Direct Lake target mismatch"]


def report_binding_errors(
    definition: dict[str, Any],
    *,
    semantic_model_id: str,
    semantic_model_name: str,
) -> list[str]:
    """Validate a report's semantic-model binding by live ID or portable path."""

    parts = decode_definition_parts(definition)
    report = json_definition_part(parts, "definition.pbir")
    serialized = json.dumps(report, sort_keys=True)
    path_binding = f"../{semantic_model_name}.SemanticModel"
    if semantic_model_id in serialized or path_binding in serialized:
        return []
    return ["report semantic-model binding mismatch"]


def queryset_binding_errors(
    definition: dict[str, Any],
    *,
    kql_database_id: str,
    kql_database_name: str,
    expected_tabs: set[str],
) -> list[str]:
    """Validate KQL Queryset database binding and source-defined tab inventory."""

    parts = decode_definition_parts(definition)
    document = json_definition_part(parts, "RealTimeQueryset.json")
    queryset = document.get("queryset", {})
    sources = queryset.get("dataSources", []) if isinstance(queryset, dict) else []
    tabs = queryset.get("tabs", []) if isinstance(queryset, dict) else []
    errors: list[str] = []
    if (
        not isinstance(sources, list)
        or len(sources) != 1
        or not isinstance(sources[0], dict)
    ):
        errors.append("queryset must have exactly one data source")
    else:
        source = sources[0]
        if str(source.get("databaseItemId", "")) != kql_database_id:
            errors.append("queryset KQL database ID mismatch")
        if str(source.get("databaseItemName", "")) != kql_database_name:
            errors.append("queryset KQL database name mismatch")
    if not isinstance(tabs, list):
        errors.append("queryset tabs are missing")
        tabs = []
    observed_tab_list = [
        str(tab.get("title", ""))
        for tab in tabs
        if isinstance(tab, dict)
    ]
    observed_tabs = set(observed_tab_list)
    if len(observed_tab_list) != len(observed_tabs):
        errors.append("queryset contains duplicate tab titles")
    if observed_tabs != expected_tabs:
        errors.append("queryset tab inventory mismatch")
    return errors


def data_agent_binding_errors(
    definition: dict[str, Any],
    *,
    workspace_id: str,
    expected_artifact_ids: set[str],
) -> list[str]:
    """Validate every live Data Agent datasource binding."""

    parts = decode_definition_parts(definition)
    datasource_parts = [
        (path, content)
        for path, content in parts.items()
        if path.casefold().endswith("datasource.json")
    ]
    if not datasource_parts:
        return ["data agent has no datasource definition"]
    errors: list[str] = []
    observed_artifacts: set[str] = set()
    for path, content in datasource_parts:
        try:
            datasource = json.loads(content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            errors.append(f"{path}: invalid datasource JSON")
            continue
        if not isinstance(datasource, dict):
            errors.append(f"{path}: datasource is not an object")
            continue
        if str(datasource.get("workspaceId", "")) != workspace_id:
            errors.append(f"{path}: workspace binding mismatch")
        artifact_id = str(datasource.get("artifactId", ""))
        if not artifact_id:
            errors.append(f"{path}: artifact binding is missing")
            continue
        observed_artifacts.add(artifact_id)
    if not observed_artifacts <= expected_artifact_ids:
        errors.append("data agent artifact binding mismatch")
    return errors


def normalize_job_evidence(run: dict[str, object]) -> dict[str, Any]:
    """Select only safe job fields used for status and freshness correlation."""

    return {
        "id": str(run.get("id", "")),
        "item_id": str(run.get("itemId", "")),
        "job_type": str(run.get("jobType", "")),
        "status": str(run.get("status", "")),
        "start_time": run.get("startTimeUtc"),
        "end_time": run.get("endTimeUtc"),
    }


def validate_terminal_job_evidence(evidence: dict[str, Any]) -> None:
    """Require a complete, chronologically valid terminal-success record."""

    if evidence.get("status") != "Completed":
        raise CheckFailed(
            f"Pipeline job is not terminal-successful ({evidence.get('status')!r})."
        )
    if not evidence.get("id"):
        raise EvidenceUnknown("pipeline job identity is missing")
    started = _parse_time(evidence.get("start_time"))
    ended = _parse_time(evidence.get("end_time"))
    if ended < started:
        raise CheckFailed("pipeline job ended before it started")


def correlated_pipeline_run(
    runs: list[dict[str, object]],
    *,
    pipeline_id: str,
    step_started: datetime,
    step_ended: datetime,
) -> dict[str, Any]:
    """Select a terminal run bounded by one successful deploy-journal step."""

    candidates: list[dict[str, object]] = []
    for run in runs:
        started = _parse_time(run.get("startTimeUtc"))
        if (
            step_started - _CORRELATION_SKEW
            <= started
            <= step_ended + _CORRELATION_SKEW
        ):
            candidates.append(run)
    if not candidates:
        raise EvidenceUnknown(
            "pipeline history has no run within the deploy-journal step"
        )
    evidence = normalize_job_evidence(
        latest_pipeline_run(candidates, pipeline_id=pipeline_id)
    )
    if not evidence["item_id"]:
        evidence["item_id"] = pipeline_id
    validate_terminal_job_evidence(evidence)
    ended = _parse_time(evidence["end_time"])
    if (
        ended < step_started - _CORRELATION_SKEW
        or ended > step_ended + _CORRELATION_SKEW
    ):
        raise CheckFailed("pipeline terminal evidence falls outside its journal step")
    return evidence


def _parse_time(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise EvidenceUnknown("timestamp evidence is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceUnknown("timestamp evidence is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def evaluate_freshness(
    source_timestamp: Any,
    *,
    observed_at: datetime,
    max_age: timedelta,
    not_before: datetime | None = None,
    lineage: str,
) -> dict[str, Any]:
    """Validate bounded freshness and optional run correlation."""

    source_time = _parse_time(source_timestamp)
    observed_at = observed_at.astimezone(UTC)
    if source_time > observed_at + _FUTURE_SKEW:
        raise CheckFailed("freshness timestamp is implausibly in the future")
    age = observed_at - source_time
    if age > max_age:
        raise CheckFailed(
            f"evidence is stale ({int(age.total_seconds())} seconds old)"
        )
    if not_before and source_time < not_before - _CORRELATION_SKEW:
        raise CheckFailed("freshness evidence predates the correlated pipeline run")
    return {
        "observed_at": observed_at.isoformat(),
        "source_timestamp": source_time.isoformat(),
        "age_seconds": max(0, int(age.total_seconds())),
        "max_age_seconds": int(max_age.total_seconds()),
        "lineage": lineage,
    }


def _profile_hash(profile: ResolvedProfile) -> str:
    document = {
        "id": profile.id,
        "deployment_name": profile.deployment_name,
        "support_status": profile.support_status,
        "manifest_hash": profile.manifest_hash,
        "assets": [asdict(asset) for asset in profile.assets],
        "notebook_groups": profile.notebook_groups,
        "pipeline_refs": profile.pipeline_refs,
        "kql_scripts": profile.kql_scripts,
        "item_types_in_scope": profile.item_types_in_scope,
        "post_deploy_pipeline_ref": profile.post_deploy_pipeline_ref,
        "reporting_gate_pipeline_ref": profile.reporting_gate_pipeline_ref,
        "post_reporting_pipeline_refs": profile.post_reporting_pipeline_refs,
        "publication": profile.publication.model_dump(mode="json"),
        "boundaries": profile.boundaries.model_dump(mode="json"),
    }
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _journal_matches_context(
    context: ReadinessContext,
    journal: dict[str, Any],
) -> bool:
    """Accept correlation timestamps only from this target and resolved profile."""

    manifest = journal.get("manifest")
    targets = journal.get("targets")
    if not isinstance(manifest, dict) or not isinstance(targets, dict):
        return False
    profile = context.config.profile
    expected = (
        journal.get("environment") == context.environment,
        manifest.get("hash") == context.manifest_hash,
        manifest.get("profile_id") == profile.id,
        manifest.get("profile_name") == profile.deployment_name,
        targets.get("profile") == profile.deployment_name,
        targets.get("workspace_name") == context.config.workspace.name,
        targets.get("lakehouse_name") == context.config.lakehouse.name,
    )
    return all(expected)


def load_readiness_context(
    repo_root: Path,
    environment: str,
    *,
    observed_at: datetime | None = None,
) -> ReadinessContext:
    """Resolve the configured profile, manifest, Terraform targets, and journal."""

    from retail_setup.contracts import load_repository_manifest

    config = load_environment(
        environment,
        config_path=repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=repo_root / "deploy" / "config" / "environments",
    )
    manifest, _validation = load_repository_manifest(repo_root)
    output_path = (
        repo_root
        / "deploy"
        / ".generated"
        / environment
        / "terraform-output.json"
    )
    outputs = load_terraform_outputs(output_path)
    validate_terraform_outputs(config, outputs)
    journal_path = (
        repo_root
        / "deploy"
        / ".generated"
        / environment
        / "deploy-run.json"
    )
    deploy_journal = None
    if journal_path.is_file():
        loaded = json.loads(journal_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            deploy_journal = loaded
    return ReadinessContext(
        repo_root=repo_root,
        environment=environment,
        config=config,
        manifest=manifest,
        outputs=outputs,
        manifest_hash=config.profile.manifest_hash,
        profile_hash=_profile_hash(config.profile),
        deploy_journal=deploy_journal,
        observed_at=(observed_at or datetime.now(UTC)).astimezone(
            UTC
        ),
        observed_at_fixed=observed_at is not None,
    )


def expected_live_items(
    context: ReadinessContext,
    *,
    include_post_ontology: bool = True,
) -> list[ExpectedItem]:
    """Build the exact profile-selected item inventory."""

    config = context.config
    profile = config.profile
    outputs = context.outputs
    expected = [
        ExpectedItem(
            "Lakehouse",
            config.lakehouse.name,
            str(outputs.get("lakehouse_id") or ""),
        ),
        ExpectedItem("SQLEndpoint", config.lakehouse.name),
    ]
    expected.extend(
        ExpectedItem("Notebook", name)
        for name in selected_notebook_names(profile)
    )
    expected.extend(
        ExpectedItem("DataPipeline", Path(reference).stem)
        for reference in profile.pipeline_refs
    )
    for group in profile.notebook_groups:
        expected.extend(
            ExpectedItem("MLExperiment", name)
            for name in ML_EXPERIMENT_GROUPS.get(group, ())
        )
    if profile.provisions_eventhouse:
        expected.extend(
            [
                ExpectedItem(
                    "Eventhouse",
                    config.eventhouse.name,
                    str(outputs.get("eventhouse_id") or ""),
                ),
                ExpectedItem(
                    "KQLDatabase",
                    config.eventhouse.kql_database_name,
                    str(outputs.get("kql_database_id") or ""),
                ),
            ]
        )
    if profile.selects("asset.semantic-model"):
        expected.append(
            ExpectedItem("SemanticModel", config.powerbi.semantic_model_name)
        )
    if profile.selects("asset.report"):
        expected.append(ExpectedItem("Report", config.powerbi.report_name))
    if profile.selects("asset.kql-queryset"):
        expected.append(ExpectedItem("KQLQueryset", "retail_querysets"))
    if include_post_ontology and profile.selects("asset.data-agents"):
        expected.extend(
            ExpectedItem("DataAgent", path.stem)
            for path in sorted(
                (context.repo_root / "fabric" / "data-agents").glob("*.DataAgent")
            )
        )
    if include_post_ontology and profile.selects("asset.ontology"):
        expected.append(
            ExpectedItem(
                "Ontology",
                "RetailOntology_AutoGen",
                required=False,
            )
        )
    return expected


def _safe_exception_summary(exc: Exception) -> str:
    return f"Live evidence unavailable ({type(exc).__name__})."


class ReadinessRunner:
    """Evaluate the fixed readiness taxonomy for one resolved profile."""

    def __init__(
        self,
        context: ReadinessContext,
        adapter: ReadinessAdapter,
        *,
        run_pipeline_requested: bool = False,
        defer_post_ontology: bool = False,
        timeout_seconds: float = 21600,
        poll_interval_seconds: float = 15,
    ) -> None:
        self.context = context
        self.adapter = adapter
        self.profile = context.config.profile
        self.run_pipeline_requested = run_pipeline_requested
        self.defer_post_ontology = defer_post_ontology
        self.timeout_seconds = timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.checks: list[CheckResult] = []
        self.items: list[dict[str, Any]] = []
        self.item_index: dict[tuple[str, str], dict[str, Any]] = {}
        self.definitions: dict[str, dict[str, Any]] = {}
        self.kql_inventory: KqlInventory | None = None
        self.pipeline_evidence: dict[str, dict[str, Any]] = {}
        self.trigger_error: Exception | None = None

    def run(self) -> list[CheckResult]:
        """Run all 26 checks; unselected capabilities are explicit SKIPPED rows."""

        self._check(
            "target.identities",
            "target",
            selected=True,
            required=True,
            function=self._target_identities,
        )
        try:
            self.items = self.adapter.list_items()
            self.item_index = {
                (str(item.get("type", "")), str(item.get("displayName", ""))): item
                for item in self.items
            }
        except Exception as exc:
            self.items = []
            self.item_index = {}
            self._items_error = exc
        else:
            self._items_error = None

        expected = expected_live_items(
            self.context,
            include_post_ontology=not self.defer_post_ontology,
        )
        required_items = [item for item in expected if item.required]
        manual_items = [item for item in expected if not item.required]
        self._check(
            "items.required",
            "inventory",
            selected=True,
            required=True,
            function=lambda: self._item_inventory(required_items),
        )
        self._check(
            "items.manual",
            "inventory",
            selected=bool(manual_items),
            required=False,
            function=lambda: self._item_inventory(manual_items),
        )

        if self.run_pipeline_requested:
            self._trigger_profile_pipeline()

        self._check(
            "bindings.notebooks",
            "binding",
            selected=bool(self.profile.notebook_groups),
            required=True,
            function=self._notebook_bindings,
        )
        self._check(
            "bindings.pipelines",
            "binding",
            selected=bool(self.profile.pipeline_refs),
            required=True,
            function=self._pipeline_bindings,
        )
        self._check(
            "bindings.semantic_model",
            "binding",
            selected=self.profile.selects("asset.semantic-model"),
            required=True,
            function=self._semantic_model_binding,
        )
        self._check(
            "bindings.report",
            "binding",
            selected=self.profile.selects("asset.report"),
            required=True,
            function=self._report_binding,
        )
        self._check(
            "bindings.queryset",
            "binding",
            selected=self.profile.selects("asset.kql-queryset"),
            required=True,
            function=self._queryset_binding,
        )
        self._check(
            "bindings.data_agents",
            "binding",
            selected=(
                self.profile.selects("asset.data-agents")
                and not self.defer_post_ontology
            ),
            required=False,
            function=self._data_agent_bindings,
        )
        self._check(
            "taskflow.bindings",
            "taskflow",
            selected=(
                self.profile.deploys_task_flow
                and not self.defer_post_ontology
            ),
            required=True,
            function=self._taskflow_bindings,
        )
        self._run_kql_checks()
        self._check(
            "schedules.pipelines",
            "schedule",
            selected=bool(self._source_schedule_paths()),
            required=True,
            function=self._schedule_bindings,
        )
        self._run_pipeline_checks()
        self._run_freshness_checks()
        validate_readiness_contract(
            self.context.manifest,
            self.profile,
            self.checks,
            deferred_check_ids=(
                _POST_ONTOLOGY_CHECKS
                if self.defer_post_ontology
                else frozenset()
            ),
        )
        return self.checks

    def _check(
        self,
        check_id: str,
        category: str,
        *,
        selected: bool,
        required: bool,
        function: Any,
    ) -> None:
        if not selected:
            self.checks.append(
                CheckResult(
                    check_id,
                    category,
                    selected=False,
                    required=False,
                    status="SKIPPED",
                    summary="Capability is not selected by the resolved profile.",
                )
            )
            return
        try:
            observation = function()
            if not isinstance(observation, Observation):
                raise TypeError("check did not return an Observation")
            status: CheckStatus = "PASS"
            summary = observation.summary
            evidence = observation.evidence
            freshness = observation.freshness
        except CheckFailed as exc:
            status = "FAIL"
            summary = str(exc)
            evidence = {}
            freshness = None
        except EvidenceUnknown as exc:
            status = "UNKNOWN"
            summary = str(exc)
            evidence = {}
            freshness = None
        except Exception as exc:
            status = "UNKNOWN"
            summary = _safe_exception_summary(exc)
            evidence = {"error_type": type(exc).__name__}
            freshness = None
        self.checks.append(
            CheckResult(
                check_id,
                category,
                selected=True,
                required=required,
                status=status,
                summary=summary,
                evidence=evidence,
                freshness=freshness,
            )
        )

    def _target_identities(self) -> Observation:
        try:
            validate_terraform_outputs(self.context.config, self.context.outputs)
        except (KeyError, TypeError, ValueError) as exc:
            raise CheckFailed("Terraform targets do not match configured targets.") from exc
        if self.profile.uses_custom_pool and not self.context.outputs.get(
            "spark_custom_pool_id"
        ):
            raise CheckFailed("Selected custom Spark pool has no Terraform output ID.")
        return Observation(
            "Configured names and Terraform target identities agree.",
            {
                "workspace_id": self.context.outputs.get("workspace_id"),
                "workspace_name": self.context.outputs.get("workspace_name"),
                "lakehouse_id": self.context.outputs.get("lakehouse_id"),
                "lakehouse_name": self.context.outputs.get("lakehouse_name"),
                "eventhouse_id": self.context.outputs.get("eventhouse_id"),
                "eventhouse_name": self.context.outputs.get("eventhouse_name"),
                "kql_database_id": self.context.outputs.get("kql_database_id"),
                "kql_database_name": self.context.outputs.get("kql_database_name"),
                "spark_custom_pool_id": self.context.outputs.get(
                    "spark_custom_pool_id"
                ),
            },
        )

    def _item_inventory(self, expected: list[ExpectedItem]) -> Observation:
        if self._items_error is not None:
            raise EvidenceUnknown("Workspace item inventory could not be listed.")
        comparison = compare_item_inventory(expected, self.items)
        if (
            comparison["missing"]
            or comparison["duplicates"]
            or comparison["missing_ids"]
            or comparison["mismatched_ids"]
        ):
            raise CheckFailed(
                "Live workspace item inventory is missing, duplicated, "
                "unidentified, or mismatched."
            )
        return Observation("Every expected live item has one matching identity.", comparison)

    def _item(self, item_type: str, display_name: str) -> dict[str, Any]:
        matches = [
            item
            for item in self.items
            if str(item.get("type", "")) == item_type
            and str(item.get("displayName", "")) == display_name
        ]
        if len(matches) != 1:
            raise CheckFailed(
                f"Expected one {item_type}:{display_name} item; found {len(matches)}."
            )
        return matches[0]

    def _definition(self, item_type: str, display_name: str) -> dict[str, Any]:
        item = self._item(item_type, display_name)
        item_id = str(item.get("id", ""))
        if item_id not in self.definitions:
            self.definitions[item_id] = self.adapter.get_definition(item_id)
        return self.definitions[item_id]

    def _notebook_bindings(self) -> Observation:
        errors: list[str] = []
        for name in selected_notebook_names(self.profile):
            try:
                errors.extend(
                    f"{name}: {error}"
                    for error in notebook_binding_errors(
                        self._definition("Notebook", name),
                        lakehouse_id=str(self.context.outputs["lakehouse_id"]),
                        lakehouse_name=self.context.config.lakehouse.name,
                        workspace_id=str(self.context.outputs["workspace_id"]),
                    )
                )
            except (CheckFailed, FabricDefinitionError) as exc:
                errors.append(f"{name}: {exc}")
        if errors:
            raise CheckFailed(
                f"Notebook definitions or Lakehouse bindings failed ({len(errors)} error(s))."
            )
        return Observation(
            "Selected notebook definitions bind to the target Lakehouse.",
            {"notebook_count": len(selected_notebook_names(self.profile))},
        )

    def _pipeline_bindings(self) -> Observation:
        errors: list[str] = []
        for reference in self.profile.pipeline_refs:
            name = Path(reference).stem
            source_path = (
                self.context.repo_root
                / "fabric"
                / "pipelines"
                / reference
                / "pipeline-content.json"
            )
            source = json.loads(source_path.read_text(encoding="utf-8"))
            expected_refs: dict[str, str] = {}
            for activity in source.get("properties", {}).get("activities", []):
                if activity.get("type") != "TridentNotebook":
                    continue
                notebook_name = str(activity.get("name", ""))
                expected_refs[notebook_name] = str(
                    self._item("Notebook", notebook_name).get("id", "")
                )
            try:
                errors.extend(
                    f"{name}: {error}"
                    for error in pipeline_binding_errors(
                        self._definition("DataPipeline", name),
                        expected_notebook_ids=expected_refs,
                        workspace_id=str(self.context.outputs["workspace_id"]),
                    )
                )
            except (CheckFailed, FabricDefinitionError) as exc:
                errors.append(f"{name}: {exc}")
        if errors:
            raise CheckFailed(
                f"Pipeline definitions or notebook bindings failed ({len(errors)} error(s))."
            )
        return Observation(
            "Selected pipeline definitions bind to selected target notebooks.",
            {"pipeline_count": len(self.profile.pipeline_refs)},
        )

    def _semantic_model_binding(self) -> Observation:
        errors = semantic_model_binding_errors(
            self._definition(
                "SemanticModel", self.context.config.powerbi.semantic_model_name
            ),
            workspace_id=str(self.context.outputs["workspace_id"]),
            lakehouse_id=str(self.context.outputs["lakehouse_id"]),
        )
        if errors:
            raise CheckFailed(errors[0])
        return Observation(
            "Semantic model Direct Lake expression targets the configured Lakehouse."
        )

    def _report_binding(self) -> Observation:
        semantic = self._item(
            "SemanticModel", self.context.config.powerbi.semantic_model_name
        )
        errors = report_binding_errors(
            self._definition("Report", self.context.config.powerbi.report_name),
            semantic_model_id=str(semantic.get("id", "")),
            semantic_model_name=self.context.config.powerbi.semantic_model_name,
        )
        if errors:
            raise CheckFailed(errors[0])
        return Observation("Report definition binds to the selected semantic model.")

    def _queryset_binding(self) -> Observation:
        tabs = {
            path.stem
            for path in (
                self.context.repo_root / "fabric" / "querysets"
            ).glob("*.kql")
        }
        errors = queryset_binding_errors(
            self._definition("KQLQueryset", "retail_querysets"),
            kql_database_id=str(self.context.outputs["kql_database_id"]),
            kql_database_name=self.context.config.eventhouse.kql_database_name,
            expected_tabs=tabs,
        )
        if errors:
            raise CheckFailed("; ".join(errors))
        return Observation(
            "KQL Queryset binds to the target database with exact source tabs.",
            {"tab_count": len(tabs)},
        )

    def _data_agent_bindings(self) -> Observation:
        workspace_id = str(self.context.outputs["workspace_id"])
        expected_artifacts = {
            str(
                self._item(
                    "SemanticModel",
                    self.context.config.powerbi.semantic_model_name,
                ).get("id", "")
            )
        }
        ontology = [
            item
            for item in self.items
            if str(item.get("type", "")) == "Ontology"
            and str(item.get("displayName", "")) == "RetailOntology_AutoGen"
        ]
        expected_artifacts.update(str(item.get("id", "")) for item in ontology)
        errors: list[str] = []
        names = [
            path.stem
            for path in sorted(
                (self.context.repo_root / "fabric" / "data-agents").glob(
                    "*.DataAgent"
                )
            )
        ]
        for name in names:
            errors.extend(
                f"{name}: {error}"
                for error in data_agent_binding_errors(
                    self._definition("DataAgent", name),
                    workspace_id=workspace_id,
                    expected_artifact_ids=expected_artifacts,
                )
            )
        if errors:
            raise CheckFailed(
                f"Data Agent bindings failed ({len(errors)} error(s))."
            )
        return Observation(
            "Selected Data Agent definitions bind only to target workspace items.",
            {"agent_count": len(names)},
        )

    def _taskflow_bindings(self) -> Observation:
        from deploy.scripts.taskflow import (
            filter_portable_items,
            profile_taskflow_artifacts,
            to_portable,
        )

        record = self.adapter.get_taskflow()
        if not record or not isinstance(record.get("taskFlow"), dict):
            raise CheckFailed("Selected workspace task flow is absent.")
        source = json.loads(
            (
                self.context.repo_root / "fabric" / "taskflow" / "taskflow.json"
            ).read_text(encoding="utf-8")
        )
        expected = filter_portable_items(
            source,
            profile_taskflow_artifacts(
                self.context.repo_root,
                self.context.config,
            ),
        )
        guid_to_name = {
            str(item.get("id", "")): str(item.get("displayName", ""))
            for item in self.items
        }
        actual = to_portable(record["taskFlow"], guid_to_name)
        expected_refs = _taskflow_references(expected)
        expected_edges = _taskflow_edges(expected)
        errors = taskflow_binding_errors(
            expected,
            record["taskFlow"],
            actual,
        )
        if errors:
            raise CheckFailed("Task-flow references or edges differ from the profile.")
        return Observation(
            "Workspace task flow contains exact selected references and edges.",
            {
                "reference_count": len(expected_refs),
                "edge_count": len(expected_edges),
            },
        )

    def _run_kql_checks(self) -> None:
        selected = self.profile.provisions_eventhouse
        expected = parse_kql_inventory(
            self.context.repo_root,
            self.profile.kql_scripts,
        )
        for check_id, attribute in (
            ("kql.tables", "tables"),
            ("kql.functions", "functions"),
            ("kql.materialized_views", "materialized_views"),
            ("kql.mappings", "mappings"),
        ):
            self._check(
                check_id,
                "kql",
                selected=selected,
                required=True,
                function=lambda attribute=attribute: self._kql_set_check(
                    attribute,
                    getattr(expected, attribute),
                    expected.tables,
                ),
            )

    def _kql_set_check(
        self,
        attribute: str,
        expected: frozenset[str],
        expected_tables: frozenset[str],
    ) -> Observation:
        if self.kql_inventory is None:
            self.kql_inventory = self.adapter.get_kql_inventory(expected_tables)
        observed = getattr(self.kql_inventory, attribute)
        comparison = compare_sets(expected, observed)
        if comparison["missing"] or comparison["unexpected"]:
            raise CheckFailed(f"Live KQL {attribute} inventory differs from source.")
        return Observation(f"Live KQL {attribute} inventory matches source.", comparison)

    def _source_schedule_paths(self) -> list[Path]:
        return [
            self.context.repo_root
            / "fabric"
            / "pipelines"
            / reference
            / ".schedules"
            for reference in self.profile.pipeline_refs
            if (
                self.context.repo_root
                / "fabric"
                / "pipelines"
                / reference
                / ".schedules"
            ).is_file()
        ]

    def _schedule_bindings(self) -> Observation:
        errors: list[str] = []
        for source_path in self._source_schedule_paths():
            name = source_path.parent.stem
            source = normalize_schedule_document(
                json.loads(source_path.read_text(encoding="utf-8"))
            )
            live = schedule_document(
                decode_definition_parts(
                    self._definition("DataPipeline", name)
                )
            )
            if live != source:
                errors.append(name)
        if errors:
            raise CheckFailed(
                "Live schedule definitions differ for: " + ", ".join(errors)
            )
        return Observation(
            "Live enabled and disabled schedules match source definitions.",
            {"scheduled_pipeline_count": len(self._source_schedule_paths())},
        )

    def _trigger_profile_pipeline(self) -> None:
        reference = self.profile.post_deploy_pipeline_ref
        if reference is None:
            raise ReadinessUsageError(
                f"profile {self.profile.deployment_name!r} has no required "
                "post-publish pipeline"
            )
        name = Path(reference).stem
        try:
            item = self._item("DataPipeline", name)
            payload = self.adapter.trigger_pipeline(
                str(item.get("id", "")),
                timeout_seconds=self.timeout_seconds,
                poll_interval_seconds=self.poll_interval_seconds,
            )
            evidence = normalize_job_evidence(payload)
            validate_terminal_job_evidence(evidence)
            self.pipeline_evidence[name] = evidence
        except Exception as exc:
            self.trigger_error = exc

    def _run_pipeline_checks(self) -> None:
        self._check(
            "pipelines.post_deploy",
            "pipeline",
            selected=self.profile.post_deploy_pipeline_ref is not None,
            required=True,
            function=lambda: self._pipeline_check(
                self.profile.post_deploy_pipeline_ref,
                "setup-pipeline-gate",
            ),
        )
        self._check(
            "pipelines.reporting_gate",
            "pipeline",
            selected=self.profile.reporting_gate_pipeline_ref is not None,
            required=True,
            function=lambda: self._pipeline_check(
                self.profile.reporting_gate_pipeline_ref,
                "required-ml-reporting-gate",
            ),
        )
        self._check(
            "pipelines.post_reporting",
            "pipeline",
            selected=bool(self.profile.post_reporting_pipeline_refs),
            required=False,
            function=self._post_reporting_pipeline_check,
        )

    def _pipeline_check(
        self,
        reference: str | None,
        journal_step: str,
    ) -> Observation:
        if reference is None:
            raise EvidenceUnknown("profile pipeline reference is missing")
        name = Path(reference).stem
        if (
            self.run_pipeline_requested
            and reference == self.profile.post_deploy_pipeline_ref
            and self.trigger_error is not None
        ):
            raise EvidenceUnknown("explicit pipeline run could not be correlated")
        evidence = self.pipeline_evidence.get(name)
        if evidence is None:
            evidence = self._latest_correlated_pipeline(name, journal_step)
            self.pipeline_evidence[name] = evidence
        validate_terminal_job_evidence(evidence)
        return Observation(
            f"Pipeline {name!r} reached exact terminal success.",
            evidence,
            {
                "source_timestamp": evidence.get("end_time"),
                "lineage": f"Fabric job instance {evidence.get('id')}",
            },
        )

    def _post_reporting_pipeline_check(self) -> Observation:
        statuses: dict[str, str] = {}
        for reference in self.profile.post_reporting_pipeline_refs:
            name = Path(reference).stem
            evidence = self._latest_correlated_pipeline(
                name,
                f"post-reporting-{name}",
            )
            self.pipeline_evidence[name] = evidence
            statuses[name] = str(evidence["status"])
        if any(status != "Completed" for status in statuses.values()):
            raise CheckFailed("A post-Reporting pipeline did not complete successfully.")
        return Observation(
            "All selected post-Reporting pipelines reached terminal success.",
            {"statuses": statuses},
        )

    def _latest_correlated_pipeline(
        self,
        name: str,
        journal_step: str,
    ) -> dict[str, Any]:
        item = self._item("DataPipeline", name)
        pipeline_id = str(item.get("id", ""))
        window = self._journal_step_window(journal_step)
        if window is None:
            raise EvidenceUnknown(
                f"Deploy journal has no correlation for pipeline {name!r}."
            )
        return correlated_pipeline_run(
            self.adapter.list_pipeline_runs(pipeline_id),
            pipeline_id=pipeline_id,
            step_started=window[0],
            step_ended=window[1],
        )

    def _journal_step_window(
        self,
        step_id: str,
    ) -> tuple[datetime, datetime] | None:
        journal = self.context.deploy_journal
        if not journal or not _journal_matches_context(self.context, journal):
            return None
        for step in journal.get("steps", []):
            if (
                isinstance(step, dict)
                and step.get("step_id") == step_id
                and step.get("status") == "SUCCEEDED"
            ):
                try:
                    started = _parse_time(step.get("started_at"))
                    ended = _parse_time(step.get("ended_at"))
                except EvidenceUnknown:
                    return None
                return (started, ended) if ended >= started else None
        return None

    def _run_freshness_checks(self) -> None:
        if not self.context.observed_at_fixed:
            self.context.observed_at = datetime.now(UTC)
        streaming = self.profile.selects("asset.stream-events")
        ml_required = "ml-required" in self.profile.notebook_groups
        ml_optional = "ml-optional" in self.profile.notebook_groups
        ml_experimental = "ml-experimental" in self.profile.notebook_groups
        self._check(
            "freshness.setup_run_log",
            "freshness",
            selected="setup" in self.profile.notebook_groups,
            required=True,
            function=self._setup_freshness,
        )
        self._check(
            "freshness.watermarks",
            "freshness",
            selected=streaming,
            required=False,
            function=self._watermark_freshness,
        )
        self._check(
            "freshness.eventhouse_ingestion",
            "freshness",
            selected=streaming,
            required=False,
            function=self._eventhouse_freshness,
        )
        self._check(
            "freshness.checkpoint",
            "freshness",
            selected=streaming,
            required=False,
            function=self._checkpoint_freshness,
        )
        self._check(
            "freshness.models.required",
            "freshness",
            selected=ml_required,
            required=True,
            function=lambda: self._model_freshness("required"),
        )
        self._check(
            "freshness.models.optional",
            "freshness",
            selected=ml_optional,
            required=False,
            function=lambda: self._model_freshness("optional"),
        )
        self._check(
            "freshness.models.experimental",
            "freshness",
            selected=ml_experimental,
            required=False,
            function=lambda: self._model_freshness("experimental"),
        )
        self._check(
            "freshness.alerts",
            "freshness",
            selected=self.profile.selects("asset.activator-rules"),
            required=False,
            function=self._alert_freshness,
        )

    def _setup_freshness(self) -> Observation:
        signal = self.adapter.setup_signal()
        if not signal:
            raise EvidenceUnknown("setup_run_log has no terminal run evidence")
        status = str(signal.get("status", ""))
        if status != "COMPLETED":
            raise CheckFailed(f"Latest setup_run_log run is {status!r}.")
        setup_name = (
            Path(self.profile.post_deploy_pipeline_ref).stem
            if self.profile.post_deploy_pipeline_ref
            else None
        )
        not_before = None
        if setup_name and setup_name in self.pipeline_evidence:
            not_before = _parse_time(
                self.pipeline_evidence[setup_name].get("start_time")
            )
        freshness = evaluate_freshness(
            signal.get("generated_at"),
            observed_at=self.context.observed_at,
            max_age=_SETUP_MAX_AGE,
            not_before=not_before,
            lineage="ag.setup_run_log latest __run__ terminal row",
        )
        return Observation(
            "Latest setup publication is terminal-successful and correlated.",
            {
                "status": status,
                "run_id_hash": _hash_identifier(signal.get("run_id")),
            },
            freshness,
        )

    def _watermark_freshness(self) -> Observation:
        signal = self.adapter.watermark_signal()
        if not signal or int(signal.get("source_count") or 0) < 1:
            raise EvidenceUnknown("ag._watermarks contains no source evidence")
        freshness = evaluate_freshness(
            signal.get("updated_at"),
            observed_at=self.context.observed_at,
            max_age=_STREAM_MAX_AGE,
            lineage="ag._watermarks maximum updated_at",
        )
        return Observation(
            "Silver watermark evidence is recent.",
            {"source_count": int(signal["source_count"])},
            freshness,
        )

    def _eventhouse_freshness(self) -> Observation:
        tables = _stream_event_tables(self.context.manifest)
        signal = self.adapter.eventhouse_signal(tables)
        if not signal:
            raise EvidenceUnknown("Eventhouse returned no ingestion evidence")
        observed_tables = frozenset(str(name) for name in signal.get("tables", []))
        comparison = compare_sets(tables, observed_tables)
        if comparison["missing"]:
            raise CheckFailed("Eventhouse ingestion is missing selected event tables.")
        freshness = evaluate_freshness(
            signal.get("latest_ingestion"),
            observed_at=self.context.observed_at,
            max_age=_STREAM_MAX_AGE,
            lineage="Eventhouse selected event tables max ingest_timestamp",
        )
        return Observation(
            "Eventhouse ingestion covers every selected event table and is recent.",
            comparison,
            freshness,
        )

    def _checkpoint_freshness(self) -> Observation:
        tables = _stream_event_tables(self.context.manifest)
        signal = self.adapter.checkpoint_signal(tables)
        if not signal or not signal.get("stream_id_hash"):
            raise EvidenceUnknown(
                "Kusto ingestion tags contain no checkpoint stream identity"
            )
        freshness = evaluate_freshness(
            signal.get("latest_extent"),
            observed_at=self.context.observed_at,
            max_age=_STREAM_MAX_AGE,
            lineage="Kusto retail-demo stream/batch ingestion tags",
        )
        return Observation(
            "Checkpoint-correlated Kusto ingestion tags are recent.",
            {
                "stream_id_hash": signal.get("stream_id_hash"),
                "latest_batch_id": signal.get("latest_batch_id"),
                "table_count": signal.get("table_count"),
            },
            freshness,
        )

    def _model_freshness(self, tier: str) -> Observation:
        contracts = [
            {
                "id": contract.id,
                "table": contract.output.table,
                "as_of_column": contract.output.as_of_column,
                "lineage_columns": list(contract.output.lineage_columns),
            }
            for contract in self.context.manifest.ml_contracts
            if contract.tier == tier
        ]
        if tier == "required":
            invalid = [
                contract["id"]
                for contract in contracts
                if contract["as_of_column"] != "generated_at"
                or "model_run_id" not in contract["lineage_columns"]
            ]
            if invalid:
                raise EvidenceUnknown(
                    "required model contracts must correlate freshness to "
                    "generated_at and model_run_id: "
                    + ", ".join(sorted(invalid))
                )
        signals = self.adapter.model_signals(contracts)
        if len(signals) != len(contracts):
            raise EvidenceUnknown(f"{tier} model evidence is incomplete")
        timestamps = [
            _parse_time(signal.get("as_of"))
            for signal in signals
            if signal.get("as_of")
        ]
        if len(timestamps) != len(contracts):
            raise EvidenceUnknown(
                f"{tier} model generation timestamp evidence is missing"
            )
        if tier == "required" and any(
            not signal.get("run_id_present") for signal in signals
        ):
            raise EvidenceUnknown(
                f"{tier} model generation evidence has a blank model_run_id"
            )
        pipeline_reference = (
            self.profile.reporting_gate_pipeline_ref
            if tier == "required"
            else next(
                (
                    reference
                    for reference in self.profile.post_reporting_pipeline_refs
                    if Path(reference).stem == f"ml-{tier}"
                ),
                None,
            )
        )
        not_before = None
        if pipeline_reference:
            name = Path(pipeline_reference).stem
            if name not in self.pipeline_evidence:
                raise EvidenceUnknown(
                    f"{tier} model pipeline correlation is missing"
                )
            not_before = _parse_time(
                self.pipeline_evidence[name].get("start_time")
            )
        source_time = min(timestamps)
        freshness = evaluate_freshness(
            source_time.isoformat(),
            observed_at=self.context.observed_at,
            max_age=_MODEL_MAX_AGE,
            not_before=not_before,
            lineage=(
                f"au model tables ({tier}) oldest latest generation timestamp"
            ),
        )
        return Observation(
            f"Every {tier} model table has fresh lineage evidence.",
            {
                "model_count": len(signals),
                "lineage_hashes": sorted(
                    str(signal.get("lineage_hash", "")) for signal in signals
                ),
            },
            freshness,
        )

    def _alert_freshness(self) -> Observation:
        signal = self.adapter.alert_signal()
        if signal is None:
            raise EvidenceUnknown("Alert signal could not be queried.")
        latest = signal.get("latest_detection")
        freshness = None
        if latest:
            freshness = evaluate_freshness(
                latest,
                observed_at=self.context.observed_at,
                max_age=_MODEL_MAX_AGE,
                lineage="Eventhouse anomaly_alerts latest detection_time",
            )
        return Observation(
            "Alert backlog query succeeded.",
            {
                "alert_count": int(signal.get("alert_count") or 0),
                "severity_count": int(signal.get("severity_count") or 0),
            },
            freshness,
        )


def _taskflow_references(taskflow: dict[str, Any]) -> list[str]:
    references: list[str] = []
    for task in taskflow.get("tasks", []):
        if not isinstance(task, dict):
            continue
        for item in task.get("items", []):
            if isinstance(item, dict):
                references.append(
                    f"{item.get('artifactType', '')}:{item.get('artifactName', '')}"
                )
    return references


def _taskflow_edges(taskflow: dict[str, Any]) -> list[str]:
    return [
        f"{edge.get('source', '')}->{edge.get('target', '')}"
        for edge in taskflow.get("edges", [])
        if isinstance(edge, dict)
    ]


def _taskflow_raw_item_count(taskflow: dict[str, Any]) -> int:
    count = 0
    for task in taskflow.get("tasks", []):
        if not isinstance(task, dict):
            continue
        items = task.get("items", [])
        if isinstance(items, list):
            count += len(items)
    return count


def taskflow_binding_errors(
    expected: dict[str, Any],
    raw_actual: dict[str, Any],
    portable_actual: dict[str, Any],
) -> list[str]:
    """Compare task-flow references and edges, including unresolved live items."""

    expected_refs = _taskflow_references(expected)
    actual_refs = _taskflow_references(portable_actual)
    expected_edges = _taskflow_edges(expected)
    actual_edges = _taskflow_edges(portable_actual)
    errors: list[str] = []
    if len(actual_refs) != _taskflow_raw_item_count(raw_actual):
        errors.append("live task flow contains an unresolved item reference")
    if Counter(expected_refs) != Counter(actual_refs):
        errors.append("task-flow item references differ")
    if Counter(expected_edges) != Counter(actual_edges):
        errors.append("task-flow edges differ")
    return errors


def _hash_identifier(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _stream_event_tables(manifest: SolutionManifest) -> frozenset[str]:
    for contract in manifest.data_contracts:
        if contract.kind == "event":
            return frozenset(event.event_type for event in contract.events)
    return frozenset()


def checkpoint_signal_from_rows(
    rows: list[dict[str, Any]],
    expected_tables: frozenset[str],
) -> dict[str, Any] | None:
    """Derive bounded checkpoint evidence from Kusto extent-tag rows."""

    parsed: list[dict[str, Any]] = []
    for row in rows:
        tag = str(row.get("tag", ""))
        marker = tag.find("retail-demo:")
        parts = tag[marker:].split(":") if marker >= 0 else []
        if len(parts) != 4 or parts[0] != "retail-demo":
            continue
        table = str(row.get("TableName", ""))
        if table not in expected_tables or parts[2] != table:
            continue
        try:
            batch_id = int(parts[3])
        except ValueError:
            continue
        parsed.append(
            {
                "table": table,
                "stream_id": parts[1],
                "batch_id": batch_id,
                "created": row.get("MaxCreatedOn"),
            }
        )
    if not parsed:
        return None
    latest_row = max(
        parsed,
        key=lambda row: _parse_time(_iso_value(row["created"])),
    )
    stream_id = str(latest_row["stream_id"])
    same_stream = [row for row in parsed if row["stream_id"] == stream_id]
    return {
        "stream_id_hash": _hash_identifier(stream_id),
        "latest_batch_id": max(int(row["batch_id"]) for row in same_stream),
        "latest_extent": _iso_value(latest_row["created"]),
        "table_count": len({str(row["table"]) for row in same_stream}),
    }


class FabricReadinessAdapter:
    """Real Fabric REST, Kusto, and Lakehouse SQL adapter."""

    def __init__(
        self,
        context: ReadinessContext,
        *,
        credential: TokenCredential | None = None,
    ) -> None:
        self.context = context
        self.config = context.config
        self.outputs = context.outputs
        self.workspace_id = str(self.outputs["workspace_id"])
        self.credential = credential or build_credential(
            self.config.auth_mode,
            tenant_id=self.config.tenant_id,
        )
        self.fabric = build_session(self.credential)
        self._query_uri: str | None = None
        self._database_name: str | None = None

    def list_items(self) -> list[dict[str, Any]]:
        return paginated_get(
            self.fabric,
            f"{FABRIC_API}/workspaces/{self.workspace_id}/items",
        )

    def get_definition(self, item_id: str) -> dict[str, Any]:
        return get_definition(self.fabric, self.workspace_id, item_id)

    def get_taskflow(self) -> dict[str, Any] | None:
        from deploy.scripts import taskflow

        pbi = taskflow._session(
            taskflow._token(taskflow.PBI_SCOPE, self.credential)
        )
        cluster = taskflow.resolve_cluster(pbi)
        return taskflow.get_taskflow(pbi, cluster, self.workspace_id)

    def get_kql_inventory(self, expected_tables: frozenset[str]) -> KqlInventory:
        tables = self._names(
            self._execute_kql(".show tables | project name=TableName", management=True),
            "name",
        )
        functions = self._names(
            self._execute_kql(
                ".show functions | project name=Name",
                management=True,
            ),
            "name",
        )
        materialized_views = self._names(
            self._execute_kql(
                ".show materialized-views | project name=Name",
                management=True,
            ),
            "name",
        )
        mappings: set[str] = set()
        for table in sorted(expected_tables):
            rows = self._execute_kql(
                f".show table {table} ingestion json mappings",
                management=True,
            )
            for row in rows:
                name = row.get("Name") or row.get("MappingName")
                if name:
                    mappings.add(f"{table}/{name}")
        return KqlInventory(
            tables=frozenset(tables),
            functions=frozenset(functions),
            materialized_views=frozenset(materialized_views),
            mappings=frozenset(mappings),
        )

    def list_pipeline_runs(self, pipeline_id: str) -> list[dict[str, object]]:
        return list_pipeline_runs(
            self.fabric,
            self.workspace_id,
            pipeline_id,
        )

    def trigger_pipeline(
        self,
        pipeline_id: str,
        *,
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> dict[str, object]:
        location = run_pipeline(self.fabric, self.workspace_id, pipeline_id)
        payload = wait_for_pipeline_job(
            self.fabric,
            location or "",
            pipeline_id=pipeline_id,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )
        correlated = dict(payload)
        if not correlated.get("id"):
            correlated["id"] = (location or "").rstrip("/").rsplit("/", 1)[-1]
        if not correlated.get("itemId"):
            correlated["itemId"] = pipeline_id
        return correlated

    def setup_signal(self) -> dict[str, Any] | None:
        rows = self._execute_sql(
            "SELECT TOP (1) run_id, status, generated_at "
            "FROM [ag].[setup_run_log] "
            "WHERE table_name = '__run__' "
            "ORDER BY generated_at DESC"
        )
        if not rows:
            return None
        row = rows[0]
        return {
            "run_id": row.get("run_id"),
            "status": row.get("status"),
            "generated_at": _iso_value(row.get("generated_at")),
        }

    def watermark_signal(self) -> dict[str, Any] | None:
        rows = self._execute_sql(
            "SELECT COUNT(*) AS source_count, MAX(updated_at) AS updated_at "
            "FROM [ag].[_watermarks]"
        )
        if not rows:
            return None
        row = rows[0]
        return {
            "source_count": row.get("source_count"),
            "updated_at": _iso_value(row.get("updated_at")),
        }

    def eventhouse_signal(
        self,
        expected_tables: frozenset[str],
    ) -> dict[str, Any] | None:
        if not expected_tables:
            return None
        union = ", ".join(sorted(expected_tables))
        rows = self._execute_kql(
            f"union withsource=table_name {union} "
            "| summarize row_count=count(), latest=max(ingest_timestamp) "
            "by table_name"
        )
        if not rows:
            return None
        latest = max(
            (
                row.get("latest")
                for row in rows
                if row.get("latest") is not None
            ),
            default=None,
        )
        return {
            "tables": [str(row.get("table_name", "")) for row in rows],
            "latest_ingestion": _iso_value(latest),
        }

    def checkpoint_signal(
        self,
        expected_tables: frozenset[str],
    ) -> dict[str, Any] | None:
        rows = self._execute_kql(
            ".show database extents "
            "| mv-expand tag=Tags "
            "| where tostring(tag) contains 'retail-demo:' "
            "| project TableName, tag=tostring(tag), MaxCreatedOn",
            management=True,
        )
        return checkpoint_signal_from_rows(rows, expected_tables)

    def model_signals(
        self,
        contracts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        signals: list[dict[str, Any]] = []
        for contract in contracts:
            table = _identifier(contract["table"])
            as_of = _identifier(contract["as_of_column"])
            lineage = [
                _identifier(column)
                for column in contract.get("lineage_columns", [])
            ]
            selected_columns = tuple(dict.fromkeys((as_of, *lineage)))
            columns = ", ".join(
                f"[{column}]" for column in selected_columns
            )
            rows = self._execute_sql(
                f"SELECT TOP (1) {columns} FROM [au].[{table}] "
                f"ORDER BY [{as_of}] DESC"
            )
            if not rows:
                continue
            row = rows[0]
            lineage_value = "|".join(
                str(row.get(column, "")) for column in lineage
            )
            signals.append(
                {
                    "contract_id": contract["id"],
                    "as_of": _iso_value(row.get(as_of)),
                    "run_id_present": bool(
                        str(row.get("model_run_id") or "").strip()
                    ),
                    "lineage_hash": _hash_identifier(lineage_value),
                }
            )
        return signals

    def alert_signal(self) -> dict[str, Any] | None:
        rows = self._execute_kql(
            "anomaly_alerts "
            "| summarize alert_count=count(), "
            "severity_count=dcount(severity), "
            "latest_detection=max(detection_time)"
        )
        if not rows:
            return None
        row = rows[0]
        return {
            "alert_count": row.get("alert_count", 0),
            "severity_count": row.get("severity_count", 0),
            "latest_detection": _iso_value(row.get("latest_detection")),
        }

    def _resolve_kql(self) -> tuple[str, str]:
        if self._query_uri is None or self._database_name is None:
            self._query_uri, self._database_name = resolve_kql_database(
                self.workspace_id,
                str(self.outputs["kql_database_id"]),
                self.credential,
            )
        return self._query_uri, self._database_name

    def _execute_kql(
        self,
        query: str,
        *,
        management: bool = False,
    ) -> list[dict[str, Any]]:
        from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

        query_uri, database_name = self._resolve_kql()
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
            query_uri,
            self.credential,
        )
        with KustoClient(kcsb) as client:
            response = (
                client.execute_mgmt(database_name, query)
                if management
                else client.execute(database_name, query)
            )
        if not response.primary_results:
            return []
        return [row.to_dict() for row in response.primary_results[0]]

    def _execute_sql(self, query: str) -> list[dict[str, Any]]:
        try:
            import pyodbc
        except ImportError as exc:
            raise EvidenceUnknown(
                "pyodbc is required for Lakehouse freshness checks"
            ) from exc

        response = self.fabric.get(
            f"{FABRIC_API}/workspaces/{self.workspace_id}/lakehouses/"
            f"{self.outputs['lakehouse_id']}"
        )
        response.raise_for_status()
        properties = response.json().get("properties", {}).get(
            "sqlEndpointProperties",
            {},
        )
        server = str(properties.get("connectionString", "")).strip()
        if not server:
            raise EvidenceUnknown("Lakehouse SQL endpoint connection is unavailable")
        driver = next(
            (
                candidate
                for candidate in (
                    "ODBC Driver 18 for SQL Server",
                    "ODBC Driver 17 for SQL Server",
                )
                if candidate in pyodbc.drivers()
            ),
            None,
        )
        if driver is None:
            raise EvidenceUnknown("No supported SQL Server ODBC driver is installed")
        token = self.credential.get_token(SQL_SCOPE).token
        token_bytes = token.encode("utf-16-le")
        access_token = struct.pack(
            f"<I{len(token_bytes)}s",
            len(token_bytes),
            token_bytes,
        )
        connection = (
            f"Driver={{{driver}}};Server={server},1433;"
            f"Database={self.context.config.lakehouse.name};"
            "Encrypt=Yes;TrustServerCertificate=No"
        )
        with pyodbc.connect(
            connection,
            attrs_before={1256: access_token},
            timeout=60,
        ) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [str(column[0]) for column in cursor.description]
            return [
                dict(zip(columns, row, strict=True))
                for row in cursor.fetchall()
            ]

    @staticmethod
    def _names(rows: list[dict[str, Any]], key: str) -> set[str]:
        return {
            str(row[key])
            for row in rows
            if row.get(key) not in (None, "")
        }


def _identifier(value: Any) -> str:
    text = str(value)
    if not text.isidentifier():
        raise ValueError(f"Unsafe SQL identifier: {text!r}")
    return text


def _iso_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return str(value)


def build_report(
    context: ReadinessContext,
    checks: list[CheckResult],
    *,
    run_pipeline_requested: bool,
    defer_post_ontology: bool = False,
) -> dict[str, Any]:
    """Build the persisted readiness report schema."""

    status = aggregate_status(checks)
    counts = Counter(check.status for check in checks)
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "environment": context.environment,
        "status": status,
        "exit_code": exit_code_for_status(status),
        "observed_at": context.observed_at.isoformat(),
        "profile": {
            "name": context.config.profile.deployment_name,
            "id": context.config.profile.id,
            "hash": context.profile_hash,
            "support_status": context.config.profile.support_status,
            "expected_item_counts": {
                "infrastructure": (
                    context.config.profile.publication.infrastructure_item_count
                ),
                "reporting": (
                    context.config.profile.publication.reporting_item_count
                ),
                "all": context.config.profile.publication.all_item_count,
            },
            "asset_boundaries": {
                "core": [
                    asset.id
                    for asset in context.config.profile.assets
                    if asset.support_status == "core"
                ],
                "optional": [
                    asset.id
                    for asset in context.config.profile.assets
                    if asset.support_status == "optional"
                ],
                "preview": list(context.config.profile.preview_asset_ids),
                "manual": list(context.config.profile.manual_asset_ids),
            },
            "boundaries": {
                **context.config.profile.boundaries.model_dump(mode="json"),
                "supported": context.config.profile.boundaries.supported,
            },
        },
        "manifest": {
            "version": context.manifest.version,
            "hash": context.manifest_hash,
        },
        "targets": {
            "workspace": {
                "id": context.outputs.get("workspace_id"),
                "name": context.outputs.get("workspace_name"),
            },
            "lakehouse": {
                "id": context.outputs.get("lakehouse_id"),
                "name": context.outputs.get("lakehouse_name"),
            },
            "eventhouse": {
                "id": context.outputs.get("eventhouse_id"),
                "name": context.outputs.get("eventhouse_name"),
            },
            "kql_database": {
                "id": context.outputs.get("kql_database_id"),
                "name": context.outputs.get("kql_database_name"),
            },
            "spark_custom_pool": {
                "id": context.outputs.get("spark_custom_pool_id"),
            },
        },
        "mode": {
            "read_only": not run_pipeline_requested,
            "pipeline_trigger_requested": run_pipeline_requested,
            "post_ontology_deferred": defer_post_ontology,
        },
        "counts": {
            "total": len(checks),
            "pass": counts["PASS"],
            "fail": counts["FAIL"],
            "unknown": counts["UNKNOWN"],
            "skipped": counts["SKIPPED"],
            "required": sum(check.required and check.selected for check in checks),
            "optional": sum(not check.required and check.selected for check in checks),
        },
        "checks": [check.to_dict() for check in checks],
        "freshness_lineage": [
            {
                "check_id": check.check_id,
                **(check.freshness or {}),
            }
            for check in checks
            if check.freshness
        ],
    }


def verify_environment(
    repo_root: Path,
    environment: str,
    *,
    run_pipeline_requested: bool = False,
    defer_post_ontology: bool = False,
    timeout_seconds: float = 21600,
    poll_interval_seconds: float = 15,
    adapter: ReadinessAdapter | None = None,
    observed_at: datetime | None = None,
) -> tuple[dict[str, Any], Path]:
    """Run readiness checks and persist their report."""

    context = load_readiness_context(
        repo_root,
        environment,
        observed_at=observed_at,
    )
    if run_pipeline_requested and context.config.profile.post_deploy_pipeline_ref is None:
        raise ReadinessUsageError(
            f"profile {context.config.profile.deployment_name!r} has no "
            "required post-publish pipeline"
        )
    if defer_post_ontology and not context.config.profile.selects(
        "asset.data-agents"
    ):
        raise ReadinessUsageError(
            "--defer-post-ontology is valid only for a profile that selects "
            "the post-ontology Data Agent boundary"
        )
    live_adapter = adapter or FabricReadinessAdapter(context)
    checks = ReadinessRunner(
        context,
        live_adapter,
        run_pipeline_requested=run_pipeline_requested,
        defer_post_ontology=defer_post_ontology,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    ).run()
    report = build_report(
        context,
        checks,
        run_pipeline_requested=run_pipeline_requested,
        defer_post_ontology=defer_post_ontology,
    )
    path = readiness_report_path(repo_root, environment)
    write_report_atomic(path, report)
    return report, path


def _print_summary(report: dict[str, Any], path: Path) -> None:
    counts = report["counts"]
    console.info(
        f"Readiness {report['status']}: {counts['pass']} passed, "
        f"{counts['fail']} failed, {counts['unknown']} unknown, "
        f"{counts['skipped']} skipped."
    )
    for check in report["checks"]:
        if check["status"] in {"FAIL", "UNKNOWN"}:
            console.error(
                f"{check['id']} [{check['status']}]: {check['summary']}"
            )
    console.detail(f"Report: {path}")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for live readiness verification."""

    parser = argparse.ArgumentParser(
        description="Verify profile-aware live Fabric readiness and freshness"
    )
    parser.add_argument("--environment", required=True)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument(
        "--run-pipeline",
        action="store_true",
        help=(
            "Explicitly run and wait for the profile-required post-publish "
            "pipeline before checking evidence."
        ),
    )
    parser.add_argument(
        "--defer-post-ontology",
        action="store_true",
        help=(
            "Skip ontology, Data Agent, and task-flow evidence until the "
            "acknowledged post-ontology publication step."
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=21600)
    parser.add_argument("--poll-interval-seconds", type=float, default=15)
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0 or args.poll_interval_seconds < 0:
        parser.error("timeout must be positive and poll interval non-negative")
    try:
        report, path = verify_environment(
            args.repo_root.resolve(),
            args.environment,
            run_pipeline_requested=args.run_pipeline,
            defer_post_ontology=args.defer_post_ontology,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
    except ReadinessUsageError as exc:
        parser.error(str(exc))
    except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        console.error(f"Readiness verification could not resolve its inputs: {exc}")
        return 1
    _print_summary(report, path)
    return int(report["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
