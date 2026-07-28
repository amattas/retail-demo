"""Adopt exact successful Fabric pipeline runs into a recovery deploy journal."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from retail_setup.cli import _deploy_journal

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES
from deploy.scripts.deploy_config import (
    DeployConfig,
    load_environment,
    load_terraform_outputs,
    validate_terraform_outputs,
)
from deploy.scripts.export_items import build_session, list_items
from deploy.scripts.run_pipeline import list_pipeline_runs
from deploy.scripts.validate_target_access import (
    TargetAccessError,
    validate_target_access,
)
from deploy.scripts.verify_readiness import (
    normalize_job_evidence,
    validate_terminal_job_evidence,
    verify_environment,
)

if TYPE_CHECKING:
    import requests
    from retail_setup.contracts import ResolvedProfile

REPO_ROOT = Path(__file__).resolve().parents[2]
_MAX_RUN_AGE = timedelta(days=7)
_FUTURE_SKEW = timedelta(minutes=5)


class PipelineAdoptionError(RuntimeError):
    """Raised when exact recovery evidence is missing, stale, or unsuccessful."""


@dataclass(frozen=True)
class PipelineAdoption:
    """Validated exact Fabric job evidence for one deployment pipeline step."""

    name: str
    step_id: str
    required: bool
    pipeline_id: str
    run_id: str
    started_at: str
    ended_at: str


def expected_pipeline_steps(
    profile: ResolvedProfile,
) -> dict[str, tuple[str, bool]]:
    """Map every profile-owned deployment pipeline to its journal step."""

    expected: dict[str, tuple[str, bool]] = {}
    if profile.post_deploy_pipeline_ref is not None:
        name = Path(profile.post_deploy_pipeline_ref).stem
        expected[name] = ("setup-pipeline-gate", True)
    if profile.reporting_gate_pipeline_ref is not None:
        name = Path(profile.reporting_gate_pipeline_ref).stem
        expected[name] = ("required-ml-reporting-gate", True)
    for reference in profile.post_reporting_pipeline_refs:
        name = Path(reference).stem
        expected[name] = (f"post-reporting-{name}", False)
    return expected


def parse_run_specs(
    values: list[str],
    expected_names: set[str],
) -> dict[str, str]:
    """Parse complete, nonduplicated ``PIPELINE=RUN_ID`` specifications."""

    parsed: dict[str, str] = {}
    for value in values:
        name, separator, run_id = value.partition("=")
        name = name.strip()
        run_id = run_id.strip()
        if not separator or not name or not run_id:
            raise PipelineAdoptionError(
                "--run values must use PIPELINE=RUN_ID"
            )
        if name in parsed:
            raise PipelineAdoptionError(
                f"Pipeline {name!r} was specified more than once."
            )
        parsed[name] = run_id
    supplied_names = set(parsed)
    if supplied_names != expected_names:
        missing = sorted(expected_names - supplied_names)
        extra = sorted(supplied_names - expected_names)
        details = []
        if missing:
            details.append(f"missing {missing}")
        if extra:
            details.append(f"unexpected {extra}")
        raise PipelineAdoptionError(
            "Recovery requires one exact run ID for every selected deployment "
            "pipeline: " + "; ".join(details)
        )
    return parsed


def adopt_pipeline_runs(
    session: requests.Session,
    workspace_id: str,
    expected: dict[str, tuple[str, bool]],
    run_ids: dict[str, str],
    *,
    observed_at: datetime | None = None,
) -> list[PipelineAdoption]:
    """Validate exact recent successful runs against live pipeline identities."""

    now = (observed_at or datetime.now(UTC)).astimezone(UTC)
    items_by_name: dict[str, list[dict[str, Any]]] = {}
    for item in list_items(session, workspace_id, "DataPipeline"):
        items_by_name.setdefault(str(item.get("displayName", "")), []).append(item)

    adopted: list[PipelineAdoption] = []
    for name, (step_id, required) in expected.items():
        matches = items_by_name.get(name, [])
        if len(matches) != 1:
            raise PipelineAdoptionError(
                f"Expected exactly one live DataPipeline named {name!r}; "
                f"found {len(matches)}."
            )
        pipeline_id = str(matches[0].get("id", ""))
        requested_run_id = run_ids[name]
        runs = list_pipeline_runs(session, workspace_id, pipeline_id)
        exact = [
            run
            for run in runs
            if str(run.get("id", "")) == requested_run_id
        ]
        if len(exact) != 1:
            raise PipelineAdoptionError(
                f"Exact run {requested_run_id!r} was not found once for "
                f"pipeline {name!r}."
            )
        evidence = normalize_job_evidence(exact[0])
        try:
            validate_terminal_job_evidence(evidence)
        except Exception as exc:
            raise PipelineAdoptionError(
                f"Run {requested_run_id!r} for pipeline {name!r} is not "
                "terminal-successful."
            ) from exc
        item_id = str(evidence.get("item_id", ""))
        if item_id and item_id != pipeline_id:
            raise PipelineAdoptionError(
                f"Run {requested_run_id!r} does not belong to pipeline {name!r}."
            )
        started = _parse_utc(evidence.get("start_time"))
        ended = _parse_utc(evidence.get("end_time"))
        if ended > now + _FUTURE_SKEW:
            raise PipelineAdoptionError(
                f"Run {requested_run_id!r} has a future completion timestamp."
            )
        if now - ended > _MAX_RUN_AGE:
            raise PipelineAdoptionError(
                f"Run {requested_run_id!r} is older than {_MAX_RUN_AGE.days} days."
            )
        adopted.append(
            PipelineAdoption(
                name=name,
                step_id=step_id,
                required=required,
                pipeline_id=pipeline_id,
                run_id=requested_run_id,
                started_at=started.isoformat(),
                ended_at=ended.isoformat(),
            )
        )
    _validate_dependency_order(adopted)
    return adopted


def build_recovery_journal(
    config: DeployConfig,
    adopted: list[PipelineAdoption],
    *,
    recovered_from_run_id: str | None = None,
) -> _deploy_journal.DeployJournal:
    """Build a current-profile journal containing exact adopted run evidence."""

    profile = config.profile
    journal = _deploy_journal.start_run(
        config.environment,
        targets={
            "workspace_name": config.workspace.name,
            "lakehouse_name": config.lakehouse.name,
            "auth_mode": config.auth_mode,
            "profile": profile.deployment_name,
            "recovery": "adopted-exact-pipeline-runs",
        },
        manifest={
            "version": profile.manifest_version,
            "hash": profile.manifest_hash,
            "profile_id": profile.id,
            "profile_name": profile.deployment_name,
            "profile_support_status": profile.support_status,
            **(
                {"recovered_from_run_id": recovered_from_run_id}
                if recovered_from_run_id
                else {}
            ),
        },
    )
    for evidence in adopted:
        _deploy_journal.add_step(
            journal,
            evidence.step_id,
            f"Adopt exact successful run for pipeline {evidence.name!r}",
            required=evidence.required,
        )
        _deploy_journal.mark_succeeded(
            journal,
            evidence.step_id,
            started_at=evidence.started_at,
            ended_at=evidence.ended_at,
            evidence_id=evidence.run_id,
        )
    _deploy_journal.add_step(
        journal,
        "verify-readiness",
        "Verify recovered live readiness and freshness",
        required=True,
        evidence_path=(
            f"deploy/.generated/{config.environment}/readiness-report.json"
        ),
    )
    return journal


def _validate_dependency_order(adopted: list[PipelineAdoption]) -> None:
    """Require recovered runs to preserve setup -> required ML -> optional ML."""

    by_step = {evidence.step_id: evidence for evidence in adopted}
    setup = by_step.get("setup-pipeline-gate")
    required_ml = by_step.get("required-ml-reporting-gate")
    if setup and required_ml:
        setup_ended = _parse_utc(setup.ended_at)
        required_started = _parse_utc(required_ml.started_at)
        if required_started < setup_ended:
            raise PipelineAdoptionError(
                "Required ML run started before the adopted setup run completed."
            )
    if required_ml:
        required_ended = _parse_utc(required_ml.ended_at)
        for evidence in adopted:
            if (
                evidence.step_id.startswith("post-reporting-")
                and _parse_utc(evidence.started_at) < required_ended
            ):
                raise PipelineAdoptionError(
                    f"Post-Reporting run for {evidence.name!r} started before "
                    "the adopted required ML run completed."
                )


def finalize_recovery_journal(
    journal: _deploy_journal.DeployJournal,
    readiness_status: str,
) -> None:
    """Finalize the pending verification step from its persisted report status."""

    if readiness_status == "SUCCEEDED":
        _deploy_journal.mark_succeeded(journal, "verify-readiness")
    elif readiness_status == "DEGRADED":
        _deploy_journal.mark_degraded(
            journal,
            "verify-readiness",
            reason="optional live readiness evidence is degraded",
        )
    elif readiness_status == "FAILED":
        _deploy_journal.mark_failed(
            journal,
            "verify-readiness",
            exit_code=1,
            error="required live readiness evidence failed or is unknown",
        )
    else:
        raise PipelineAdoptionError(
            f"Readiness returned unknown status {readiness_status!r}."
        )


def _parse_utc(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise PipelineAdoptionError("Pipeline run timestamp is missing.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PipelineAdoptionError("Pipeline run timestamp is invalid.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _previous_run_id(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    value = payload.get("run_id") if isinstance(payload, dict) else None
    return str(value) if value else None


def main() -> int:
    """Adopt explicitly selected live pipeline runs for one environment."""

    parser = argparse.ArgumentParser(
        description="Adopt exact successful Fabric pipeline runs"
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--auth-mode", choices=AUTH_MODES, default=None)
    parser.add_argument("--tenant-id")
    parser.add_argument(
        "--run",
        action="append",
        default=[],
        metavar="PIPELINE=RUN_ID",
        help="Exact successful Fabric run to adopt; repeat for every selected pipeline.",
    )
    args = parser.parse_args()

    config_root = args.repo_root / "deploy" / "config"
    config = load_environment(
        args.environment,
        config_path=config_root / "deploy.yml",
        environments_root=config_root / "environments",
    )
    output_path = (
        args.repo_root
        / "deploy"
        / ".generated"
        / args.environment
        / "terraform-output.json"
    )
    try:
        outputs = load_terraform_outputs(output_path)
        validate_terraform_outputs(config, outputs)
        expected = expected_pipeline_steps(config.profile)
        run_ids = parse_run_specs(args.run, set(expected))
        tenant_id = args.tenant_id or config.tenant_id
        if (
            args.tenant_id
            and config.tenant_id
            and args.tenant_id.casefold() != config.tenant_id.casefold()
        ):
            raise PipelineAdoptionError(
                "--tenant-id does not match the configured tenant"
            )
        session = build_session(
            auth_mode=args.auth_mode or config.auth_mode,
            tenant_id=tenant_id,
        )
        target = validate_target_access(session, config, outputs)
        adopted = adopt_pipeline_runs(
            session,
            target.workspace_id,
            expected,
            run_ids,
        )
        journal_path = _deploy_journal.journal_path(
            args.repo_root,
            args.environment,
        )
        journal = build_recovery_journal(
            config,
            adopted,
            recovered_from_run_id=_previous_run_id(journal_path),
        )
        _deploy_journal.write(args.repo_root, journal)
        report, _report_path = verify_environment(
            args.repo_root,
            args.environment,
            defer_post_ontology=config.profile.selects("asset.data-agents"),
        )
        readiness_status = str(report["status"])
        finalize_recovery_journal(journal, readiness_status)
        _deploy_journal.write(args.repo_root, journal)
    except (
        FileNotFoundError,
        KeyError,
        PipelineAdoptionError,
        TargetAccessError,
        ValueError,
    ) as exc:
        console.error(str(exc))
        return 1

    console.info(
        f"Adopted {len(adopted)} exact successful pipeline runs into "
        f"{journal_path}."
    )
    for evidence in adopted:
        console.detail(
            f"{evidence.name}: {evidence.run_id} ({evidence.ended_at})"
        )
    if readiness_status == "DEGRADED":
        console.warn(
            "Recovered deployment is ready with optional degraded evidence; "
            "see the linked readiness report."
        )
    return 1 if readiness_status == "FAILED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
