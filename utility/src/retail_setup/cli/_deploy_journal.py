"""Durable atomic journal for `retail-setup deploy` runs.

Writes ``deploy/.generated/<env>/deploy-run.json`` so an operator (or a later
support investigation) can see what a deploy run did without any live
secrets: tokens, environment variables, and raw subprocess stdout/stderr are
never recorded here, only step descriptions, statuses, timestamps, exit
codes, and short redacted error summaries.

Writes are atomic (temp file + `os.replace`) so a crash mid-write never
leaves a half-written, unparseable journal behind.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

StepStatus = Literal["PENDING", "RUNNING", "SUCCEEDED", "SKIPPED", "FAILED"]
RunStatus = Literal["RUNNING", "SUCCEEDED", "DEGRADED", "FAILED"]
StepClassification = Literal["required", "optional"]

# Error summaries are trimmed to this length so an unexpectedly verbose
# exception message (e.g. one that embeds a URL or path) can't balloon the
# journal or leak more context than a short diagnostic needs.
_MAX_ERROR_LENGTH = 300

# `Bearer <token>` credentials, wherever they show up in an exception message.
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9\-_.~+/]+=*")

# Standalone JWT-like values (three dot-separated base64url segments), e.g. an
# access token embedded in a message without a leading "Bearer" prefix.
_JWT_RE = re.compile(r"\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b")

# `key=value`, `key: value`, or `key="value"` assignments and query
# parameters naming a secret. Matches case-insensitively and across
# snake_case/kebab-case/camelCase spellings (e.g. access_token, access-token,
# accessToken, ACCESS_TOKEN) since the separator between words is optional.
_SECRET_KV_RE = re.compile(
    r"(?i)\b(access[-_]?token|client[-_]?secret|api[-_]?key|password|token)"
    r"(\s*[:=]\s*)"
    r"(['\"]?)"
    r"([^\s&'\",;]+)"
    r"\3"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _redact_kv(match: re.Match[str]) -> str:
    key, sep, quote = match.group(1), match.group(2), match.group(3)
    return f"{key}{sep}{quote}[REDACTED]{quote}"


def _redact(message: str | None) -> str | None:
    """Collapse, redact, and trim a message to a concise, secret-free summary.

    Callers must only ever pass an exception's own short message here -- never
    raw subprocess stdout/stderr or environment variables. Even so, this
    conservatively strips `Bearer <token>` credentials, standalone JWT-like
    values, and any `access_token`/`token`/`client_secret`/`password`/
    `api_key` assignment or query parameter (in any case/underscore/hyphen
    spelling) before the message is ever written to disk.
    """
    if not message:
        return None
    flat = " ".join(str(message).split())
    flat = _BEARER_RE.sub("Bearer [REDACTED]", flat)
    flat = _JWT_RE.sub("[REDACTED]", flat)
    flat = _SECRET_KV_RE.sub(_redact_kv, flat)
    return flat[:_MAX_ERROR_LENGTH]


@dataclass
class JournalStep:
    """One step's durable status record."""

    step_id: str
    description: str
    classification: StepClassification
    status: StepStatus = "PENDING"
    started_at: str | None = None
    ended_at: str | None = None
    exit_code: int | None = None
    error: str | None = None


@dataclass
class DeployJournal:
    """The full durable record for one `retail-setup deploy` run."""

    run_id: str
    environment: str
    status: RunStatus
    started_at: str
    updated_at: str
    targets: dict[str, str] = field(default_factory=dict)
    steps: list[JournalStep] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "environment": self.environment,
            "status": self.status,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "targets": dict(self.targets),
            "steps": [asdict(step) for step in self.steps],
        }


def journal_path(repo_root: Path, env: str) -> Path:
    """The durable journal path for one environment's deploy run."""
    return repo_root / "deploy" / ".generated" / env / "deploy-run.json"


def start_run(env: str, targets: dict[str, str]) -> DeployJournal:
    """Begin a new run with a unique id; `targets` must hold no secrets."""
    now = _utc_now()
    return DeployJournal(
        run_id=str(uuid.uuid4()),
        environment=env,
        status="RUNNING",
        started_at=now,
        updated_at=now,
        targets=dict(targets),
    )


def add_step(journal: DeployJournal, step_id: str, description: str, *, required: bool) -> None:
    """Register a step as PENDING before it runs."""
    journal.steps.append(
        JournalStep(
            step_id=step_id,
            description=description,
            classification="required" if required else "optional",
        )
    )


def _find_step(journal: DeployJournal, step_id: str) -> JournalStep:
    for step in journal.steps:
        if step.step_id == step_id:
            return step
    raise KeyError(f"Unknown journal step_id: {step_id!r}")


def mark_required(journal: DeployJournal, step_id: str) -> None:
    """Promote a step to required (e.g. once the operator requests it)."""
    _find_step(journal, step_id).classification = "required"


def mark_running(journal: DeployJournal, step_id: str) -> None:
    step = _find_step(journal, step_id)
    step.status = "RUNNING"
    step.started_at = _utc_now()


def mark_succeeded(journal: DeployJournal, step_id: str, *, exit_code: int = 0) -> None:
    step = _find_step(journal, step_id)
    step.status = "SUCCEEDED"
    step.exit_code = exit_code
    step.ended_at = _utc_now()


def mark_skipped(journal: DeployJournal, step_id: str, *, reason: str | None = None) -> None:
    step = _find_step(journal, step_id)
    step.status = "SKIPPED"
    step.error = _redact(reason)
    step.ended_at = _utc_now()


def mark_failed(
    journal: DeployJournal,
    step_id: str,
    *,
    exit_code: int | None = None,
    error: str | None = None,
) -> None:
    step = _find_step(journal, step_id)
    step.status = "FAILED"
    step.exit_code = exit_code
    step.error = _redact(error)
    step.ended_at = _utc_now()


def compute_status(journal: DeployJournal) -> RunStatus:
    """Derive the overall run status from its steps' status/classification.

    A required step's failure always fails the run. Otherwise the run stays
    RUNNING while any step is still pending/running, degrades if an optional
    step failed, and succeeds otherwise.
    """
    if any(s.status == "FAILED" and s.classification == "required" for s in journal.steps):
        return "FAILED"
    if any(s.status in ("PENDING", "RUNNING") for s in journal.steps):
        return "RUNNING"
    if any(s.status == "FAILED" and s.classification == "optional" for s in journal.steps):
        return "DEGRADED"
    return "SUCCEEDED"


def write(repo_root: Path, journal: DeployJournal) -> None:
    """Recompute status and atomically persist the journal (temp file + replace)."""
    journal.status = compute_status(journal)
    journal.updated_at = _utc_now()
    path = journal_path(repo_root, journal.environment)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=".deploy-run-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(journal.to_dict(), handle, indent=2, sort_keys=False)
            handle.write("\n")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
