"""Run the full-demo ontology notebook and require one validated target item."""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES
from deploy.scripts.deploy_config import (
    ONTOLOGY_ITEM_NAME,
    load_environment,
    load_terraform_outputs,
    validate_terraform_outputs,
)
from deploy.scripts.export_items import FABRIC_API, build_session, list_items
from deploy.scripts.run_pipeline import PipelineRunError, wait_for_pipeline_job

if TYPE_CHECKING:
    import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
ONTOLOGY_NOTEBOOK_NAME = "30-create-ontology"
DEFAULT_JOB_TIMEOUT_SECONDS = 21600.0
DEFAULT_DISCOVERY_TIMEOUT_SECONDS = 600.0
DEFAULT_POLL_INTERVAL_SECONDS = 15.0


class OntologyEnsureError(RuntimeError):
    """Raised when one exact target ontology cannot be established."""


def _unique_named_item(
    items: list[dict[str, Any]],
    display_name: str,
    item_type: str,
) -> dict[str, Any] | None:
    matches = [
        item
        for item in items
        if str(item.get("displayName", "")) == display_name
        and item.get("id")
    ]
    if len(matches) > 1:
        raise OntologyEnsureError(
            f"Expected at most one {item_type} named {display_name!r}; "
            f"found {len(matches)}."
        )
    return matches[0] if matches else None


def start_notebook_job(
    session: requests.Session,
    workspace_id: str,
    notebook_id: str,
) -> str:
    """Start one exact RunNotebook job and return its status URL."""

    response = session.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/items/{notebook_id}/"
        "jobs/RunNotebook/instances"
    )
    response.raise_for_status()
    location = response.headers.get("Location")
    if not location:
        raise OntologyEnsureError(
            "Fabric accepted the ontology notebook without a job Location."
        )
    return location


def wait_for_ontology(
    session_factory: Callable[[], requests.Session],
    workspace_id: str,
    *,
    timeout_seconds: float = DEFAULT_DISCOVERY_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Wait for exactly one stable ontology item after notebook completion."""

    if timeout_seconds <= 0 or poll_interval_seconds < 0:
        raise ValueError(
            "ontology discovery timeout must be positive and interval non-negative"
        )
    deadline = clock() + timeout_seconds
    while True:
        session = session_factory()
        ontology = _unique_named_item(
            list_items(session, workspace_id, "Ontology"),
            ONTOLOGY_ITEM_NAME,
            "Ontology",
        )
        if ontology is not None:
            return str(ontology["id"])
        if clock() >= deadline:
            raise OntologyEnsureError(
                f"Ontology {ONTOLOGY_ITEM_NAME!r} did not appear within "
                f"{timeout_seconds:g} seconds after notebook completion."
            )
        sleep(poll_interval_seconds)


def ensure_ontology(
    session_factory: Callable[[], requests.Session],
    workspace_id: str,
    *,
    job_timeout_seconds: float = DEFAULT_JOB_TIMEOUT_SECONDS,
    discovery_timeout_seconds: float = DEFAULT_DISCOVERY_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[str, bool]:
    """Run the validation notebook and return the resulting ontology ID."""

    session = session_factory()
    existing = _unique_named_item(
        list_items(session, workspace_id, "Ontology"),
        ONTOLOGY_ITEM_NAME,
        "Ontology",
    )

    notebook = _unique_named_item(
        list_items(session, workspace_id, "Notebook"),
        ONTOLOGY_NOTEBOOK_NAME,
        "Notebook",
    )
    if notebook is None:
        raise OntologyEnsureError(
            f"Notebook {ONTOLOGY_NOTEBOOK_NAME!r} is not deployed."
        )
    notebook_id = str(notebook["id"])
    location = start_notebook_job(session, workspace_id, notebook_id)
    wait_for_pipeline_job(
        session,
        location,
        pipeline_id=notebook_id,
        timeout_seconds=job_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        refresh_session=session_factory,
    )
    ontology_id = wait_for_ontology(
        session_factory,
        workspace_id,
        timeout_seconds=discovery_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        clock=clock,
        sleep=sleep,
    )
    return ontology_id, existing is None


def main() -> int:
    """Ensure one environment's full-demo ontology exists."""

    parser = argparse.ArgumentParser(
        description="Run the ontology notebook and validate its target item"
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--auth-mode", choices=AUTH_MODES, default=None)
    parser.add_argument("--tenant-id")
    parser.add_argument(
        "--job-timeout-seconds",
        type=float,
        default=DEFAULT_JOB_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--discovery-timeout-seconds",
        type=float,
        default=DEFAULT_DISCOVERY_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
    )
    args = parser.parse_args()

    config_root = args.repo_root / "deploy" / "config"
    config = load_environment(
        args.environment,
        config_path=config_root / "deploy.yml",
        environments_root=config_root / "environments",
    )
    if not config.profile.selects("asset.ontology"):
        console.error(
            f"Profile {config.profile.deployment_name!r} does not select ontology."
        )
        return 1
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
        tenant_id = args.tenant_id or config.tenant_id
        if (
            args.tenant_id
            and config.tenant_id
            and args.tenant_id.casefold() != config.tenant_id.casefold()
        ):
            raise OntologyEnsureError(
                "--tenant-id does not match the configured tenant"
            )
        auth_mode = args.auth_mode or config.auth_mode

        def fresh_session() -> requests.Session:
            return build_session(
                auth_mode=auth_mode,
                tenant_id=tenant_id,
            )

        ontology_id, ontology_created = ensure_ontology(
            fresh_session,
            str(outputs["workspace_id"]),
            job_timeout_seconds=args.job_timeout_seconds,
            discovery_timeout_seconds=args.discovery_timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
    except (
        FileNotFoundError,
        KeyError,
        OntologyEnsureError,
        PipelineRunError,
        ValueError,
    ) as exc:
        console.error(str(exc))
        return 1

    if ontology_created:
        console.info(
            f"Ontology notebook completed and {ONTOLOGY_ITEM_NAME!r} is available "
            f"({ontology_id})."
        )
    else:
        console.info(
            f"Ontology notebook revalidated {ONTOLOGY_ITEM_NAME!r} "
            f"({ontology_id})."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
