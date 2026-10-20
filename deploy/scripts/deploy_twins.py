"""Publish connected operational twins as an SSO-backed native Power BI report."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from requests import RequestException
from retail_setup.cli._deploy_journal import _write_atomic

from deploy.scripts import _output as console
from deploy.scripts._auth import build_credential
from deploy.scripts._native_items import (
    API,
    NativeItemError,
    complete_operation,
    ensure_native_item,
    get_definition,
)
from deploy.scripts._twin_model import load_metrics, merge_twin_model
from deploy.scripts._twin_refresh import refresh_model
from deploy.scripts._twin_report_checks import validate_report_model_fields
from deploy.scripts._twins_runtime import (
    discover_cohort,
    ensure_workspace_active,
    require_sso,
)
from deploy.scripts.capacity_resume import CapacityResumeError
from deploy.scripts.deploy_config import (
    load_environment,
    load_terraform_outputs,
    validate_terraform_outputs,
)
from deploy.scripts.export_items import build_session, list_items
from deploy.scripts.taskflow import PBI_SCOPE, _session, _token

ROOT = Path(__file__).resolve().parents[2]
REPORT_NAME = "retail-twins"


def entity_queries(cohort: dict[str, Any], metrics: list[dict[str, str]]) -> dict[str, str]:
    scopes = {
        "dim_distribution_centers": [cohort["dc_id"]],
        "dim_trucks": cohort["truck_ids"],
        "dim_stores": cohort["store_ids"],
        "dim_products": cohort["product_ids"],
    }
    if any(type(value) is not int or value < 1 for values in scopes.values() for value in values):
        raise ValueError("DAX cohort identifiers must be positive source integers.")
    filters = [
        f"TREATAS({{{', '.join(str(value) for value in values)}}}, '{table}'[ID])"
        for table, values in scopes.items()
    ]
    queries = {}
    for table in ("dim_stores", "dim_distribution_centers", "dim_trucks"):
        selected = [row["name"] for row in metrics if row["table"] == table]
        if not selected:
            raise ValueError(f"Twin metrics missing for {table}.")
        measures = [
            f'"{name.replace(chr(34), chr(34) * 2)}", [{name.replace("]", "]]")}]'
            for name in selected
        ]
        arguments = ",\n".join([f"'{table}'[ID]", *filters, *measures])
        queries[table] = f"EVALUATE SUMMARIZECOLUMNS(\n{arguments}\n) ORDER BY '{table}'[ID]"
    return queries


def verify_entity_queries(
    credential: Any, workspace_id: str, model_id: str,
    cohort: dict[str, Any], metrics: list[dict[str, str]],
) -> dict[str, Any]:
    expected = {
        "dim_stores": cohort["store_ids"],
        "dim_distribution_centers": [cohort["dc_id"]],
        "dim_trucks": cohort["truck_ids"],
    }
    evidence = {}
    session = _session(_token(PBI_SCOPE, credential))
    for table, query in entity_queries(cohort, metrics).items():
        response = session.post(
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{model_id}/executeQueries",
            json={"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}},
            timeout=120, allow_redirects=False,
        )
        response.raise_for_status()
        body = response.json()
        results = body.get("results", [])
        if body.get("error") or len(results) != 1 or results[0].get("error"):
            raise NativeItemError(f"Twin DAX query failed for {table}: {body.get('error') or results[:1]}")
        tables = results[0].get("tables", [])
        rows = tables[0].get("rows", []) if len(tables) == 1 else []
        identities = [row.get(f"{table}[ID]") for row in rows]
        if any(type(value) is not int for value in identities) or sorted(identities) != sorted(expected[table]):
            raise NativeItemError(f"Twin DAX evidence is missing or outside the selected {table} cohort.")
        anchor_name = {
            "dim_stores": "Twin Store Inventory As Of",
            "dim_distribution_centers": "Twin Warehouse Inventory As Of",
            "dim_trucks": "Twin Truck State As Of",
        }[table]
        if any(row.get(f"[{anchor_name}]") is None for row in rows):
            raise NativeItemError(f"Twin {table} has no inventory/state timestamp evidence.")
        evidence[table] = {"entity_count": len(rows), "rows": rows}
    return evidence


def publish_twins(repo_root: Path, config: Any, outputs: dict[str, Any]) -> dict[str, Any]:
    validate_terraform_outputs(config, outputs)
    if not config.twins.enabled:
        raise NativeItemError("Enable twins.enabled in this environment before publishing.")
    if config.powerbi.report_name.casefold() == REPORT_NAME.casefold():
        raise NativeItemError("Connected twins cannot replace the primary report.")
    credential = build_credential(config.auth_mode, tenant_id=config.tenant_id)
    session = build_session(credential)
    workspace_id = outputs["workspace_id"]
    models = [
        item for item in list_items(session, workspace_id, "SemanticModel")
        if item.get("displayName") == config.powerbi.semantic_model_name
    ]
    if len(models) != 1:
        raise NativeItemError("Connected twins require one existing deployed semantic model.")
    model_id = models[0]["id"]
    original_bindings = require_sso(session, workspace_id, model_id)
    ensure_workspace_active(session, config, workspace_id)
    cohort = discover_cohort(session, config, outputs, credential)
    from deploy.scripts._twins_report import render_twins_report

    report_definition = render_twins_report(
        semantic_model_id=model_id, cohort=cohort, display_name=REPORT_NAME,
    )
    current = get_definition(session, workspace_id, "SemanticModel", model_id, "TMDL")
    updated, changes = merge_twin_model(current, repo_root)
    path = repo_root / "deploy" / ".generated" / config.environment / "twins-report.json"
    report = {
        "schema_version": 1, "environment": config.environment,
        "workspace_id": workspace_id, "semantic_model_id": model_id,
        "observed_at": datetime.now(UTC).isoformat(), "status": "IN_PROGRESS",
        "cohort": cohort, "model_objects_changed": changes,
        "auth": "Signed-in-user SSO; no fixed credentials or connection changes.",
        "boundaries": [
            "Synthetic/historical source observations, not live GPS or physical geometry.",
            "Warehouse balance is not available-to-promise stock.",
            "Delay scenario is a labeled +60 minute what-if, not a revised actual ETA.",
            "No native Digital Twin Builder, Planning or Operations Agent dependency.",
            "Report appearance and interactive navigation require separate UI acceptance.",
        ],
    }
    _write_atomic(path, report)
    try:
        live_definition = current
        if changes:
            digest = hashlib.sha256(json.dumps(current, sort_keys=True).encode()).hexdigest()[:12]
            backup = path.parent / f"twins-model-before-{digest}.json"
            if not backup.exists():
                _write_atomic(backup, current)
            report["model_backup"] = str(backup.relative_to(repo_root))
            _write_atomic(path, report)
            console.info(f"Publishing {len(changes)} additive twin model objects...")
            response = session.post(
                f"{API}/workspaces/{workspace_id}/semanticModels/{model_id}/updateDefinition",
                json={"definition": updated}, timeout=30, allow_redirects=False,
            )
            complete_operation(session, response, operation_name="Update twin semantic model")
            reread = get_definition(session, workspace_id, "SemanticModel", model_id, "TMDL")
            _definition, remaining = merge_twin_model(reread, repo_root)
            if remaining:
                raise NativeItemError(f"Twin model readback differs for: {', '.join(remaining)}")
            live_definition = reread
        validate_report_model_fields(report_definition, live_definition)
        if require_sso(session, workspace_id, model_id) != original_bindings:
            raise NativeItemError("Model binding metadata changed; stop and inspect before continuing.")

        def refresh_started(request_id: str) -> None:
            report["model_refresh_id"] = request_id
            _write_atomic(path, report)

        console.info("Refreshing Direct Lake framing for the twin source snapshot...")
        refresh_model(credential, workspace_id, model_id, on_started=refresh_started)
        if require_sso(session, workspace_id, model_id) != original_bindings:
            raise NativeItemError("Model binding metadata changed after refresh.")
        report["query_evidence"] = verify_entity_queries(
            credential, workspace_id, model_id, cohort, load_metrics(repo_root),
        )
        console.info("Publishing connected Store, Warehouse and Truck views...")
        report_id = ensure_native_item(
            session, workspace_id=workspace_id, item_type="Report", display_name=REPORT_NAME,
            description="Connected store, warehouse and truck operational twins; SSO, source evidence and read-only what-if.",
            definition=report_definition,
        )
        report.update(
            status="PUBLISHED",
            report_id=report_id,
            report_url=f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}",
            sso_preserved=True,
        )
        _write_atomic(path, report)
        return report
    except (NativeItemError, RequestException, ValueError) as exc:
        report.update(status="FAILED", failure_type=type(exc).__name__)
        _write_atomic(path, report)
        raise


def main(argv: list[str] | None = None) -> int:
    from deploy.scripts.workspace_folders import organize_after_publish

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    config = load_environment(
        args.environment, config_path=args.repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=args.repo_root / "deploy" / "config" / "environments",
    )
    if args.dry_run:
        console.info(
            "Would publish the connected twin report and additive model objects using SSO."
            if config.twins.enabled else "Connected twin publication is disabled."
        )
        return 0
    outputs = load_terraform_outputs(
        args.repo_root / "deploy" / ".generated" / args.environment / "terraform-output.json"
    )
    try:
        report = publish_twins(args.repo_root, config, outputs)
    except (NativeItemError, CapacityResumeError, RequestException, ValueError) as exc:
        console.error(str(exc))
        return 1
    if not organize_after_publish(args.repo_root, args.environment):
        return 1
    console.info("Published connected retail twins; existing SSO preserved.")
    console.detail(report["report_url"])
    console.detail("Evidence: " + str(args.repo_root / "deploy" / ".generated" / args.environment / "twins-report.json"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
