"""Publish the opt-in, stopped Operations Agent and native replenishment Plan."""

from __future__ import annotations

import argparse
import json
import re
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from requests import RequestException

from deploy.scripts import _output as console
from deploy.scripts._auth import build_credential
from deploy.scripts._native_items import (
    API,
    NativeItemError,
    _guid,
    ensure_native_item,
    get_definition,
)
from deploy.scripts._operations_definition import (
    ACTION_NAME,
    AGENT_NAME,
    render_case_notebook,
    render_operations_definition,
    render_operations_instructions,
)
from deploy.scripts._operations_scenario import (
    SCENARIO_SQL,
    scenario_signals,
    signal_ingestion_query,
    validate_signal_readback,
)
from deploy.scripts._operations_ui import ensure_operations_ui_configuration
from deploy.scripts.apply_kql import (
    build_database_script,
    execute_database_script,
    resolve_kql_database,
)
from deploy.scripts.capacity_resume import (
    CapacityResumeError,
    build_arm_session,
    ensure_capacity_active,
)
from deploy.scripts.deploy_config import (
    DeployConfig,
    load_environment,
    load_terraform_outputs,
    validate_terraform_outputs,
)
from deploy.scripts.export_items import build_session, list_items
from deploy.scripts.fabric_runtime import decode_definition_parts, paginated_get
from deploy.scripts.verify_readiness import _connect_lakehouse_sql, write_report_atomic

ROOT = Path(__file__).resolve().parents[2]
PLAN_NAME = "retail-replenishment"
PLAN_MEASURES = (
    "Forecasted Units Next 7 Days", "Forecast Lower Bound Next 7 Days",
    "Forecast Upper Bound Next 7 Days",
)


def validate_planning_measures(session: Any, workspace_id: str, model_id: str) -> None:
    definition = get_definition(session, workspace_id, "SemanticModel", model_id, "TMDL")
    parts = decode_definition_parts(definition)
    tables = [
        value.decode("utf-8") for path, value in parts.items()
        if path.endswith("/demand_forecast.tmdl")
    ]
    if len(tables) != 1 or any(
        not re.search(rf"(?m)^\s*measure '{re.escape(name)}'\s*=", tables[0])
        for name in PLAN_MEASURES
    ):
        raise NativeItemError(
            "Publish the updated Reporting semantic model before this native pack; "
            "its Next 7 Days forecast measures are missing."
        )


def validate_plan_connection(
    session: Any,
    connection_id: str,
) -> None:
    """Require Planning's shared Power BI Semantic Model connection type."""
    response = session.get(
        f"{API}/connections/{_guid(connection_id)}",
        timeout=30,
        allow_redirects=False,
    )
    response.raise_for_status()
    item = response.json()
    details = item.get("connectionDetails", {})
    credentials = item.get("credentialDetails", {})
    if (
        item.get("connectivityType") != "ShareableCloud"
        or details.get("type") != "PowerBIDatasets"
        or credentials.get("singleSignOnType") != "None"
        or credentials.get("credentialType")
        not in {"OAuth2", "ServicePrincipal"}
    ):
        raise NativeItemError(
            "Planning needs a shared Power BI Semantic Model connection using OAuth2 or "
            "ServicePrincipal. AnalysisServices and WorkspaceIdentity are not supported substitutes."
        )


def validate_model_connections(session: Any, workspace_id: str, model_id: str) -> None:
    """Check the separate Direct Lake-to-data hop without changing its binding."""
    bindings = paginated_get(
        session,
        f"{API}/workspaces/{_guid(workspace_id)}/items/{_guid(model_id)}/connections",
        timeout=30,
    )
    if not bindings:
        raise NativeItemError("The semantic model has no inspectable data-source bindings.")
    for binding in bindings:
        if binding.get("connectivityType") not in {"ShareableCloud", "PersonalCloud"}:
            raise NativeItemError(
                "Configure the Direct Lake model's Gateway and cloud connections with "
                "fixed credentials, not Automatic/SSO. The Plan connection does not configure this hop."
            )
        response = session.get(
            f"{API}/connections/{_guid(binding.get('id'))}", timeout=30, allow_redirects=False,
        )
        response.raise_for_status()
        details = response.json().get("credentialDetails", {})
        if (
            details.get("singleSignOnType") != "None"
            or details.get("credentialType") not in {"OAuth2", "ServicePrincipal", "WorkspaceIdentity"}
        ):
            raise NativeItemError("The Direct Lake data-source connection is not fixed-credential.")


def _source_rows(
    session: Any,
    outputs: dict[str, Any],
    config: DeployConfig,
    credential: Any,
) -> list[dict[str, Any]]:
    response = session.get(
        f"{API}/workspaces/{outputs['workspace_id']}/lakehouses/{outputs['lakehouse_id']}",
        timeout=30,
        allow_redirects=False,
    )
    response.raise_for_status()
    server = (
        response.json()
        .get("properties", {})
        .get("sqlEndpointProperties", {})
        .get("connectionString")
    )
    if not server:
        raise NativeItemError("Lakehouse SQL endpoint is unavailable.")
    with closing(
        _connect_lakehouse_sql(server, config.lakehouse.name, credential)
    ) as connection:
        cursor = connection.cursor()
        cursor.execute(SCENARIO_SQL)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _ensure_native_capacity(
    session: Any, config: DeployConfig, workspace_id: str
) -> None:
    response = session.get(
        f"{API}/workspaces/{workspace_id}", timeout=30, allow_redirects=False
    )
    response.raise_for_status()
    workspace = response.json()
    if workspace.get("displayName") != config.workspace.name:
        raise NativeItemError(
            "The live workspace name no longer matches the deployment target."
        )
    capacity_id = _guid(workspace.get("capacityId"))

    def get_capacity() -> dict[str, Any]:
        response = session.get(
            f"{API}/capacities/{capacity_id}", timeout=30, allow_redirects=False
        )
        response.raise_for_status()
        capacity = response.json()
        if (
            str(capacity.get("sku", "")).casefold().startswith("ft")
            or capacity.get("region", "").replace(" ", "").casefold() == "eastus"
        ):
            raise NativeItemError(
                "Operations Agent requires paid capacity outside East US."
            )
        return capacity

    ensure_capacity_active(
        get_capacity,
        resource_id=config.workspace.capacity_resource_id,
        expected_name=config.workspace.capacity_name,
        subscription_id=config.subscription_id,
        tenant_id=config.tenant_id,
        arm_session_factory=lambda: build_arm_session(
            config.auth_mode, config.tenant_id
        ),
    )


def _verify_signals(
    kusto_uri: str,
    database_name: str,
    credential: Any,
    signals: list[dict[str, Any]],
) -> None:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    expected = {row["case_id"] for row in signals}
    identities = ", ".join(json.dumps(value) for value in sorted(expected))
    connection = KustoConnectionStringBuilder.with_azure_token_credential(
        kusto_uri, credential
    )
    with KustoClient(connection) as client:
        result = client.execute_query(
            database_name,
            f"ops_availability_signals | where case_id in ({identities}) "
            "| summarize arg_max(event_ts, *) by case_id",
        )
        table = result.primary_results[0]
        columns = [column.column_name for column in table.columns]
        actual = [dict(zip(columns, row, strict=True)) for row in table]
    validate_signal_readback(signals, actual)


def _failure_message(exc: BaseException, limit: int = 500) -> str:
    """Bounded, single-line failure text for the operations report."""
    return " ".join(str(exc).split())[:limit] or type(exc).__name__


def publish(
    *,
    repo_root: Path,
    config: DeployConfig,
    outputs: dict[str, Any],
    operations_only: bool = False,
    connection_id: str | None = None,
    acknowledge_plan_billing: bool = False,
) -> dict[str, Any]:
    """Publish requested components; never activate monitoring or place orders."""
    validate_terraform_outputs(config, outputs)
    if not config.operations_planning.enabled:
        raise NativeItemError(
            "Enable operations_planning.enabled in this environment first."
        )
    operations_only = operations_only or not config.operations_planning.include_plan
    connection_id = connection_id or config.operations_planning.plan_connection_id
    if not operations_only:
        if not connection_id:
            raise NativeItemError(
                "Create a shared Power BI Semantic Model connection for Planning, "
                "then set operations_planning.plan_connection_id. "
                "Use --operations-only to publish the stopped Operations Agent separately."
            )
        if not (
            acknowledge_plan_billing
            or config.operations_planning.plan_billing_acknowledged
        ):
            raise NativeItemError(
                "Planning can start a non-cancellable 30-day billable session. "
                "Set plan_billing_acknowledged or pass --acknowledge-plan-billing."
            )
    credential = build_credential(config.auth_mode, tenant_id=config.tenant_id)
    session = build_session(credential)
    workspace_id = _guid(outputs["workspace_id"])
    models = [
        item
        for item in list_items(session, workspace_id, "SemanticModel")
        if item.get("displayName") == config.powerbi.semantic_model_name
    ]
    if len(models) != 1:
        raise NativeItemError("Expected exactly one deployed retail semantic model.")
    plan_definition = None
    if not operations_only:
        from deploy.scripts._plan_definition import render_plan_definition

        validate_plan_connection(session, connection_id)
        validate_model_connections(session, workspace_id, models[0]["id"])
        plan_definition = render_plan_definition(
            workspace_id=workspace_id,
            workspace_name=config.workspace.name,
            semantic_model_id=models[0]["id"],
            semantic_model_name=config.powerbi.semantic_model_name,
            connection_id=connection_id,
            display_name=PLAN_NAME,
        )
    _ensure_native_capacity(session, config, workspace_id)
    if not operations_only:
        validate_planning_measures(session, workspace_id, models[0]["id"])
    kusto_uri, database_name = resolve_kql_database(
        workspace_id,
        outputs["kql_database_id"],
        credential,
    )
    execute_database_script(
        kusto_uri,
        database_name,
        build_database_script(
            [repo_root / "fabric" / "operations" / "01-availability-tables.kql"]
        ),
        credential,
    )
    action_id = ensure_native_item(
        session,
        workspace_id=workspace_id,
        item_type="Notebook",
        display_name=ACTION_NAME,
        description="Human-approved synthetic replenishment draft; no order execution.",
        definition=render_case_notebook(
            workspace_id=workspace_id,
            lakehouse_id=outputs["lakehouse_id"],
            lakehouse_name=config.lakehouse.name,
            kusto_uri=kusto_uri,
            database_name=database_name,
            include_plan=not operations_only,
        ),
    )
    report: dict[str, Any] = {
        "environment": config.environment,
        "workspace_id": workspace_id,
        "observed_at": datetime.now(UTC).isoformat(),
        "signal_count": None,
        "operations_agent": {"state": "PENDING", "name": AGENT_NAME},
        "action_notebook": {"id": action_id, "name": ACTION_NAME},
        "plan": {
            "state": (
                "PENDING" if plan_definition is not None
                else "NOT_REQUESTED" if config.operations_planning.include_plan else "NOT_SELECTED"
            )
        },
        "acceptance": (
            "Generate/review the playbook and Teams destination before activation. "
            "The prepare-replenishment-case Notebook action requires human approval. "
            + (
                "Review ops.replenishment_cases and the SSO report. No Plan. "
                if operations_only else
                "Confirm Plan model access and application database. "
            )
            + "No approval routing, writeback, or ERP execution."
        ),
    }
    report_path = (
        repo_root
        / "deploy"
        / ".generated"
        / config.environment
        / "operations-planning-report.json"
    )
    write_report_atomic(report_path, report)
    try:
        signals = scenario_signals(
            _source_rows(session, outputs, config, credential),
            datetime.now(UTC),
        )
        execute_database_script(
            kusto_uri,
            database_name,
            signal_ingestion_query(signals),
            credential,
        )
        _verify_signals(kusto_uri, database_name, credential, signals)
        report["signal_count"] = len(signals)
        write_report_atomic(report_path, report)
        agent_id = ensure_native_item(
            session,
            workspace_id=workspace_id,
            item_type="OperationsAgent",
            display_name=AGENT_NAME,
            description="Monitor bounded availability cases; recommend human-approved drafts.",
            definition=render_operations_definition(
                workspace_id=workspace_id,
                kql_database_id=outputs["kql_database_id"],
                recipient=config.operations_planning.recipient,
                include_plan=not operations_only,
            ),
        )
        ui_configuration = ensure_operations_ui_configuration(
            credential,
            workspace_id=workspace_id,
            agent_id=agent_id,
            kql_database_id=outputs["kql_database_id"],
            kusto_uri=kusto_uri,
            notebook_id=action_id,
            instructions=render_operations_instructions(
                include_plan=not operations_only
            ),
        )
    except (NativeItemError, RequestException, ValueError) as exc:
        report["operations_agent"] = {
            "name": AGENT_NAME, "state": "PUBLICATION_FAILED",
            "failure_type": type(exc).__name__,
            "failure_message": _failure_message(exc),
        }
        if plan_definition is not None:
            report["plan"]["state"] = "NOT_ATTEMPTED"
        write_report_atomic(report_path, report)
        raise
    report["operations_agent"] = {
        "id": agent_id,
        "state": "STOPPED",
        "name": AGENT_NAME,
        "source_count": len(ui_configuration["dataSources"]),
        "action_count": len(ui_configuration["actions"]),
    }
    write_report_atomic(report_path, report)
    if plan_definition is not None:
        try:
            plan_id = ensure_native_item(
                session,
                workspace_id=workspace_id,
                item_type="Plan",
                display_name=PLAN_NAME,
                description="Bounded replenishment what-if workbench; human approval, no real purchasing.",
                definition=plan_definition,
            )
        except (NativeItemError, RequestException, ValueError) as exc:
            report["plan"] = {
                "state": "PUBLICATION_FAILED",
                "failure_type": type(exc).__name__,
                "failure_message": _failure_message(exc),
            }
            write_report_atomic(report_path, report)
            raise
        report["plan"] = {
            "id": plan_id,
            "name": PLAN_NAME,
            "semantic_model_id": models[0]["id"],
            "connection_id": connection_id,
            "state": "DEFINITION_PUBLISHED",
        }
        write_report_atomic(report_path, report)
    return report


def publish_existing_agent(
    *,
    repo_root: Path,
    config: DeployConfig,
    outputs: dict[str, Any],
) -> dict[str, Any]:
    """Retry only agent publication using previously verified availability cases."""

    if not config.operations_planning.enabled:
        raise NativeItemError(
            "Enable operations_planning.enabled in this environment first."
        )
    if config.operations_planning.include_plan:
        raise NativeItemError(
            "Agent-only recovery is available only when native Planning is excluded."
        )
    validate_terraform_outputs(config, outputs)
    workspace_id = _guid(outputs["workspace_id"])
    report_path = (
        repo_root
        / "deploy"
        / ".generated"
        / config.environment
        / "operations-planning-report.json"
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise NativeItemError(
            "The previous Operations report is not a JSON object."
        )
    if (
        report.get("environment") != config.environment
        or report.get("workspace_id") != workspace_id
        or not isinstance(report.get("signal_count"), int)
        or report["signal_count"] < 1
    ):
        raise NativeItemError(
            "Agent-only recovery requires previously verified signals in the "
            "current configured workspace."
        )
    credential = build_credential(config.auth_mode, tenant_id=config.tenant_id)
    session = build_session(credential)
    action = report.get("action_notebook", {})
    action_id = action.get("id") if isinstance(action, dict) else None
    notebooks = [
        item
        for item in list_items(session, workspace_id, "Notebook")
        if str(item.get("id", "")) == action_id
        and item.get("displayName") == ACTION_NAME
    ]
    if len(notebooks) != 1:
        raise NativeItemError(
            "The previously recorded replenishment action notebook is not "
            "uniquely present in the configured workspace."
        )

    _ensure_native_capacity(session, config, workspace_id)
    kusto_uri, _database_name = resolve_kql_database(
        workspace_id,
        outputs["kql_database_id"],
        credential,
    )
    agent_id = ensure_native_item(
        session,
        workspace_id=workspace_id,
        item_type="OperationsAgent",
        display_name=AGENT_NAME,
        description="Monitor bounded availability cases; recommend human-approved drafts.",
        definition=render_operations_definition(
            workspace_id=workspace_id,
            kql_database_id=_guid(outputs["kql_database_id"]),
            recipient=config.operations_planning.recipient,
            include_plan=False,
        ),
    )
    ui_configuration = ensure_operations_ui_configuration(
        credential,
        workspace_id=workspace_id,
        agent_id=agent_id,
        kql_database_id=_guid(outputs["kql_database_id"]),
        kusto_uri=kusto_uri,
        notebook_id=_guid(action_id),
        instructions=render_operations_instructions(include_plan=False),
    )
    report["observed_at"] = datetime.now(UTC).isoformat()
    report["operations_agent"] = {
        "id": agent_id,
        "state": "STOPPED",
        "name": AGENT_NAME,
        "source_count": len(ui_configuration["dataSources"]),
        "action_count": len(ui_configuration["actions"]),
    }
    report["acceptance"] = (
        "Generate/review the playbook and Teams destination before activation. "
        "The prepare-replenishment-case Notebook action is configured but still "
        "requires human approval. Review ops.replenishment_cases and the SSO "
        "report. No Plan, approval routing, writeback, or ERP execution."
    )
    write_report_atomic(report_path, report)
    return report


def main(argv: list[str] | None = None) -> int:
    from deploy.scripts.workspace_folders import organize_after_publish

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--operations-only", action="store_true")
    parser.add_argument(
        "--agent-only",
        action="store_true",
        help="Retry agent publication without reingesting existing availability signals.",
    )
    parser.add_argument("--plan-connection-id")
    parser.add_argument("--acknowledge-plan-billing", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.agent_only and args.dry_run:
        console.error("--agent-only cannot be combined with --dry-run.")
        return 1
    config = load_environment(
        args.environment,
        config_path=args.repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=args.repo_root / "deploy" / "config" / "environments",
    )
    if args.dry_run:
        if not config.operations_planning.enabled:
            console.info("Operations/Planning publication is disabled for this environment.")
            return 0
        console.info(
            "Would publish bounded signals, a draft notebook, and a STOPPED Operations Agent."
        )
        if not args.operations_only and config.operations_planning.include_plan:
            console.info(
                "Would publish native Plan using a shared fixed-credential connection."
            )
        return 0
    outputs = load_terraform_outputs(
        args.repo_root
        / "deploy"
        / ".generated"
        / args.environment
        / "terraform-output.json"
    )
    if args.agent_only:
        try:
            report = publish_existing_agent(
                repo_root=args.repo_root,
                config=config,
                outputs=outputs,
            )
        except (
            OSError,
            NativeItemError,
            CapacityResumeError,
            RequestException,
            ValueError,
        ) as exc:
            console.error(str(exc))
            return 1
        console.info(
            "Published the stopped Operations Agent using existing verified signals."
        )
        if not organize_after_publish(args.repo_root, args.environment):
            return 1
        console.detail(report["acceptance"])
        return 0
    try:
        report = publish(
            repo_root=args.repo_root,
            config=config,
            outputs=outputs,
            operations_only=args.operations_only,
            connection_id=args.plan_connection_id,
            acknowledge_plan_billing=args.acknowledge_plan_billing,
        )
    except (NativeItemError, CapacityResumeError, ValueError) as exc:
        console.error(str(exc))
        return 1
    if not organize_after_publish(args.repo_root, args.environment):
        return 1
    console.info(
        f"Published stopped Operations Agent and {report['signal_count']} availability signals."
    )
    console.info(
        f"Planning: {report['plan']['state']}. See operations-planning-report.json."
    )
    console.detail(report["acceptance"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
