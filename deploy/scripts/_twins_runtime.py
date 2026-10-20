"""Read-only twin data discovery and explicit SSO preflight."""

from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime
from typing import Any

from deploy.scripts._native_items import API, NativeItemError, _guid
from deploy.scripts._twin_cohort import (
    ROUTES_SQL,
    products_query,
    select_cohort,
    utc_time,
    validate_product_ids,
)
from deploy.scripts.capacity_resume import build_arm_session, ensure_capacity_active
from deploy.scripts.deploy_config import DeployConfig
from deploy.scripts.fabric_runtime import paginated_get
from deploy.scripts.verify_readiness import _connect_lakehouse_sql


def ensure_workspace_active(session: Any, config: DeployConfig, workspace_id: str) -> None:
    response = session.get(
        f"{API}/workspaces/{_guid(workspace_id)}", timeout=30, allow_redirects=False,
    )
    response.raise_for_status()
    workspace = response.json()
    if workspace.get("displayName") != config.workspace.name:
        raise NativeItemError("Live twin workspace does not match the configured target.")
    capacity_id = _guid(workspace.get("capacityId"))

    def capacity() -> dict[str, Any]:
        result = session.get(f"{API}/capacities/{capacity_id}", timeout=30, allow_redirects=False)
        result.raise_for_status()
        return result.json()

    ensure_capacity_active(
        capacity,
        resource_id=config.workspace.capacity_resource_id,
        expected_name=config.workspace.capacity_name,
        subscription_id=config.subscription_id,
        tenant_id=config.tenant_id,
        arm_session_factory=lambda: build_arm_session(config.auth_mode, config.tenant_id),
    )


def require_sso(session: Any, workspace_id: str, model_id: str) -> list[dict[str, Any]]:
    bindings = paginated_get(
        session,
        f"{API}/workspaces/{_guid(workspace_id)}/items/{_guid(model_id)}/connections",
        timeout=30,
    )
    if not bindings or any(row.get("connectivityType") != "Automatic" for row in bindings):
        raise NativeItemError(
            "Connected twins require the existing Automatic/SSO model binding. "
            "No connection or credential will be changed."
        )
    return bindings


def discover_cohort(
    session: Any, config: DeployConfig, outputs: dict[str, Any], credential: Any,
    *, observed_at: datetime | None = None,
) -> dict[str, Any]:
    response = session.get(
        f"{API}/workspaces/{_guid(outputs['workspace_id'])}/lakehouses/{_guid(outputs['lakehouse_id'])}",
        timeout=30, allow_redirects=False,
    )
    response.raise_for_status()
    server = response.json().get("properties", {}).get("sqlEndpointProperties", {}).get("connectionString")
    if not server:
        raise NativeItemError("The twin source Lakehouse SQL endpoint is unavailable.")
    now = utc_time(observed_at or datetime.now(UTC))
    with closing(_connect_lakehouse_sql(server, config.lakehouse.name, credential)) as connection:
        cursor = connection.cursor()
        cursor.execute(ROUTES_SQL, (now.replace(tzinfo=None),))
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        if not rows:
            raise NativeItemError("No observed logistics history is available for connected twins.")
        anchors = {utc_time(row["source_as_of"]) for row in rows}
        if len(anchors) != 1 or next(iter(anchors)) > now:
            raise NativeItemError("Twin source anchors are inconsistent or future-dated.")
        cohort = select_cohort(rows, as_of=next(iter(anchors)))
        query, parameters = products_query(cohort)
        cursor.execute(query, parameters)
        cohort["product_ids"] = validate_product_ids([int(row[0]) for row in cursor.fetchall()])
    return cohort
