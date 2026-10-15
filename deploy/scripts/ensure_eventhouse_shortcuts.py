"""Provision the opt-in Eventhouse-to-Lakehouse streaming projection."""

from __future__ import annotations

import argparse
import ast
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoApiError, KustoNetworkError

from deploy.scripts import _output as console
from deploy.scripts._auth import build_credential
from deploy.scripts._retry import retry_call
from deploy.scripts.apply_kql import resolve_kql_database
from deploy.scripts.deploy_config import (
    DeployConfig,
    validate_terraform_outputs,
)
from deploy.scripts.export_items import build_session
from deploy.scripts.fabric_runtime import retry_read

ROOT = Path(__file__).resolve().parents[2]
API = "https://api.fabric.microsoft.com/v1"
_EVENT_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_MIRROR_LATENCY_MINUTES = 5


class ShortcutProvisionError(RuntimeError):
    """An Eventhouse table or Lakehouse shortcut failed validation."""


def expected_event_tables(repo_root: Path = ROOT) -> tuple[str, ...]:
    """Read event names from the generator and confirm the KQL tables exist in source."""

    driver = repo_root / "utility" / "notebooks" / "templates" / "driver-05-stream.py"
    tree = ast.parse(driver.read_text(encoding="utf-8"), filename=str(driver))
    payloads: Any = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "EVENT_PAYLOADS"
            for target in node.targets
        ):
            payloads = ast.literal_eval(node.value)
            break
    if not isinstance(payloads, dict) or not payloads:
        raise ShortcutProvisionError("Stream source has no usable EVENT_PAYLOADS mapping.")
    tables = tuple(sorted(str(name) for name in payloads))
    if any(not _EVENT_NAME.fullmatch(name) for name in tables):
        raise ShortcutProvisionError("Stream source contains an invalid KQL table name.")

    kql = (repo_root / "fabric" / "kql_database" / "01-create-tables.kql").read_text(
        encoding="utf-8"
    )
    declared = set(
        re.findall(
            r"(?im)^\s*\.create-merge\s+table\s+([A-Za-z][A-Za-z0-9_]*)",
            kql,
        )
    )
    missing = sorted(set(tables) - declared)
    if missing:
        raise ShortcutProvisionError(
            "Stream payload tables are missing from KQL source: " + ", ".join(missing)
        )
    return tables


def _read_mirror_policy(
    kusto_uri: str,
    database: str,
    credential: Any,
    table: str,
) -> dict[str, Any]:
    from azure.kusto.data import KustoConnectionStringBuilder

    def read() -> dict[str, Any]:
        connection = KustoConnectionStringBuilder.with_azure_token_credential(
            kusto_uri, credential
        )
        with KustoClient(connection) as client:
            response = client.execute_mgmt(
                database, f".show table {table} policy mirroring"
            )
        if not response.primary_results or len(response.primary_results[0]) != 1:
            raise ShortcutProvisionError(
                f"Mirroring-policy readback for {table} is incomplete."
            )
        value = response.primary_results[0][0].to_dict().get("Policy")
        if value in (None, "", "null"):
            return {}
        policy = json.loads(value) if isinstance(value, str) else value
        if not isinstance(policy, dict):
            raise ShortcutProvisionError(
                f"Mirroring policy for {table} is not a JSON object."
            )
        return policy

    return retry_call(
        read,
        retry_on=(KustoNetworkError,),
        on_retry=lambda attempt, _exc: console.warn(
            f"Mirroring-policy read for {table}, attempt {attempt}/3, hit a network error."
        ),
    )


def _read_live_event_tables(
    kusto_uri: str,
    database: str,
    credential: Any,
) -> set[str]:
    def read() -> set[str]:
        connection = KustoConnectionStringBuilder.with_azure_token_credential(
            kusto_uri, credential
        )
        with KustoClient(connection) as client:
            response = client.execute_mgmt(
                database, ".show tables | project name=TableName"
            )
        if not response.primary_results:
            raise ShortcutProvisionError("KQL table inventory returned no result set.")
        return {
            str(row.to_dict()["name"])
            for row in response.primary_results[0]
        }

    return retry_call(
        read,
        retry_on=(KustoNetworkError,),
        on_retry=lambda attempt, _exc: console.warn(
            f"KQL table inventory attempt {attempt}/3 hit a network error; retrying..."
        ),
    )


def _shortcut_url(workspace_id: str, lakehouse_id: str, table: str) -> str:
    return (
        f"{API}/workspaces/{workspace_id}/items/{lakehouse_id}/"
        f"shortcuts/Tables/cusn/{table}"
    )


def _validate_shortcut(
    shortcut: Any,
    workspace_id: str,
    kql_database_id: str,
    table: str,
) -> dict[str, Any]:
    if not isinstance(shortcut, dict):
        raise ShortcutProvisionError(
            f"Shortcut Tables/cusn/{table} returned a malformed response."
        )
    target = shortcut.get("target")
    one_lake = target.get("oneLake") if isinstance(target, dict) else None
    if not isinstance(one_lake, dict) or (
        shortcut.get("name") != table
        or not isinstance(shortcut.get("path"), str)
        or shortcut.get("path", "").strip("/") != "Tables/cusn"
        or target.get("type") != "OneLake"
        or str(one_lake.get("workspaceId", "")).casefold() != workspace_id.casefold()
        or str(one_lake.get("itemId", "")).casefold() != kql_database_id.casefold()
        or one_lake.get("path") != f"Tables/{table}"
    ):
        raise ShortcutProvisionError(
            f"Shortcut Tables/cusn/{table} does not target the expected "
            "Eventhouse table in this workspace."
        )
    return shortcut


def _read_shortcut(
    session: requests.Session,
    workspace_id: str,
    lakehouse_id: str,
    kql_database_id: str,
    table: str,
) -> dict[str, Any] | None:
    response = retry_read(
        lambda: session.get(
            _shortcut_url(workspace_id, lakehouse_id, table),
            timeout=30,
            allow_redirects=False,
        )
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return _validate_shortcut(
        response.json(), workspace_id, kql_database_id, table
    )


def _ensure_mirroring(
    kusto_uri: str,
    database: str,
    credential: Any,
    table: str,
) -> dict[str, Any]:
    policy = _read_mirror_policy(kusto_uri, database, credential, table)
    enabled = policy.get("IsEnabled") is True
    backfill = policy.get("Backfill") is True
    if enabled and backfill:
        return policy
    if enabled and not backfill:
        raise ShortcutProvisionError(
            f"OneLake mirroring is already enabled for {table} without historical "
            "backfill. It must be disabled and reviewed before changing Backfill."
        )

    command = (
        f".alter-merge table {table} policy mirroring dataformat=parquet "
        f"with (IsEnabled=true, Backfill=true, "
        f"TargetLatencyInMinutes={_MIRROR_LATENCY_MINUTES})"
    )
    connection = KustoConnectionStringBuilder.with_azure_token_credential(
        kusto_uri, credential
    )
    try:
        with KustoClient(connection) as client:
            client.execute_mgmt(database, command)
    except KustoNetworkError:
        # A management request may be accepted before a connection drops. Read
        # the exact state before deciding whether this table needs another attempt.
        confirmed = _read_mirror_policy(kusto_uri, database, credential, table)
        if confirmed.get("IsEnabled") is True and confirmed.get("Backfill") is True:
            return confirmed
        raise
    confirmed = _read_mirror_policy(kusto_uri, database, credential, table)
    if confirmed.get("IsEnabled") is not True or confirmed.get("Backfill") is not True:
        raise ShortcutProvisionError(
            f"OneLake mirroring for {table} did not read back enabled with Backfill."
        )
    return confirmed


def ensure_shortcuts(
    repo_root: Path,
    config: DeployConfig,
    outputs: dict[str, Any],
) -> dict[str, Any]:
    """Enable backfill mirrors and create only missing shortcuts with Abort semantics."""

    if not config.streaming_shortcuts.enabled:
        raise ShortcutProvisionError(
            "Enable streaming_shortcuts.enabled before provisioning mirrors and shortcuts."
        )
    validate_terraform_outputs(config, outputs)
    workspace_id = str(outputs["workspace_id"])
    lakehouse_id = str(outputs["lakehouse_id"])
    kql_database_id = str(outputs["kql_database_id"])
    tables = expected_event_tables(repo_root)
    credential = build_credential(config.auth_mode, tenant_id=config.tenant_id)
    kusto_uri, database = retry_read(
        lambda: resolve_kql_database(workspace_id, kql_database_id, credential)
    )
    session = build_session(credential)

    # Validate every existing shortcut and mirror policy before the first write.
    live_tables = _read_live_event_tables(kusto_uri, database, credential)
    missing_tables = sorted(set(tables) - live_tables)
    if missing_tables:
        raise ShortcutProvisionError(
            "Eventhouse source tables are missing: " + ", ".join(missing_tables)
        )
    shortcuts = {
        table: _read_shortcut(
            session, workspace_id, lakehouse_id, kql_database_id, table
        )
        for table in tables
    }
    policies = {
        table: _read_mirror_policy(kusto_uri, database, credential, table)
        for table in tables
    }
    unsafe = [
        table
        for table, policy in policies.items()
        if policy.get("IsEnabled") is True and policy.get("Backfill") is not True
    ]
    if unsafe:
        raise ShortcutProvisionError(
            "OneLake mirroring is already enabled without historical Backfill for: "
            + ", ".join(unsafe)
            + ". No shortcuts were changed."
        )

    report = {
        "schema_version": 1,
        "environment": config.environment,
        "workspace_id": workspace_id,
        "lakehouse_id": lakehouse_id,
        "kql_database_id": kql_database_id,
        "observed_at": datetime.now(UTC).isoformat(),
        "status": "RUNNING",
        "table_count": len(tables),
        "tables": [],
    }
    for table in tables:
        policy = policies[table]
        if policy.get("IsEnabled") is not True or policy.get("Backfill") is not True:
            policy = _ensure_mirroring(kusto_uri, database, credential, table)
        shortcut = shortcuts[table]
        if shortcut is None:
            url = (
                f"{API}/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
                "?shortcutConflictPolicy=Abort"
            )
            body = {
                "name": table,
                "path": "Tables/cusn",
                "target": {
                    "oneLake": {
                        "workspaceId": workspace_id,
                        "itemId": kql_database_id,
                        "path": f"Tables/{table}",
                    }
                },
            }
            try:
                response = session.post(
                    url, json=body, timeout=60, allow_redirects=False
                )
                if response.status_code not in (200, 201):
                    response.raise_for_status()
                    raise ShortcutProvisionError(
                        f"Unexpected shortcut-create status for {table}: "
                        f"{response.status_code}."
                    )
                shortcut = response.json()
            except requests.RequestException:
                # Never replay a shortcut-create POST after a lost response.
                shortcut = _read_shortcut(
                    session, workspace_id, lakehouse_id, kql_database_id, table
                )
                if shortcut is None:
                    raise
            shortcut = _validate_shortcut(
                shortcut, workspace_id, kql_database_id, table
            )
        report["tables"].append(
            {
                "name": table,
                "mirroring_enabled": policy.get("IsEnabled") is True,
                "backfill": policy.get("Backfill") is True,
                "shortcut_path": f"Tables/cusn/{table}",
                "shortcut_status": "VERIFIED",
            }
        )

    report["status"] = "SUCCEEDED"
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--environment", required=True)
    args = parser.parse_args(argv)

    from deploy.scripts.deploy_config import load_environment, load_terraform_outputs

    generated = args.repo_root / "deploy" / ".generated" / args.environment
    report_path = generated / "eventhouse-shortcuts-report.json"
    try:
        config = load_environment(
            args.environment,
            config_path=args.repo_root / "deploy" / "config" / "deploy.yml",
            environments_root=args.repo_root / "deploy" / "config" / "environments",
        )
        report = ensure_shortcuts(
            args.repo_root,
            config,
            load_terraform_outputs(generated / "terraform-output.json"),
        )
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    except (
        OSError,
        ValueError,
        requests.RequestException,
        KustoApiError,
        ShortcutProvisionError,
    ) as exc:
        console.error(str(exc))
        failure_report = {
            "schema_version": 1,
            "environment": args.environment,
            "observed_at": datetime.now(UTC).isoformat(),
            "status": "FAILED",
            "error": str(exc),
        }
        try:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                json.dumps(failure_report, indent=2) + "\n", encoding="utf-8"
            )
        except OSError as report_error:
            console.error(f"Could not write failure evidence: {report_error}")
        return 1
    console.info(f"Verified {report['table_count']} Eventhouse shortcuts.")
    console.detail(f"Evidence: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
