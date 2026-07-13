"""Prepare or execute ordered KQL database scripts.

``--execute`` applies the combined script directly to the Fabric Eventhouse KQL
database using the configured operator credential. This is the supported path:
the deploy runs locally with the user's permissions, which include Eventhouse
administration — unlike a Fabric notebook's identity. The script resolves the
database's ``queryServiceUri`` from the Fabric REST API, then runs the batch with
the Kusto Python SDK.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts import _output as console
from deploy.scripts._auth import AUTH_MODES, build_credential

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

REPO_ROOT = Path(__file__).resolve().parents[2]
KQL_SOURCE_DIR = REPO_ROOT / "fabric" / "kql_database"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
FABRIC_API = "https://api.fabric.microsoft.com/v1"


def collect_kql_scripts(source_dir: Path = KQL_SOURCE_DIR) -> list[Path]:
    """Collect ordered KQL scripts from the source directory."""

    if not source_dir.exists():
        raise FileNotFoundError(f"KQL source directory not found: {source_dir}")
    scripts = sorted(source_dir.glob("*.kql"))
    if not scripts:
        raise ValueError(f"No KQL scripts found in {source_dir}")
    return scripts


def build_database_script(scripts: list[Path]) -> str:
    """Build one `.execute database script` payload from ordered KQL files.

    ``ThrowOnErrors=true`` makes the batch fail (and raise) on the first command
    error. Without it, ``.execute database script`` *always* reports success even
    when individual commands fail, which silently leaves the schema unapplied.
    """

    parts = [".execute database script with (ThrowOnErrors=true) <|"]
    for script in scripts:
        parts.append(f"\n// BEGIN {script.name}")
        parts.append(script.read_text(encoding="utf-8").strip())
        parts.append(f"// END {script.name}")
    return "\n".join(parts).rstrip() + "\n"


def _credential(
    credential: TokenCredential | None = None,
    *,
    auth_mode: str = "azure_cli",
) -> TokenCredential:
    """Return an injected credential or construct the configured operator login."""

    return credential or build_credential(auth_mode)


def resolve_kql_database(
    workspace_id: str,
    kql_database_id: str,
    credential: TokenCredential,
) -> tuple[str, str]:
    """Return ``(query_service_uri, database_name)`` for a Fabric KQL database."""

    import requests

    token = credential.get_token(FABRIC_SCOPE).token
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/kqlDatabases/{kql_database_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    item = resp.json()
    properties = item.get("properties", {})
    query_uri = properties.get("queryServiceUri")
    if not query_uri:
        raise RuntimeError(
            f"KQL database {kql_database_id} has no queryServiceUri "
            f"(properties: {sorted(properties)})"
        )
    database_name = item.get("displayName") or properties.get("databaseName")
    return query_uri, str(database_name)


def execute_database_script(
    query_uri: str,
    database_name: str,
    script: str,
    credential: TokenCredential,
) -> Any:
    """Run a KQL management script against the database with the Kusto SDK."""

    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
        query_uri, credential
    )
    with KustoClient(kcsb) as client:
        return client.execute_mgmt(database_name, script)


def _terraform_outputs(environment: str) -> dict[str, Any]:
    from deploy.scripts.deploy_config import load_terraform_outputs

    path = REPO_ROOT / "deploy" / ".generated" / environment / "terraform-output.json"
    if not path.exists():
        raise SystemExit(
            f"Terraform outputs not found: {path}\n"
            "Run a full deploy first (the Terraform steps write this file), or "
            "pass --workspace-id, --kql-database-id, and --kql-database-name."
        )
    return load_terraform_outputs(path)


def _summarize_result(table: Any) -> None:
    """Print a concise summary of a ``.execute database script`` result.

    The raw Kusto result has one wide row per command (including the full
    command text), which is hundreds of rows for the retail schema. Collapse it
    to a completed/failed count and only print details for failed commands.

    Rows are read straight from the Kusto result table (each ``KustoResultRow``
    exposes ``to_dict``), so this avoids pulling in pandas — an optional
    ``azure-kusto-data`` extra that the deploy environment does not install.
    """

    rows = [row.to_dict() for row in table]
    total = len(rows)
    if not rows or "Result" not in rows[0]:
        console.info(f"KQL applied: {total} command(s).")
        return

    failures = [row for row in rows if row.get("Result") != "Completed"]
    completed = total - len(failures)
    if not failures:
        console.info(f"KQL applied: {completed}/{total} commands completed.")
        return

    console.info(f"KQL applied: {completed}/{total} completed, {len(failures)} failed:")
    for row in failures:
        lines = str(row.get("CommandText", "")).strip().splitlines()
        first_line = lines[0][:100] if lines else ""
        reason = str(row.get("Reason", "")).strip()
        console.detail(f"[{row.get('CommandType', '')}] {first_line} -> {reason}")


def apply_to_database(
    *,
    script: str,
    workspace_id: str,
    kql_database_id: str,
    kql_database_name: str | None = None,
    auth_mode: str = "azure_cli",
    credential: TokenCredential | None = None,
) -> int:
    """Resolve the KQL database and apply the combined script. Returns row count."""

    credential = _credential(credential, auth_mode=auth_mode)
    query_uri, resolved_name = resolve_kql_database(
        workspace_id, kql_database_id, credential
    )
    database_name = kql_database_name or resolved_name
    console.info(f"Applying KQL to '{database_name}' @ {query_uri}")

    from deploy.scripts._retry import retry_call

    # The Kusto SDK acquires the token inside execute_mgmt; a cold az token can
    # raise KustoAuthenticationError. Retrying re-runs the (idempotent) script.
    # The exception type is only available when the Kusto SDK is installed (it is
    # in the deploy env); without it, retry_on=() simply means "don't retry".
    try:
        from azure.kusto.data.exceptions import KustoAuthenticationError

        retry_on: tuple[type[BaseException], ...] = (KustoAuthenticationError,)
    except ModuleNotFoundError:
        retry_on = ()

    response = retry_call(
        lambda: execute_database_script(query_uri, database_name, script, credential),
        retry_on=retry_on,
        on_retry=lambda n, exc: console.warn(
            f"Kusto auth attempt {n} failed ({type(exc).__name__}); retrying..."
        ),
    )

    table = response.primary_results[0]
    _summarize_result(table)
    return len(table)


def main() -> int:
    """Write a combined KQL script and optionally execute it against the database."""

    parser = argparse.ArgumentParser(description="Prepare or apply ordered KQL scripts")
    parser.add_argument("--source-dir", type=Path, default=KQL_SOURCE_DIR)
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "deploy" / ".generated" / "kql" / "database.kql",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the script to the Fabric Eventhouse KQL database.",
    )
    parser.add_argument(
        "--environment",
        help="Read workspace/database ids from deploy/.generated/<env>/terraform-output.json.",
    )
    parser.add_argument(
        "--workspace-id", help="Fabric workspace id (overrides --environment)."
    )
    parser.add_argument(
        "--kql-database-id", help="Fabric KQL database id (overrides --environment)."
    )
    parser.add_argument(
        "--kql-database-name",
        help="KQL database name (overrides resolved display name).",
    )
    parser.add_argument(
        "--auth-mode",
        choices=AUTH_MODES,
        default="azure_cli",
        help="Operator credential used for Fabric and Kusto requests.",
    )
    args = parser.parse_args()

    script = build_database_script(collect_kql_scripts(args.source_dir))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(script, encoding="utf-8")
    console.info(f"Wrote combined KQL script to {args.output}")

    if not args.execute:
        return 0

    workspace_id = args.workspace_id
    kql_database_id = args.kql_database_id
    kql_database_name = args.kql_database_name
    if (not workspace_id or not kql_database_id) and args.environment:
        outputs = _terraform_outputs(args.environment)
        workspace_id = workspace_id or outputs.get("workspace_id")
        kql_database_id = kql_database_id or outputs.get("kql_database_id")
        kql_database_name = kql_database_name or outputs.get("kql_database_name")
    if not workspace_id or not kql_database_id:
        raise SystemExit(
            "--execute requires --workspace-id and --kql-database-id, or "
            "--environment with generated Terraform outputs."
        )

    apply_to_database(
        script=script,
        workspace_id=str(workspace_id),
        kql_database_id=str(kql_database_id),
        kql_database_name=kql_database_name and str(kql_database_name),
        auth_mode=args.auth_mode,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
