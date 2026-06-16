"""Prepare or execute ordered KQL database scripts.

``--execute`` applies the combined script directly to the Fabric Eventhouse KQL
database using the operator's Azure CLI login (``AzureCliCredential``). This is
the supported path: the deploy runs locally with the user's credentials, which
have Eventhouse admin rights — unlike a Fabric notebook's identity, which does
not. The script resolves the database's ``queryServiceUri`` from the Fabric REST
API, then runs the batch with the Kusto Python SDK.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts import _output as console

if TYPE_CHECKING:
    from azure.identity import AzureCliCredential

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


def _credential(credential: AzureCliCredential | None = None) -> AzureCliCredential:
    """Azure CLI credential with a generous process timeout.

    A *cold* ``az account get-access-token`` for the Kusto/Eventhouse audience can
    take ~90s on Windows (warm calls are ~1s). 120s absorbs the cold-start
    without making a genuinely-broken ``az`` hang excessively.
    """

    from azure.identity import AzureCliCredential

    return credential or AzureCliCredential(process_timeout=120)


def resolve_kql_database(
    workspace_id: str,
    kql_database_id: str,
    credential: AzureCliCredential,
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
    credential: AzureCliCredential,
) -> Any:
    """Run a KQL management script against the database with the Kusto SDK."""

    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    kcsb = KustoConnectionStringBuilder.with_azure_token_credential(query_uri, credential)
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


def _result_to_frame(response: Any) -> Any:
    """Convert a Kusto response's primary result to a pandas DataFrame."""

    from azure.kusto.data.helpers import dataframe_from_result_table

    return dataframe_from_result_table(response.primary_results[0])


def _summarize_result(frame: Any) -> None:
    """Print a concise summary of a ``.execute database script`` result.

    The raw Kusto result has one wide row per command (including the full
    command text), which is hundreds of rows for the retail schema. Collapse it
    to a completed/failed count and only print details for failed commands.
    """

    total = len(frame)
    columns = getattr(frame, "columns", None)
    results = frame["Result"] if columns is not None and "Result" in columns else None
    if results is None:
        console.info(f"KQL applied: {total} command(s).")
        return

    failures = frame[results != "Completed"]
    completed = total - len(failures)
    if len(failures) == 0:
        console.info(f"KQL applied: {completed}/{total} commands completed.")
        return

    console.info(f"KQL applied: {completed}/{total} completed, {len(failures)} failed:")
    for _, row in failures.iterrows():
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
    credential: AzureCliCredential | None = None,
) -> int:
    """Resolve the KQL database and apply the combined script. Returns row count."""

    credential = _credential(credential)
    query_uri, resolved_name = resolve_kql_database(
        workspace_id, kql_database_id, credential
    )
    database_name = kql_database_name or resolved_name
    console.info(f"Applying KQL to '{database_name}' @ {query_uri}")
    response = execute_database_script(query_uri, database_name, script, credential)

    frame = _result_to_frame(response)
    _summarize_result(frame)
    return len(frame)


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
    parser.add_argument("--workspace-id", help="Fabric workspace id (overrides --environment).")
    parser.add_argument(
        "--kql-database-id", help="Fabric KQL database id (overrides --environment)."
    )
    parser.add_argument(
        "--kql-database-name", help="KQL database name (overrides resolved display name)."
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
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
