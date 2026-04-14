#!/usr/bin/env python3
"""Deploy updated notebooks to Fabric and drop old gold_-prefixed tables.

Authenticates via interactive browser login so you can use different credentials
than your default Azure CLI session.

Usage:
    # Deploy notebooks + drop old tables (will prompt for browser login)
    python scripts/deploy_notebooks.py

    # Deploy notebooks only (skip table cleanup)
    python scripts/deploy_notebooks.py --skip-table-cleanup

    # Drop old tables only (skip notebook upload)
    python scripts/deploy_notebooks.py --skip-notebooks

    # Dry run
    python scripts/deploy_notebooks.py --dry-run

    # Override workspace/lakehouse (defaults read from expressions.tmdl)
    python scripts/deploy_notebooks.py \
        --workspace-id <guid> --lakehouse-id <guid>

Requirements:
    pip install azure-identity requests
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
SQL_SCOPE = "https://database.windows.net/.default"

# Notebooks to deploy (repo path relative to REPO_ROOT)
NOTEBOOK_DIR = REPO_ROOT / "fabric" / "lakehouse"
NOTEBOOK_FILES = sorted(NOTEBOOK_DIR.glob("*.ipynb"))

# Old gold_-prefixed tables to drop from the au schema
OLD_TABLES = [
    "gold_churn_predictions",
    "gold_customer_segments",
    "gold_demand_forecast",
    "gold_dwell_predictions",
    "gold_journey_patterns",
    "gold_price_elasticity",
    "gold_pricing_recommendations",
    "gold_promotion_lift",
    "gold_product_associations",
    "gold_stockout_risk",
    "gold_zone_dwell_stats",
    "gold_zone_transitions",
]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_credential():
    """Get an interactive browser credential for a separate login."""
    try:
        from azure.identity import InteractiveBrowserCredential
    except ImportError:
        print("ERROR: azure-identity not installed. Run: pip install azure-identity")
        sys.exit(1)
    return InteractiveBrowserCredential(additionally_allowed_tenants=["*"])


def get_token(credential, scope: str) -> str:
    token = credential.get_token(scope)
    return token.token


def api_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Workspace / lakehouse discovery
# ---------------------------------------------------------------------------

def read_ids_from_tmdl() -> tuple[str, str]:
    """Extract workspace and lakehouse GUIDs from expressions.tmdl."""
    expr_path = (
        REPO_ROOT / "fabric" / "semantic_model"
        / "retail_model.SemanticModel" / "definition" / "expressions.tmdl"
    )
    content = expr_path.read_text(encoding="utf-8")
    match = re.search(
        r"onelake\.dfs\.fabric\.microsoft\.com/"
        r"([0-9a-f-]{36})/([0-9a-f-]{36})",
        content,
    )
    if not match:
        raise ValueError("Could not extract workspace/lakehouse IDs from expressions.tmdl")
    return match.group(1), match.group(2)


# ---------------------------------------------------------------------------
# Notebook deployment
# ---------------------------------------------------------------------------

def list_notebooks(token: str, workspace_id: str) -> dict[str, str]:
    """Return {displayName: itemId} for all notebooks in the workspace."""
    import requests

    url = f"{FABRIC_API}/workspaces/{workspace_id}/items?type=Notebook"
    notebooks: dict[str, str] = {}
    while url:
        resp = requests.get(url, headers=api_headers(token))
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("value", []):
            notebooks[item["displayName"]] = item["id"]
        url = data.get("continuationUri")
    return notebooks


def build_platform_json(
    notebook_name: str,
    lakehouse_id: str,
    lakehouse_name: str = "retail_lakehouse",
    workspace_id: str | None = None,
) -> str:
    """Build the .platform JSON that attaches a default lakehouse to a notebook."""
    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {
            "type": "Notebook",
            "displayName": notebook_name,
        },
        "config": {
            "version": "2.0",
            "logicalId": "00000000-0000-0000-0000-000000000000",
        },
    }

    # Lakehouse binding
    if lakehouse_id:
        lh_config: dict = {
            "default_lakehouse": lakehouse_id,
            "default_lakehouse_name": lakehouse_name,
            "known_lakehouses": [{"id": lakehouse_id}],
        }
        if workspace_id:
            lh_config["default_lakehouse_workspace_id"] = workspace_id
        platform["config"]["lakehouse"] = lh_config

    return json.dumps(platform, indent=2)


def update_notebook(
    token: str,
    workspace_id: str,
    item_id: str,
    notebook_path: Path,
    lakehouse_id: str | None = None,
) -> None:
    """Update an existing notebook definition via the Fabric Items API.

    Uploads the .ipynb file directly using the ipynb format, plus a .platform
    file that binds the notebook to the default lakehouse.
    """
    import requests

    # Notebook content — send .ipynb as-is
    content_b64 = base64.b64encode(notebook_path.read_bytes()).decode("ascii")

    # .platform metadata with lakehouse binding
    platform_json = build_platform_json(
        notebook_name=notebook_path.stem,
        lakehouse_id=lakehouse_id or "",
        workspace_id=workspace_id,
    )
    platform_b64 = base64.b64encode(platform_json.encode("utf-8")).decode("ascii")

    body = {
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "artifact.content.ipynb",
                    "payload": content_b64,
                    "payloadType": "InlineBase64",
                },
                {
                    "path": ".platform",
                    "payload": platform_b64,
                    "payloadType": "InlineBase64",
                },
            ]
        }
    }

    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/updateDefinition?updateMetadata=true"
    resp = requests.post(url, headers=api_headers(token), json=body)

    if resp.status_code == 202:
        # Long-running operation — poll until complete
        op_url = resp.headers.get("Location") or resp.headers.get("Operation-Location")
        if op_url:
            poll_operation(token, op_url)
    elif resp.status_code in (200, 204):
        pass  # immediate success
    else:
        raise RuntimeError(f"status {resp.status_code}: {resp.text[:300]}")


def poll_operation(token: str, url: str, timeout: int = 120) -> None:
    """Poll a long-running Fabric operation until it completes."""
    import requests

    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(3)
        resp = requests.get(url, headers=api_headers(token))
        if resp.status_code == 200:
            data = resp.json()
            status = data.get("status", "Unknown")
            if status in ("Succeeded", "Completed"):
                return
            if status in ("Failed", "Cancelled"):
                raise RuntimeError(f"Operation {status}: {data.get('error', data)}")
        elif resp.status_code == 202:
            continue
    print("  WARNING: operation timed out")


def deploy_notebooks(
    token: str,
    workspace_id: str,
    lakehouse_id: str,
    dry_run: bool = False,
) -> None:
    """Upload all local notebooks to Fabric workspace."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Deploying notebooks...")
    remote = list_notebooks(token, workspace_id)
    print(f"  Found {len(remote)} notebooks in workspace")

    for nb_path in NOTEBOOK_FILES:
        name = nb_path.stem  # e.g. "06-ml-demand-forecast"
        if name not in remote:
            print(f"  SKIP {name} (not found in workspace)")
            continue

        print(f"  {'Would update' if dry_run else 'Updating'}: {name}")
        if not dry_run:
            try:
                update_notebook(token, workspace_id, remote[name], nb_path, lakehouse_id)
                print(f"    ✓ {name}")
            except Exception as e:
                print(f"    ✗ {name}: {e}")


# ---------------------------------------------------------------------------
# Table cleanup
# ---------------------------------------------------------------------------

def get_sql_connection_string(token: str, workspace_id: str, lakehouse_id: str) -> str:
    """Get the SQL analytics endpoint connection string for the lakehouse."""
    import requests

    url = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
    resp = requests.get(url, headers=api_headers(token))
    resp.raise_for_status()
    data = resp.json()
    props = data.get("properties", {}).get("sqlEndpointProperties", {})
    conn_str = props.get("connectionString", "")
    return conn_str


def drop_tables_pyodbc(sql_endpoint: str, sql_token: str, dry_run: bool) -> bool:
    """Drop old gold_ tables via pyodbc. Returns True if successful."""
    try:
        import pyodbc
    except ImportError:
        return False

    # Find the best available SQL Server ODBC driver
    drivers = pyodbc.drivers()
    odbc_driver = None
    for candidate in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]:
        if candidate in drivers:
            odbc_driver = candidate
            break
    if not odbc_driver:
        print(f"  No SQL Server ODBC driver found. Available: {drivers}")
        return False

    conn_str = (
        f"Driver={{{odbc_driver}}};"
        f"Server={sql_endpoint},1433;"
        f"Database=retail_lakehouse;"
        f"Encrypt=Yes;"
        f"TrustServerCertificate=No"
    )

    try:
        conn = pyodbc.connect(conn_str, attrs_before={
            # SQL_COPT_SS_ACCESS_TOKEN (1256) with the token bytes
            1256: _prepare_token(sql_token)
        })
    except Exception as e:
        print(f"  pyodbc connection failed: {e}")
        return False

    cursor = conn.cursor()
    for table in OLD_TABLES:
        stmt = f"DROP TABLE IF EXISTS [au].[{table}]"
        print(f"  {'Would run' if dry_run else 'Running'}: {stmt}")
        if not dry_run:
            try:
                cursor.execute(stmt)
                print(f"    ✓ dropped {table}")
            except Exception as e:
                print(f"    ✗ {table}: {e}")
    conn.commit()
    conn.close()
    return True


def _prepare_token(token: str) -> bytes:
    """Encode a token for pyodbc SQL_COPT_SS_ACCESS_TOKEN."""
    import struct
    token_bytes = token.encode("utf-16-le")
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


def drop_tables_fallback(dry_run: bool) -> None:
    """Print SQL statements for manual execution."""
    print("\n  pyodbc not available. Run these in your Lakehouse SQL endpoint:")
    print("  " + "-" * 60)
    for table in OLD_TABLES:
        print(f"  DROP TABLE IF EXISTS [au].[{table}];")
    print("  " + "-" * 60)
    print("  Or run this PySpark in a Fabric notebook:")
    print("  " + "-" * 60)
    for table in OLD_TABLES:
        print(f'  spark.sql("DROP TABLE IF EXISTS au.{table}")')
    print("  " + "-" * 60)


def cleanup_old_tables(
    credential,
    fabric_token: str,
    workspace_id: str,
    lakehouse_id: str,
    dry_run: bool = False,
) -> None:
    """Drop old gold_-prefixed tables from the lakehouse."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Cleaning up old gold_ tables...")

    # Get SQL endpoint
    sql_endpoint = get_sql_connection_string(fabric_token, workspace_id, lakehouse_id)
    if not sql_endpoint:
        print("  Could not discover SQL endpoint")
        drop_tables_fallback(dry_run)
        return

    print(f"  SQL endpoint: {sql_endpoint}")

    # Get a token for the SQL endpoint
    try:
        sql_token = get_token(credential, SQL_SCOPE)
    except Exception as e:
        print(f"  Could not get SQL token: {e}")
        drop_tables_fallback(dry_run)
        return

    # Try pyodbc first
    if not drop_tables_pyodbc(sql_endpoint, sql_token, dry_run):
        drop_tables_fallback(dry_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deploy notebooks to Fabric and drop old gold_ tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--workspace-id",
        help="Fabric workspace GUID (default: read from expressions.tmdl)",
    )
    parser.add_argument(
        "--lakehouse-id",
        help="Lakehouse GUID (default: read from expressions.tmdl)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without changes")
    parser.add_argument("--skip-notebooks", action="store_true", help="Skip notebook upload")
    parser.add_argument("--skip-table-cleanup", action="store_true", help="Skip table drops")
    args = parser.parse_args()

    # Resolve IDs
    if args.workspace_id and args.lakehouse_id:
        workspace_id = args.workspace_id
        lakehouse_id = args.lakehouse_id
    else:
        workspace_id, lakehouse_id = read_ids_from_tmdl()
    print(f"Workspace: {workspace_id}")
    print(f"Lakehouse: {lakehouse_id}")

    # Authenticate (opens browser for interactive login)
    print("\nOpening browser for authentication...")
    credential = get_credential()
    fabric_token = get_token(credential, FABRIC_SCOPE)
    print("✓ Authenticated")

    # Deploy notebooks
    if not args.skip_notebooks:
        deploy_notebooks(fabric_token, workspace_id, lakehouse_id, args.dry_run)

    # Drop old tables
    if not args.skip_table_cleanup:
        cleanup_old_tables(credential, fabric_token, workspace_id, lakehouse_id, args.dry_run)

    print("\nDone!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
