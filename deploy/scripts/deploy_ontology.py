"""Publish the retail ontology in the new (TMDL) Fabric ontology experience.

The old entity-type JSON experience retires on Jan 31, 2027. This publisher
renders the definition from ``_ontology_model`` and:

* creates ``RetailOntology_AutoGen`` when it is absent;
* updates it in place when it already uses the TMDL format;
* deletes and recreates it when it still uses the old format, keeping the
  display name so name-based consumers (Data Agent rebinding, task flow,
  readiness) resolve the new item.

``--render`` writes the parts locally without calling any service.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from deploy.scripts import _output as console
from deploy.scripts._native_items import (
    API,
    NativeItemError,
    _guid,
    complete_operation,
    get_definition,
)
from deploy.scripts._ontology_model import (
    DESCRIPTION,
    EVENT_BINDINGS,
    ONTOLOGY_NAME,
    build_definition,
    definition_payload,
)
from deploy.scripts.fabric_runtime import decode_definition_parts, paginated_get

if TYPE_CHECKING:
    import requests

ROOT = Path(__file__).resolve().parents[2]
ONELAKE = "https://onelake.dfs.fabric.microsoft.com"
STORAGE_SCOPE = "https://storage.azure.com/.default"
BRONZE_SCHEMA = "cusn"
# The current V4 service rejects multiple anonymous additionalBackingTable
# declarations. Keep discovery evidence, but publish only supported core tables.
_ENABLE_EVENT_BACKING_TABLES = False


def is_old_format(parts: dict[str, bytes]) -> bool:
    """True when a definition uses the retiring entity-type JSON experience."""
    if "definition.json" in parts or any(path.startswith("EntityTypes/") for path in parts):
        return True
    platform = parts.get(".platform")
    if platform is None:
        return not any(path.endswith(".tmdl") for path in parts)
    try:
        version = json.loads(platform).get("config", {}).get("version")
    except ValueError:
        return True
    return str(version) != "2.0"


def sql_endpoint(session: requests.Session, workspace_id: str, lakehouse_id: str) -> tuple[str, str]:
    response = session.get(
        f"{API}/workspaces/{_guid(workspace_id)}/lakehouses/{_guid(lakehouse_id)}",
        timeout=30, allow_redirects=False,
    )
    if response.status_code != 200:
        raise NativeItemError(f"Read Lakehouse failed: HTTP {response.status_code}")
    properties = (response.json().get("properties") or {}).get("sqlEndpointProperties") or {}
    host, database = properties.get("connectionString"), properties.get("id")
    if not host or not database or properties.get("provisioningStatus", "Success") != "Success":
        raise NativeItemError("The Lakehouse SQL analytics endpoint is not provisioned.")
    return str(host), _guid(str(database))


def bronze_tables(
    workspace_id: str,
    lakehouse_id: str,
    storage_token: str,
    get: Callable[..., Any] | None = None,
) -> set[str]:
    """List the ``Tables/cusn`` shortcut directories that exist in OneLake."""
    if get is None:
        import requests

        get = requests.get
    directory = f"{_guid(lakehouse_id)}/Tables/{BRONZE_SCHEMA}"
    response = get(
        f"{ONELAKE}/{_guid(workspace_id)}",
        params={"resource": "filesystem", "directory": directory, "recursive": "false"},
        headers={"Authorization": f"Bearer {storage_token}"},
        timeout=30,
    )
    if response.status_code == 404:
        return set()
    if response.status_code != 200:
        raise NativeItemError(f"List Bronze shortcuts failed: HTTP {response.status_code}")
    names = set()
    for path in response.json().get("paths", []):
        name = str(path.get("name", "")).rstrip("/").rsplit("/", 1)[-1]
        if str(path.get("isDirectory", "")).lower() == "true" and name:
            names.add(name)
    return names


def _ontologies(session: requests.Session, workspace_id: str) -> list[dict[str, Any]]:
    return paginated_get(session, f"{API}/workspaces/{_guid(workspace_id)}/ontologies", timeout=30)


def _matching(session: requests.Session, workspace_id: str, name: str) -> list[dict[str, Any]]:
    return [
        item for item in _ontologies(session, workspace_id)
        if str(item.get("displayName", "")).casefold() == name.casefold()
    ]


def _create(
    session: requests.Session,
    workspace_id: str,
    name: str,
    definition: dict[str, Any],
    *,
    attempts: int = 10,
    wait_seconds: float = 30,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    for attempt in range(attempts):
        response = session.post(
            f"{API}/workspaces/{_guid(workspace_id)}/ontologies",
            json={"displayName": name, "description": DESCRIPTION, "definition": definition},
            timeout=60, allow_redirects=False,
        )
        name_reserved = response.status_code in (400, 409) and "NotAvailable" in response.text
        if name_reserved and attempt < attempts - 1:
            sleep(wait_seconds)
            continue
        complete_operation(session, response, timeout_seconds=1800,
                           operation_name="Create ontology")
        return


def _delete(session: requests.Session, workspace_id: str, item_id: str) -> None:
    response = session.delete(
        f"{API}/workspaces/{_guid(workspace_id)}/ontologies/{_guid(item_id)}",
        timeout=60, allow_redirects=False,
    )
    complete_operation(session, response, operation_name="Delete ontology")


def _rename(
    session: requests.Session,
    workspace_id: str,
    item_id: str,
    display_name: str,
    *,
    attempts: int = 10,
    wait_seconds: float = 30,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Rename after delete; Fabric reserves a deleted display name briefly."""
    for attempt in range(attempts):
        response = session.patch(
            f"{API}/workspaces/{_guid(workspace_id)}/ontologies/{_guid(item_id)}",
            json={"displayName": display_name}, timeout=60, allow_redirects=False,
        )
        name_reserved = response.status_code in (400, 409) and "NotAvailable" in response.text
        if name_reserved and attempt < attempts - 1:
            sleep(wait_seconds)
            continue
        complete_operation(session, response, operation_name="Rename ontology")
        return


def _wait_for_single(
    session: requests.Session,
    workspace_id: str,
    display_name: str,
    sleep: Callable[[float], None],
) -> str:
    for _ in range(10):
        matches = _matching(session, workspace_id, display_name)
        if len(matches) == 1:
            return _guid(matches[0]["id"])
        if len(matches) > 1:
            raise NativeItemError(f"Ontology {display_name!r} has ambiguous identity.")
        sleep(5)
    raise NativeItemError(f"Ontology {display_name!r} is not visible after publication.")


def _replace(
    session: requests.Session,
    *,
    workspace_id: str,
    item_id: str,
    display_name: str,
    definition: dict[str, Any],
    expected_parts: dict[str, str],
    sleep: Callable[[float], None],
) -> str:
    """Publish and verify a staging item before replacing an existing ontology."""

    staging = f"{display_name}_tmdl"[:256]
    for item in _matching(session, workspace_id, staging):
        _delete(session, workspace_id, _guid(item["id"]))
    _create(session, workspace_id, staging, definition, sleep=sleep)
    staged_id = _wait_for_single(session, workspace_id, staging, sleep)
    _verify(
        expected_parts,
        decode_definition_parts(
            get_definition(session, workspace_id, "Ontology", staged_id)
        ),
    )
    _delete(session, workspace_id, item_id)
    _rename(session, workspace_id, staged_id, display_name, sleep=sleep)
    return staged_id


def _verify(expected: dict[str, str], actual: dict[str, bytes]) -> None:
    """The service elides defaults, so compare element sets, not exact text."""
    wanted = {path for path in expected if path.startswith(("entities/", "rules/", "tables/"))}
    observed = {path for path in actual if path.startswith(("entities/", "rules/", "tables/"))}
    missing = sorted(wanted - observed)
    if missing or is_old_format(actual):
        raise NativeItemError(
            "Ontology readback is incomplete or not in the TMDL format: "
            + ", ".join(missing[:10])
        )


def publish_ontology(
    session: requests.Session,
    *,
    workspace_id: str,
    lakehouse_id: str,
    storage_token: str,
    display_name: str = ONTOLOGY_NAME,
    list_bronze: Callable[[str, str, str], Iterable[str]] = bronze_tables,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    host, database = sql_endpoint(session, workspace_id, lakehouse_id)
    discovered_bronze = set(
        list_bronze(workspace_id, lakehouse_id, storage_token)
    )
    available = discovered_bronze if _ENABLE_EVENT_BACKING_TABLES else set()
    parts = build_definition(
        sql_endpoint=host, sql_database_id=database,
        available_bronze_tables=available, display_name=display_name,
    )
    definition = definition_payload(parts)
    matches = _matching(session, workspace_id, display_name)
    if len(matches) > 1:
        raise NativeItemError(f"Multiple ontologies are named {display_name!r}.")
    action = "created"
    if matches:
        item_id = _guid(matches[0]["id"])
        current = decode_definition_parts(
            get_definition(session, workspace_id, "Ontology", item_id)
        )
        if is_old_format(current):
            action = "migrated"
            _replace(
                session,
                workspace_id=workspace_id,
                item_id=item_id,
                display_name=display_name,
                definition=definition,
                expected_parts=parts,
                sleep=sleep,
            )
        else:
            action = "updated"
            response = session.post(
                f"{API}/workspaces/{_guid(workspace_id)}/ontologies/{item_id}"
                "/updateDefinition?updateMetadata=true",
                json={"definition": definition}, timeout=60, allow_redirects=False,
            )
            merge_failure = (
                response.status_code == 400
                and "TMDL objects cannot be merged" in response.text
            )
            if merge_failure:
                action = "recreated"
                _replace(
                    session,
                    workspace_id=workspace_id,
                    item_id=item_id,
                    display_name=display_name,
                    definition=definition,
                    expected_parts=parts,
                    sleep=sleep,
                )
            else:
                complete_operation(
                    session,
                    response,
                    timeout_seconds=1800,
                    operation_name="Update ontology",
                )
    else:
        _create(session, workspace_id, display_name, definition, sleep=sleep)
    item_id = _wait_for_single(session, workspace_id, display_name, sleep)
    _verify(parts, decode_definition_parts(get_definition(session, workspace_id, "Ontology", item_id)))
    return {
        "action": action,
        "ontology_id": item_id,
        "bronze_tables": sorted(available),
        "discovered_bronze_tables": sorted(discovered_bronze),
        "skipped_bronze_tables": sorted({b.table for b in EVENT_BINDINGS} - available),
        "entities": sorted(p.split("/", 1)[1][:-5] for p in parts if p.startswith("entities/")),
        "rules": sorted(p.split("/", 1)[1][:-5] for p in parts if p.startswith("rules/")),
    }


def render(output_dir: Path) -> list[Path]:
    """Write a placeholder-bound definition for offline review."""
    parts = build_definition(
        sql_endpoint="<lakehouse-sql-endpoint>.datawarehouse.fabric.microsoft.com",
        sql_database_id="00000000-0000-0000-0000-000000000000",
        available_bronze_tables={binding.table for binding in EVENT_BINDINGS},
    )
    written = []
    for path, text in sorted(parts.items()):
        target = output_dir.joinpath(*(quote(segment, safe="") for segment in path.split("/")))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8", newline="\n")
        written.append(target)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--environment")
    parser.add_argument("--render", type=Path, help="Write parts locally; no service calls.")
    args = parser.parse_args(argv)
    if args.render:
        files = render(args.render)
        console.info(f"Rendered {len(files)} ontology definition parts to {args.render}.")
        return 0
    if not args.environment:
        parser.error("--environment is required unless --render is used")

    from requests import RequestException

    from deploy.scripts._auth import build_credential
    from deploy.scripts.deploy_config import (
        load_environment,
        load_terraform_outputs,
        validate_terraform_outputs,
    )
    from deploy.scripts.export_items import build_session

    config = load_environment(
        args.environment,
        config_path=args.repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=args.repo_root / "deploy" / "config" / "environments",
    )
    generated = args.repo_root / "deploy" / ".generated" / args.environment
    outputs = load_terraform_outputs(generated / "terraform-output.json")
    validate_terraform_outputs(config, outputs)
    credential = build_credential(config.auth_mode, tenant_id=config.tenant_id)
    report: dict[str, Any] = {
        "schema_version": 1,
        "environment": args.environment,
        "observed_at": datetime.now(UTC).isoformat(),
    }
    try:
        report |= publish_ontology(
            build_session(credential),
            workspace_id=outputs["workspace_id"],
            lakehouse_id=outputs["lakehouse_id"],
            storage_token=credential.get_token(STORAGE_SCOPE).token,
        )
        report["status"] = "SUCCEEDED"
    except (NativeItemError, RequestException, ValueError) as exc:
        report |= {"status": "FAILED", "failure_type": type(exc).__name__, "error": str(exc)}
        console.error(str(exc))
    (generated / "ontology-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    if report["status"] != "SUCCEEDED":
        return 1
    from deploy.scripts.workspace_folders import organize_after_publish

    if not organize_after_publish(args.repo_root, args.environment):
        return 1
    console.info(f"Ontology {report['action']}: {report['ontology_id']}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
