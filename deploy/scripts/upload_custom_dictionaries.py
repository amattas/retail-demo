"""Upload a custom business layout's dictionaries to the Lakehouse for setup-01.

Custom layouts are generated locally (``retail-setup customize``) and never
committed, so setup-01 cannot fetch them from GitHub. This step writes the
custom store type's ``profile.json``, ``products.json``, and ``brands.json`` to
``Files/setup/custom-dictionaries/<store_type>/`` through the OneLake DFS API and
reads every file back to confirm its SHA-256. Setup-01 then verifies the same
digests (rendered into the notebook) before publishing the files.

No-op (exit 0) when the environment uses a preset store type.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from retail_setup.customization.store import (
    active_dictionary_root,
    active_spec,
    custom_file_digests,
)

from deploy.scripts import _output as console
from deploy.scripts._auth import build_credential

ROOT = Path(__file__).resolve().parents[2]
ONELAKE = "https://onelake.dfs.fabric.microsoft.com"
STORAGE_SCOPE = "https://storage.azure.com/.default"
INBOX = "Files/setup/custom-dictionaries"


class UploadError(RuntimeError):
    """A custom dictionary could not be uploaded or verified."""


def _file_url(workspace_id: str, lakehouse_id: str, relative_path: str) -> str:
    return f"{ONELAKE}/{workspace_id}/{lakehouse_id}/{INBOX}/{relative_path}"


def _check(response: requests.Response, action: str, relative_path: str) -> None:
    if response.status_code not in (200, 201, 202):
        raise UploadError(f"{action} {relative_path} failed: HTTP {response.status_code}")


def upload_file(
    session: requests.Session,
    workspace_id: str,
    lakehouse_id: str,
    relative_path: str,
    data: bytes,
) -> str:
    """Create/overwrite one OneLake file, then read it back; return its SHA-256."""

    url = _file_url(workspace_id, lakehouse_id, relative_path)
    _check(session.put(url, params={"resource": "file"}, timeout=60), "create", relative_path)
    _check(
        session.patch(
            url,
            params={"action": "append", "position": "0"},
            data=data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=120,
        ),
        "append",
        relative_path,
    )
    _check(
        session.patch(url, params={"action": "flush", "position": str(len(data))}, timeout=60),
        "flush",
        relative_path,
    )
    readback = session.get(url, timeout=120)
    _check(readback, "read back", relative_path)
    return hashlib.sha256(readback.content).hexdigest()


def upload_custom_dictionaries(
    repo_root: Path,
    environment: str,
    outputs: dict[str, Any],
    session_factory: Callable[[], requests.Session],
) -> dict[str, Any]:
    spec = active_spec(repo_root, environment)
    report: dict[str, Any] = {
        "schema_version": 1,
        "environment": environment,
        "observed_at": datetime.now(UTC).isoformat(),
    }
    if spec is None:
        return report | {"status": "SKIPPED", "reason": "preset store type"}
    root = active_dictionary_root(repo_root, environment)
    assert root is not None
    digests = custom_file_digests(root, spec.store_type)
    workspace_id, lakehouse_id = str(outputs["workspace_id"]), str(outputs["lakehouse_id"])
    session = session_factory()
    files = []
    for relative_path, expected in digests.items():
        actual = upload_file(
            session, workspace_id, lakehouse_id, relative_path, (root / relative_path).read_bytes()
        )
        if actual != expected:
            raise UploadError(f"{relative_path} read back with a different SHA-256")
        files.append({"path": f"{INBOX}/{relative_path}", "sha256": actual})
    return report | {
        "status": "SUCCEEDED",
        "store_type": spec.store_type,
        "business_name": spec.business_name,
        "spec_sha256": spec.content_hash(),
        "files": files,
    }


def _storage_session(auth_mode: str, tenant_id: str | None) -> requests.Session:
    credential = build_credential(auth_mode, tenant_id=tenant_id)
    token = credential.get_token(STORAGE_SCOPE).token
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "x-ms-version": "2023-11-03"})
    return session


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--environment", required=True)
    args = parser.parse_args(argv)

    from deploy.scripts.deploy_config import (
        load_environment,
        load_terraform_outputs,
        validate_terraform_outputs,
    )

    generated = args.repo_root / "deploy" / ".generated" / args.environment
    report_path = generated / "custom-dictionaries-report.json"
    try:
        config = load_environment(
            args.environment,
            config_path=args.repo_root / "deploy" / "config" / "deploy.yml",
            environments_root=args.repo_root / "deploy" / "config" / "environments",
        )
        outputs = load_terraform_outputs(generated / "terraform-output.json")
        validate_terraform_outputs(config, outputs)
        report = upload_custom_dictionaries(
            args.repo_root,
            args.environment,
            outputs,
            lambda: _storage_session(config.auth_mode, config.tenant_id),
        )
    except (OSError, ValueError, requests.RequestException, UploadError) as exc:
        console.error(str(exc))
        report = {
            "schema_version": 1,
            "environment": args.environment,
            "observed_at": datetime.now(UTC).isoformat(),
            "status": "FAILED",
            "error": str(exc),
        }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if report["status"] == "FAILED":
        return 1
    if report["status"] == "SKIPPED":
        console.info("Preset store type; no custom dictionaries to upload.")
    else:
        console.info(f"Uploaded {len(report['files'])} custom dictionary files for {report['store_type']}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
