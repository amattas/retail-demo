"""Environment-gated live readiness smoke scaffold."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from deploy.scripts.verify_readiness import verify_environment


@pytest.mark.skipif(
    not os.getenv("RETAIL_DEMO_LIVE_ENV"),
    reason="set RETAIL_DEMO_LIVE_ENV to run live Fabric readiness verification",
)
def test_live_readiness_prerequisites_and_report() -> None:
    """Run only when the operator explicitly supplies a configured live environment."""

    environment = os.environ["RETAIL_DEMO_LIVE_ENV"]
    repo_root = Path(__file__).resolve().parents[2]
    missing = [
        str(path)
        for path in (
            repo_root / "deploy" / "config" / "environments" / f"{environment}.yml",
            repo_root
            / "deploy"
            / ".generated"
            / environment
            / "terraform-output.json",
        )
        if not path.is_file()
    ]
    if missing:
        pytest.fail(
            "RETAIL_DEMO_LIVE_ENV is set but live prerequisites are missing: "
            + ", ".join(missing)
        )
    try:
        import pyodbc
    except ImportError:
        pytest.fail(
            "RETAIL_DEMO_LIVE_ENV is set but pyodbc is not installed; "
            "install utility/requirements-deploy.txt",
            pytrace=False,
        )
    supported_drivers = {
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 18 for SQL Server",
    }
    if not supported_drivers.intersection(pyodbc.drivers()):
        pytest.fail(
            "RETAIL_DEMO_LIVE_ENV is set but Microsoft ODBC Driver 17 or 18 "
            "for SQL Server is not installed",
            pytrace=False,
        )

    report, path = verify_environment(repo_root, environment)

    assert path.is_file()
    assert report["status"] in {"SUCCEEDED", "DEGRADED", "FAILED"}
    assert report["counts"]["unknown"] == 0, (
        "live scaffold reached Fabric but evidence remains UNKNOWN"
    )
