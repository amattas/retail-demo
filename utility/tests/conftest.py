"""Shared pytest configuration for the utility test suite."""

import sys
from pathlib import Path

# Make utility/scripts importable so catalog_builder and catalogs/* can be imported directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local Spark for unit tests. Small and quiet; one JVM per test session."""
    import os
    from pyspark.sql import SparkSession

    # Redirect JVM temp dir to sandbox-writable TMPDIR (macOS sandbox blocks /var/folders).
    tmpdir = os.environ.get("TMPDIR", "/tmp")
    java_opts = f"-Djava.io.tmpdir={tmpdir}"

    session = (
        SparkSession.builder.master("local[2]")
        .appName("retail-setup-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", tmpdir)
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
        .getOrCreate()
    )
    yield session
    session.stop()
