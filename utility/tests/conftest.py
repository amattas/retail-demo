"""Shared pytest configuration for the utility test suite."""

import sys
from pathlib import Path

# Make utility/scripts importable so catalog_builder and catalogs/* can be imported directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

# Make the repo root importable so the deploy framework (deploy.scripts.*) resolves
# the same way it does from a real repo checkout. The retail-setup CLI shells out to
# and imports deploy.scripts for target-safety validation; without the repo root on
# sys.path those imports fail under the installed-package CI job (which runs from
# utility/), so exercise them here exactly as an operator would.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local Spark for unit tests. Small and quiet; one JVM per test session."""
    import os
    from pathlib import Path
    from pyspark.sql import SparkSession

    # Redirect JVM temp dir to sandbox-writable TMPDIR (macOS sandbox blocks /var/folders).
    # `or` guards CI environments where TMPDIR is set but empty.
    tmpdir = os.environ.get("TMPDIR") or "/tmp"
    java_opts = f"-Djava.io.tmpdir={tmpdir}"

    # Pin worker Python to the driver's interpreter — otherwise local mode can
    # pick up a different system Python and fail with a version mismatch.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    # Prefer the conda-env JDK (17) over any system JDK when available.
    # PySpark 3.5 is incompatible with Java 21+ due to Subject.getSubject removal.
    if "JAVA_HOME" not in os.environ:
        # Derive conda env root from the running Python interpreter path.
        _python = Path(sys.executable).resolve()
        # Typical layout: <env>/bin/python  -> <env>/lib/jvm
        _env_jdk = _python.parents[1] / "lib" / "jvm"
        if _env_jdk.exists():
            os.environ["JAVA_HOME"] = str(_env_jdk)

    # generate_all caches ~25 DataFrames to avoid recomputing the generation DAG;
    # under the default ~1g local heap that starves the driver JVM (CI hit
    # java.lang.OutOfMemoryError on .count()). _JAVA_OPTIONS wins over Spark's
    # default -Xmx, so it reliably raises the local driver heap before launch.
    os.environ.setdefault("_JAVA_OPTIONS", "-Xmx3g")

    session = (
        SparkSession.builder.master("local[2]")
        .appName("retail-setup-tests")
        .config("spark.sql.shuffle.partitions", "4")
        # Disable broadcast joins: the constrained local driver heap cannot build
        # and broadcast some intermediate tables in the generation DAG (CI hit
        # "Not enough memory to build and broadcast the table"). Forcing sort-merge
        # joins yields identical results with a smaller memory footprint.
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", tmpdir)
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
        .getOrCreate()
    )
    yield session
    session.stop()
