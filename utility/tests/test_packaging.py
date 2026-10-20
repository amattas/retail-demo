"""Wheel packaging and clean-install smoke tests."""

from __future__ import annotations

import email
import os
import subprocess
import sys
import tomllib
import venv
import zipfile
from pathlib import Path

import pytest
from packaging.requirements import Requirement

UTILITY = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def built_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    wheel_dir = tmp_path_factory.mktemp("wheel")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--wheel-dir",
            str(wheel_dir),
            ".",
        ],
        cwd=UTILITY,
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(wheel_dir.glob("retail_setup-*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def test_wheel_contains_runtime_assets(built_wheel: Path):
    with zipfile.ZipFile(built_wheel) as archive:
        names = set(archive.namelist())

    expected_notebooks = {
        f"retail_setup/resources/notebooks/{path.name}"
        for path in (UTILITY / "notebooks").glob("*.ipynb")
    }
    expected_dictionaries = {
        "retail_setup/resources/dictionaries/"
        + path.relative_to(UTILITY / "data" / "dictionaries").as_posix()
        for path in (UTILITY / "data" / "dictionaries").rglob("*.json")
    }
    assert expected_notebooks <= names
    assert expected_dictionaries <= names


def test_clean_wheel_install_loads_dictionaries_and_renders(
    built_wheel: Path, tmp_path: Path
):
    environment = tmp_path / "venv"
    venv.EnvBuilder(with_pip=True, system_site_packages=True).create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    subprocess.run(
        [str(python), "-m", "pip", "install", "--no-deps", str(built_wheel)],
        check=True,
        capture_output=True,
        text=True,
    )

    smoke_dir = tmp_path / "smoke"
    smoke_dir.mkdir()
    script = """
import json
from pathlib import Path

import retail_setup
from retail_setup.dictionaries.loader import (
    available_store_types,
    default_dictionary_root,
    load_dictionaries,
)
from retail_setup.notebooks.inject import NOTEBOOKS, render_notebooks

package_path = Path(retail_setup.__file__).resolve()
assert "site-packages" in package_path.parts, package_path

root = default_dictionary_root()
assert "grocery" in available_store_types(root)
assert load_dictionaries(root, "grocery").profile.store_type == "grocery"

values = {
    "LAKEHOUSE_NAME": "demo_lh",
    "SILVER_DB": "ag",
    "GOLD_DB": "au",
    "STORE_TYPE": "grocery",
    "START_DATE": "2025-01-01",
    "END_DATE": "2025-02-28",
    "STORE_COUNT": "3",
    "SEED": "7",
    "DICTIONARY_REF": "deadbeef",
}
output = Path.cwd() / "rendered"
written = render_notebooks(
    values,
    output,
    notebook_dir=Path.cwd() / "utility" / "notebooks",
)
assert sorted(path.stem for path in written) == sorted(NOTEBOOKS)
for path in written:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    assert notebook["nbformat"] == 4
    assert "{{" not in path.read_text(encoding="utf-8")
"""
    subprocess.run(
        [str(python), "-I", "-c", script],
        cwd=smoke_dir,
        check=True,
        capture_output=True,
        text=True,
    )


def test_spark_extra_metadata_declares_numpy(built_wheel: Path):
    with zipfile.ZipFile(built_wheel) as archive:
        metadata_name = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = email.message_from_bytes(archive.read(metadata_name))

    spark_requirements = {
        requirement.name: requirement.specifier
        for value in metadata.get_all("Requires-Dist", [])
        if (requirement := Requirement(value)).marker is not None
        and requirement.marker.evaluate({"extra": "spark"})
    }
    assert str(spark_requirements["numpy"]) == ">=1.26"
    assert str(spark_requirements["pyspark"]) == "<4.3,>=3.5.2"


def test_hatchling_build_requirement_is_pinned():
    pyproject = tomllib.loads((UTILITY / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["build-system"]["requires"] == ["hatchling==1.27.0"]
