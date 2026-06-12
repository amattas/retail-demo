"""Shared pytest configuration for the utility test suite."""

import sys
from pathlib import Path

# Make utility/scripts importable so catalog_builder and catalogs/* can be imported directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
