import json
from pathlib import Path

import pytest

from retail_setup.notebooks.inject import (
    NOTEBOOKS, REQUIRED_TOKENS, render_notebooks,
)

UTILITY = Path(__file__).resolve().parents[1]

VALUES = {
    "LAKEHOUSE_NAME": "demo_lh", "SILVER_DB": "ag", "GOLD_DB": "au",
    "STORE_TYPE": "hardware", "START_DATE": "2025-01-01",
    "END_DATE": "2025-02-28", "STORE_COUNT": "12", "SEED": "7",
    "DICTIONARY_REF": "abc123",
}


def test_render_produces_token_free_copies(tmp_path):
    written = render_notebooks(VALUES, output_dir=tmp_path)
    assert sorted(p.name for p in written) == sorted(f"{n}.ipynb" for n in NOTEBOOKS)
    for p in written:
        src = p.read_text()
        assert "{{" not in src, p.name
        nb = json.loads(src)  # still valid notebooks
        assert nb["nbformat"] == 4
    s3 = (tmp_path / "setup-03-generate-facts.ipynb").read_text()
    assert '"demo_lh"' in s3 and '"hardware"' in s3


def test_render_refuses_missing_values(tmp_path):
    bad = dict(VALUES)
    del bad["STORE_TYPE"]
    with pytest.raises(ValueError, match="STORE_TYPE"):
        render_notebooks(bad, output_dir=tmp_path)
    assert not list(tmp_path.glob("*.ipynb"))  # no partial renders


def test_render_refuses_unknown_keys(tmp_path):
    with pytest.raises(ValueError, match="WORKSPACE_NAME"):
        render_notebooks({**VALUES, "WORKSPACE_NAME": "x"}, output_dir=tmp_path)


def test_required_tokens_match_committed_notebooks():
    found = set()
    for name in NOTEBOOKS:
        src = (UTILITY / "notebooks" / f"{name}.ipynb").read_text()
        import re
        found |= set(re.findall(r"\{\{(\w+)\}\}", src))
    assert found == set(REQUIRED_TOKENS)


def test_originals_untouched(tmp_path):
    before = {n: (UTILITY / "notebooks" / f"{n}.ipynb").read_bytes() for n in NOTEBOOKS}
    render_notebooks(VALUES, output_dir=tmp_path)
    for n, b in before.items():
        assert (UTILITY / "notebooks" / f"{n}.ipynb").read_bytes() == b
