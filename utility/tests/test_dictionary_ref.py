"""Tests for dictionary-ref pinning used by the setup notebooks.

setup-01 fetches dictionaries from raw.githubusercontent at ``DICTIONARY_REF``,
so an unpushed HEAD (common when deploy auto-renders after local commits) must
not be pinned verbatim or the fetch 404s in Fabric.
"""

from __future__ import annotations

from pathlib import Path

import retail_setup.cli.main as cli


def _fake_git(mapping: dict[tuple[str, ...], str | None]):
    def _git(_repo_root: Path, args: list[str]) -> str | None:
        return mapping.get(tuple(args))

    return _git


def test_explicit_ref_is_returned_unchanged():
    assert cli._resolve_dictionary_ref(Path("."), "v1.2.3") == "v1.2.3"


def test_pushed_head_pins_to_head_sha(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_git_text",
        _fake_git(
            {
                ("rev-parse", "HEAD"): "abc1234def",
                ("branch", "-r", "--contains", "abc1234def"): "origin/main",
            }
        ),
    )
    assert cli._resolve_dictionary_ref(Path("."), None) == "abc1234def"


def test_unpushed_head_falls_back_to_upstream_with_warning(monkeypatch, capsys):
    monkeypatch.setattr(
        cli,
        "_git_text",
        _fake_git(
            {
                ("rev-parse", "HEAD"): "deadbeefcafe",
                ("branch", "-r", "--contains", "deadbeefcafe"): None,
                ("rev-parse", "@{u}"): "0badf00d1234",
            }
        ),
    )
    assert cli._resolve_dictionary_ref(Path("."), None) == "0badf00d1234"
    err = capsys.readouterr().err
    assert "not pushed" in err
    assert "deadbee" in err  # short HEAD sha surfaced


def test_unpushed_head_without_upstream_uses_main(monkeypatch, capsys):
    monkeypatch.setattr(
        cli,
        "_git_text",
        _fake_git(
            {
                ("rev-parse", "HEAD"): "deadbeefcafe",
                ("branch", "-r", "--contains", "deadbeefcafe"): None,
                ("rev-parse", "@{u}"): None,
            }
        ),
    )
    assert cli._resolve_dictionary_ref(Path("."), None) == "main"
    assert "main" in capsys.readouterr().err


def test_missing_git_head_uses_main(monkeypatch, capsys):
    monkeypatch.setattr(cli, "_git_text", _fake_git({}))
    assert cli._resolve_dictionary_ref(Path("."), None) == "main"
    assert "could not resolve git HEAD" in capsys.readouterr().err
