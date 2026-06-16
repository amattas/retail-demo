"""Tests for the shared deploy console output helpers."""

from __future__ import annotations

from deploy.scripts import _output as console


def test_info_indents_with_dash(capsys) -> None:
    console.info("hello")
    assert capsys.readouterr().out == "    - hello\n"


def test_detail_indents_further(capsys) -> None:
    console.detail("retail_model.Report")
    assert capsys.readouterr().out == "        retail_model.Report\n"


def test_warn_and_error_go_to_stderr(capsys) -> None:
    console.warn("careful")
    console.error("broke")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "! careful" in captured.err
    assert "ERROR: broke" in captured.err
