"""Tests for preparing and applying ordered KQL database scripts."""

from __future__ import annotations

from pathlib import Path

import pytest

from deploy.scripts import apply_kql


class _FakeRow:
    """Stand-in for ``KustoResultRow`` — only ``to_dict`` is used."""

    def __init__(self, data: dict) -> None:
        self._data = data

    def to_dict(self) -> dict:
        return self._data


class _FakeTable:
    """Stand-in for ``KustoResultTable`` — iterable of rows with a length."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = [_FakeRow(row) for row in rows]

    def __iter__(self):
        return iter(self._rows)

    def __len__(self) -> int:
        return len(self._rows)


def _write_scripts(source_dir: Path) -> None:
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "01-create-tables.kql").write_text(
        ".create-merge table receipts (id:string)", encoding="utf-8"
    )
    (source_dir / "02-create-functions.kql").write_text(
        ".create-or-alter function foo() { receipts | count }", encoding="utf-8"
    )


def test_build_database_script_sets_throw_on_errors(tmp_path: Path) -> None:
    _write_scripts(tmp_path)
    script = apply_kql.build_database_script(apply_kql.collect_kql_scripts(tmp_path))

    # The script must fail loudly on the first command error instead of the
    # default silent success.
    assert ".execute database script with (ThrowOnErrors=true) <|" in script
    assert "create-merge table receipts" in script
    assert "create-or-alter function foo" in script


def test_build_database_script_starts_with_dot_command(tmp_path: Path) -> None:
    # The Kusto management endpoint rejects a script whose first non-whitespace
    # character is not a dot (e.g. a leading // comment).
    _write_scripts(tmp_path)
    script = apply_kql.build_database_script(apply_kql.collect_kql_scripts(tmp_path))
    assert script.lstrip().startswith(".execute database script")


def test_collect_kql_scripts_orders_and_validates(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        apply_kql.collect_kql_scripts(tmp_path / "missing")

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError):
        apply_kql.collect_kql_scripts(empty)


def test_resolve_kql_database_returns_uri_and_name(monkeypatch) -> None:
    requests = pytest.importorskip("requests")

    class _Resp:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict:
            return {
                "displayName": "retail_kql",
                "properties": {"queryServiceUri": "https://cluster.kusto.fabric.microsoft.com"},
            }

    captured: dict[str, object] = {}

    def fake_get(url, headers=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        return _Resp()

    monkeypatch.setattr(requests, "get", fake_get)

    class _Cred:
        def get_token(self, scope):
            captured["scope"] = scope
            return type("T", (), {"token": "tok"})()

    uri, name = apply_kql.resolve_kql_database("ws-1", "db-1", _Cred())

    assert uri == "https://cluster.kusto.fabric.microsoft.com"
    assert name == "retail_kql"
    assert "/workspaces/ws-1/kqlDatabases/db-1" in captured["url"]
    assert captured["headers"]["Authorization"] == "Bearer tok"
    assert captured["scope"] == apply_kql.FABRIC_SCOPE


def test_repo_kql_source_satisfies_kusto_constraints() -> None:
    """Guard the real KQL source against constraints that aborted the deploy.

    - ``ingestionbatching`` requires ``MaximumRawDataSizeMB >= 100``.
    - ``backfill`` is not a valid property when *altering* an existing
      materialized view, so it must not appear in ``.create-or-alter`` (which
      would break idempotent re-deploys).
    """

    import re

    for path in apply_kql.collect_kql_scripts():
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(r'"MaximumRawDataSizeMB":\s*(\d+)', text):
            assert int(match.group(1)) >= 100, f"{path.name}: {match.group(0)} < 100"
        assert "backfill" not in text, (
            f"{path.name}: remove the materialized-view 'backfill' property "
            "(invalid on alter; breaks idempotent re-deploys)"
        )


def test_apply_to_database_runs_resolved_script(monkeypatch) -> None:
    """apply_to_database resolves the db then runs the script (no real Kusto SDK)."""

    calls: dict[str, object] = {}

    monkeypatch.setattr(apply_kql, "_credential", lambda *_a, **_k: object())
    monkeypatch.setattr(
        apply_kql,
        "resolve_kql_database",
        lambda ws, db, cred: ("https://cluster", "retail_kql"),
    )

    table = _FakeTable(
        [
            {
                "CommandType": "TableCreate",
                "CommandText": ".show tables",
                "Result": "Completed",
                "Reason": "",
            }
            for _ in range(3)
        ]
    )

    class _Response:
        primary_results = [table]

    def fake_execute(query_uri, database_name, script, credential):
        calls["query_uri"] = query_uri
        calls["database_name"] = database_name
        calls["script"] = script
        return _Response()

    monkeypatch.setattr(apply_kql, "execute_database_script", fake_execute)

    count = apply_kql.apply_to_database(
        script=".execute database script with (ThrowOnErrors=true) <|\n.show tables",
        workspace_id="ws-1",
        kql_database_id="db-1",
    )

    assert count == 3
    assert calls["query_uri"] == "https://cluster"
    assert calls["database_name"] == "retail_kql"
    assert "ThrowOnErrors=true" in calls["script"]


def test_summarize_result_is_concise_on_success(capsys) -> None:
    """A successful apply collapses to one line; no per-command text is dumped."""

    table = _FakeTable(
        [
            {
                "CommandType": "DatabaseScriptExecute",
                "CommandText": ".execute database script",
                "Result": "Completed",
                "Reason": "",
            },
            {
                "CommandType": "TableCreate",
                "CommandText": ".create-merge table receipt_created (store_id:long)",
                "Result": "Completed",
                "Reason": "",
            },
            {
                "CommandType": "TableCreate",
                "CommandText": ".create-merge table receipt_line_added (line_number:long)",
                "Result": "Completed",
                "Reason": "",
            },
        ]
    )

    apply_kql._summarize_result(table)

    out = capsys.readouterr().out
    assert "KQL applied: 3/3 commands completed." in out
    # The verbose command text must not be printed on success.
    assert "create-merge table receipt_created" not in out


def test_summarize_result_lists_only_failures(capsys) -> None:
    table = _FakeTable(
        [
            {
                "CommandType": "TableCreate",
                "CommandText": ".create-merge table ok (x:long)",
                "Result": "Completed",
                "Reason": "",
            },
            {
                "CommandType": "FunctionCreate",
                "CommandText": ".create-or-alter function bad() { nope }",
                "Result": "Failed",
                "Reason": "Semantic error: 'nope' is not defined",
            },
        ]
    )

    apply_kql._summarize_result(table)

    out = capsys.readouterr().out
    assert "1/2 completed, 1 failed" in out
    assert "FunctionCreate" in out and "not defined" in out
    # The command that succeeded is not listed in the failure detail.
    assert "create-merge table ok" not in out


def test_summarize_result_falls_back_without_columns(capsys) -> None:
    """A result object without a Result column still prints a count, not a dump."""

    table = _FakeTable([{"CommandType": "X"} for _ in range(7)])

    apply_kql._summarize_result(table)

    assert "KQL applied: 7 command(s)." in capsys.readouterr().out
