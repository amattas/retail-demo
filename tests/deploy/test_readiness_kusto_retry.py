"""Readiness retries transient Kusto reads without weakening inventory checks."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoNetworkError, KustoServiceError

from deploy.scripts import _retry
from deploy.scripts.verify_readiness import (
    FabricReadinessAdapter,
    ReadinessRunner,
    parse_kql_inventory,
)

ROOT = Path(__file__).resolve().parents[2]


def _adapter(monkeypatch, results):
    calls = []
    closed = []
    waits = []
    outcomes = iter(results)

    class Client:
        def __init__(self, _connection):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            closed.append(self)

        def _execute(self, database, query, method):
            calls.append((database, query, method))
            result = next(outcomes)
            if isinstance(result, Exception):
                raise result
            return result

        def execute_mgmt(self, database, query):
            return self._execute(database, query, "management")

        def execute(self, database, query):
            return self._execute(database, query, "query")

    monkeypatch.setattr("azure.kusto.data.KustoClient", Client)
    monkeypatch.setattr(
        KustoConnectionStringBuilder,
        "with_azure_token_credential",
        lambda _uri, _credential: object(),
    )
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    adapter = object.__new__(FabricReadinessAdapter)
    adapter.credential = object()
    adapter._resolve_kql = lambda: ("https://unit.invalid", "retail_eventhouse")
    return adapter, calls, closed, waits


def _response():
    return SimpleNamespace(
        primary_results=[[SimpleNamespace(to_dict=lambda: {"name": "receipt_created"})]]
    )


@pytest.mark.parametrize("management", [False, True])
def test_transient_kusto_reads_retry_with_fresh_clients(monkeypatch, management):
    adapter, calls, closed, waits = _adapter(
        monkeypatch,
        [
            KustoNetworkError("https://unit.invalid"),
            KustoNetworkError("https://unit.invalid"),
            _response(),
        ],
    )
    query = ".show tables" if management else "receipt_created | take 1"

    result = adapter._execute_kql(query, management=management)

    assert result == [{"name": "receipt_created"}]
    assert len(calls) == len(closed) == 3
    assert waits == [5.0, 10.0]
    assert all(
        call == ("retail_eventhouse", query, "management" if management else "query")
        for call in calls
    )


def test_exhausted_network_retries_propagate_instead_of_returning_empty_evidence(
    monkeypatch,
):
    adapter, calls, closed, waits = _adapter(
        monkeypatch, [KustoNetworkError("https://unit.invalid") for _ in range(3)]
    )
    with pytest.raises(KustoNetworkError):
        adapter._execute_kql(".show tables", management=True)
    assert len(calls) == len(closed) == 3
    assert waits == [5.0, 10.0]


@pytest.mark.parametrize(
    "error", [KustoServiceError("Forbidden"), ValueError("bad response")]
)
def test_non_network_errors_are_not_retried(monkeypatch, error):
    adapter, calls, closed, waits = _adapter(monkeypatch, [error])
    with pytest.raises(type(error)):
        adapter._execute_kql(".show tables", management=True)
    assert len(calls) == len(closed) == 1
    assert waits == []


def test_inventory_failure_does_not_mix_old_unknowns_with_a_later_snapshot():
    scripts = tuple(
        path.name for path in (ROOT / "fabric" / "kql_database").glob("*.kql")
    )
    expected = parse_kql_inventory(ROOT, scripts)
    getter = Mock(side_effect=[KustoNetworkError("https://unit.invalid"), expected])
    runner = object.__new__(ReadinessRunner)
    runner.context = SimpleNamespace(repo_root=ROOT, config=SimpleNamespace())
    runner.profile = SimpleNamespace(provisions_eventhouse=True, kql_scripts=scripts)
    runner.adapter = SimpleNamespace(get_kql_inventory=getter)
    runner.kql_inventory = None
    runner._kql_inventory_attempted = False
    runner.checks = []

    runner._run_kql_checks()

    getter.assert_called_once_with(expected.tables)
    assert [check.status for check in runner.checks] == ["UNKNOWN"] * 4
    assert all(check.required for check in runner.checks)


@pytest.mark.parametrize("missing_tables", [False, True])
def test_complete_inventory_is_reused_without_hiding_missing_objects(missing_tables):
    scripts = tuple(
        path.name for path in (ROOT / "fabric" / "kql_database").glob("*.kql")
    )
    expected = parse_kql_inventory(ROOT, scripts)
    observed = replace(expected, tables=frozenset()) if missing_tables else expected
    getter = Mock(return_value=observed)
    runner = object.__new__(ReadinessRunner)
    runner.context = SimpleNamespace(repo_root=ROOT, config=SimpleNamespace())
    runner.profile = SimpleNamespace(provisions_eventhouse=True, kql_scripts=scripts)
    runner.adapter = SimpleNamespace(get_kql_inventory=getter)
    runner.kql_inventory = None
    runner._kql_inventory_attempted = False
    runner.checks = []

    runner._run_kql_checks()

    getter.assert_called_once_with(expected.tables)
    assert [check.status for check in runner.checks] == [
        "FAIL" if missing_tables else "PASS",
        "PASS",
        "PASS",
        "PASS",
    ]
