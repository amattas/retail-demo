"""Readiness retries transient Kusto reads without weakening inventory checks."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoNetworkError, KustoServiceError

from deploy.scripts import _retry
from deploy.scripts.verify_readiness import (
    FabricReadinessAdapter,
    KqlInventory,
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
            created.append(self)

        def close(self):
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
    created = []
    adapter = object.__new__(FabricReadinessAdapter)
    adapter.credential = object()
    adapter._sql_connection = adapter._kusto_client = None
    adapter._resolve_kql = lambda: ("https://unit.invalid", "retail_eventhouse")
    adapter.created_clients = created
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
    # Each network failure discards its client; the retry builds a fresh one.
    assert len(calls) == len(adapter.created_clients) == 3
    assert len(closed) == 2
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
    assert len(calls) == len(closed) == len(adapter.created_clients) == 3
    assert waits == [5.0, 10.0]


def test_healthy_client_is_reused_across_reads_and_closed_once(monkeypatch):
    adapter, calls, closed, _waits = _adapter(monkeypatch, [_response(), _response()])
    adapter._execute_kql(".show tables", management=True)
    adapter._execute_kql("receipt_created | take 1")
    assert len(calls) == 2 and len(adapter.created_clients) == 1 and closed == []
    adapter.close()
    assert closed == adapter.created_clients


@pytest.mark.parametrize(
    "error", [KustoServiceError("Forbidden"), ValueError("bad response")]
)
def test_non_network_errors_are_not_retried(monkeypatch, error):
    adapter, calls, closed, waits = _adapter(monkeypatch, [error])
    with pytest.raises(type(error)):
        adapter._execute_kql(".show tables", management=True)
    assert len(calls) == 1
    assert waits == []


@pytest.mark.parametrize("error_type", [requests.ConnectionError, requests.Timeout])
def test_kql_database_lookup_retries_transient_network_errors(monkeypatch, error_type):
    waits = []
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    outcomes = iter([error_type("reset"), ("https://unit.invalid", "retail_eventhouse")])

    def resolve(*_args):
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr("deploy.scripts.verify_readiness.resolve_kql_database", resolve)
    adapter = object.__new__(FabricReadinessAdapter)
    adapter._query_uri = adapter._database_name = None
    adapter.workspace_id = "workspace"
    adapter.outputs = {"kql_database_id": "database"}
    adapter.credential = object()

    assert adapter._resolve_kql() == ("https://unit.invalid", "retail_eventhouse")
    assert waits == [5.0]


def test_taskflow_read_retries_transient_network_errors(monkeypatch):
    from deploy.scripts import taskflow

    waits = []
    monkeypatch.setattr(_retry.time, "sleep", waits.append)
    monkeypatch.setattr(taskflow, "_token", lambda *_args: "token")
    monkeypatch.setattr(taskflow, "_session", lambda _token: object())
    clusters = iter([requests.ConnectionError("reset"), "https://cluster.invalid"])

    def resolve_cluster(_session):
        outcome = next(clusters)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(taskflow, "resolve_cluster", resolve_cluster)
    monkeypatch.setattr(
        taskflow, "get_taskflow", lambda *_args: {"tasks": [], "edges": []}
    )
    adapter = object.__new__(FabricReadinessAdapter)
    adapter.credential = object()
    adapter.workspace_id = "workspace"

    assert adapter.get_taskflow() == {"tasks": [], "edges": []}
    assert waits == [5.0]


@pytest.mark.parametrize("database_listing", [True, False])
def test_inventory_maps_only_existing_expected_tables(database_listing):
    calls = []
    adapter = object.__new__(FabricReadinessAdapter)
    adapter._resolve_kql = lambda: ("https://unit.invalid", "retail_eventhouse")
    mappings = [
        {"Name": "EventMapping", "Table": "receipt_created"},
        {"Name": "OtherMapping", "Table": "unrelated_table"},
    ]

    def execute(query, *, management=False):
        assert management
        calls.append(query)
        if query == ".show tables | project name=TableName":
            return [{"name": "receipt_created"}, {"name": "unrelated_table"}]
        if query == ".show functions | project name=Name":
            return [{"name": "fn_attribution"}]
        if query == ".show materialized-views | project name=Name":
            return [{"name": "mv_store_sales_minute"}]
        if query == ".show database ['retail_eventhouse'] ingestion json mappings":
            # Older service versions omit the Table column: fall back per table.
            return mappings if database_listing else [{"Name": "EventMapping"}]
        if query == ".show table receipt_created ingestion json mappings":
            return [{"Name": "EventMapping"}]
        raise ValueError("Entity ID was not found")

    adapter._execute_kql = execute

    inventory = adapter.get_kql_inventory(
        frozenset({"receipt_created", "ops_availability_signals"})
    )

    assert inventory == KqlInventory(
        tables=frozenset({"receipt_created", "unrelated_table"}),
        functions=frozenset({"fn_attribution"}),
        materialized_views=frozenset({"mv_store_sales_minute"}),
        mappings=frozenset({"receipt_created/EventMapping"}),
    )
    assert len(calls) == (4 if database_listing else 5)
    assert all("ops_availability_signals" not in query for query in calls)


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
