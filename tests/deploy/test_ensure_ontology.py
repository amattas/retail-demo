"""Tests for idempotent full-demo ontology creation."""

from __future__ import annotations

from collections import deque

import pytest

from deploy.scripts import ensure_ontology


class _Response:
    status_code = 202

    def __init__(self, location: str = "https://fabric/jobs/run-id") -> None:
        self.headers = {"Location": location}

    def raise_for_status(self) -> None:
        return None


class _Session:
    def __init__(self) -> None:
        self.posts: list[str] = []

    def post(self, url: str) -> _Response:
        self.posts.append(url)
        return _Response()


def test_existing_ontology_runs_validation_notebook(monkeypatch) -> None:
    session = _Session()
    monkeypatch.setattr(
        ensure_ontology,
        "list_items",
        lambda _session, _workspace, item_type: (
            [{"id": "ontology-id", "displayName": "RetailOntology_AutoGen"}]
            if item_type == "Ontology"
            else [{"id": "notebook-id", "displayName": "30-create-ontology"}]
        ),
    )
    monkeypatch.setattr(
        ensure_ontology,
        "wait_for_pipeline_job",
        lambda *_args, **_kwargs: {"status": "Completed"},
    )

    result = ensure_ontology.ensure_ontology(lambda: session, "workspace-id")

    assert result == ("ontology-id", False)
    assert session.posts == [
        "https://api.fabric.microsoft.com/v1/workspaces/workspace-id/items/"
        "notebook-id/jobs/RunNotebook/instances"
    ]


def test_absent_ontology_runs_exact_notebook_and_waits(monkeypatch) -> None:
    session = _Session()
    ontology_results = deque(
        [
            [],
            [],
            [{"id": "ontology-id", "displayName": "RetailOntology_AutoGen"}],
        ]
    )

    def fake_list(_session, _workspace, item_type):
        if item_type == "Ontology":
            return ontology_results.popleft()
        assert item_type == "Notebook"
        return [{"id": "notebook-id", "displayName": "30-create-ontology"}]

    waits = []
    monkeypatch.setattr(ensure_ontology, "list_items", fake_list)
    monkeypatch.setattr(
        ensure_ontology,
        "wait_for_pipeline_job",
        lambda _session, location, **kwargs: waits.append(
            (location, kwargs)
        )
        or {"status": "Completed"},
    )

    result = ensure_ontology.ensure_ontology(
        lambda: session,
        "workspace-id",
        poll_interval_seconds=0,
        sleep=lambda _seconds: None,
    )

    assert result == ("ontology-id", True)
    assert session.posts == [
        "https://api.fabric.microsoft.com/v1/workspaces/workspace-id/items/"
        "notebook-id/jobs/RunNotebook/instances"
    ]
    assert waits[0][1]["pipeline_id"] == "notebook-id"


def test_absent_ontology_requires_deployed_notebook(monkeypatch) -> None:
    monkeypatch.setattr(
        ensure_ontology,
        "list_items",
        lambda _session, _workspace, _item_type: [],
    )

    with pytest.raises(
        ensure_ontology.OntologyEnsureError,
        match="is not deployed",
    ):
        ensure_ontology.ensure_ontology(lambda: _Session(), "workspace-id")


def test_duplicate_ontology_fails_closed(monkeypatch) -> None:
    monkeypatch.setattr(
        ensure_ontology,
        "list_items",
        lambda _session, _workspace, _item_type: [
            {"id": "one", "displayName": "RetailOntology_AutoGen"},
            {"id": "two", "displayName": "RetailOntology_AutoGen"},
        ],
    )

    with pytest.raises(
        ensure_ontology.OntologyEnsureError,
        match="found 2",
    ):
        ensure_ontology.ensure_ontology(lambda: _Session(), "workspace-id")
