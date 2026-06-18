"""Tests for the Fabric Task Flow portable export/deploy remapping."""

from __future__ import annotations

import re

import pytest

from deploy.scripts import taskflow


def test_token_retries_transient_auth_failure(monkeypatch) -> None:
    """A cold-az ClientAuthenticationError is retried; a later success returns."""

    azcore = pytest.importorskip("azure.core.exceptions")
    monkeypatch.setattr("time.sleep", lambda *_a: None)
    calls = {"n": 0}

    class _Cred:
        def get_token(self, _scope):
            calls["n"] += 1
            if calls["n"] == 1:
                raise azcore.ClientAuthenticationError("cold az")
            return type("T", (), {"token": "tok"})()

    assert taskflow._token("scope", _Cred()) == "tok"
    assert calls["n"] == 2


def test_to_portable_resolves_guids_to_names() -> None:
    task_flow = {
        "tasks": [
            {
                "id": "t1",
                "name": "Load",
                "type": "get data",
                "items": [
                    {
                        "artifactUniqueId": "SynapseNotebook:nb-guid",
                        "artifactType": "SynapseNotebook",
                        "artifactObjectId": "nb-guid",
                    },
                    {
                        "artifactUniqueId": "Pipeline:pl-guid",
                        "artifactType": "Pipeline",
                        "artifactObjectId": None,
                    },
                ],
            }
        ],
        "edges": [],
    }
    guid_to_name = {"nb-guid": "02-historical-data-load", "pl-guid": "historical-data-load"}

    portable = taskflow.to_portable(task_flow, guid_to_name)

    items = portable["tasks"][0]["items"]
    assert items[0]["artifactName"] == "02-historical-data-load"
    # Falls back to the GUID parsed from artifactUniqueId when artifactObjectId is null.
    assert items[1]["artifactName"] == "historical-data-load"
    # Original task flow is not mutated.
    assert "artifactName" not in task_flow["tasks"][0]["items"][0]


def test_to_portable_drops_unnameable_items() -> None:
    task_flow = {
        "tasks": [
            {
                "id": "t1",
                "items": [
                    {
                        "artifactUniqueId": "SynapseNotebook:nb1",
                        "artifactType": "SynapseNotebook",
                        "artifactObjectId": "nb1",
                    },
                    {
                        "artifactUniqueId": "SynapseNotebook:gone",
                        "artifactType": "SynapseNotebook",
                        "artifactObjectId": "gone",
                    },
                ],
            }
        ],
        "edges": [],
    }

    portable = taskflow.to_portable(task_flow, {"nb1": "01-create-bronze-shortcuts"})

    items = portable["tasks"][0]["items"]
    assert len(items) == 1  # the unnameable (deleted) item is dropped
    assert items[0]["artifactName"] == "01-create-bronze-shortcuts"


def test_committed_taskflow_has_only_bindable_items() -> None:
    """The committed portable task flow must not carry null-name or workspace-
    specific auto-generated references; both can never bind on deploy."""

    import json as _json
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    data = _json.loads(
        (repo_root / "fabric" / "taskflow" / "taskflow.json").read_text(encoding="utf-8")
    )
    # Hash-suffixed runtime artifacts (e.g. RetailOntology_AutoGen_graph_<32 hex>,
    # *_lh_<32 hex>) get a fresh GUID-derived suffix every run and can never bind by
    # name. The stable ontology item name (RetailOntology_AutoGen, no hash) is fine.
    hashed_autogen = re.compile(r"_AutoGen_(?:graph|lh)_[0-9a-f]{16,}", re.IGNORECASE)
    for task in data["tasks"]:
        for item in task["items"]:
            name = item.get("artifactName")
            assert name, f"null artifactName in task {task.get('name')!r}"
            assert not hashed_autogen.search(str(name)), (
                f"unportable hash-suffixed reference: {name}"
            )


def test_to_workspace_resolves_names_to_target_guids_and_reports_unresolved() -> None:
    portable = {
        "tasks": [
            {
                "id": "t1",
                "items": [
                    {"artifactType": "SynapseNotebook", "artifactName": "02-historical-data-load"},
                    {"artifactType": "Pipeline", "artifactName": "missing-pipeline"},
                ],
            }
        ],
        "edges": [],
    }
    name_type_to_guid = {("Notebook", "02-historical-data-load"): "new-nb-guid"}

    resolved, unresolved = taskflow.to_workspace(portable, name_type_to_guid)

    items = resolved["tasks"][0]["items"]
    # Unresolved items are dropped; only the resolved one remains.
    assert len(items) == 1
    assert items[0]["artifactUniqueId"] == "SynapseNotebook:new-nb-guid"
    assert items[0]["artifactObjectId"] == "new-nb-guid"
    assert "artifactName" not in items[0]  # name stripped after resolution
    assert unresolved == ["Pipeline:missing-pipeline"]


def test_deploy_creates_taskflow_when_workspace_has_none(monkeypatch, tmp_path) -> None:
    import json as _json

    path = tmp_path / "taskflow.json"
    path.write_text(
        _json.dumps(
            {
                "id": "tf-id",
                "name": "Retail Demo",
                "tasks": [
                    {
                        "id": "t1",
                        "items": [
                            {"artifactType": "Pipeline", "artifactName": "setup-pipeline"}
                        ],
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    calls = {"create": 0, "put": 0}

    monkeypatch.setattr(taskflow, "_session", lambda *_a, **_k: object())
    monkeypatch.setattr(taskflow, "_token", lambda *_a, **_k: "tok")
    monkeypatch.setattr(taskflow, "_credential", lambda *_a, **_k: object())
    monkeypatch.setattr(taskflow, "find_workspace_id", lambda *_a: "ws")
    monkeypatch.setattr(
        taskflow,
        "list_workspace_items",
        lambda *_a: [{"type": "DataPipeline", "displayName": "setup-pipeline", "id": "pl"}],
    )
    monkeypatch.setattr(taskflow, "resolve_cluster", lambda *_a: "https://c")
    monkeypatch.setattr(taskflow, "get_taskflow", lambda *_a: None)  # no existing flow

    def fake_create(_s, _c, _w, tf, **_k):
        calls["create"] += 1
        assert tf["tasks"][0]["items"][0]["artifactObjectId"] == "pl"
        return 201

    monkeypatch.setattr(taskflow, "create_taskflow", fake_create)
    monkeypatch.setattr(taskflow, "put_taskflow", lambda *a, **k: calls.update(put=calls["put"] + 1))

    unresolved = taskflow.deploy_taskflow("retail-demo-dev", path)

    assert calls["create"] == 1 and calls["put"] == 0
    assert unresolved == []


def test_deploy_updates_taskflow_when_workspace_has_one(monkeypatch, tmp_path) -> None:
    import json as _json

    path = tmp_path / "taskflow.json"
    path.write_text(_json.dumps({"id": "x", "tasks": [], "edges": []}), encoding="utf-8")
    calls = {"create": 0, "put": 0}

    monkeypatch.setattr(taskflow, "_session", lambda *_a, **_k: object())
    monkeypatch.setattr(taskflow, "_token", lambda *_a, **_k: "tok")
    monkeypatch.setattr(taskflow, "_credential", lambda *_a, **_k: object())
    monkeypatch.setattr(taskflow, "find_workspace_id", lambda *_a: "ws")
    monkeypatch.setattr(taskflow, "list_workspace_items", lambda *_a: [])
    monkeypatch.setattr(taskflow, "resolve_cluster", lambda *_a: "https://c")
    monkeypatch.setattr(
        taskflow,
        "get_taskflow",
        lambda *_a: {"resourceId": "r", "etag": "e", "taskFlow": {"id": "i"}},
    )
    monkeypatch.setattr(taskflow, "create_taskflow", lambda *a, **k: calls.update(create=calls["create"] + 1))
    monkeypatch.setattr(taskflow, "put_taskflow", lambda *a, **k: calls.update(put=calls["put"] + 1))

    taskflow.deploy_taskflow("retail-demo-dev", path)

    assert calls["put"] == 1 and calls["create"] == 0


def test_artifact_type_mapping_covers_key_fabric_types() -> None:
    m = taskflow.ARTIFACT_TO_ITEM_TYPE
    assert m["SynapseNotebook"] == "Notebook"
    assert m["Pipeline"] == "DataPipeline"
    assert m["LLMPlugin"] == "DataAgent"
    assert m["dataset"] == "SemanticModel"
    assert m["KustoEventHouse"] == "Eventhouse"


def test_looks_like_guid() -> None:
    assert taskflow._looks_like_guid("5219ac70-71d4-4dfc-af32-5b8a6c29a471")
    assert not taskflow._looks_like_guid("Retail Demo")
