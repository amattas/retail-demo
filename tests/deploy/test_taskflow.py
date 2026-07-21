"""Tests for the Fabric Task Flow portable export/deploy remapping."""

from __future__ import annotations

import inspect
import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from retail_setup.contracts import load_repository_manifest, resolve_profile

from deploy.scripts import taskflow

TENANT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
ACCESS_TOKEN = "request-contract-token"  # noqa: S105 - synthetic test token
REPO_ROOT = Path(__file__).resolve().parents[2]


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


def test_taskflow_clients_accept_selected_auth_mode() -> None:
    assert "auth_mode" in inspect.signature(taskflow.export_taskflow).parameters
    assert "auth_mode" in inspect.signature(taskflow.deploy_taskflow).parameters
    assert "tenant_id" in inspect.signature(taskflow.export_taskflow).parameters
    assert "tenant_id" in inspect.signature(taskflow.deploy_taskflow).parameters


def test_session_uses_real_bearer_header() -> None:
    session = taskflow._session(ACCESS_TOKEN)

    assert session.headers["Authorization"] == f"Bearer {ACCESS_TOKEN}"


def test_credential_uses_selected_auth_mode(monkeypatch) -> None:
    assert "auth_mode" in inspect.signature(taskflow._credential).parameters
    calls: list[str] = []
    expected = object()
    monkeypatch.setattr(
        taskflow,
        "build_credential",
        lambda auth_mode, *, tenant_id: (
            calls.append(f"{auth_mode}:{tenant_id}") or expected
        ),
        raising=False,
    )

    actual = taskflow._credential(
        auth_mode="azure_powershell",
        tenant_id=TENANT_ID,
    )

    assert actual is expected
    assert calls == [f"azure_powershell:{TENANT_ID}"]


def test_main_passes_selected_auth_mode(monkeypatch, tmp_path) -> None:
    calls: dict[str, object] = {}
    path = tmp_path / "taskflow.json"
    path.write_text('{"tasks": [], "edges": []}', encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "taskflow",
            "deploy",
            "--workspace",
            "retail-demo",
            "--environment",
            "dev",
            "--profile",
            "full-demo",
            "--path",
            str(path),
            "--auth-mode",
            "azure_powershell",
            "--tenant-id",
            TENANT_ID,
        ],
    )
    monkeypatch.setattr(
        taskflow,
        "deploy_taskflow",
        lambda *args, **kwargs: calls.update(kwargs) or [],
    )
    monkeypatch.setattr(taskflow, "profile_taskflow_artifacts", lambda *_args: set())
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_environment",
        lambda _env: SimpleNamespace(
            deployment=SimpleNamespace(profile="full-demo"),
            profile=SimpleNamespace(deploys_task_flow=True),
        ),
    )

    try:
        result = taskflow.main()
    except SystemExit as exc:
        pytest.fail(f"--auth-mode was not accepted: {exc}")

    assert result == 0
    assert calls["auth_mode"] == "azure_powershell"
    assert calls["tenant_id"] == TENANT_ID


def test_main_uses_terraform_workspace_and_item_ids(monkeypatch, tmp_path) -> None:
    calls: dict[str, object] = {}
    taskflow_path = tmp_path / "taskflow.json"
    taskflow_path.write_text('{"tasks": [], "edges": []}', encoding="utf-8")
    output_path = tmp_path / "terraform-output.json"
    output_path.write_text("{}", encoding="utf-8")
    resolved_ids = {
        "Lakehouse": "resolved-lakehouse-id",
        "Eventhouse": "resolved-eventhouse-id",
        "KQLDatabase": "resolved-kql-id",
    }
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "taskflow",
            "deploy",
            "--terraform-output",
            str(output_path),
            "--environment",
            "dev",
            "--profile",
            "full-demo",
            "--path",
            str(taskflow_path),
        ],
    )
    monkeypatch.setattr(
        taskflow,
        "terraform_taskflow_targets",
        lambda path, **_kwargs: (
            calls.update(output_path=path)
            or ("11111111-1111-4111-8111-111111111111", resolved_ids)
        ),
    )

    def fake_deploy(workspace, path, **kwargs):
        calls.update(workspace=workspace, path=path, **kwargs)
        return []

    monkeypatch.setattr(taskflow, "deploy_taskflow", fake_deploy)
    monkeypatch.setattr(taskflow, "profile_taskflow_artifacts", lambda *_args: set())
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.load_environment",
        lambda _env: SimpleNamespace(
            deployment=SimpleNamespace(profile="full-demo"),
            profile=SimpleNamespace(deploys_task_flow=True),
        ),
    )

    assert taskflow.main() == 0
    assert calls["output_path"] == output_path
    assert calls["workspace"] == "11111111-1111-4111-8111-111111111111"
    assert calls["item_type_to_guid"] == resolved_ids


def test_filter_portable_items_drops_assets_outside_profile() -> None:
    portable = {
        "tasks": [
            {
                "items": [
                    {
                        "artifactType": "SynapseNotebook",
                        "artifactName": "setup-01-seed-dictionaries",
                    },
                    {
                        "artifactType": "SynapseNotebook",
                        "artifactName": "99-reset-lakehouse",
                    },
                ]
            }
        ]
    }

    filtered = taskflow.filter_portable_items(
        portable,
        {("SynapseNotebook", "setup-01-seed-dictionaries")},
    )

    assert filtered["tasks"][0]["items"] == [
        {
            "artifactType": "SynapseNotebook",
            "artifactName": "setup-01-seed-dictionaries",
        }
    ]
    assert len(portable["tasks"][0]["items"]) == 2


def test_profile_taskflow_inventory_uses_only_selected_ml_tiers() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    manifest, validation = load_repository_manifest(repo_root)
    profile = resolve_profile(manifest, validation, "standard")
    config = SimpleNamespace(
        profile=profile,
        lakehouse=SimpleNamespace(name="retail_lakehouse"),
        eventhouse=SimpleNamespace(
            name="retail_eventhouse",
            kql_database_name="retail_eventhouse",
        ),
        powerbi=SimpleNamespace(
            semantic_model_name="retail_model",
            report_name="retail_model",
        ),
    )

    allowed = taskflow.profile_taskflow_artifacts(repo_root, config)

    assert ("MLExperiment", "demand_forecast") in allowed
    assert ("MLExperiment", "market_basket") not in allowed
    assert ("MLExperiment", "dynamic_pricing") not in allowed


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
    guid_to_name = {
        "nb-guid": "02-historical-data-load",
        "pl-guid": "historical-data-load",
    }

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
        (repo_root / "fabric" / "taskflow" / "taskflow.json").read_text(
            encoding="utf-8"
        )
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


def test_committed_taskflow_places_reporting_after_required_ml() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    data = json.loads(
        (repo_root / "fabric" / "taskflow" / "taskflow.json").read_text(
            encoding="utf-8"
        )
    )
    tasks = {task["name"]: task for task in data["tasks"]}
    required_ml = tasks["Required ML Reporting Gate"]
    extended_ml = tasks["Post-Reporting Extended ML"]
    semantic = tasks["Semantic Model"]

    required_pipelines = {
        item["artifactName"]
        for item in required_ml["items"]
        if item["artifactType"] == "Pipeline"
    }
    extended_pipelines = {
        item["artifactName"]
        for item in extended_ml["items"]
        if item["artifactType"] == "Pipeline"
    }
    required_notebooks = {
        item["artifactName"]
        for item in required_ml["items"]
        if item["artifactType"] == "SynapseNotebook"
    }
    extended_notebooks = {
        item["artifactName"]
        for item in extended_ml["items"]
        if item["artifactType"] == "SynapseNotebook"
    }
    assert required_pipelines == {"ml-required"}
    assert extended_pipelines == {"ml-optional", "ml-experimental"}
    assert required_notebooks == {
        "06-ml-demand-forecast",
        "08-ml-customer-segmentation",
        "09-ml-churn-prediction",
        "12-ml-stockout-prediction",
        "15-validate-required-ml-contract",
    }
    assert extended_notebooks == {
        "07-ml-market-basket",
        "10-ml-promotion-effectiveness",
        "11-ml-journey-analysis",
        "13-ml-delivery-prediction",
        "14-ml-dynamic-pricing",
    }
    assert any(
        edge["source"] == required_ml["id"]
        and edge["target"] == semantic["id"]
        for edge in data["edges"]
    )
    assert any(
        edge["source"] == semantic["id"]
        and edge["target"] == extended_ml["id"]
        for edge in data["edges"]
    )
    assert not any(
        edge["source"] == extended_ml["id"]
        and edge["target"] == semantic["id"]
        for edge in data["edges"]
    )


def test_to_workspace_resolves_names_to_target_guids_and_reports_unresolved() -> None:
    portable = {
        "tasks": [
            {
                "id": "t1",
                "items": [
                    {
                        "artifactType": "SynapseNotebook",
                        "artifactName": "02-historical-data-load",
                    },
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


def test_to_workspace_prefers_terraform_owned_ids_over_ambiguous_names() -> None:
    portable = {
        "tasks": [
            {
                "id": "t1",
                "items": [
                    {
                        "artifactType": "Lakehouse",
                        "artifactName": "retail_lakehouse",
                    },
                    {
                        "artifactType": "KustoEventHouse",
                        "artifactName": "retail_eventhouse",
                    },
                    {
                        "artifactType": "KustoDatabase",
                        "artifactName": "retail_eventhouse",
                    },
                ],
            }
        ],
        "edges": [],
    }
    ambiguous_name_matches = {
        ("Lakehouse", "retail_lakehouse"): "wrong-lakehouse-id",
        ("Eventhouse", "retail_eventhouse"): "wrong-eventhouse-id",
        ("KQLDatabase", "retail_eventhouse"): "wrong-kql-id",
    }
    terraform_ids = {
        "Lakehouse": "resolved-lakehouse-id",
        "Eventhouse": "resolved-eventhouse-id",
        "KQLDatabase": "resolved-kql-id",
    }

    resolved, unresolved = taskflow.to_workspace(
        portable,
        ambiguous_name_matches,
        terraform_ids,
    )

    assert unresolved == []
    assert [
        item["artifactObjectId"]
        for item in resolved["tasks"][0]["items"]
    ] == [
        "resolved-lakehouse-id",
        "resolved-eventhouse-id",
        "resolved-kql-id",
    ]


def test_terraform_taskflow_targets_load_non_default_ids(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "terraform-output.json"
    output_path.write_text(
        json.dumps(
            {
                "workspace_id": {"value": "11111111-1111-4111-8111-111111111111"},
                "lakehouse_id": {"value": "22222222-2222-4222-8222-222222222222"},
                "eventhouse_id": {"value": "33333333-3333-4333-8333-333333333333"},
                "kql_database_id": {
                    "value": "44444444-4444-4444-8444-444444444444"
                },
            }
        ),
        encoding="utf-8",
    )

    calls: list[tuple[object, dict[str, object]]] = []
    config = object()
    monkeypatch.setattr(
        "deploy.scripts.deploy_config.validate_terraform_outputs",
        lambda candidate, outputs: calls.append((candidate, outputs)),
    )

    workspace_id, item_ids = taskflow.terraform_taskflow_targets(
        output_path,
        config=config,
    )

    assert workspace_id == "11111111-1111-4111-8111-111111111111"
    assert item_ids == {
        "Lakehouse": "22222222-2222-4222-8222-222222222222",
        "Eventhouse": "33333333-3333-4333-8333-333333333333",
        "KQLDatabase": "44444444-4444-4444-8444-444444444444",
    }
    assert calls and calls[0][0] is config


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
                            {
                                "artifactType": "Pipeline",
                                "artifactName": "setup-pipeline",
                            }
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
        lambda *_a: [
            {"type": "DataPipeline", "displayName": "setup-pipeline", "id": "pl"}
        ],
    )
    monkeypatch.setattr(taskflow, "resolve_cluster", lambda *_a: "https://c")
    monkeypatch.setattr(taskflow, "get_taskflow", lambda *_a: None)  # no existing flow

    def fake_create(_s, _c, _w, tf, **_k):
        calls["create"] += 1
        assert tf["tasks"][0]["items"][0]["artifactObjectId"] == "pl"
        return 201

    monkeypatch.setattr(taskflow, "create_taskflow", fake_create)
    monkeypatch.setattr(
        taskflow, "put_taskflow", lambda *a, **k: calls.update(put=calls["put"] + 1)
    )

    unresolved = taskflow.deploy_taskflow("retail-demo-dev", path)

    assert calls["create"] == 1 and calls["put"] == 0
    assert unresolved == []


def test_deploy_updates_taskflow_when_workspace_has_one(monkeypatch, tmp_path) -> None:
    import json as _json

    path = tmp_path / "taskflow.json"
    path.write_text(
        _json.dumps({"id": "x", "tasks": [], "edges": []}), encoding="utf-8"
    )
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
    monkeypatch.setattr(
        taskflow,
        "create_taskflow",
        lambda *a, **k: calls.update(create=calls["create"] + 1),
    )
    monkeypatch.setattr(
        taskflow, "put_taskflow", lambda *a, **k: calls.update(put=calls["put"] + 1)
    )

    taskflow.deploy_taskflow("retail-demo-dev", path)

    assert calls["put"] == 1 and calls["create"] == 0


def test_deploy_rejects_unresolved_references_before_taskflow_mutation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "taskflow.json"
    path.write_text(
        json.dumps(
            {
                "tasks": [
                    {
                        "items": [
                            {
                                "artifactType": "Ontology",
                                "artifactName": "RetailOntology_AutoGen",
                            }
                        ]
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(taskflow, "_session", lambda *_a, **_k: object())
    monkeypatch.setattr(taskflow, "_token", lambda *_a, **_k: "tok")
    monkeypatch.setattr(taskflow, "_credential", lambda *_a, **_k: object())
    monkeypatch.setattr(taskflow, "find_workspace_id", lambda *_a: "ws")
    monkeypatch.setattr(taskflow, "list_workspace_items", lambda *_a: [])
    monkeypatch.setattr(
        taskflow,
        "resolve_cluster",
        lambda *_a: pytest.fail("task-flow API called with unresolved references"),
    )

    with pytest.raises(ValueError, match="unresolved"):
        taskflow.deploy_taskflow("retail-demo-dev", path)


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


def test_taskflow_readme_scopes_deploy_and_fails_unresolved_items() -> None:
    readme = (
        REPO_ROOT / "fabric" / "taskflow" / "README.md"
    ).read_text(encoding="utf-8")

    assert "--environment <environment> --profile full-demo" in readme
    assert "selected references are never" in readme
    assert "silently omitted" in readme
    assert "retail-setup post-ontology --env <environment>" in readme
