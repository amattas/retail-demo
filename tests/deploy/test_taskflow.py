"""Tests for the Fabric Task Flow portable export/deploy remapping."""

from __future__ import annotations

from deploy.scripts import taskflow


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

    item0 = resolved["tasks"][0]["items"][0]
    assert item0["artifactUniqueId"] == "SynapseNotebook:new-nb-guid"
    assert item0["artifactObjectId"] == "new-nb-guid"
    assert "artifactName" not in item0  # name stripped after resolution
    assert unresolved == ["Pipeline:missing-pipeline"]


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
