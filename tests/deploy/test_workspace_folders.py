"""Object-type folder moves preserve item IDs and Fabric parent/child ownership."""

from __future__ import annotations

from copy import deepcopy
from unittest.mock import Mock
from uuid import NAMESPACE_URL, uuid5

import pytest
from requests import ConnectionError, HTTPError

from deploy.scripts.workspace_folders import WorkspaceFolderError, organize_workspace


def guid(name):
    return str(uuid5(NAMESPACE_URL, name))


def response(status, body):
    result = Mock(status_code=status)
    result.json.return_value = deepcopy(body)
    if status >= 400:
        result.raise_for_status.side_effect = HTTPError(str(status))
    return result


class Session:
    def __init__(self):
        self.folders = [
            {"id": guid(name), "displayName": name}
            for name in ("Setup", "Streaming", "Reporting", "ML")
        ]
        self.items = []
        self.children = {}
        self.posts = []
        self.deletes = []

    def add(self, name, item_type, folder=None):
        item = {
            "id": guid(f"{item_type}:{name}"),
            "type": item_type,
            "displayName": name,
            "folderId": guid(folder) if folder else None,
        }
        self.items.append(item)
        return item["id"]

    def get(self, url, **_kwargs):
        return response(200, {"value": self.folders if url.endswith("/folders") else self.items})

    def post(self, url, *, json, **_kwargs):
        self.posts.append((url, json))
        if url.endswith("/folders"):
            folder = {"id": guid(json["displayName"]), **json}
            self.folders.append(folder)
            return response(201, folder)
        item_id = url.split("/")[-2]
        child_ids = {child for children in self.children.values() for child in children}
        if item_id in child_ids:
            return response(400, {"errorCode": "CannotMoveChildOnly"})
        moved_ids = {item_id, *self.children.get(item_id, [])}
        moved = []
        for item in self.items:
            if item["id"] in moved_ids:
                item["folderId"] = json["targetFolderId"]
                moved.append(item)
        return response(200, {"value": moved})

    def delete(self, url, **_kwargs):
        folder_id = url.rsplit("/", 1)[1]
        assert not any(item["folderId"] == folder_id for item in self.items)
        assert not any(folder.get("parentFolderId") == folder_id for folder in self.folders)
        self.deletes.append(folder_id)
        self.folders = [folder for folder in self.folders if folder["id"] != folder_id]
        return response(200, {})


def test_type_folders_include_new_assets_and_keep_children_with_parent():
    session = Session()
    session.add("setup-01", "Notebook", "Setup")
    session.add("stream-events", "Notebook", "Streaming")
    session.add("prepare-replenishment-case", "Notebook")
    session.add("setup-pipeline", "DataPipeline", "Setup")
    session.add("retail_model", "Report", "Reporting")
    session.add("retail-twins", "Report")
    session.add("retail_model", "SemanticModel", "Reporting")
    session.add("forecast", "MLExperiment", "ML")
    lakehouse = session.add("retail", "Lakehouse")
    endpoint = session.add("retail", "SQLEndpoint")
    eventhouse = session.add("events", "Eventhouse")
    kql = session.add("events", "KQLDatabase")
    app = session.add("retail-approvals", "AppBackend")
    database = session.add("retail-approvals", "SQLDatabase")
    sql = session.add("retail-approvals", "SQLEndpoint")
    session.children = {lakehouse: [endpoint], eventhouse: [kql], app: [database, sql]}
    before = {item["id"] for item in session.items}

    report = organize_workspace(session, guid("workspace"))

    assert report["folder_counts"] == {
        "Notebooks": 3, "Pipelines": 1, "Reports": 2, "Semantic Models": 1,
        "ML Experiments": 1, "Lakehouses": 2, "Eventhouses": 2, "Fabric Apps": 3,
    }
    assert {item["id"] for item in session.items} == before
    assert set(report["deleted_empty_legacy_folders"]) == {
        "Setup", "Streaming", "Reporting", "ML",
    }
    posted_ids = {url.split("/")[-2] for url, _ in session.posts if url.endswith("/move")}
    assert not posted_ids & {endpoint, kql, database, sql}
    initial_folder_count = len(session.folders)
    second = organize_workspace(session, guid("workspace"))
    assert second["folder_counts"] == report["folder_counts"]
    assert len(session.folders) == initial_folder_count


def test_unknown_types_fail_before_creating_folders_or_moving_items():
    session = Session()
    session.add("new-preview", "UnrecognizedType")
    with pytest.raises(WorkspaceFolderError, match="No object-type folder policy"):
        organize_workspace(session, guid("workspace"))
    assert not session.posts and not session.deletes


def test_ambiguous_type_folder_fails_before_mutation():
    session = Session()
    session.folders.extend([
        {"id": guid("Notebooks1"), "displayName": "Notebooks"},
        {"id": guid("Notebooks2"), "displayName": "notebooks"},
    ])
    with pytest.raises(WorkspaceFolderError, match="Ambiguous"):
        organize_workspace(session, guid("workspace"))
    assert not session.posts


def test_nested_legacy_folder_is_preserved():
    session = Session()
    session.folders.append({
        "id": guid("nested"), "displayName": "User folder", "parentFolderId": guid("Setup"),
    })
    report = organize_workspace(session, guid("workspace"))
    assert report["retained_legacy_folders"] == ["Setup"]
    assert guid("Setup") not in session.deletes


def test_mutation_connection_error_is_not_replayed():
    session = Session()
    session.add("case", "Notebook")
    session.folders.append({"id": guid("Notebooks"), "displayName": "Notebooks"})
    session.post = Mock(side_effect=ConnectionError("reset"))
    with pytest.raises(ConnectionError):
        organize_workspace(session, guid("workspace"))
    session.post.assert_called_once()


def test_move_response_must_include_the_requested_item():
    session = Session()
    session.add("case", "Notebook")
    session.folders.append({"id": guid("Notebooks"), "displayName": "Notebooks"})
    session.post = Mock(return_value=response(200, {"value": []}))
    with pytest.raises(WorkspaceFolderError, match="no item identities"):
        organize_workspace(session, guid("workspace"))
