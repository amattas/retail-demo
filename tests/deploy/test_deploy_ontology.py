"""Publisher behavior for the new-experience ontology, with a fake Fabric API."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from deploy.scripts import deploy_ontology as publisher
from deploy.scripts._native_items import NativeItemError
from deploy.scripts._ontology_model import ONTOLOGY_NAME

WS = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
LH = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
SQL = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
OLD = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
NEW = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


def _part(path: str, text: str) -> dict[str, str]:
    return {"path": path, "payload": base64.b64encode(text.encode()).decode(),
            "payloadType": "InlineBase64"}


class Response:
    def __init__(self, status=200, body=None, text=""):
        self.status_code = status
        self._body = body if body is not None else {}
        self.text = text or json.dumps(self._body)
        self.headers = {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class FakeFabric:
    def __init__(self, existing=None, sql_status="Success", readback_ok=True):
        self.items = dict(existing or {})  # id -> (name, definition)
        self.sql_status = sql_status
        self.readback_ok = readback_ok
        self.calls: list[tuple[str, str]] = []
        self.created: list[str] = []

    def get(self, url, **_):
        self.calls.append(("GET", url))
        if url.endswith(f"/lakehouses/{LH}"):
            return Response(body={"properties": {"sqlEndpointProperties": {
                "connectionString": "abc.datawarehouse.fabric.microsoft.com",
                "id": SQL, "provisioningStatus": self.sql_status}}})
        if url.endswith("/ontologies"):
            return Response(body={"value": [
                {"id": item_id, "displayName": name} for item_id, (name, _) in self.items.items()
            ]})
        raise AssertionError(url)

    def post(self, url, json=None, **_):
        self.calls.append(("POST", url))
        if url.endswith("/getDefinition"):
            item_id = url.split("/")[-2]
            definition = self.items[item_id][1]
            if not self.readback_ok and item_id in self.created:
                definition = {"parts": [p for p in definition["parts"]
                                        if not p["path"].startswith("rules/")]}
            return Response(body={"definition": definition})
        if url.endswith("/ontologies"):
            item_id = NEW if not self.created else f"{len(self.created):08d}-0000-4000-8000-000000000000"
            self.created.append(item_id)
            self.items[item_id] = (json["displayName"], json["definition"])
            return Response(201, {"id": item_id})
        if "/updateDefinition" in url:
            item_id = url.split("/")[-2]
            self.items[item_id] = (self.items[item_id][0], json["definition"])
            return Response(200)
        raise AssertionError(url)

    def patch(self, url, json=None, **_):
        self.calls.append(("PATCH", url))
        item_id = url.rsplit("/", 1)[-1]
        self.items[item_id] = (json["displayName"], self.items[item_id][1])
        return Response(200)

    def delete(self, url, **_):
        self.calls.append(("DELETE", url))
        self.items.pop(url.rsplit("/", 1)[-1])
        return Response(200)


def _publish(fake, bronze=("receipt_created",)):
    return publisher.publish_ontology(
        fake, workspace_id=WS, lakehouse_id=LH, storage_token="t",
        list_bronze=lambda *_: set(bronze), sleep=lambda _: None,
    )


OLD_DEFINITION = {"parts": [
    _part(".platform", json.dumps({"metadata": {"type": "Ontology"}})),
    _part("definition.json", "{}"),
    _part("EntityTypes/1/definition.json", "{}"),
]}


@pytest.mark.parametrize(
    ("parts", "old"),
    [
        ({"definition.json": b"{}"}, True),
        ({"EntityTypes/1/definition.json": b"{}"}, True),
        ({".platform": b'{"config": {"version": "1.0"}}'}, True),
        ({".platform": b'{"config": {"version": "2.0"}}', "model.tmdl": b""}, False),
        ({"model.tmdl": b""}, False),
    ],
)
def test_format_detection(parts, old):
    assert publisher.is_old_format(parts) is old


def test_creates_when_absent_and_reports_skipped_bronze_tables():
    fake = FakeFabric()
    report = _publish(fake)
    assert report["action"] == "created" and report["ontology_id"] == NEW
    assert report["bronze_tables"] == ["receipt_created"]
    assert "payment_processed" in report["skipped_bronze_tables"]
    assert len(report["entities"]) == 12 and len(report["rules"]) == 9
    name, definition = fake.items[NEW]
    paths = {part["path"] for part in definition["parts"]}
    assert name == ONTOLOGY_NAME and "tables/receipt_created.tmdl" in paths


def test_migrates_an_old_format_item_through_a_verified_staging_item():
    fake = FakeFabric({OLD: (ONTOLOGY_NAME, OLD_DEFINITION)})
    report = _publish(fake)
    assert report["action"] == "migrated" and report["ontology_id"] == NEW
    methods = [method for method, _ in fake.calls]
    assert methods.index("DELETE") < methods.index("PATCH")
    assert fake.items == {NEW: (ONTOLOGY_NAME, fake.items[NEW][1])}


def test_failed_staging_readback_keeps_the_old_ontology():
    fake = FakeFabric({OLD: (ONTOLOGY_NAME, OLD_DEFINITION)}, readback_ok=False)
    with pytest.raises(NativeItemError, match="incomplete"):
        _publish(fake)
    assert fake.items[OLD][0] == ONTOLOGY_NAME
    assert not any(method == "DELETE" for method, _ in fake.calls)


def test_updates_a_tmdl_item_in_place():
    fake = FakeFabric()
    _publish(fake)
    report = _publish(fake, bronze=())
    assert report["action"] == "updated"
    assert not any(method == "DELETE" for method, _ in fake.calls)
    paths = {part["path"] for part in fake.items[NEW][1]["parts"]}
    assert "tables/receipt_created.tmdl" not in paths


def test_incomplete_readback_fails():
    with pytest.raises(NativeItemError, match="incomplete"):
        _publish(FakeFabric(readback_ok=False))


def test_duplicate_names_and_unprovisioned_sql_endpoint_fail_closed():
    with pytest.raises(NativeItemError, match="Multiple"):
        _publish(FakeFabric({OLD: (ONTOLOGY_NAME, OLD_DEFINITION),
                             NEW: (ONTOLOGY_NAME, OLD_DEFINITION)}))
    with pytest.raises(NativeItemError, match="not provisioned"):
        _publish(FakeFabric(sql_status="InProgress"))


def test_bronze_tables_lists_only_shortcut_directories():
    seen = {}

    def get(url, **kwargs):
        seen.update(url=url, **kwargs)
        return SimpleNamespace(status_code=200, json=lambda: {"paths": [
            {"name": f"{LH}/Tables/cusn/receipt_created", "isDirectory": "true"},
            {"name": f"{LH}/Tables/cusn/_delta_log.json", "isDirectory": "false"},
        ]})

    assert publisher.bronze_tables(WS, LH, "token", get=get) == {"receipt_created"}
    assert seen["url"] == f"{publisher.ONELAKE}/{WS}"
    assert seen["params"]["directory"] == f"{LH}/Tables/cusn"
    assert seen["headers"]["Authorization"] == "Bearer token"
    missing = lambda *_a, **_k: SimpleNamespace(status_code=404)  # noqa: E731
    assert publisher.bronze_tables(WS, LH, "token", get=missing) == set()


def test_render_writes_parts_without_service_calls(tmp_path: Path):
    files = publisher.render(tmp_path)
    names = {path.relative_to(tmp_path).as_posix() for path in files}
    assert {"model.tmdl", "entities/Store.tmdl", "rules/ReturnWindow.tmdl"} <= names
    assert all(b"\r\n" not in path.read_bytes() for path in files)
