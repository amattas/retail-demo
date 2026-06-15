"""Tests for the generic Fabric item exporter."""

from __future__ import annotations

import base64
import json
from pathlib import Path

from deploy.scripts import export_items


def _b64(payload: object) -> str:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def test_write_item_writes_nested_parts_for_data_agent(tmp_path: Path) -> None:
    definition = {
        "parts": [
            {"path": ".platform", "payload": _b64({"metadata": {"type": "DataAgent"}})},
            {
                "path": "Files/Config/data_agent.json",
                "payload": _b64({"$schema": "dataAgent/2.1.0"}),
            },
            {
                "path": "Files/Config/draft/semantic-model-retail_model/datasource.json",
                "payload": _b64({"artifactId": "abc", "type": "semantic_model"}),
            },
        ]
    }

    item_dir = export_items.write_item(
        tmp_path, "retail-semantic-model-agent", "DataAgent", definition
    )

    assert item_dir == tmp_path / "retail-semantic-model-agent.DataAgent"
    platform = json.loads((item_dir / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"]["type"] == "DataAgent"
    # Nested part directories are created.
    datasource = item_dir / "Files/Config/draft/semantic-model-retail_model/datasource.json"
    assert json.loads(datasource.read_text(encoding="utf-8"))["artifactId"] == "abc"


def test_list_items_filters_by_type_and_sorts() -> None:
    class _FakeResponse:
        def __init__(self, payload: dict) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._payload

    class _FakeSession:
        def __init__(self) -> None:
            self.params: dict | None = None

        def get(self, url: str, params: dict | None = None) -> _FakeResponse:
            self.params = params
            return _FakeResponse(
                {
                    "value": [
                        {"displayName": "b-agent", "id": "2"},
                        {"displayName": "a-agent", "id": "1"},
                    ]
                }
            )

    session = _FakeSession()
    items = export_items.list_items(session, "ws-id", "DataAgent")

    assert session.params == {"type": "DataAgent"}
    assert [item["displayName"] for item in items] == ["a-agent", "b-agent"]
