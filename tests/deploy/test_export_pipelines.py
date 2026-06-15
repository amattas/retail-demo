"""Tests for exporting Fabric pipelines into source-control item folders."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from deploy.scripts import export_pipelines


def _b64(payload: object) -> str:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def test_write_item_decodes_parts_into_datapipeline_folder(tmp_path: Path) -> None:
    definition = {
        "parts": [
            {
                "path": ".platform",
                "payload": _b64({"metadata": {"type": "DataPipeline"}}),
                "payloadType": "InlineBase64",
            },
            {
                "path": "pipeline-content.json",
                "payload": _b64({"properties": {"activities": []}}),
                "payloadType": "InlineBase64",
            },
            {
                "path": ".schedules",
                "payload": _b64({"schedules": []}),
                "payloadType": "InlineBase64",
            },
        ]
    }

    item_dir = export_pipelines.write_item(tmp_path, "daily-maintenance", definition)

    assert item_dir == tmp_path / "daily-maintenance.DataPipeline"
    platform = json.loads((item_dir / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"]["type"] == "DataPipeline"
    content = json.loads((item_dir / "pipeline-content.json").read_text(encoding="utf-8"))
    assert content["properties"]["activities"] == []
    assert (item_dir / ".schedules").exists()


def test_find_workspace_id_matches_case_insensitively() -> None:
    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, list[dict[str, str]]]:
            return {
                "value": [
                    {"displayName": "Other", "id": "1"},
                    {"displayName": "Retail Demo", "id": "abc-123"},
                ]
            }

    class _FakeSession:
        def get(self, url: str) -> _FakeResponse:
            assert url.endswith("/workspaces")
            return _FakeResponse()

    assert (
        export_pipelines.find_workspace_id(_FakeSession(), "retail demo") == "abc-123"
    )


def test_find_workspace_id_raises_when_missing() -> None:
    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, list[dict[str, str]]]:
            return {"value": [{"displayName": "Other", "id": "1"}]}

    class _FakeSession:
        def get(self, url: str) -> _FakeResponse:
            return _FakeResponse()

    with pytest.raises(ValueError, match="Workspace not found"):
        export_pipelines.find_workspace_id(_FakeSession(), "Retail Demo")
