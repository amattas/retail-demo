"""Duplicate-prevention contracts for Eventhouse and Silver streaming paths."""

from __future__ import annotations

import ast
import json
import re
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
STREAM_TEMPLATE = ROOT / "utility" / "notebooks" / "templates" / "driver-05-stream.py"
SILVER_NOTEBOOK = ROOT / "fabric" / "lakehouse" / "03-streaming-to-silver.ipynb"


def _function(tree: ast.Module, name: str) -> ast.FunctionDef:
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def _assignment(tree: ast.Module, name: str) -> ast.Assign:
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        )
    )


def _notebook_code(path: Path) -> str:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return "\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    )


def _exec_nodes(tree: ast.Module, names: set[str], globals_: dict) -> dict:
    nodes = [
        node
        for node in tree.body
        if (
            isinstance(node, ast.FunctionDef)
            and node.name in names
            or isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id in names
                for target in node.targets
            )
        )
    ]
    module = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    exec(compile(module, "<contract>", "exec"), globals_)
    return globals_


def test_stream_identity_persists_with_checkpoint_metadata() -> None:
    source = STREAM_TEMPLATE.read_text(encoding="utf-8")
    tree = ast.parse(source)

    class _FakeFs:
        def __init__(self) -> None:
            self.files: dict[str, str] = {}
            self.race_value: str | None = None
            self.raise_on_race = False

        def exists(self, path: str) -> bool:
            return path in self.files

        def head(self, path: str, _size: int) -> str:
            return self.files[path]

        def mkdirs(self, _path: str) -> bool:
            return True

        def put(self, path: str, value: str, overwrite: bool) -> bool:
            if path in self.files and not overwrite:
                return False
            if self.race_value is not None and not overwrite:
                self.files[path] = self.race_value
                if self.raise_on_race:
                    raise FileExistsError(path)
                return False
            self.files[path] = value
            return True

    fake_utils = type("_Utils", (), {"fs": _FakeFs()})()
    namespace = _exec_nodes(
        tree,
        {"_STREAM_ID_PATTERN", "_validate_stream_id", "_resolve_stream_id"},
        {"re": re, "uuid": uuid, "mssparkutils": fake_utils},
    )

    first = namespace["_resolve_stream_id"]("Files/checkpoint", "")
    second = namespace["_resolve_stream_id"]("Files/checkpoint", "")

    assert first == second
    assert uuid.UUID(first)
    with pytest.raises(ValueError, match="does not match checkpoint identity"):
        namespace["_resolve_stream_id"]("Files/checkpoint", "different-stream")

    fake_utils.fs.files.clear()
    race_winner = uuid.uuid4().hex
    fake_utils.fs.race_value = race_winner
    assert namespace["_resolve_stream_id"]("Files/checkpoint", "") == race_winner

    fake_utils.fs.files.clear()
    exception_winner = uuid.uuid4().hex
    fake_utils.fs.race_value = exception_winner
    fake_utils.fs.raise_on_race = True
    assert namespace["_resolve_stream_id"]("Files/checkpoint", "") == exception_winner


def test_eventhouse_batch_uses_idempotent_transaction_metadata() -> None:
    source = STREAM_TEMPLATE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    namespace = _exec_nodes(
        tree,
        {"_kusto_write_metadata"},
        {"json": json, "uuid": uuid, "STREAM_ID": "stream-a"},
    )

    properties_json, request_id = namespace["_kusto_write_metadata"](
        "receipt_created", 17
    )
    properties = json.loads(properties_json)
    expected_tag = "retail-demo:stream-a:receipt_created:17"

    assert properties == {
        "flushImmediately": True,
        "ingestByTags": [expected_tag],
        "ingestIfNotExists": [expected_tag],
    }
    assert uuid.UUID(request_id)

    write_source = ast.get_source_segment(source, _function(tree, "_write_event_table"))
    assert write_source is not None
    assert '"writeMode", "Transactional"' in write_source
    assert '"ensureNoDuplicatedBlobs", "true"' in write_source
    assert '"sparkIngestionPropertiesJson", ingestion_properties' in write_source
    assert '"sparkIngestionProperties", ingestion_properties' not in write_source


def test_eventhouse_batch_fails_before_checkpoint_commit() -> None:
    source = STREAM_TEMPLATE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    sink = _function(tree, "write_to_eventhouse")

    raises = [node for node in ast.walk(sink) if isinstance(node, ast.Raise)]
    assert len(raises) >= 2
    assert "unmapped event_types" in source
    assert "failed required Eventhouse writes" in source

    slot_source = ast.get_source_segment(source, _function(tree, "slot"))
    assert slot_source is not None
    assert "F.lit(STREAM_ID)" in slot_source
    assert "stream_prefix" in source


def test_silver_replay_uses_complete_dedupe_key_contract() -> None:
    code = _notebook_code(SILVER_NOTEBOOK)
    compile(code, "<03-streaming-to-silver>", "exec")
    tree = ast.parse(code)
    dedupe_keys = ast.literal_eval(_assignment(tree, "STREAM_DEDUPE_KEYS").value)

    assert dedupe_keys == {
        "receipt_created": ["receipt_id_ext"],
        "receipt_line_added": ["receipt_id_ext", "line_num"],
        "payment_processed": ["transaction_id"],
        "inventory_updated": ["trace_id"],
        "customer_entered": ["trace_id"],
        "stockout_detected": ["trace_id"],
        "reorder_triggered": ["trace_id"],
        "store_opened": ["trace_id"],
        "store_closed": ["trace_id"],
        "ad_impression": ["trace_id"],
        "promotion_applied": ["trace_id"],
        "customer_zone_changed": ["trace_id"],
        "ble_ping_detected": ["trace_id"],
        "online_order_created": ["order_id_ext"],
        "online_order_picked": ["order_id_ext", "status"],
        "online_order_shipped": ["order_id_ext", "status"],
    }

    watermark = _function(tree, "get_watermark")
    assert not any(isinstance(node, ast.ExceptHandler) for node in ast.walk(watermark))

    dedupe_source = ast.get_source_segment(
        code, _function(tree, "deduplicate_candidates")
    )
    merge_source = ast.get_source_segment(code, _function(tree, "merge_new_rows"))
    process_source = ast.get_source_segment(code, _function(tree, "process_events"))
    assert dedupe_source is not None and "dropDuplicates" in dedupe_source
    assert "conflicting rows share dedupe key" in dedupe_source
    assert merge_source is not None and "MERGE INTO" in merge_source
    assert process_source is not None
    assert process_source.index("merge_new_rows") < process_source.index(
        "update_watermark"
    )


@pytest.mark.parametrize(
    "function_name",
    [
        "transform_receipt_created",
        "transform_inventory_updated",
        "transform_customer_entered",
        "transform_ad_impression",
    ],
)
def test_silver_transforms_preserve_contract_trace_id(function_name: str) -> None:
    code = _notebook_code(SILVER_NOTEBOOK)
    tree = ast.parse(code)
    function_source = ast.get_source_segment(code, _function(tree, function_name))

    assert function_source is not None
    assert 'F.col("trace_id")' in function_source
