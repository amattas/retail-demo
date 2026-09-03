"""Read physical inventories and contracts from authoritative repository sources."""

from __future__ import annotations

import json
import tomllib
from collections.abc import Iterable
from pathlib import Path

import yaml

from .models import InventoryDeclaration, SourceAgreement, SourcePointer
from .source_parsers import (
    flatten_sequences as _flatten_sequences,
)
from .source_parsers import (
    gold_output_contract as _gold_output_contract,
)
from .source_parsers import (
    glob_values as _glob_values,
)
from .source_parsers import (
    kql_mapping_schemas as _kql_mapping_schemas,
)
from .source_parsers import (
    kql_table_schemas as _kql_table_schemas,
)
from .source_parsers import (
    nested_value as _nested_value,
)
from .source_parsers import (
    notebook_ml_output_schemas as _notebook_ml_output_schemas,
)
from .source_parsers import (
    notebook_ml_source_tables as _notebook_ml_source_tables,
)
from .source_parsers import (
    notebook_ml_validation_rules as _notebook_ml_validation_rules,
)
from .source_parsers import (
    python_symbol as _python_symbol,
)
from .source_parsers import (
    streaming_silver_contract as _streaming_silver_contract,
)
from .source_parsers import (
    tmdl_active_table_schemas as _tmdl_active_table_schemas,
)
from .source_parsers import (
    tmdl_tables as _tmdl_tables,
)


class ManifestSourceError(ValueError):
    """Raised when a manifest source cannot be resolved or does not agree."""


def validate_source_pointer(pointer: SourcePointer, repo_root: Path) -> None:
    """Validate that a source and its selector resolve inside ``repo_root``."""

    path = _resolve_path(pointer, repo_root)
    selector = pointer.selector
    if selector is None:
        return

    try:
        if selector.kind == "python_symbol":
            _python_symbol(path, _selector_value(pointer))
        elif selector.kind == "yaml_path":
            _nested_value(
                yaml.safe_load(path.read_text(encoding="utf-8")),
                _selector_value(pointer),
            )
        elif selector.kind == "toml_path":
            _nested_value(
                tomllib.loads(path.read_text(encoding="utf-8")),
                _selector_value(pointer),
            )
        elif selector.kind == "text":
            value = _selector_value(pointer)
            if value not in path.read_text(encoding="utf-8"):
                raise KeyError(value)
        elif selector.kind == "directory_glob":
            _glob_values(path, _selector_value(pointer))
        elif selector.kind == "kql_create_merge_tables":
            _kql_table_schemas(path)
        elif selector.kind == "kql_ingestion_mapping_tables":
            _kql_mapping_schemas(path)
        elif selector.kind == "notebook_streaming_contract":
            _streaming_silver_contract(path)
        elif selector.kind == "notebook_gold_contract":
            _gold_output_contract(path)
        elif selector.kind == "notebook_ml_contract":
            _notebook_ml_output_schemas(path)
            _notebook_ml_source_tables(path)
        elif selector.kind == "notebook_ml_validator":
            _notebook_ml_output_schemas(
                path,
                require_runtime_validation=False,
            )
            _notebook_ml_validation_rules(path)
        elif selector.kind == "tmdl_active_table_schemas":
            _tmdl_active_table_schemas(path)
        elif selector.kind == "tmdl_ref_tables":
            _tmdl_tables(path)
        else:  # pragma: no cover - Pydantic constrains selector kinds
            raise AssertionError(f"unhandled selector kind: {selector.kind}")
    except (
        FileNotFoundError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
        SyntaxError,
    ) as exc:
        raise ManifestSourceError(
            f"source selector did not resolve: {pointer.path} ({selector.kind})"
        ) from exc


def resolve_source_path(pointer: SourcePointer, repo_root: Path) -> Path:
    """Resolve a validated repository-local source pointer."""

    return _resolve_path(pointer, repo_root)


def derive_inventory(
    declaration: InventoryDeclaration,
    repo_root: Path,
) -> frozenset[str]:
    """Derive one physical inventory without reading item names from the manifest."""

    pointer = declaration.source
    path = _resolve_path(pointer, repo_root)
    selector_value = _selector_value(pointer)

    try:
        if declaration.derivation == "python_dict_keys":
            value = _python_symbol(path, selector_value)
            if not isinstance(value, dict):
                raise TypeError("symbol is not a dictionary")
            items = value.keys()
        elif declaration.derivation == "python_dict_values":
            value = _python_symbol(path, selector_value)
            if not isinstance(value, dict):
                raise TypeError("symbol is not a dictionary")
            items = _flatten_sequences(value.values())
        elif declaration.derivation == "python_sequence":
            items = _python_symbol(path, selector_value)
        elif declaration.derivation == "yaml_sequence":
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
            items = _nested_value(document, selector_value)
        elif declaration.derivation == "directory_glob":
            items = _glob_values(path, selector_value)
        elif declaration.derivation == "kql_create_merge_tables":
            items = _kql_table_schemas(path)
        elif declaration.derivation == "kql_ingestion_mapping_tables":
            items = _kql_mapping_schemas(path)
        elif declaration.derivation == "tmdl_ref_tables":
            items = _tmdl_tables(path)
        else:  # pragma: no cover - Pydantic constrains derivations
            raise AssertionError(f"unhandled derivation: {declaration.derivation}")
        return _inventory_set(items, declaration.id)
    except (KeyError, TypeError, ValueError, SyntaxError) as exc:
        raise ManifestSourceError(
            f"could not derive inventory {declaration.id!r} from {pointer.path}"
        ) from exc


def validate_source_agreement(
    agreement: SourceAgreement,
    repo_root: Path,
) -> None:
    """Validate a declared cross-source agreement."""

    if agreement.kind == "event_schema":
        _validate_event_schema(agreement, repo_root)
        return
    raise ManifestSourceError(f"unknown source agreement kind: {agreement.kind}")


def _resolve_path(pointer: SourcePointer, repo_root: Path) -> Path:
    root = repo_root.resolve()
    path = (root / Path(*pointer.path.split("/"))).resolve()
    if not path.is_relative_to(root):
        raise ManifestSourceError(f"source escapes repository root: {pointer.path}")
    if not path.exists():
        raise ManifestSourceError(f"manifest source does not exist: {pointer.path}")
    return path


def _selector_value(pointer: SourcePointer) -> str:
    if pointer.selector is None or pointer.selector.value is None:
        return ""
    return pointer.selector.value


def _inventory_set(items: Iterable[object], inventory_id: str) -> frozenset[str]:
    values = list(items)
    if not all(isinstance(item, str) and item for item in values):
        raise TypeError(f"inventory {inventory_id!r} contains a non-string item")
    duplicates = sorted({item for item in values if values.count(item) > 1})
    if duplicates:
        raise ValueError(f"inventory {inventory_id!r} contains duplicates: {duplicates}")
    return frozenset(values)


def _validate_event_schema(agreement: SourceAgreement, repo_root: Path) -> None:
    pointers = {
        (
            source.selector.kind if source.selector else None,
            source.selector.value if source.selector else None,
        ): source
        for source in agreement.sources
    }
    ddl_path = _required_source(pointers, "kql_create_merge_tables", None, repo_root)
    mapping_path = _required_source(
        pointers,
        "kql_ingestion_mapping_tables",
        None,
        repo_root,
    )
    payload_path = _required_source(
        pointers,
        "python_symbol",
        "EVENT_PAYLOADS",
        repo_root,
    )
    envelope_path = _required_source(
        pointers,
        "python_symbol",
        "ENVELOPE",
        repo_root,
    )

    ddl = _kql_table_schemas(ddl_path)
    mappings = _kql_mapping_schemas(mapping_path)
    _assert_mapping_matches_ddl(ddl, mappings)

    payloads = _python_symbol(payload_path, "EVENT_PAYLOADS")
    envelope = _python_symbol(envelope_path, "ENVELOPE")
    for event_type, payload in payloads.items():
        if event_type not in ddl:
            raise ManifestSourceError(f"driver event {event_type!r} has no KQL table")
        expected_types = {column: data_type for column, _, data_type in payload}
        expected_types.update(dict(envelope))
        if expected_types != ddl[event_type]:
            raise ManifestSourceError(f"driver/KQL field or type mismatch for {event_type!r}")

        expected_paths = {column: f"$.payload.{json_field}" for column, json_field, _ in payload}
        expected_paths.update({column: f"$.{column}" for column, _ in envelope})
        actual_paths = {
            column: json_path for column, (_, json_path) in mappings[event_type].items()
        }
        if expected_paths != actual_paths:
            raise ManifestSourceError(f"driver/KQL mapping path mismatch for {event_type!r}")


def _required_source(
    pointers: dict[tuple[str | None, str | None], SourcePointer],
    kind: str,
    value: str | None,
    repo_root: Path,
) -> Path:
    try:
        return _resolve_path(pointers[(kind, value)], repo_root)
    except KeyError as exc:
        raise ManifestSourceError(f"source agreement is missing selector {kind}:{value}") from exc


def _assert_mapping_matches_ddl(
    ddl: dict[str, dict[str, str]],
    mappings: dict[str, dict[str, tuple[str, str]]],
) -> None:
    if ddl.keys() != mappings.keys():
        raise ManifestSourceError("KQL DDL and mapping table inventories differ")
    for table_name, fields in ddl.items():
        mapping_types = {
            column: data_type for column, (data_type, _) in mappings[table_name].items()
        }
        if fields != mapping_types:
            raise ManifestSourceError(f"KQL DDL and mapping fields/types differ for {table_name!r}")
