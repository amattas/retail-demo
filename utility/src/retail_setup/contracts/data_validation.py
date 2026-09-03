"""Cross-layer, source-derived validation for live data contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .fixtures import EventFixture, EventFixtureScenario, load_event_fixture_scenarios
from .manifest import SolutionManifest
from .models import DataContract, EventDeclaration, EventPath, ManifestException, SourcePointer
from .source_parsers import (
    DriverEventSchemas,
    GoldRoute,
    PhysicalField,
    StreamingSilverContract,
    TmdlTableSchema,
    driver_event_schemas,
    gold_output_contract,
    kql_mapping_schemas,
    kql_table_schemas,
    python_table_schemas,
    streaming_silver_contract,
    tmdl_active_table_schemas,
)
from .sources import ManifestSourceError, resolve_source_path

EXPECTED_SCENARIOS = frozenset(
    {
        "customer-ble-zone",
        "inventory-stockout-reorder",
        "nullable-variants",
        "online-create-pick-ship",
        "sale-attribution",
        "store-lifecycle",
        "truck-lifecycle",
        "unknown-event-dlq",
    }
)

_TMDL_TYPE_COMPATIBILITY = {
    "boolean": frozenset({"boolean"}),
    "dateTime": frozenset({"date", "timestamp"}),
    "decimal": frozenset({"double"}),
    "double": frozenset({"double"}),
    "int64": frozenset({"int", "long"}),
    "string": frozenset({"string"}),
}


@dataclass(frozen=True)
class DataContractSnapshot:
    """Physical contracts derived from repository sources at validation time."""

    driver: DriverEventSchemas
    kql_tables: dict[str, dict[str, str]]
    kql_mappings: dict[str, dict[str, tuple[str, str]]]
    historical_tables: dict[str, tuple[PhysicalField, ...]]
    silver: StreamingSilverContract
    gold: dict[str, GoldRoute]
    semantic_tables: dict[str, TmdlTableSchema]
    path_sources: dict[str, "EventPathSources"]
    fixture_scenarios: tuple[EventFixtureScenario, ...]


@dataclass(frozen=True)
class EventPathSources:
    """Physical contracts parsed from one event path's own source pointers."""

    silver: StreamingSilverContract
    gold: dict[str, GoldRoute]
    semantic_tables: dict[str, TmdlTableSchema]


@dataclass(frozen=True)
class DataContractValidation:
    """Counts and intentional source-derived boundaries from a valid contract."""

    event_count: int
    envelope_field_count: int
    typed_kql_table_count: int
    operational_kql_table_count: int
    mapping_count: int
    path_count: int
    exception_count: int
    fixture_scenario_count: int
    fixture_event_count: int
    historical_only_targets: frozenset[str]
    operational_targets: frozenset[str]
    streaming_only_targets: frozenset[str]


def derive_data_contract_snapshot(
    manifest: SolutionManifest,
    repo_root: Path,
) -> DataContractSnapshot:
    """Parse every authoritative physical source without importing production code."""

    event_contract = _single_contract(manifest, "event")
    historical_contract = _single_contract(manifest, "historical")
    semantic_contract = _single_contract(manifest, "semantic_model")

    driver_path = _selected_path(
        event_contract.sources,
        "python_symbol",
        "EVENT_PAYLOADS",
        repo_root,
    )
    ddl_path = _selected_path(
        event_contract.sources,
        "kql_create_merge_tables",
        None,
        repo_root,
    )
    mapping_path = _selected_path(
        event_contract.sources,
        "kql_ingestion_mapping_tables",
        None,
        repo_root,
    )
    historical_path = _selected_path(
        historical_contract.sources,
        "python_symbol",
        "TABLES",
        repo_root,
    )
    semantic_path = _selected_path(
        semantic_contract.sources,
        "tmdl_active_table_schemas",
        None,
        repo_root,
    )
    envelope_path = resolve_source_path(event_contract.envelope.source, repo_root)
    if envelope_path != driver_path:
        raise ManifestSourceError(
            "event envelope and payload declarations must share one source"
        )

    scenarios: list[EventFixtureScenario] = []
    for source in event_contract.fixture_sources:
        scenarios.extend(
            load_event_fixture_scenarios(resolve_source_path(source, repo_root))
        )
    if not scenarios:
        raise ManifestSourceError("event contract has no fixture scenarios")

    try:
        path_sources = _derive_event_path_sources(manifest, repo_root)
        silver = _shared_silver_contract(path_sources)
        gold = _shared_gold_contract(path_sources)
        semantic_tables = tmdl_active_table_schemas(semantic_path)
        for path_id, sources in path_sources.items():
            if sources.semantic_tables and (
                sources.semantic_tables != semantic_tables
            ):
                raise ManifestSourceError(
                    f"{path_id!r} semantic source differs from its data contract"
                )
        return DataContractSnapshot(
            driver=driver_event_schemas(driver_path),
            kql_tables=kql_table_schemas(ddl_path),
            kql_mappings=kql_mapping_schemas(mapping_path),
            historical_tables=python_table_schemas(historical_path),
            silver=silver,
            gold=gold,
            semantic_tables=semantic_tables,
            path_sources=path_sources,
            fixture_scenarios=tuple(scenarios),
        )
    except ManifestSourceError:
        raise
    except (
        FileNotFoundError,
        KeyError,
        TypeError,
        ValueError,
        SyntaxError,
    ) as exc:
        raise ManifestSourceError("could not derive the live data contract snapshot") from exc


def _derive_event_path_sources(
    manifest: SolutionManifest,
    repo_root: Path,
) -> dict[str, EventPathSources]:
    silver_cache: dict[Path, StreamingSilverContract] = {}
    gold_cache: dict[Path, dict[str, GoldRoute]] = {}
    semantic_cache: dict[Path, dict[str, TmdlTableSchema]] = {}
    result: dict[str, EventPathSources] = {}

    for path in manifest.event_paths:
        required_kinds = ["notebook_streaming_contract"]
        if _targets(path, "gold"):
            required_kinds.append("notebook_gold_contract")
        if _targets(path, "semantic"):
            required_kinds.append("tmdl_active_table_schemas")
        actual_kinds = [
            source.selector.kind if source.selector is not None else None
            for source in path.sources
        ]
        if sorted(actual_kinds, key=str) != sorted(required_kinds):
            raise ManifestSourceError(
                f"{path.id!r} sources must exactly cover its physical route"
            )

        silver_path = _selected_path(
            path.sources,
            "notebook_streaming_contract",
            None,
            repo_root,
        )
        if silver_path not in silver_cache:
            silver_cache[silver_path] = streaming_silver_contract(silver_path)

        gold: dict[str, GoldRoute] = {}
        if "notebook_gold_contract" in required_kinds:
            gold_path = _selected_path(
                path.sources,
                "notebook_gold_contract",
                None,
                repo_root,
            )
            if gold_path not in gold_cache:
                gold_cache[gold_path] = gold_output_contract(gold_path)
            gold = gold_cache[gold_path]

        semantic: dict[str, TmdlTableSchema] = {}
        if "tmdl_active_table_schemas" in required_kinds:
            semantic_path = _selected_path(
                path.sources,
                "tmdl_active_table_schemas",
                None,
                repo_root,
            )
            if semantic_path not in semantic_cache:
                semantic_cache[semantic_path] = tmdl_active_table_schemas(
                    semantic_path
                )
            semantic = semantic_cache[semantic_path]

        result[path.id] = EventPathSources(
            silver=silver_cache[silver_path],
            gold=gold,
            semantic_tables=semantic,
        )
    if not result:
        raise ManifestSourceError("manifest has no event paths")
    return result


def _shared_silver_contract(
    path_sources: dict[str, EventPathSources],
) -> StreamingSilverContract:
    contracts = [sources.silver for sources in path_sources.values()]
    first = contracts[0]
    if any(contract != first for contract in contracts[1:]):
        raise ManifestSourceError("event paths declare contradictory Silver sources")
    return first


def _shared_gold_contract(
    path_sources: dict[str, EventPathSources],
) -> dict[str, GoldRoute]:
    contracts = [
        sources.gold for sources in path_sources.values() if sources.gold
    ]
    if not contracts:
        return {}
    first = contracts[0]
    if any(contract != first for contract in contracts[1:]):
        raise ManifestSourceError("event paths declare contradictory Gold sources")
    return first


def validate_data_contracts(
    manifest: SolutionManifest,
    repo_root: Path,
) -> DataContractValidation:
    """Validate event metadata, physical routes, fixtures, and named boundaries."""

    snapshot = derive_data_contract_snapshot(manifest, repo_root)
    event_contract = _single_contract(manifest, "event")
    declarations = {event.id: event for event in event_contract.events}
    declarations_by_type = {event.event_type: event for event in event_contract.events}

    _validate_physical_event_contract(snapshot, declarations_by_type)
    _validate_semantic_contract(manifest, snapshot)
    _validate_paths(manifest, snapshot, declarations)
    _validate_fixtures(snapshot, declarations_by_type)
    historical_only, operational, streaming_only = _validate_boundaries(
        manifest,
        snapshot,
        declarations,
    )

    fixture_event_count = sum(
        len(scenario.events) for scenario in snapshot.fixture_scenarios
    )
    return DataContractValidation(
        event_count=len(declarations),
        envelope_field_count=len(snapshot.driver.envelope),
        typed_kql_table_count=len(snapshot.driver.payloads),
        operational_kql_table_count=len(operational),
        mapping_count=len(snapshot.kql_mappings),
        path_count=len(manifest.event_paths),
        exception_count=len(manifest.exceptions),
        fixture_scenario_count=len(snapshot.fixture_scenarios),
        fixture_event_count=fixture_event_count,
        historical_only_targets=frozenset(historical_only),
        operational_targets=frozenset(operational),
        streaming_only_targets=frozenset(streaming_only),
    )


def _validate_physical_event_contract(
    snapshot: DataContractSnapshot,
    declarations: dict[str, EventDeclaration],
) -> None:
    event_types = set(snapshot.driver.payloads)
    if len(snapshot.driver.envelope) != 9:
        raise ManifestSourceError("driver envelope must contain exactly nine fields")
    if len(event_types) != 18:
        raise ManifestSourceError("driver must declare exactly eighteen event types")
    if set(declarations) != event_types:
        raise ManifestSourceError("manifest and driver event inventories differ")
    if set(snapshot.kql_tables) != set(snapshot.kql_mappings):
        raise ManifestSourceError("KQL DDL and mapping inventories differ")
    if not event_types < set(snapshot.kql_tables):
        raise ManifestSourceError("typed KQL table inventory does not cover driver events")
    if set(snapshot.silver.routes) != event_types:
        raise ManifestSourceError("streaming Silver routes do not cover every event")

    envelope = {field.name: field for field in snapshot.driver.envelope}
    for event_type, payload_fields in snapshot.driver.payloads.items():
        declaration = declarations[event_type]
        payload = {field.name: field for field in payload_fields}
        wire_fields = {**envelope, **payload}
        missing_keys = set(declaration.business_keys) - set(wire_fields)
        if missing_keys:
            raise ManifestSourceError(
                f"{event_type!r} business keys are not wire fields: {sorted(missing_keys)}"
            )
        time_field = wire_fields.get(declaration.event_time.field)
        if time_field is None or time_field.data_type != "datetime":
            raise ManifestSourceError(
                f"{event_type!r} event-time field is not a datetime"
            )
        if declaration.event_time.fallback_field is not None:
            fallback = wire_fields.get(declaration.event_time.fallback_field)
            if fallback is None or fallback.data_type != "datetime":
                raise ManifestSourceError(
                    f"{event_type!r} event-time fallback is not a datetime"
                )
        derived_dedupe = snapshot.silver.dedupe_keys.get(event_type)
        if derived_dedupe != declaration.dedupe_keys:
            raise ManifestSourceError(
                f"{event_type!r} dedupe keys differ from streaming Silver"
            )

        expected_fields = {
            field.name: field.data_type for field in (*payload_fields, *snapshot.driver.envelope)
        }
        if snapshot.kql_tables[event_type] != expected_fields:
            raise ManifestSourceError(f"{event_type!r} driver/KQL schema differs")
        mapping = snapshot.kql_mappings[event_type]
        mapping_types = {name: value[0] for name, value in mapping.items()}
        if mapping_types != expected_fields:
            raise ManifestSourceError(f"{event_type!r} KQL mapping types differ")
        expected_paths = {
            field.name: f"$.payload.{field.source_name}" for field in payload_fields
        }
        expected_paths.update(
            {field.name: f"$.{field.name}" for field in snapshot.driver.envelope}
        )
        mapping_paths = {name: value[1] for name, value in mapping.items()}
        if mapping_paths != expected_paths:
            raise ManifestSourceError(f"{event_type!r} KQL mapping paths differ")

        target = snapshot.silver.routes[event_type]
        target_schema = snapshot.historical_tables.get(target)
        if target_schema is not None:
            target_fields = {field.name for field in target_schema}
            missing_dedupe = set(declaration.dedupe_keys) - target_fields
            if missing_dedupe:
                raise ManifestSourceError(
                    f"{event_type!r} dedupe keys missing from {target!r}: "
                    f"{sorted(missing_dedupe)}"
                )


def _validate_semantic_contract(
    manifest: SolutionManifest,
    snapshot: DataContractSnapshot,
) -> None:
    active = set(snapshot.semantic_tables)
    historical = set(snapshot.historical_tables)
    required_ml = {
        contract.output.table
        for contract in manifest.ml_contracts
        if contract.tier == "required"
    }
    expected_active = historical | required_ml
    if active != expected_active:
        raise ManifestSourceError(
            "active semantic model inventory differs from historical and "
            "required ML contracts"
        )
    gold = set(snapshot.gold)
    for table_name, tmdl in snapshot.semantic_tables.items():
        expected_schema = (
            "au" if table_name in gold or table_name in required_ml else "ag"
        )
        if (
            tmdl.source_schema != expected_schema
            or tmdl.source_table != table_name
        ):
            raise ManifestSourceError(
                f"TMDL binding differs for {table_name!r}: expected "
                f"{expected_schema}.{table_name}"
            )
    for table_name, table_schema in snapshot.historical_tables.items():
        tmdl = snapshot.semantic_tables[table_name]
        physical = {field.name: field.data_type for field in table_schema}
        for field in tmdl.fields:
            if field.name not in physical:
                raise ManifestSourceError(
                    f"TMDL field {table_name}.{field.name} is absent from schemas.py"
                )
            compatible = _TMDL_TYPE_COMPATIBILITY[field.data_type]
            if physical[field.name] not in compatible:
                raise ManifestSourceError(
                    f"TMDL type differs for {table_name}.{field.name}"
                )


def _validate_paths(
    manifest: SolutionManifest,
    snapshot: DataContractSnapshot,
    declarations: dict[str, EventDeclaration],
) -> None:
    emitted = [path for path in manifest.event_paths if path.path_kind == "emitted"]
    derived = [path for path in manifest.event_paths if path.path_kind == "derived"]
    emitted_by_event: dict[str, EventPath] = {}
    for path in emitted:
        event_id = path.event_ids[0]
        if event_id in emitted_by_event:
            raise ManifestSourceError(f"multiple emitted paths reference {event_id!r}")
        emitted_by_event[event_id] = path
    if set(emitted_by_event) != set(declarations):
        raise ManifestSourceError("emitted paths do not cover every declared event")

    for event_id, path in emitted_by_event.items():
        declaration = declarations[event_id]
        path_sources = snapshot.path_sources[path.id]
        kql_targets = _targets(path, "eventhouse")
        silver_targets = _targets(path, "silver")
        if kql_targets != (declaration.event_type,):
            raise ManifestSourceError(f"{path.id!r} has the wrong Eventhouse target")
        expected_silver = path_sources.silver.routes[declaration.event_type]
        if silver_targets != (expected_silver,):
            raise ManifestSourceError(f"{path.id!r} has the wrong Silver target")
        _validate_downstream_targets(path, path_sources, expected_silver)

    if len(derived) != len(snapshot.silver.derived_routes):
        raise ManifestSourceError("derived path inventory differs from Silver")
    derived_by_target = {
        _single_target(path, "silver"): path for path in derived
    }
    for route in snapshot.silver.derived_routes.values():
        path = derived_by_target.get(route.target_table)
        if path is None:
            raise ManifestSourceError(
                f"derived Silver target {route.target_table!r} has no path"
            )
        source_silver = {
            _single_target(emitted_by_event[event_id], "silver")
            for event_id in path.event_ids
        }
        if source_silver != set(route.source_tables):
            raise ManifestSourceError(
                f"{path.id!r} derived source coverage differs from Silver"
            )
        path_sources = snapshot.path_sources[path.id]
        own_routes = {
            item.target_table: item
            for item in path_sources.silver.derived_routes.values()
        }
        own_route = own_routes.get(route.target_table)
        if own_route is None or own_route.source_tables != route.source_tables:
            raise ManifestSourceError(
                f"{path.id!r} source does not declare its derived Silver route"
            )
        _validate_downstream_targets(path, path_sources, route.target_table)


def _validate_downstream_targets(
    path: EventPath,
    sources: EventPathSources,
    silver_target: str,
) -> None:
    gold_targets = _targets(path, "gold")
    for target in gold_targets:
        route = sources.gold.get(target)
        if route is None or silver_target not in route.source_tables:
            raise ManifestSourceError(
                f"{path.id!r} Gold target {target!r} is not derived from "
                f"{silver_target!r}"
            )
    semantic_targets = _targets(path, "semantic")
    if any(target not in sources.semantic_tables for target in semantic_targets):
        raise ManifestSourceError(f"{path.id!r} references an inactive semantic table")
    lakehouse_targets = {silver_target, *gold_targets}
    if any(target not in lakehouse_targets for target in semantic_targets):
        raise ManifestSourceError(
            f"{path.id!r} semantic terminal is not on its Lakehouse route"
        )


def _validate_fixtures(
    snapshot: DataContractSnapshot,
    declarations: dict[str, EventDeclaration],
) -> None:
    scenario_ids = {scenario.id for scenario in snapshot.fixture_scenarios}
    if scenario_ids != EXPECTED_SCENARIOS:
        raise ManifestSourceError(
            f"fixture scenarios differ: {sorted(scenario_ids ^ EXPECTED_SCENARIOS)}"
        )
    seen_events: set[str] = set()
    unknown_count = 0
    for scenario in snapshot.fixture_scenarios:
        for event in scenario.events:
            if event.event_type in snapshot.driver.payloads:
                _validate_typed_fixture(event, snapshot, declarations[event.event_type])
                seen_events.add(event.event_type)
            else:
                _validate_unknown_fixture(event, snapshot)
                unknown_count += 1
    if seen_events != set(snapshot.driver.payloads):
        raise ManifestSourceError("fixtures do not cover every emitted event type")
    if unknown_count == 0:
        raise ManifestSourceError("fixtures do not cover the unknown-event boundary")


def _validate_typed_fixture(
    event: EventFixture,
    snapshot: DataContractSnapshot,
    declaration: EventDeclaration,
) -> None:
    envelope = {field.name: field for field in snapshot.driver.envelope}
    payload_fields = snapshot.driver.payloads[event.event_type]
    payload = {
        field.source_name: field
        for field in payload_fields
        if field.source_name is not None
    }
    if set(event.envelope) != set(envelope):
        raise ManifestSourceError(
            f"fixture {event.event_type!r} has the wrong envelope fields"
        )
    if set(event.payload) != set(payload):
        raise ManifestSourceError(
            f"fixture {event.event_type!r} has the wrong payload fields"
        )
    if event.envelope["event_type"] != event.event_type:
        raise ManifestSourceError(
            f"fixture {event.event_type!r} envelope event_type differs"
        )
    for name, field in envelope.items():
        _validate_wire_value(event.envelope[name], field, f"{event.event_type}.{name}")
    for name, field in payload.items():
        _validate_wire_value(event.payload[name], field, f"{event.event_type}.{name}")

    wire: dict[str, Any] = {**event.envelope, **event.payload}
    for key in declaration.business_keys:
        value = wire.get(key)
        if value is None:
            raise ManifestSourceError(
                f"fixture {event.event_type!r} has a null business key {key!r}"
            )
    event_time = wire.get(declaration.event_time.field)
    if event_time is None and declaration.event_time.fallback_field is not None:
        event_time = wire.get(declaration.event_time.fallback_field)
    if event_time is None:
        raise ManifestSourceError(f"fixture {event.event_type!r} has no event time")
    _require_utc_timestamp(event_time, f"{event.event_type} event time")


def _validate_unknown_fixture(
    event: EventFixture,
    snapshot: DataContractSnapshot,
) -> None:
    table = snapshot.kql_tables.get("unknown_event")
    mapping = snapshot.kql_mappings.get("unknown_event")
    if table is None or mapping is None:
        raise ManifestSourceError("unknown_event DDL or mapping is absent")
    root = {**event.envelope, "payload": event.payload}
    if set(root) != set(table):
        raise ManifestSourceError("unknown-event fixture differs from KQL DDL")
    for name, data_type in table.items():
        _validate_wire_value(
            root[name],
            PhysicalField(name=name, data_type=data_type, nullable=True),
            f"unknown_event.{name}",
        )
        if mapping[name][1] != f"$.{name}" or mapping[name][0] != data_type:
            raise ManifestSourceError("unknown_event mapping differs from its DDL")
    _require_utc_timestamp(root["ingest_timestamp"], "unknown_event ingest time")


def _validate_wire_value(value: Any, field: PhysicalField, context: str) -> None:
    if value is None:
        if not field.nullable:
            raise ManifestSourceError(f"{context} is unexpectedly null")
        return
    valid = {
        "bool": lambda item: isinstance(item, bool),
        "boolean": lambda item: isinstance(item, bool),
        "date": lambda item: isinstance(item, str),
        "datetime": lambda item: isinstance(item, str),
        "double": _is_real,
        "dynamic": lambda item: isinstance(item, (dict, list)),
        "int": _is_integer,
        "int64": _is_integer,
        "long": _is_integer,
        "real": _is_real,
        "string": lambda item: isinstance(item, str),
        "timestamp": lambda item: isinstance(item, str),
    }[field.data_type](value)
    if not valid:
        raise ManifestSourceError(
            f"{context} does not match {field.data_type!r}"
        )
    if field.data_type in {"datetime", "timestamp"}:
        _require_utc_timestamp(value, context)


def _require_utc_timestamp(value: object, context: str) -> None:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ManifestSourceError(f"{context} must use a UTC Z timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ManifestSourceError(f"{context} is not an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ManifestSourceError(f"{context} is not UTC")


def _validate_boundaries(
    manifest: SolutionManifest,
    snapshot: DataContractSnapshot,
    declarations: dict[str, EventDeclaration],
) -> tuple[set[str], set[str], set[str]]:
    operational = set(snapshot.kql_tables) - set(snapshot.driver.payloads)
    streaming_only = (
        set(snapshot.silver.routes.values())
        | {
            route.target_table
            for route in snapshot.silver.derived_routes.values()
        }
    ) - set(snapshot.historical_tables)
    live_lakehouse_targets = {
        target.name
        for path in manifest.event_paths
        for target in path.targets
        if target.layer in {"silver", "gold"}
    }
    historical_only = set(snapshot.historical_tables) - live_lakehouse_targets
    ml_only = set(snapshot.semantic_tables) - set(snapshot.historical_tables)

    operational_exception = _single_exception(manifest, "operational-catch-all")
    streaming_exception = _single_exception(manifest, "streaming-only")
    historical_exception = _single_exception(manifest, "historical-only")
    semantic_exception = next(
        (
            exception
            for exception in manifest.exceptions
            if exception.id == "exception.semantic-model.ml-outputs"
        ),
        None,
    )
    if semantic_exception is None:
        raise ManifestSourceError("semantic-model ML exception is absent")

    _require_exception_targets(operational_exception, operational)
    _require_exception_targets(streaming_exception, streaming_only)
    _require_exception_targets(historical_exception, historical_only)
    _require_exception_targets(semantic_exception, ml_only)

    streaming_event_ids = {
        event_id
        for event_id, declaration in declarations.items()
        if snapshot.silver.routes[declaration.event_type] in streaming_only
    }
    if set(streaming_exception.event_ids) != streaming_event_ids:
        raise ManifestSourceError("streaming-only exception event coverage differs")
    if operational_exception.event_ids or historical_exception.event_ids:
        raise ManifestSourceError(
            "operational and historical exceptions must not claim emitted events"
        )
    for exception in (
        operational_exception,
        streaming_exception,
        historical_exception,
    ):
        if not exception.rationale or not exception.verification_owner:
            raise ManifestSourceError(f"{exception.id!r} lacks owned rationale")
    return historical_only, operational, streaming_only


def _single_contract(manifest: SolutionManifest, kind: str) -> DataContract:
    matches = [contract for contract in manifest.data_contracts if contract.kind == kind]
    if len(matches) != 1:
        raise ManifestSourceError(f"manifest requires one {kind!r} data contract")
    return matches[0]


def _selected_path(
    pointers: tuple[SourcePointer, ...],
    selector_kind: str,
    selector_value: str | None,
    repo_root: Path,
) -> Path:
    matches = [
        pointer
        for pointer in pointers
        if pointer.selector is not None
        and pointer.selector.kind == selector_kind
        and pointer.selector.value == selector_value
    ]
    if len(matches) != 1:
        raise ManifestSourceError(
            f"expected one source selector {selector_kind}:{selector_value}"
        )
    return resolve_source_path(matches[0], repo_root)


def _targets(path: EventPath, layer: str) -> tuple[str, ...]:
    return tuple(target.name for target in path.targets if target.layer == layer)


def _single_target(path: EventPath, layer: str) -> str:
    targets = _targets(path, layer)
    if len(targets) != 1:
        raise ManifestSourceError(f"{path.id!r} requires one {layer} target")
    return targets[0]


def _single_exception(
    manifest: SolutionManifest,
    kind: str,
) -> ManifestException:
    matches = [exception for exception in manifest.exceptions if exception.kind == kind]
    if len(matches) != 1:
        raise ManifestSourceError(f"manifest requires one {kind!r} exception")
    return matches[0]


def _require_exception_targets(
    exception: ManifestException,
    expected: set[str],
) -> None:
    if set(exception.target_names) != expected:
        raise ManifestSourceError(
            f"{exception.id!r} targets differ: "
            f"{sorted(set(exception.target_names) ^ expected)}"
        )


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_real(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
