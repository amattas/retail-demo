"""Repository-aware validation for the solution manifest."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .data_validation import DataContractValidation, validate_data_contracts
from .manifest import SolutionManifest
from .models import (
    CatalogEntry,
    InventoryDeclaration,
    InventoryRelationship,
    SourcePointer,
)
from .sources import (
    ManifestSourceError,
    derive_inventory,
    resolve_source_path,
    validate_source_agreement,
    validate_source_pointer,
)
from .source_parsers import (
    MlValidationRule,
    PhysicalField,
    notebook_ml_output_schemas,
    notebook_ml_source_tables,
    notebook_ml_validation_rules,
    tmdl_active_table_schemas,
)

_REQUIRED_ML_TABLES = frozenset(
    {
        "demand_forecast",
        "customer_segments",
        "churn_predictions",
        "stockout_risk",
    }
)
_OPTIONAL_ML_TABLES = frozenset(
    {
        "product_associations",
        "product_recommendations",
        "journey_patterns",
        "zone_transitions",
        "zone_dwell_stats",
        "dwell_predictions",
    }
)
_EXPERIMENTAL_ML_TABLES = frozenset(
    {
        "price_elasticity",
        "promotion_lift",
        "pricing_constraints",
        "pricing_recommendations",
    }
)


@dataclass(frozen=True)
class InventoryDrift:
    """A declared difference calculated from two authoritative inventories."""

    exception_id: str
    left_only: frozenset[str]
    right_only: frozenset[str]


@dataclass(frozen=True)
class RepositoryValidation:
    """Derived inventories and declared drift from a successful validation."""

    inventories: dict[str, frozenset[str]]
    drifts: tuple[InventoryDrift, ...]
    data_contracts: DataContractValidation
    ml_contracts: "MlContractValidation"


@dataclass(frozen=True)
class MlContractValidation:
    """Source-validated ML output inventory by support tier."""

    required_tables: frozenset[str]
    optional_tables: frozenset[str]
    experimental_tables: frozenset[str]


def validate_manifest_repository(
    manifest: SolutionManifest,
    repo_root: Path,
) -> RepositoryValidation:
    """Validate sources, derived inventories, agreements, profiles, and exceptions."""

    validate_manifest_sources(manifest, repo_root)
    inventories = derive_manifest_inventories(manifest, repo_root)
    _validate_inventory_rules(manifest, inventories)
    _validate_source_agreements(manifest, repo_root)
    _validate_profiles(manifest, inventories)
    drifts = _validate_exceptions(manifest, inventories)
    data_contracts = validate_data_contracts(manifest, repo_root)
    ml_contracts = _validate_ml_contracts(manifest, repo_root)
    return RepositoryValidation(
        inventories=inventories,
        drifts=drifts,
        data_contracts=data_contracts,
        ml_contracts=ml_contracts,
    )


def validate_manifest_sources(
    manifest: SolutionManifest,
    repo_root: Path,
) -> None:
    """Validate every repository source pointer declared by the manifest."""

    validated: set[tuple[str, str | None, str | None]] = set()
    for pointer in iter_manifest_sources(manifest):
        selector_kind = pointer.selector.kind if pointer.selector else None
        selector_value = pointer.selector.value if pointer.selector else None
        key = (pointer.path, selector_kind, selector_value)
        if key in validated:
            continue
        validate_source_pointer(pointer, repo_root)
        validated.add(key)


def derive_manifest_inventories(
    manifest: SolutionManifest,
    repo_root: Path,
) -> dict[str, frozenset[str]]:
    """Derive all physical inventory values from their authoritative sources."""

    return {
        declaration.id: derive_inventory(declaration, repo_root)
        for declaration in iter_inventory_declarations(manifest)
    }


def iter_inventory_declarations(
    manifest: SolutionManifest,
) -> tuple[InventoryDeclaration, ...]:
    """Return every inventory declaration in stable manifest order."""

    return tuple(
        inventory
        for entry in (*manifest.assets, *manifest.data_contracts)
        for inventory in entry.inventories
    )


def iter_manifest_sources(manifest: SolutionManifest) -> tuple[SourcePointer, ...]:
    """Return every source pointer, including inventory and agreement sources."""

    pointers: list[SourcePointer] = []
    entries: tuple[CatalogEntry, ...] = (
        manifest.metadata,
        *manifest.prerequisites,
        *manifest.commands,
        *manifest.assets,
        *manifest.profiles,
        *manifest.readiness_expectations,
        *manifest.data_contracts,
        *manifest.event_paths,
        *manifest.exceptions,
        *manifest.ml_contracts,
    )
    for entry in entries:
        pointers.extend(entry.sources)
    for owner in manifest.source_owners:
        pointers.extend(owner.sources)
    for declaration in iter_inventory_declarations(manifest):
        pointers.append(declaration.source)
    for contract in manifest.data_contracts:
        for agreement in contract.source_agreements:
            pointers.extend(agreement.sources)
        if contract.envelope is not None:
            pointers.append(contract.envelope.source)
        pointers.extend(contract.fixture_sources)
    return tuple(pointers)


def _validate_inventory_rules(
    manifest: SolutionManifest,
    inventories: dict[str, frozenset[str]],
) -> None:
    for entry in (*manifest.assets, *manifest.data_contracts):
        for rule in entry.inventory_rules:
            _validate_relationship(rule, inventories, rule.id)


def _validate_source_agreements(
    manifest: SolutionManifest,
    repo_root: Path,
) -> None:
    for contract in manifest.data_contracts:
        for agreement in contract.source_agreements:
            validate_source_agreement(agreement, repo_root)


def _validate_ml_contracts(
    manifest: SolutionManifest,
    repo_root: Path,
) -> MlContractValidation:
    """Validate manifest ML schemas against producers, validator, and active TMDL."""

    by_tier: dict[str, set[str]] = {
        "required": set(),
        "optional": set(),
        "experimental": set(),
    }
    seen_tables: set[str] = set()
    producer_cache: dict[Path, dict[str, tuple[PhysicalField, ...]]] = {}
    source_cache: dict[Path, tuple[str, ...]] = {}
    validator_cache: dict[Path, dict[str, tuple[PhysicalField, ...]]] = {}
    validator_rule_cache: dict[Path, dict[str, MlValidationRule]] = {}
    model_cache: dict[Path, object] = {}

    for contract in manifest.ml_contracts:
        table_name = contract.output.table
        if table_name in seen_tables:
            raise ManifestSourceError(
                f"ML output table is declared more than once: {table_name!r}"
            )
        seen_tables.add(table_name)
        by_tier[contract.tier].add(table_name)

        producer_path = resolve_source_path(contract.producer, repo_root)
        producer_schemas = producer_cache.setdefault(
            producer_path,
            notebook_ml_output_schemas(producer_path),
        )
        source_tables = source_cache.setdefault(
            producer_path,
            notebook_ml_source_tables(producer_path),
        )
        if tuple(contract.source_tables) != source_tables:
            raise ManifestSourceError(
                f"ML source-table mismatch for {table_name!r}"
            )
        try:
            producer_schema = producer_schemas[table_name]
        except KeyError as exc:
            raise ManifestSourceError(
                f"ML producer does not declare {table_name!r}"
            ) from exc
        manifest_schema = tuple(
            PhysicalField(
                name=field.name,
                data_type=field.data_type,
                nullable=field.nullable,
            )
            for field in contract.output.fields
        )
        if producer_schema != manifest_schema:
            raise ManifestSourceError(
                f"ML producer/manifest schema mismatch for {table_name!r}"
            )

        if contract.tier != "required":
            continue
        assert contract.validator is not None
        validator_path = resolve_source_path(contract.validator, repo_root)
        validator_schemas = validator_cache.setdefault(
            validator_path,
            notebook_ml_output_schemas(
                validator_path,
                require_runtime_validation=False,
            ),
        )
        validator_rules = validator_rule_cache.setdefault(
            validator_path,
            notebook_ml_validation_rules(validator_path),
        )
        if set(validator_rules) != set(validator_schemas):
            raise ManifestSourceError(
                "required ML validator schema and semantic-rule inventories differ"
            )
        if validator_schemas.get(table_name) != manifest_schema:
            raise ManifestSourceError(
                f"required ML validator schema mismatch for {table_name!r}"
            )
        expected_rule = MlValidationRule(
            grain=tuple(contract.output.grain),
            as_of=contract.output.as_of_column,
            lineage=tuple(contract.output.lineage_columns),
            probabilities=tuple(contract.output.probability_columns),
            horizon=(
                contract.output.forecast_horizon_column
                if contract.output.forecast_horizon_column is not None
                else contract.output.forecast_horizon_days
            ),
        )
        if validator_rules.get(table_name) != expected_rule:
            raise ManifestSourceError(
                f"required ML runtime/manifest semantics mismatch for {table_name!r}"
            )

        model_pointers = [
            source
            for source in contract.sources
            if source.selector is not None
            and source.selector.kind == "tmdl_active_table_schemas"
        ]
        if len(model_pointers) != 1:
            raise ManifestSourceError(
                f"required ML contract {table_name!r} must reference one active TMDL model"
            )
        model_path = resolve_source_path(model_pointers[0], repo_root)
        model_tables = model_cache.setdefault(
            model_path,
            tmdl_active_table_schemas(model_path),
        )
        model_table = model_tables.get(table_name)  # type: ignore[union-attr]
        if model_table is None:
            raise ManifestSourceError(
                f"required ML output {table_name!r} is not active in model.tmdl"
            )
        if model_table.source_schema != "au" or model_table.source_table != table_name:
            raise ManifestSourceError(
                f"required ML TMDL binding mismatch for {table_name!r}"
            )
        expected_tmdl = tuple(
            (field.name, _spark_to_tmdl_type(field.data_type))
            for field in manifest_schema
        )
        actual_tmdl = tuple(
            (field.name, field.data_type) for field in model_table.fields
        )
        if actual_tmdl != expected_tmdl:
            raise ManifestSourceError(
                f"required ML producer/TMDL schema mismatch for {table_name!r}"
            )

    expected = {
        "required": _REQUIRED_ML_TABLES,
        "optional": _OPTIONAL_ML_TABLES,
        "experimental": _EXPERIMENTAL_ML_TABLES,
    }
    for tier, expected_tables in expected.items():
        actual = frozenset(by_tier[tier])
        if actual != expected_tables:
            raise ManifestSourceError(
                f"ML {tier} inventory mismatch: expected "
                f"{sorted(expected_tables)}, got {sorted(actual)}"
            )
    return MlContractValidation(
        required_tables=frozenset(by_tier["required"]),
        optional_tables=frozenset(by_tier["optional"]),
        experimental_tables=frozenset(by_tier["experimental"]),
    )


def _spark_to_tmdl_type(data_type: str) -> str:
    try:
        return {
            "boolean": "boolean",
            "date": "dateTime",
            "double": "double",
            "int": "int64",
            "long": "int64",
            "string": "string",
            "timestamp": "dateTime",
        }[data_type]
    except KeyError as exc:
        raise ManifestSourceError(
            f"ML type {data_type!r} cannot be exposed through TMDL"
        ) from exc


def _validate_profiles(
    manifest: SolutionManifest,
    inventories: dict[str, frozenset[str]],
) -> None:
    for profile in manifest.profiles:
        available = inventories[profile.available_group_inventory_id]
        selected = frozenset(profile.group_refs)
        if not selected <= available:
            unknown = sorted(selected - available)
            raise ManifestSourceError(
                f"profile {profile.id!r} references unknown groups: {unknown}"
            )
        available_pipelines = inventories[profile.available_pipeline_inventory_id]
        selected_pipelines = frozenset(profile.pipeline_refs)
        if not selected_pipelines <= available_pipelines:
            unknown = sorted(selected_pipelines - available_pipelines)
            raise ManifestSourceError(
                f"profile {profile.id!r} references unknown pipelines: {unknown}"
            )


def _validate_exceptions(
    manifest: SolutionManifest,
    inventories: dict[str, frozenset[str]],
) -> tuple[InventoryDrift, ...]:
    drifts: list[InventoryDrift] = []
    for exception in manifest.exceptions:
        comparison = exception.inventory_comparison
        if comparison is None:
            continue
        left, right = _validate_relationship(
            comparison,
            inventories,
            exception.id,
        )
        drifts.append(
            InventoryDrift(
                exception_id=exception.id,
                left_only=left - right,
                right_only=right - left,
            )
        )
    return tuple(drifts)


def _validate_relationship(
    relationship: InventoryRelationship,
    inventories: dict[str, frozenset[str]],
    record_id: str,
) -> tuple[frozenset[str], frozenset[str]]:
    left = inventories[relationship.left_inventory_id]
    right = inventories[relationship.right_inventory_id]
    valid = {
        "equal": left == right,
        "left_superset": right <= left,
        "right_superset": left <= right,
    }[relationship.relation]
    if not valid:
        raise ManifestSourceError(
            f"inventory relationship {record_id!r} failed: {relationship.relation}"
        )
    if relationship.require_difference and left == right:
        raise ManifestSourceError(f"inventory exception {record_id!r} no longer has a difference")
    return left, right
