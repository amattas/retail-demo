"""Typed schema for the cross-domain solution manifest."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    StringConstraints,
)
from pydantic.functional_validators import field_validator, model_validator

StableId = Annotated[
    StrictStr,
    StringConstraints(pattern=r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$"),
]
ManifestVersion = Annotated[
    StrictStr,
    StringConstraints(pattern=r"^[0-9]+\.[0-9]+\.[0-9]+$"),
]
SupportStatus = Literal[
    "core",
    "optional",
    "preview",
    "manual",
    "experimental",
    "proposed",
]
SelectorKind = Literal[
    "directory_glob",
    "kql_create_merge_tables",
    "kql_ingestion_mapping_tables",
    "notebook_gold_contract",
    "notebook_ml_contract",
    "notebook_ml_validator",
    "notebook_streaming_contract",
    "python_symbol",
    "text",
    "tmdl_active_table_schemas",
    "tmdl_ref_tables",
    "toml_path",
    "yaml_path",
]
InventoryDerivation = Literal[
    "directory_glob",
    "kql_create_merge_tables",
    "kql_ingestion_mapping_tables",
    "python_dict_keys",
    "python_dict_values",
    "python_sequence",
    "tmdl_ref_tables",
    "yaml_sequence",
]
InventoryRelation = Literal["equal", "left_superset", "right_superset"]
EventPathKind = Literal["emitted", "derived"]
EventPathMode = Literal[
    "derived-last-touch-7d",
    "direct-spark-kusto-to-silver",
    "direct-spark-kusto-to-streaming-only-silver",
    "paired-lifecycle-to-silver",
]
PathTargetLayer = Literal["eventhouse", "silver", "gold", "semantic"]


class ManifestModel(BaseModel):
    """Strict immutable base for manifest data."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceSelector(ManifestModel):
    """Selector locating a symbol or collection within a repository source."""

    kind: SelectorKind
    value: StrictStr | None = None

    @model_validator(mode="after")
    def _validate_value(self) -> Self:
        value_kinds = {
            "directory_glob",
            "python_symbol",
            "text",
            "toml_path",
            "yaml_path",
        }
        if self.kind in value_kinds and not self.value:
            raise ValueError(f"selector {self.kind!r} requires a value")
        if self.kind not in value_kinds and self.value is not None:
            raise ValueError(f"selector {self.kind!r} does not accept a value")
        if self.kind == "directory_glob" and self.value is not None:
            if "\\" in self.value or ".." in PurePosixPath(self.value).parts:
                raise ValueError("directory glob must be repository-local")
        return self


class SourcePointer(ManifestModel):
    """Repository-relative pointer to an authoritative source."""

    path: StrictStr
    selector: SourceSelector | None = None

    @field_validator("path")
    @classmethod
    def _repository_relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if not value or "\\" in value or path.is_absolute() or ".." in path.parts:
            raise ValueError("source path must be a repository-relative POSIX path")
        return value


class InventoryDeclaration(ManifestModel):
    """A physical inventory derived at validation time from one source."""

    id: StableId
    description: StrictStr
    derivation: InventoryDerivation
    source: SourcePointer

    @model_validator(mode="after")
    def _compatible_selector(self) -> Self:
        expected = {
            "directory_glob": "directory_glob",
            "kql_create_merge_tables": "kql_create_merge_tables",
            "kql_ingestion_mapping_tables": "kql_ingestion_mapping_tables",
            "python_dict_keys": "python_symbol",
            "python_dict_values": "python_symbol",
            "python_sequence": "python_symbol",
            "tmdl_ref_tables": "tmdl_ref_tables",
            "yaml_sequence": "yaml_path",
        }[self.derivation]
        actual = self.source.selector.kind if self.source.selector else None
        if actual != expected:
            raise ValueError(
                f"inventory derivation {self.derivation!r} requires "
                f"selector {expected!r}, not {actual!r}"
            )
        return self


class InventoryRelationship(ManifestModel):
    """Expected relationship between two source-derived inventories."""

    left_inventory_id: StableId
    right_inventory_id: StableId
    relation: InventoryRelation
    require_difference: StrictBool = False


class InventoryRule(InventoryRelationship):
    """Named inventory invariant owned by an asset or contract."""

    id: StableId
    description: StrictStr


class SourceAgreement(ManifestModel):
    """Named cross-source validation that does not copy physical fields."""

    id: StableId
    kind: Literal["event_schema"]
    description: StrictStr
    sources: tuple[SourcePointer, ...] = Field(min_length=1)


class EnvelopeDeclaration(ManifestModel):
    """Stable identity and source pointer for the physical event envelope."""

    id: StableId
    source: SourcePointer


class EventTimeSemantics(ManifestModel):
    """Event-time selection without copying a physical field inventory."""

    field: StrictStr
    fallback_field: StrictStr | None = None
    timezone: Literal["UTC"]
    description: StrictStr

    @field_validator("field", "fallback_field")
    @classmethod
    def _field_identifier(cls, value: str | None) -> str | None:
        if value is not None and (not value or not value.isidentifier()):
            raise ValueError("event-time fields must be identifiers")
        return value


class EventDeclaration(ManifestModel):
    """Stable event metadata validated against source-derived physical schemas."""

    id: StableId
    event_type: StableId
    business_keys: tuple[StrictStr, ...] = Field(min_length=1)
    dedupe_keys: tuple[StrictStr, ...] = Field(min_length=1)
    event_time: EventTimeSemantics

    @field_validator("business_keys", "dedupe_keys")
    @classmethod
    def _unique_key_identifiers(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(values)) != len(values):
            raise ValueError("event keys must not contain duplicates")
        if any(not value.isidentifier() for value in values):
            raise ValueError("event keys must be identifiers")
        return values


class PathTarget(ManifestModel):
    """One physical target in a declared payload-to-model route."""

    layer: PathTargetLayer
    name: StrictStr

    @field_validator("name")
    @classmethod
    def _target_identifier(cls, value: str) -> str:
        if not value or not value.isidentifier():
            raise ValueError("path target names must be identifiers")
        return value


class CatalogEntry(ManifestModel):
    """Shared cross-domain metadata for owned manifest records."""

    id: StableId
    name: StrictStr
    owner: StableId
    support_status: SupportStatus
    description: StrictStr
    sources: tuple[SourcePointer, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_sources(self) -> Self:
        if len(self.sources) != len(set(self.sources)):
            raise ValueError("catalog source pointers must be unique")
        return self


class ManifestMetadata(CatalogEntry):
    """Identity and authority metadata for the solution."""


class Prerequisite(CatalogEntry):
    """Bootstrap-safe prerequisite metadata."""

    kind: Literal["driver", "service", "tool"]
    requirement: StrictStr | None = None
    check_command: tuple[StrictStr, ...] = ()
    bootstrap_required: StrictBool


class Command(CatalogEntry):
    """Bootstrap-safe command metadata, not an execution plan."""

    argv: tuple[StrictStr, ...] = Field(min_length=1)
    examples: tuple[tuple[StrictStr, ...], ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_examples(self) -> Self:
        if len(self.examples) != len(set(self.examples)):
            raise ValueError("command examples must be unique")
        if any(not example for example in self.examples):
            raise ValueError("command examples must not be empty")
        if any(example[: len(self.argv)] != self.argv for example in self.examples):
            raise ValueError("command examples must begin with argv")
        return self


class Asset(CatalogEntry):
    """Logical asset metadata with optional source-derived inventories."""

    kind: StrictStr
    depends_on: tuple[StableId, ...] = ()
    inventories: tuple[InventoryDeclaration, ...] = ()
    inventory_rules: tuple[InventoryRule, ...] = ()

    @model_validator(mode="after")
    def _validate_asset_references(self) -> Self:
        if len(self.depends_on) != len(set(self.depends_on)):
            raise ValueError("asset dependencies must be unique")
        return self


class ProfileAcknowledgement(ManifestModel):
    """Explicit operator acknowledgement for an undetectable boundary."""

    id: StableId
    kind: Literal["preview", "capacity", "manual"]
    description: StrictStr


class ProfileBlocker(ManifestModel):
    """Correctness gate that cannot be bypassed with an acknowledgement."""

    id: StableId
    tracking_issue: StrictStr
    description: StrictStr


class ProfileBoundaries(ManifestModel):
    """Qualitative operating boundaries for one deployment profile."""

    runtime: StrictStr
    capacity: StrictStr
    cost: StrictStr
    manual: StrictStr
    preview: StrictStr

    @property
    def supported(self) -> str:
        """Combined supported runtime, capacity, and cost boundary."""

        return " ".join((self.runtime, self.capacity, self.cost))


class PublicationExpectation(ManifestModel):
    """Expected staged item and folder counts for two-phase publication."""

    infrastructure_item_count: StrictInt = Field(ge=0)
    reporting_item_count: StrictInt = Field(ge=0)
    infrastructure_folders: tuple[StrictStr, ...] = ()
    reporting_folders: tuple[StrictStr, ...] = ()

    @model_validator(mode="after")
    def _validate_folders(self) -> Self:
        folders = (*self.infrastructure_folders, *self.reporting_folders)
        if any(not folder.strip() or "/" in folder or "\\" in folder for folder in folders):
            raise ValueError("workspace folders must be non-empty top-level names")
        if len(self.infrastructure_folders) != len(set(self.infrastructure_folders)):
            raise ValueError("infrastructure workspace folders must be unique")
        if len(self.reporting_folders) != len(set(self.reporting_folders)):
            raise ValueError("Reporting workspace folders must be unique")
        if set(self.infrastructure_folders) & set(self.reporting_folders):
            raise ValueError("publication phases must not duplicate workspace folders")
        return self

    @property
    def all_item_count(self) -> int:
        """Total expected items across both publication phases."""

        return self.infrastructure_item_count + self.reporting_item_count

    @property
    def all_folders(self) -> tuple[str, ...]:
        """Top-level workspace folders across both publication phases."""

        return (*self.infrastructure_folders, *self.reporting_folders)


class Profile(CatalogEntry):
    """Executable selection of existing deployment groups and assets."""

    implementation: Literal["executable"]
    deployment_name: StableId
    default: StrictBool = False
    available_group_inventory_id: StableId
    available_pipeline_inventory_id: StableId
    group_refs: tuple[StrictStr, ...] = Field(min_length=1)
    asset_refs: tuple[StableId, ...] = Field(min_length=1)
    pipeline_refs: tuple[StrictStr, ...] = ()
    post_deploy_pipeline_ref: StrictStr | None = None
    reporting_gate_pipeline_ref: StrictStr | None = None
    post_reporting_pipeline_refs: tuple[StrictStr, ...] = ()
    required_acknowledgements: tuple[ProfileAcknowledgement, ...] = ()
    blockers: tuple[ProfileBlocker, ...] = ()
    boundaries: ProfileBoundaries
    publication: PublicationExpectation

    @model_validator(mode="after")
    def _validate_pipeline_trigger(self) -> Self:
        for field_name, references in (
            ("group_refs", self.group_refs),
            ("asset_refs", self.asset_refs),
            ("pipeline_refs", self.pipeline_refs),
        ):
            if len(references) != len(set(references)):
                raise ValueError(f"{field_name} must be unique")
        gated_refs = (
            self.post_deploy_pipeline_ref,
            self.reporting_gate_pipeline_ref,
            *self.post_reporting_pipeline_refs,
        )
        for reference in gated_refs:
            if reference is not None and reference not in self.pipeline_refs:
                raise ValueError(
                    "pipeline gate references must also appear in pipeline_refs"
                )
        if self.reporting_gate_pipeline_ref is not None:
            if self.post_deploy_pipeline_ref is None:
                raise ValueError(
                    "reporting_gate_pipeline_ref requires post_deploy_pipeline_ref"
                )
            if self.reporting_gate_pipeline_ref == self.post_deploy_pipeline_ref:
                raise ValueError("setup and Reporting gate pipelines must differ")
        if len(set(self.post_reporting_pipeline_refs)) != len(
            self.post_reporting_pipeline_refs
        ):
            raise ValueError("post_reporting_pipeline_refs must be unique")
        if self.reporting_gate_pipeline_ref in self.post_reporting_pipeline_refs:
            raise ValueError(
                "the required Reporting gate cannot be a post-Reporting pipeline"
            )
        return self


class ReadinessExpectation(CatalogEntry):
    """Stable readiness check metadata validated by the live runner."""

    category: Literal[
        "binding",
        "freshness",
        "inventory",
        "kql",
        "pipeline",
        "schedule",
        "target",
        "taskflow",
    ]
    required_when_selected: StrictBool
    profile_refs: tuple[StableId, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_profile_refs(self) -> Self:
        if len(self.profile_refs) != len(set(self.profile_refs)):
            raise ValueError("readiness profile_refs must be unique")
        return self


class DataContract(CatalogEntry):
    """Declaration pointing to authoritative physical data contracts."""

    kind: Literal["event", "historical", "semantic_model"]
    inventories: tuple[InventoryDeclaration, ...] = Field(min_length=1)
    inventory_rules: tuple[InventoryRule, ...] = ()
    source_agreements: tuple[SourceAgreement, ...] = ()
    envelope: EnvelopeDeclaration | None = None
    events: tuple[EventDeclaration, ...] = ()
    fixture_sources: tuple[SourcePointer, ...] = ()

    @model_validator(mode="after")
    def _validate_event_extensions(self) -> Self:
        for field_name, values in (
            ("source_agreements", self.source_agreements),
            ("events", self.events),
            ("fixture_sources", self.fixture_sources),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"data contract {field_name} must be unique")
        if self.kind == "event":
            if self.envelope is None or not self.events or not self.fixture_sources:
                raise ValueError(
                    "event contracts require an envelope, events, and fixture sources"
                )
        elif self.envelope is not None or self.events or self.fixture_sources:
            raise ValueError(
                "only event contracts may declare envelope, events, or fixtures"
            )
        event_types = [event.event_type for event in self.events]
        if len(set(event_types)) != len(event_types):
            raise ValueError("event contract event_type values must be unique")
        return self


class EventPath(CatalogEntry):
    """Declared event flow between logical assets."""

    source_asset_id: StableId
    target_asset_id: StableId
    contract_ids: tuple[StableId, ...] = Field(min_length=1)
    path_kind: EventPathKind
    event_ids: tuple[StableId, ...] = Field(min_length=1)
    targets: tuple[PathTarget, ...] = Field(min_length=1)
    terminal_exception_id: StableId | None = None
    mode: EventPathMode

    @model_validator(mode="after")
    def _validate_route_shape(self) -> Self:
        for field_name, values in (
            ("contract_ids", self.contract_ids),
            ("event_ids", self.event_ids),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"event path {field_name} must be unique")
        if self.path_kind == "emitted" and len(self.event_ids) != 1:
            raise ValueError("emitted paths must reference exactly one event")
        target_keys = [(target.layer, target.name) for target in self.targets]
        if len(set(target_keys)) != len(target_keys):
            raise ValueError("event path targets must be unique")
        layer_order = {"eventhouse": 0, "silver": 1, "gold": 2, "semantic": 3}
        positions = [layer_order[target.layer] for target in self.targets]
        if positions != sorted(positions):
            raise ValueError("event path targets must follow layer order")
        layers = {target.layer for target in self.targets}
        if self.path_kind == "emitted" and not {"eventhouse", "silver"} <= layers:
            raise ValueError("emitted paths require Eventhouse and Silver targets")
        if self.path_kind == "derived" and "eventhouse" in layers:
            raise ValueError("derived paths cannot declare an Eventhouse target")
        has_semantic_terminal = "semantic" in layers
        if has_semantic_terminal == (self.terminal_exception_id is not None):
            raise ValueError(
                "paths require either a semantic terminal or one named exception"
            )
        return self


class ManifestException(CatalogEntry):
    """Explicit, owned exception to normal cross-domain agreement."""

    kind: StrictStr
    rationale: StrictStr
    verification_owner: StableId
    event_ids: tuple[StableId, ...] = ()
    target_names: tuple[StrictStr, ...] = ()
    tracking_issue: StrictStr | None = None
    inventory_comparison: InventoryRelationship | None = None

    @field_validator("target_names")
    @classmethod
    def _unique_target_identifiers(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(values)) != len(values):
            raise ValueError("exception targets must not contain duplicates")
        if any(not value.isidentifier() for value in values):
            raise ValueError("exception target names must be identifiers")
        return values


MlTier = Literal["required", "optional", "experimental"]
MlDataType = Literal[
    "array<long>",
    "array<string>",
    "boolean",
    "date",
    "double",
    "int",
    "long",
    "string",
    "timestamp",
]


class MlOutputField(ManifestModel):
    """One exact physical field produced by an ML notebook."""

    name: StrictStr
    data_type: MlDataType
    nullable: StrictBool

    @field_validator("name")
    @classmethod
    def _field_identifier(cls, value: str) -> str:
        if not value.isidentifier():
            raise ValueError("ML output field names must be identifiers")
        return value


class MlOutputContract(ManifestModel):
    """Physical output, grain, temporal, and lineage semantics."""

    table: StrictStr
    fields: tuple[MlOutputField, ...] = Field(min_length=1)
    grain: tuple[StrictStr, ...] = Field(min_length=1)
    as_of_column: StrictStr
    lineage_columns: tuple[StrictStr, ...] = ()
    probability_columns: tuple[StrictStr, ...] = ()
    forecast_horizon_days: StrictInt | None = Field(default=None, gt=0)
    forecast_horizon_column: StrictStr | None = None

    @model_validator(mode="after")
    def _validate_fields(self) -> Self:
        if not self.table.isidentifier():
            raise ValueError("ML output table must be an identifier")
        names = [field.name for field in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("ML output schema fields must be unique")
        references = (
            *self.grain,
            self.as_of_column,
            *self.lineage_columns,
            *self.probability_columns,
            *(
                (self.forecast_horizon_column,)
                if self.forecast_horizon_column is not None
                else ()
            ),
        )
        missing = sorted(set(references) - set(names))
        if missing:
            raise ValueError(f"ML output metadata references missing fields: {missing}")
        if len(self.grain) != len(set(self.grain)):
            raise ValueError("ML output grain fields must be unique")
        if len(self.lineage_columns) != len(set(self.lineage_columns)):
            raise ValueError("ML lineage fields must be unique")
        if len(self.probability_columns) != len(set(self.probability_columns)):
            raise ValueError("ML probability fields must be unique")
        required_non_null = {
            *self.grain,
            self.as_of_column,
            *self.lineage_columns,
            *self.probability_columns,
            *(
                (self.forecast_horizon_column,)
                if self.forecast_horizon_column is not None
                else ()
            ),
        }
        nullable = {
            field.name for field in self.fields if field.nullable
        }
        invalid_nullable = sorted(required_non_null & nullable)
        if invalid_nullable:
            raise ValueError(
                "ML grain/as-of/lineage/probability fields cannot be nullable: "
                f"{invalid_nullable}"
            )
        if (
            self.forecast_horizon_days is not None
            and self.forecast_horizon_column is not None
        ):
            raise ValueError(
                "declare either a fixed or row-level forecast horizon, not both"
            )
        return self


class MlContract(CatalogEntry):
    """Executable ML producer contract validated against notebook and TMDL sources."""

    implementation: Literal["executable"]
    tier: MlTier
    producer: SourcePointer
    validator: SourcePointer | None = None
    source_tables: tuple[StrictStr, ...] = Field(min_length=1)
    output: MlOutputContract
    reporting_required: StrictBool
    intended_use: StrictStr
    limitations: tuple[StrictStr, ...] = Field(min_length=1)
    asset_ids: tuple[StableId, ...] = Field(min_length=1)
    data_contract_ids: tuple[StableId, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_ml_contract(self) -> Self:
        if len(self.source_tables) != len(set(self.source_tables)):
            raise ValueError("ML source tables must be unique")
        if any(not table.isidentifier() for table in self.source_tables):
            raise ValueError("ML source tables must be identifiers")
        for field_name, values in (
            ("limitations", self.limitations),
            ("asset_ids", self.asset_ids),
            ("data_contract_ids", self.data_contract_ids),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"ML {field_name} must be unique")
        if self.producer not in self.sources:
            raise ValueError("ML producer must also appear in sources")
        if (
            self.producer.selector is None
            or self.producer.selector.kind != "notebook_ml_contract"
        ):
            raise ValueError("ML producer requires notebook_ml_contract selector")
        if self.tier == "required":
            if (
                self.validator is None
                or self.validator.selector is None
                or self.validator.selector.kind != "notebook_ml_validator"
            ):
                raise ValueError(
                    "required ML contracts require notebook_ml_validator"
                )
            if self.validator not in self.sources:
                raise ValueError("ML validator must also appear in sources")
        elif self.validator is not None:
            raise ValueError("only required ML contracts declare a validator")
        if self.reporting_required != (self.tier == "required"):
            raise ValueError(
                "only required-tier ML outputs may be required by Reporting"
            )
        if self.tier == "required" and not self.output.lineage_columns:
            raise ValueError("required ML outputs must expose row-level lineage")
        return self


class SourceOwner(ManifestModel):
    """Logical owner of authoritative repository sources."""

    id: StableId
    name: StrictStr
    description: StrictStr
    sources: tuple[SourcePointer, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_sources(self) -> Self:
        if len(self.sources) != len(set(self.sources)):
            raise ValueError("source owner pointers must be unique")
        return self
