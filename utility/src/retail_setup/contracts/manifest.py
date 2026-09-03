"""Top-level solution manifest and cross-section reference validation."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
from typing import Self

from pydantic import model_validator

from .models import (
    Asset,
    Command,
    DataContract,
    EventPath,
    InventoryRelationship,
    ManifestException,
    ManifestMetadata,
    ManifestModel,
    ManifestVersion,
    MlContract,
    Prerequisite,
    Profile,
    ReadinessExpectation,
    SourceOwner,
)


class SolutionManifest(ManifestModel):
    """Canonical solution manifest with frozen top-level sections."""

    metadata: ManifestMetadata
    version: ManifestVersion
    prerequisites: tuple[Prerequisite, ...]
    commands: tuple[Command, ...]
    assets: tuple[Asset, ...]
    profiles: tuple[Profile, ...]
    readiness_expectations: tuple[ReadinessExpectation, ...]
    data_contracts: tuple[DataContract, ...]
    event_paths: tuple[EventPath, ...]
    exceptions: tuple[ManifestException, ...]
    ml_contracts: tuple[MlContract, ...]
    source_owners: tuple[SourceOwner, ...]

    @model_validator(mode="after")
    def _validate_ids_and_references(self) -> Self:
        identified = list(_identified_records(self))
        duplicate_ids = sorted(
            item_id
            for item_id, count in Counter(item.id for item in identified).items()
            if count > 1
        )
        if duplicate_ids:
            raise ValueError(f"manifest IDs must be unique: {duplicate_ids}")

        owner_ids = {owner.id for owner in self.source_owners}
        inventory_ids = {
            inventory.id
            for entry in (*self.assets, *self.data_contracts)
            for inventory in entry.inventories
        }
        asset_ids = {asset.id for asset in self.assets}
        contract_ids = {contract.id for contract in self.data_contracts}
        errors: list[str] = []

        owned = (
            self.metadata,
            *self.prerequisites,
            *self.commands,
            *self.assets,
            *self.profiles,
            *self.readiness_expectations,
            *self.data_contracts,
            *self.event_paths,
            *self.exceptions,
            *self.ml_contracts,
        )
        for entry in owned:
            _require_reference(errors, entry.id, "owner", entry.owner, owner_ids)

        for entry in (*self.assets, *self.data_contracts):
            for rule in entry.inventory_rules:
                _require_inventory_relationship(errors, rule, inventory_ids)
        for asset in self.assets:
            for dependency_id in asset.depends_on:
                _require_reference(
                    errors,
                    asset.id,
                    "depends_on",
                    dependency_id,
                    asset_ids,
                )
                if dependency_id == asset.id:
                    errors.append(f"{asset.id}.depends_on cannot reference itself")
        for profile in self.profiles:
            _require_reference(
                errors,
                profile.id,
                "available_group_inventory_id",
                profile.available_group_inventory_id,
                inventory_ids,
            )
            _require_reference(
                errors,
                profile.id,
                "available_pipeline_inventory_id",
                profile.available_pipeline_inventory_id,
                inventory_ids,
            )
            for asset_id in profile.asset_refs:
                _require_reference(
                    errors,
                    profile.id,
                    "asset_refs",
                    asset_id,
                    asset_ids,
                )
        defaults = [profile.deployment_name for profile in self.profiles if profile.default]
        if len(defaults) != 1:
            errors.append(
                "manifest must define exactly one default executable profile"
            )
        profile_names = [profile.deployment_name for profile in self.profiles]
        duplicate_profile_names = sorted(
            name
            for name, count in Counter(profile_names).items()
            if count > 1
        )
        if duplicate_profile_names:
            errors.append(
                f"profile deployment names must be unique: {duplicate_profile_names}"
            )
        profile_ids = {profile.id for profile in self.profiles}
        for expectation in self.readiness_expectations:
            for profile_id in expectation.profile_refs:
                _require_reference(
                    errors,
                    expectation.id,
                    "profile_refs",
                    profile_id,
                    profile_ids,
                )
        _validate_event_contract_sources(self, errors)
        _validate_path_references(
            self,
            errors,
            asset_ids,
            contract_ids,
        )
        _validate_exception_references(self, errors, inventory_ids)
        _validate_ml_references(self, errors, asset_ids, contract_ids)
        if errors:
            raise ValueError("; ".join(errors))
        return self


def _identified_records(manifest: SolutionManifest) -> tuple[ManifestModel, ...]:
    records: list[ManifestModel] = [
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
        *manifest.source_owners,
    ]
    for entry in (*manifest.assets, *manifest.data_contracts):
        records.extend(entry.inventories)
        records.extend(entry.inventory_rules)
    for profile in manifest.profiles:
        records.extend(profile.required_acknowledgements)
        records.extend(profile.blockers)
    for contract in manifest.data_contracts:
        records.extend(contract.source_agreements)
        if contract.envelope is not None:
            records.append(contract.envelope)
        records.extend(contract.events)
    return tuple(records)


def manifest_sha256(manifest: SolutionManifest) -> str:
    """Return a formatting-independent SHA-256 for manifest content."""

    document = manifest.model_dump(
        mode="json",
        exclude_none=True,
        exclude_unset=True,
    )
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_path_references(
    manifest: SolutionManifest,
    errors: list[str],
    asset_ids: set[str],
    contract_ids: set[str],
) -> None:
    assets = {asset.id: asset for asset in manifest.assets}
    event_owners = {
        event.id: contract.id
        for contract in manifest.data_contracts
        for event in contract.events
    }
    event_ids = set(event_owners)
    exception_ids = {exception.id for exception in manifest.exceptions}
    exceptions = {exception.id: exception for exception in manifest.exceptions}
    expected_contract_ids = {
        contract.id
        for contract in manifest.data_contracts
        if contract.kind in {"event", "historical", "semantic_model"}
    }
    terminal_events: dict[str, set[str]] = {}
    terminal_targets: dict[str, set[str]] = {}

    for path in manifest.event_paths:
        _require_reference(
            errors,
            path.id,
            "source_asset_id",
            path.source_asset_id,
            asset_ids,
        )
        _require_reference(
            errors,
            path.id,
            "target_asset_id",
            path.target_asset_id,
            asset_ids,
        )
        for contract_id in path.contract_ids:
            _require_reference(
                errors,
                path.id,
                "contract_ids",
                contract_id,
                contract_ids,
            )
        for event_id in path.event_ids:
            _require_reference(errors, path.id, "event_ids", event_id, event_ids)
            owner_id = event_owners.get(event_id)
            if owner_id is not None and owner_id not in path.contract_ids:
                errors.append(
                    f"{path.id}.event_ids references {event_id!r} without "
                    f"its owning contract {owner_id!r}"
                )

        if set(path.contract_ids) != expected_contract_ids:
            errors.append(
                f"{path.id}.contract_ids must identify the event, historical, "
                "and semantic-model contracts"
            )
        source_kind = (
            "streaming-notebook"
            if path.path_kind == "emitted"
            else "eventhouse"
        )
        _require_asset_kind(
            errors,
            path.id,
            "source_asset_id",
            assets.get(path.source_asset_id),
            source_kind,
        )
        has_semantic_target = any(
            target.layer == "semantic" for target in path.targets
        )
        target_kind = "semantic-model" if has_semantic_target else "lakehouse"
        _require_asset_kind(
            errors,
            path.id,
            "target_asset_id",
            assets.get(path.target_asset_id),
            target_kind,
        )
        _validate_path_mode(path, errors)

        if path.terminal_exception_id is not None:
            _require_reference(
                errors,
                path.id,
                "terminal_exception_id",
                path.terminal_exception_id,
                exception_ids,
            )
            terminal_events.setdefault(path.terminal_exception_id, set()).update(
                path.event_ids
            )
            terminal_targets.setdefault(
                path.terminal_exception_id,
                set(),
            ).add(path.targets[-1].name)
            exception = exceptions.get(path.terminal_exception_id)
            if exception is not None and exception.kind != "streaming-only":
                errors.append(
                    f"{path.id}.terminal_exception_id must reference a "
                    "streaming-only exception"
                )

    for exception in manifest.exceptions:
        covered_events = terminal_events.get(exception.id, set())
        if (
            exception.event_ids or exception.id in terminal_events
        ) and set(exception.event_ids) != covered_events:
            errors.append(
                f"{exception.id}.event_ids must equal terminal path coverage"
            )
        if exception.id in terminal_targets:
            if set(exception.target_names) != terminal_targets[exception.id]:
                errors.append(
                    f"{exception.id}.target_names must equal terminal path coverage"
                )

    paired_paths = [
        path
        for path in manifest.event_paths
        if path.mode == "paired-lifecycle-to-silver"
    ]
    paired_routes = Counter(
        (
            tuple(
                target.name
                for target in path.targets
                if target.layer == "silver"
            ),
            tuple(
                target.name
                for target in path.targets
                if target.layer == "gold"
            ),
        )
        for path in paired_paths
    )
    if any(count != 2 for count in paired_routes.values()):
        errors.append(
            "paired-lifecycle-to-silver paths must occur in route-identical pairs"
        )


def _validate_event_contract_sources(
    manifest: SolutionManifest,
    errors: list[str],
) -> None:
    event_contracts = [
        contract
        for contract in manifest.data_contracts
        if contract.kind == "event"
    ]
    if len(event_contracts) != 1:
        errors.append("manifest must define exactly one event data contract")
        return
    contract = event_contracts[0]
    assert contract.envelope is not None
    envelope_source = contract.envelope.source
    selector = envelope_source.selector
    if (
        selector is None
        or selector.kind != "python_symbol"
        or selector.value != "ENVELOPE"
    ):
        errors.append(
            f"{contract.envelope.id}.source must select the ENVELOPE symbol"
        )
    if envelope_source not in contract.sources:
        errors.append(
            f"{contract.envelope.id}.source must also appear in "
            f"{contract.id}.sources"
        )
    payload_sources = [
        source
        for source in contract.sources
        if source.selector is not None
        and source.selector.kind == "python_symbol"
        and source.selector.value == "EVENT_PAYLOADS"
    ]
    if len(payload_sources) != 1:
        errors.append(
            f"{contract.id}.sources must select EVENT_PAYLOADS exactly once"
        )
    elif payload_sources[0].path != envelope_source.path:
        errors.append(
            f"{contract.id} envelope and payload symbols must share one source"
        )

    stream_assets = [
        asset for asset in manifest.assets if asset.kind == "streaming-notebook"
    ]
    if len(stream_assets) != 1:
        errors.append(
            "manifest must define exactly one streaming-notebook asset"
        )
    elif payload_sources and payload_sources[0] not in stream_assets[0].sources:
        errors.append(
            f"{stream_assets[0].id}.sources must own the event payload source"
        )


def _require_asset_kind(
    errors: list[str],
    path_id: str,
    field: str,
    asset: Asset | None,
    expected_kind: str,
) -> None:
    if asset is not None and asset.kind != expected_kind:
        errors.append(
            f"{path_id}.{field} must reference an {expected_kind!r} asset"
        )


def _validate_path_mode(path: EventPath, errors: list[str]) -> None:
    has_semantic = any(target.layer == "semantic" for target in path.targets)
    has_exception = path.terminal_exception_id is not None
    if path.mode == "derived-last-touch-7d":
        if path.path_kind != "derived" or not has_semantic or has_exception:
            errors.append(
                f"{path.id}.mode derived-last-touch-7d requires a derived "
                "semantic route"
            )
        return
    if path.path_kind != "emitted":
        errors.append(f"{path.id}.mode is not valid for a derived path")
        return
    if path.mode == "direct-spark-kusto-to-streaming-only-silver":
        layers = {target.layer for target in path.targets}
        if layers != {"eventhouse", "silver"} or not has_exception:
            errors.append(
                f"{path.id}.mode streaming-only requires an Eventhouse-to-"
                "Silver route with a terminal exception"
            )
        return
    if not has_semantic or has_exception:
        errors.append(
            f"{path.id}.mode requires a semantic terminal without an exception"
        )


def _validate_exception_references(
    manifest: SolutionManifest,
    errors: list[str],
    inventory_ids: set[str],
) -> None:
    owner_ids = {owner.id for owner in manifest.source_owners}
    event_ids = {
        event.id for contract in manifest.data_contracts for event in contract.events
    }
    for exception in manifest.exceptions:
        _require_reference(
            errors,
            exception.id,
            "verification_owner",
            exception.verification_owner,
            owner_ids,
        )
        for event_id in exception.event_ids:
            _require_reference(
                errors,
                exception.id,
                "event_ids",
                event_id,
                event_ids,
            )
        if exception.inventory_comparison:
            _require_inventory_relationship(errors, exception.inventory_comparison, inventory_ids)


def _validate_ml_references(
    manifest: SolutionManifest,
    errors: list[str],
    asset_ids: set[str],
    contract_ids: set[str],
) -> None:
    for contract in manifest.ml_contracts:
        for asset_id in contract.asset_ids:
            _require_reference(errors, contract.id, "asset_ids", asset_id, asset_ids)
        for data_contract_id in contract.data_contract_ids:
            _require_reference(
                errors,
                contract.id,
                "data_contract_ids",
                data_contract_id,
                contract_ids,
            )


def _require_reference(
    errors: list[str],
    record_id: str,
    field: str,
    reference: str,
    valid_ids: set[str],
) -> None:
    if reference not in valid_ids:
        errors.append(f"{record_id}.{field} references unknown ID {reference!r}")


def _require_inventory_relationship(
    errors: list[str],
    relationship: InventoryRelationship,
    inventory_ids: set[str],
) -> None:
    for field in ("left_inventory_id", "right_inventory_id"):
        reference = getattr(relationship, field)
        _require_reference(
            errors,
            "inventory relationship",
            field,
            reference,
            inventory_ids,
        )
