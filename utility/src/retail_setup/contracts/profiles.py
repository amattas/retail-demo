"""Strict executable deployment-profile resolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .manifest import SolutionManifest, manifest_sha256
from .models import (
    Asset,
    Profile,
    ProfileAcknowledgement,
    ProfileBlocker,
    ProfileBoundaries,
    PublicationExpectation,
)
from .validation import RepositoryValidation

AssetDisposition = Literal[
    "terraform",
    "workspace",
    "notebook-group",
    "kql",
    "post-publish",
    "manual",
]

_ASSET_DISPOSITIONS: dict[str, AssetDisposition] = {
    "lakehouse": "workspace",
    "eventhouse": "kql",
    "streaming-notebook": "notebook-group",
    "semantic-model": "workspace",
    "report": "workspace",
    "data-pipelines": "workspace",
    "kql-queryset": "workspace",
    "dashboard-templates": "manual",
    "rule-definitions": "manual",
    "task-flow": "post-publish",
    "ml-notebooks": "notebook-group",
    "ontology": "notebook-group",
    "data-agents": "workspace",
    "custom-spark-pool": "terraform",
}

_GROUP_ASSET_REQUIREMENTS = {
    "core": "asset.lakehouse",
    "setup": "asset.lakehouse",
    "utility": "asset.lakehouse",
    "stream": "asset.stream-events",
    "ml-required": "asset.ml-notebooks",
    "ml-optional": "asset.ml-notebooks",
    "ml-experimental": "asset.ml-notebooks",
    "ontology": "asset.ontology",
}

_ASSET_REQUIRED_GROUPS = {
    "asset.stream-events": "stream",
    "asset.ontology": "ontology",
}
_ML_GROUPS = frozenset({"ml-required", "ml-optional", "ml-experimental"})

_ASSET_ITEM_TYPES = {
    "asset.lakehouse": ("Lakehouse",),
    "asset.semantic-model": ("SemanticModel",),
    "asset.report": ("Report",),
    "asset.kql-queryset": ("KQLQueryset",),
    "asset.data-agents": ("DataAgent",),
}

_DEFAULT_ITEM_TYPE_ORDER = (
    "Lakehouse",
    "Notebook",
    "SemanticModel",
    "Report",
    "KQLQueryset",
    "DataPipeline",
    "MLExperiment",
    "DataAgent",
)


class ProfileResolutionError(ValueError):
    """Raised when a profile cannot be resolved to a safe exact inventory."""


@dataclass(frozen=True)
class ResolvedAsset:
    """One selected manifest asset and its deployment classification."""

    id: str
    name: str
    kind: str
    description: str
    support_status: str
    disposition: AssetDisposition
    direct: bool


@dataclass(frozen=True)
class ResolvedProfile:
    """Dependency-closed, classified deployment inventory."""

    id: str
    name: str
    deployment_name: str
    description: str
    support_status: str
    manifest_version: str
    manifest_hash: str
    assets: tuple[ResolvedAsset, ...]
    notebook_groups: tuple[str, ...]
    pipeline_refs: tuple[str, ...]
    kql_scripts: tuple[str, ...]
    item_types_in_scope: tuple[str, ...]
    post_deploy_pipeline_ref: str | None
    reporting_gate_pipeline_ref: str | None
    post_reporting_pipeline_refs: tuple[str, ...]
    required_acknowledgements: tuple[ProfileAcknowledgement, ...]
    blockers: tuple[ProfileBlocker, ...]
    boundaries: ProfileBoundaries
    publication: PublicationExpectation

    @property
    def asset_ids(self) -> tuple[str, ...]:
        return tuple(asset.id for asset in self.assets)

    @property
    def manual_asset_ids(self) -> tuple[str, ...]:
        return tuple(
            asset.id for asset in self.assets if asset.disposition == "manual"
        )

    @property
    def preview_asset_ids(self) -> tuple[str, ...]:
        return tuple(
            asset.id for asset in self.assets if asset.support_status == "preview"
        )

    def expected_staged_item_count(
        self,
        publication_phase: Literal["all", "infrastructure", "reporting"],
    ) -> int:
        """Return the manifest-declared staged item count for one phase."""

        if publication_phase == "infrastructure":
            return self.publication.infrastructure_item_count
        if publication_phase == "reporting":
            return self.publication.reporting_item_count
        return self.publication.all_item_count

    def expected_workspace_folders(
        self,
        publication_phase: Literal["all", "infrastructure", "reporting"],
    ) -> tuple[str, ...]:
        """Return the manifest-declared top-level folders for one phase."""

        if publication_phase == "infrastructure":
            return self.publication.infrastructure_folders
        if publication_phase == "reporting":
            return self.publication.reporting_folders
        return self.publication.all_folders

    def selects(self, asset_id: str) -> bool:
        return asset_id in self.asset_ids

    @property
    def uses_custom_pool(self) -> bool:
        return self.selects("asset.custom-spark-pool")

    @property
    def provisions_eventhouse(self) -> bool:
        return self.selects("asset.eventhouse")

    @property
    def deploys_task_flow(self) -> bool:
        return self.selects("asset.task-flow")


def deployment_profile_names(manifest: SolutionManifest) -> tuple[str, ...]:
    """Return executable profile names in manifest order."""

    return tuple(profile.deployment_name for profile in manifest.profiles)


def resolve_profile(
    manifest: SolutionManifest,
    validation: RepositoryValidation,
    deployment_name: str | None = None,
    *,
    available_item_types: tuple[str, ...] | list[str] | None = None,
    configured_kql_scripts: tuple[str, ...] | list[str] | None = None,
) -> ResolvedProfile:
    """Resolve one profile with dependency closure and strict classification."""

    profile = _find_profile(manifest, deployment_name)
    assets_by_id = {asset.id: asset for asset in manifest.assets}
    direct_ids = set(profile.asset_refs)
    selected_ids = _dependency_closure(profile, assets_by_id)

    resolved_assets: list[ResolvedAsset] = []
    for asset in manifest.assets:
        if asset.id not in selected_ids:
            continue
        disposition = _ASSET_DISPOSITIONS.get(asset.kind)
        if disposition is None:
            raise ProfileResolutionError(
                f"profile {profile.deployment_name!r} selects unclassified asset "
                f"{asset.id!r} (kind {asset.kind!r})"
            )
        resolved_assets.append(
            ResolvedAsset(
                id=asset.id,
                name=asset.name,
                kind=asset.kind,
                description=asset.description,
                support_status=asset.support_status,
                disposition=disposition,
                direct=asset.id in direct_ids,
            )
        )

    if "reset" in profile.group_refs:
        raise ProfileResolutionError(
            "destructive reset group is excluded from every automatic profile"
        )
    _validate_group_asset_agreement(profile, selected_ids)

    kql_scripts = _resolve_kql_scripts(
        profile,
        selected_ids,
        validation,
        configured_kql_scripts,
    )
    item_types = _resolve_item_types(
        profile,
        selected_ids,
        available_item_types,
    )
    return ResolvedProfile(
        id=profile.id,
        name=profile.name,
        deployment_name=profile.deployment_name,
        description=profile.description,
        support_status=profile.support_status,
        manifest_version=manifest.version,
        manifest_hash=manifest_sha256(manifest),
        assets=tuple(resolved_assets),
        notebook_groups=profile.group_refs,
        pipeline_refs=profile.pipeline_refs,
        kql_scripts=kql_scripts,
        item_types_in_scope=item_types,
        post_deploy_pipeline_ref=profile.post_deploy_pipeline_ref,
        reporting_gate_pipeline_ref=profile.reporting_gate_pipeline_ref,
        post_reporting_pipeline_refs=profile.post_reporting_pipeline_refs,
        required_acknowledgements=profile.required_acknowledgements,
        blockers=profile.blockers,
        boundaries=profile.boundaries,
        publication=profile.publication,
    )


def _find_profile(
    manifest: SolutionManifest,
    deployment_name: str | None,
) -> Profile:
    if deployment_name is None:
        return next(profile for profile in manifest.profiles if profile.default)
    for profile in manifest.profiles:
        if profile.deployment_name == deployment_name:
            return profile
    expected = ", ".join(deployment_profile_names(manifest))
    raise ProfileResolutionError(
        f"unknown deployment profile {deployment_name!r}; expected one of: {expected}"
    )


def _dependency_closure(
    profile: Profile,
    assets_by_id: dict[str, Asset],
) -> set[str]:
    selected: set[str] = set()
    active: list[str] = []

    def visit(asset_id: str) -> None:
        if asset_id in selected:
            return
        if asset_id in active:
            cycle = " -> ".join((*active, asset_id))
            raise ProfileResolutionError(f"asset dependency cycle: {cycle}")
        active.append(asset_id)
        asset = assets_by_id[asset_id]
        for dependency_id in asset.depends_on:
            visit(dependency_id)
        active.pop()
        selected.add(asset_id)

    for asset_id in profile.asset_refs:
        visit(asset_id)
    return selected


def _validate_group_asset_agreement(
    profile: Profile,
    selected_ids: set[str],
) -> None:
    groups = set(profile.group_refs)
    for group in groups:
        required_asset = _GROUP_ASSET_REQUIREMENTS.get(group)
        if required_asset is None:
            raise ProfileResolutionError(
                f"profile {profile.deployment_name!r} has no asset classification "
                f"for notebook group {group!r}"
            )
        if required_asset not in selected_ids:
            raise ProfileResolutionError(
                f"notebook group {group!r} requires selected asset "
                f"{required_asset!r}"
            )
    for asset_id, required_group in _ASSET_REQUIRED_GROUPS.items():
        if asset_id in selected_ids and required_group not in groups:
            raise ProfileResolutionError(
                f"selected asset {asset_id!r} requires notebook group "
                f"{required_group!r}"
            )
    if "asset.ml-notebooks" in selected_ids and "ml-required" not in groups:
        raise ProfileResolutionError(
            "selected asset 'asset.ml-notebooks' requires notebook group "
            "'ml-required'"
        )
    if groups & _ML_GROUPS and "ml-required" not in groups:
        raise ProfileResolutionError(
            "optional or experimental ML groups require 'ml-required'"
        )


def _resolve_kql_scripts(
    profile: Profile,
    selected_ids: set[str],
    validation: RepositoryValidation,
    configured: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    available = validation.inventories[
        "inventory.eventhouse.configured-kql-scripts"
    ]
    ordered = tuple(configured) if configured is not None else tuple(sorted(available))
    if len(ordered) != len(set(ordered)):
        raise ProfileResolutionError(
            "eventhouse.kql_scripts must not contain duplicates"
        )
    if frozenset(ordered) != available:
        raise ProfileResolutionError(
            "eventhouse.kql_scripts differs from the manifest-validated source inventory"
        )
    if "asset.eventhouse" not in selected_ids:
        return ()
    return ordered


def _resolve_item_types(
    profile: Profile,
    selected_ids: set[str],
    available: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    required: set[str] = set()
    for asset_id in selected_ids:
        required.update(_ASSET_ITEM_TYPES.get(asset_id, ()))
    if profile.group_refs:
        required.add("Notebook")
    if profile.pipeline_refs:
        required.add("DataPipeline")
    if "asset.ml-notebooks" in selected_ids:
        required.add("MLExperiment")

    available_order = (
        tuple(available) if available is not None else _DEFAULT_ITEM_TYPE_ORDER
    )
    if len(available_order) != len(set(available_order)):
        raise ProfileResolutionError(
            "deployment.item_types_in_scope must not contain duplicates"
        )
    missing = sorted(required - set(available_order))
    if missing:
        raise ProfileResolutionError(
            "deployment.item_types_in_scope does not support profile "
            f"{profile.deployment_name!r}: {missing}"
        )
    return tuple(item_type for item_type in available_order if item_type in required)
