"""Typed access to the retail-demo solution manifest."""

from .data_validation import (
    DataContractSnapshot,
    DataContractValidation,
    derive_data_contract_snapshot,
    validate_data_contracts,
)
from .loader import (
    MANIFEST_RELATIVE_PATH,
    ManifestLoadError,
    load_repository_manifest,
    load_solution_manifest,
)
from .manifest import SolutionManifest, manifest_sha256
from .profiles import (
    ProfileResolutionError,
    ResolvedAsset,
    ResolvedProfile,
    deployment_profile_names,
    resolve_profile,
)
from .sources import ManifestSourceError
from .validation import (
    InventoryDrift,
    RepositoryValidation,
    derive_manifest_inventories,
    validate_manifest_repository,
    validate_manifest_sources,
)

__all__ = [
    "MANIFEST_RELATIVE_PATH",
    "DataContractSnapshot",
    "DataContractValidation",
    "InventoryDrift",
    "ManifestLoadError",
    "ManifestSourceError",
    "ProfileResolutionError",
    "RepositoryValidation",
    "ResolvedAsset",
    "ResolvedProfile",
    "SolutionManifest",
    "derive_manifest_inventories",
    "derive_data_contract_snapshot",
    "load_repository_manifest",
    "load_solution_manifest",
    "manifest_sha256",
    "deployment_profile_names",
    "resolve_profile",
    "validate_manifest_repository",
    "validate_manifest_sources",
    "validate_data_contracts",
]
