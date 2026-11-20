"""Per-deployment storage for custom business layouts.

Layout (gitignored, beside the environment's generation config)::

    utility/config/environments/<env>.customization/
        customization.json                 # the saved CustomizationSpec
        logo.svg                           # optional sanitized logo (SHA-256 in the spec)
        dictionaries/_shared/*.json        # copied from the packaged dictionaries
        dictionaries/custom-<slug>/{profile,products,brands}.json

The dictionaries are rebuilt from the spec deterministically, so the spec is the
only input that matters; :func:`materialize` is idempotent.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import yaml

from retail_setup.config.generation import resolve_generation_config_path
from retail_setup.customization.logo import Logo, restore_logo
from retail_setup.customization.spec import CUSTOM_PREFIX, CustomizationSpec
from retail_setup.customization.theme import Theme, derive_theme
from retail_setup.dictionaries.catalog import DEFAULT_SEED, catalog_json, write_catalog
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries

SPEC_FILE = "customization.json"
SPEC_HASH_FILE = ".spec-sha256"
SHARED_FILES = ("first_names.json", "last_names.json", "geographies.json", "tax_rates.json")
CUSTOM_FILES = ("profile.json", "products.json", "brands.json")


def customization_dir(repo_root: Path, env: str) -> Path:
    return repo_root / "utility" / "config" / "environments" / f"{env}.customization"


def dictionary_root(repo_root: Path, env: str) -> Path:
    return customization_dir(repo_root, env) / "dictionaries"


def is_custom_store_type(store_type: str) -> bool:
    return store_type.startswith(CUSTOM_PREFIX)


def save_spec(spec: CustomizationSpec, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / SPEC_FILE
    path.write_text(
        json.dumps(spec.model_dump(mode="json"), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return path


def load_spec(directory: Path) -> CustomizationSpec | None:
    path = directory / SPEC_FILE
    if not path.is_file():
        return None
    return CustomizationSpec.model_validate_json(path.read_text(encoding="utf-8"))


def materialize(
    spec: CustomizationSpec, directory: Path, *, shared_root: Path | None = None
) -> Path:
    """Write the spec's dictionary set under ``directory/dictionaries``; return that root."""

    shared_root = shared_root or default_dictionary_root()
    root = directory / "dictionaries"
    if root.exists():
        shutil.rmtree(root)
    (root / "_shared").mkdir(parents=True)
    for name in SHARED_FILES:
        shutil.copyfile(shared_root / "_shared" / name, root / "_shared" / name)
    type_dir = root / spec.store_type
    write_catalog(spec.catalog_tree, DEFAULT_SEED, type_dir)
    profile = spec.profile.model_dump(mode="json")
    (type_dir / "profile.json").write_text(catalog_json(profile), encoding="utf-8", newline="\n")
    # Validate exactly what Fabric will load.
    load_dictionaries(root, spec.store_type)
    (root / SPEC_HASH_FILE).write_text(spec.content_hash() + "\n", encoding="utf-8")
    return root


def custom_file_digests(root: Path, store_type: str) -> dict[str, str]:
    """``{"<store_type>/<file>": sha256}`` for the custom files uploaded to Fabric."""

    return {
        f"{store_type}/{name}": hashlib.sha256((root / store_type / name).read_bytes()).hexdigest()
        for name in CUSTOM_FILES
    }


def _configured_store_type(repo_root: Path, env: str) -> str | None:
    path = resolve_generation_config_path(repo_root, env)
    if not path.is_file():
        return None
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    value = data.get("store_type") if isinstance(data, dict) else None
    return value if isinstance(value, str) else None


def active_spec(repo_root: Path, env: str) -> CustomizationSpec | None:
    """The env's spec when its generation config selects the custom store type.

    Raises ValueError when the config selects a custom type whose spec is missing
    or describes a different store type.
    """

    store_type = _configured_store_type(repo_root, env)
    if store_type is None or not is_custom_store_type(store_type):
        return None
    spec = load_spec(customization_dir(repo_root, env))
    if spec is None:
        raise ValueError(
            f"store_type {store_type!r} is custom but no {SPEC_FILE} exists for "
            f"environment {env!r}; run `retail-setup customize --env {env}`."
        )
    if spec.store_type != store_type:
        raise ValueError(
            f"environment {env!r} selects {store_type!r} but its saved customization "
            f"is {spec.store_type!r}; re-run `retail-setup configure`."
        )
    return spec


def ensure_materialized(spec: CustomizationSpec, directory: Path) -> Path:
    """Return the spec's dictionary root, rebuilding it when missing or stale."""

    root = directory / "dictionaries"
    marker = root / SPEC_HASH_FILE
    current = marker.read_text(encoding="utf-8").strip() if marker.is_file() else None
    if current != spec.content_hash():
        materialize(spec, directory)
    return root


def active_dictionary_root(repo_root: Path, env: str) -> Path | None:
    """Materialized dictionary root for the env's active custom type, rebuilt if stale."""

    spec = active_spec(repo_root, env)
    if spec is None:
        return None
    return ensure_materialized(spec, customization_dir(repo_root, env))


def active_theme(repo_root: Path, env: str) -> Theme | None:
    spec = active_spec(repo_root, env)
    return None if spec is None else derive_theme(spec.palette, spec.display_name)


def active_logo(repo_root: Path, env: str) -> Logo | None:
    """The env's verified logo, or None when its layout has none.

    Raises LogoError when the saved logo is missing or altered and cannot be
    re-downloaded with the recorded SHA-256.
    """

    spec = active_spec(repo_root, env)
    if spec is None or spec.logo is None:
        return None
    return restore_logo(customization_dir(repo_root, env), spec.logo)
