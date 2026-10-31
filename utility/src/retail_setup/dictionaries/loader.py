"""Load and validate dictionary JSON sets (shared + one store type)."""

import json
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path

from pydantic import BaseModel, ValidationError

from retail_setup.dictionaries.models import (
    GeographyEntry,
    NameEntry,
    ProductBrandEntry,
    ProductEntry,
    ProductTagEntry,
    StoreTypeProfile,
    TaxJurisdictionEntry,
)


@dataclass
class DictionarySet:
    store_type: str
    profile: StoreTypeProfile
    first_names: list[NameEntry] = field(default_factory=list)
    last_names: list[NameEntry] = field(default_factory=list)
    geographies: list[GeographyEntry] = field(default_factory=list)
    tax_rates: list[TaxJurisdictionEntry] = field(default_factory=list)
    products: list[ProductEntry] = field(default_factory=list)
    brands: list[ProductBrandEntry] = field(default_factory=list)
    tags: list[ProductTagEntry] = field(default_factory=list)


def default_dictionary_root() -> Path:
    """Return the packaged dictionary root, with a source-tree fallback."""
    packaged_root = files("retail_setup").joinpath("resources", "dictionaries")
    if packaged_root.is_dir():
        if not isinstance(packaged_root, Path):
            raise RuntimeError("Packaged dictionaries are not available as filesystem paths.")
        return packaged_root

    source_root = Path(__file__).resolve().parents[3] / "data" / "dictionaries"
    if source_root.is_dir():
        return source_root
    raise RuntimeError("Default dictionaries are missing from the installed package.")


def available_store_types(root: Path) -> list[str]:
    if not root.is_dir():
        return []
    return sorted(
        p.name
        for p in root.iterdir()
        if p.is_dir() and not p.name.startswith("_") and (p / "profile.json").exists()
    )


def load_list(path: Path, model: type[BaseModel]) -> list:
    """Load and validate a JSON array file, returning a list of model instances.

    Raises ValueError with filename and row index on validation failure.
    """
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        raise ValueError(f"missing dictionary file: {path}") from None
    if not isinstance(raw, list):
        raise ValueError(f"{path.name}: expected a JSON array")
    result = []
    for i, row in enumerate(raw):
        try:
            result.append(model.model_validate(row))
        except ValidationError as exc:
            raise ValueError(f"{path.name}[{i}]: {exc}") from exc
    return result


# Keep the private alias for any callers that haven't migrated yet.
_load_list = load_list


def load_dictionaries(root: Path, store_type: str) -> DictionarySet:
    type_dir = root / store_type
    if not (type_dir / "profile.json").exists():
        raise ValueError(
            f"unknown store type {store_type!r}; available: {available_store_types(root)}"
        )
    shared = root / "_shared"

    try:
        profile = StoreTypeProfile.model_validate(
            json.loads((type_dir / "profile.json").read_text())
        )
    except (json.JSONDecodeError, ValidationError) as exc:
        raise ValueError(f"profile.json: {exc}") from exc
    if profile.store_type != store_type:
        raise ValueError(
            f"profile.json store_type {profile.store_type!r} does not match folder {store_type!r}"
        )

    tags_path = type_dir / "tags.json"
    return DictionarySet(
        store_type=store_type,
        profile=profile,
        first_names=load_list(shared / "first_names.json", NameEntry),
        last_names=load_list(shared / "last_names.json", NameEntry),
        geographies=load_list(shared / "geographies.json", GeographyEntry),
        tax_rates=load_list(shared / "tax_rates.json", TaxJurisdictionEntry),
        products=load_list(type_dir / "products.json", ProductEntry),
        brands=load_list(type_dir / "brands.json", ProductBrandEntry),
        tags=load_list(tags_path, ProductTagEntry) if tags_path.exists() else [],
    )
