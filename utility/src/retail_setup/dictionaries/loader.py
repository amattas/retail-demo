"""Load and validate dictionary JSON sets (shared + one store type)."""

import json
from dataclasses import dataclass, field
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
    """utility/data/dictionaries, resolved relative to this package."""
    return Path(__file__).resolve().parents[3] / "data" / "dictionaries"


def available_store_types(root: Path) -> list[str]:
    return sorted(
        p.name for p in root.iterdir()
        if p.is_dir() and not p.name.startswith("_") and (p / "profile.json").exists()
    )


def _load_list(path: Path, model: type[BaseModel]) -> list:
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        raise ValueError(f"missing dictionary file: {path}") from None
    if not isinstance(raw, list):
        raise ValueError(f"{path.name}: expected a JSON array")
    try:
        return [model.model_validate(row) for row in raw]
    except ValidationError as exc:
        raise ValueError(f"{path.name}: {exc}") from exc


def load_dictionaries(root: Path, store_type: str) -> DictionarySet:
    type_dir = root / store_type
    if not (type_dir / "profile.json").exists():
        raise ValueError(
            f"unknown store type {store_type!r}; available: {available_store_types(root)}"
        )
    shared = root / "_shared"

    profile = StoreTypeProfile.model_validate(json.loads((type_dir / "profile.json").read_text()))
    if profile.store_type != store_type:
        raise ValueError(
            f"profile.json store_type {profile.store_type!r} does not match folder {store_type!r}"
        )

    tags_path = type_dir / "tags.json"
    return DictionarySet(
        store_type=store_type,
        profile=profile,
        first_names=_load_list(shared / "first_names.json", NameEntry),
        last_names=_load_list(shared / "last_names.json", NameEntry),
        geographies=_load_list(shared / "geographies.json", GeographyEntry),
        tax_rates=_load_list(shared / "tax_rates.json", TaxJurisdictionEntry),
        products=_load_list(type_dir / "products.json", ProductEntry),
        brands=_load_list(type_dir / "brands.json", ProductBrandEntry),
        tags=_load_list(tags_path, ProductTagEntry) if tags_path.exists() else [],
    )
