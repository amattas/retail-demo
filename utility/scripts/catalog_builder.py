"""Compose the preset store-type product catalogs from category trees. Deterministic.

The tree schema and builder live in ``retail_setup.dictionaries.catalog`` so the
same code builds preset catalogs here and custom business catalogs at runtime.

Usage:
    python scripts/catalog_builder.py            # builds all three tree-based types
"""

from pathlib import Path

from retail_setup.dictionaries.catalog import DEFAULT_SEED, build_catalog, write_catalog

UTILITY_ROOT = Path(__file__).resolve().parents[1]
OUT = UTILITY_ROOT / "data" / "dictionaries"

__all__ = ["DEFAULT_SEED", "OUT", "build_catalog", "write_store_type"]


def write_store_type(tree: dict, seed: int) -> None:
    written = write_catalog(tree, seed, OUT / tree["store_type"])
    for path in written.values():
        print(f"wrote {path.relative_to(UTILITY_ROOT)}")


if __name__ == "__main__":
    from catalogs.grocery import GROCERY_TREE
    from catalogs.hardware import HARDWARE_TREE
    from catalogs.luxury import LUXURY_TREE

    write_store_type(GROCERY_TREE, seed=DEFAULT_SEED)
    write_store_type(HARDWARE_TREE, seed=DEFAULT_SEED)
    write_store_type(LUXURY_TREE, seed=DEFAULT_SEED)