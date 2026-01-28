"""
Dictionary data loading system for the retail data generator.

This module provides a system for loading dictionary data from Python
sourcedata modules. Data is validated against Pydantic models with
caching and error handling.

The sourcedata module provides pre-curated data organized by retail profile
(e.g., supercenter, fashion). This eliminates file I/O and ensures data
is version-controlled with the code.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from ..config.models import RetailConfig
from ..config.settings import load_config_with_fallback
from .exceptions import (
    DictionaryConsistencyError,
    DictionaryLoadError,
)
from .models import (
    FirstNameDict,
    GeographyDict,
    LastNameDict,
    ProductBrandDict,
    ProductDict,
    ProductTagDict,
    TaxJurisdiction,
)

# Import sourcedata for Python-based dictionary loading
try:
    from retail_datagen.sourcedata import default as sourcedata_default

    SOURCEDATA_AVAILABLE = True
except ImportError:
    SOURCEDATA_AVAILABLE = False
    sourcedata_default = None

logger = logging.getLogger(__name__)


@dataclass
class DictionaryInfo:
    """Information about a dictionary file and its configuration."""

    name: str
    filename: str
    model_class: type[BaseModel]
    required: bool = True
    expected_rows: int | None = None
    description: str = ""
    # Name of the attribute in sourcedata module (e.g., "GEOGRAPHIES")
    sourcedata_attr: str | None = None


@dataclass
class LoadResult:
    """Result of loading a dictionary file."""

    name: str
    data: list[BaseModel]
    load_time: float
    row_count: int
    validation_errors: list[str]
    warnings: list[str]


class DictionaryLoader:
    """
    Main dictionary loading system that loads data from sourcedata modules
    and validates them against Pydantic models.
    """

    # Dictionary definitions
    DICTIONARIES = {
        "geographies": DictionaryInfo(
            name="geographies",
            filename="geographies.csv",
            model_class=GeographyDict,
            expected_rows=1000,
            description="Geographic locations with synthetic addresses",
            sourcedata_attr="GEOGRAPHIES",
        ),
        "product_tags": DictionaryInfo(
            name="product_tags",
            filename="product_tags.csv",
            model_class=ProductTagDict,
            required=False,
            expected_rows=None,
            description="Optional product tag overlay (ProductName, Tags)",
            sourcedata_attr="PRODUCT_TAGS",
        ),
        "first_names": DictionaryInfo(
            name="first_names",
            filename="first_names.csv",
            model_class=FirstNameDict,
            expected_rows=250,
            description="Synthetic first names",
            sourcedata_attr="FIRST_NAMES",
        ),
        "last_names": DictionaryInfo(
            name="last_names",
            filename="last_names.csv",
            model_class=LastNameDict,
            expected_rows=250,
            description="Synthetic last names",
            sourcedata_attr="LAST_NAMES",
        ),
        "product_brands": DictionaryInfo(
            name="product_brands",
            filename="product_brands.csv",
            model_class=ProductBrandDict,
            expected_rows=500,
            description="Product brands with company relationships",
            sourcedata_attr="PRODUCT_BRANDS",
        ),
        "products": DictionaryInfo(
            name="products",
            filename="products.csv",
            model_class=ProductDict,
            expected_rows=10000,
            description="Product catalog with base pricing",
            sourcedata_attr="PRODUCTS",
        ),
        "tax_rates": DictionaryInfo(
            name="tax_rates",
            filename="tax_rates.csv",
            model_class=TaxJurisdiction,
            expected_rows=164,
            description="Tax rates by jurisdiction (state, county, city)",
            sourcedata_attr="TAX_RATES",
        ),
    }

    def __init__(
        self,
        dictionary_path: str | Path | None = None,
        config: RetailConfig | None = None,
        use_cache: bool = True,
        encoding: str = "utf-8",
        chunk_size: int = 1000,
        max_workers: int = 4,
    ):
        """
        Initialize the DictionaryLoader.

        Args:
            dictionary_path: Path to dictionary files directory
                (deprecated, kept for compatibility)
            config: Configuration object (will be loaded if not provided)
            use_cache: Whether to use caching for loaded data
                (deprecated, kept for compatibility)
            encoding: File encoding to use (deprecated, kept for compatibility)
            chunk_size: Size of chunks for processing large files
                (deprecated, kept for compatibility)
            max_workers: Number of threads for parallel loading
        """
        self.config = config or load_config_with_fallback()

        # Set dictionary path from config or parameter (kept for backward compatibility)
        if dictionary_path:
            self.dictionary_path = Path(dictionary_path)
        else:
            self.dictionary_path = Path(self.config.paths.dictionaries)

        self.encoding = encoding
        self.chunk_size = chunk_size
        self.max_workers = max_workers

        # Storage for loaded data (in-memory cache)
        self._loaded_data: dict[str, list[BaseModel]] = {}
        self._load_results: dict[str, LoadResult] = {}

        logger.info("DictionaryLoader initialized")

    def _check_data_consistency(self) -> None:
        """
        Check consistency across loaded dictionaries.

        Raises:
            DictionaryConsistencyError: If consistency checks fail
        """
        if not self._loaded_data:
            return

        logger.debug("Data consistency checks passed")

    def _load_from_sourcedata(
        self, dict_info: DictionaryInfo
    ) -> tuple[list[BaseModel], list[str], list[str]] | None:
        """
        Try to load dictionary data from the sourcedata module.

        Args:
            dict_info: Dictionary information

        Returns:
            Tuple of (validated_data, validation_errors, warnings) or None if
            sourcedata is not available.
        """
        if not SOURCEDATA_AVAILABLE or sourcedata_default is None:
            return None

        if dict_info.sourcedata_attr is None:
            return None

        # Try to get the data from sourcedata
        try:
            raw_data = getattr(sourcedata_default, dict_info.sourcedata_attr, None)
        except Exception as e:
            logger.debug(
                f"Failed to get {dict_info.sourcedata_attr} from sourcedata: {e}"
            )
            return None

        if raw_data is None:
            return None

        # Validate each row against the Pydantic model
        validated_data = []
        validation_errors = []
        warnings = []

        for idx, row_dict in enumerate(raw_data):
            try:
                instance = dict_info.model_class(**row_dict)
                validated_data.append(instance)
            except ValidationError as e:
                error_details = []
                for error in e.errors():
                    field = error.get("loc", ["unknown"])[0]
                    msg = error.get("msg", "Unknown validation error")
                    error_details.append(f"{field}: {msg}")
                validation_errors.append(f"Row {idx + 1}: {'; '.join(error_details)}")
            except Exception as e:
                validation_errors.append(f"Row {idx + 1}: Unexpected error: {e}")

        # Check expected row count
        if dict_info.expected_rows and len(validated_data) != dict_info.expected_rows:
            warnings.append(
                f"Expected {dict_info.expected_rows} rows, got {len(validated_data)}"
            )

        warnings.append("Loaded from sourcedata module")

        return validated_data, validation_errors, warnings

    def load_dictionary(self, name: str, force_reload: bool = False) -> LoadResult:
        """
        Load a single dictionary from sourcedata module.

        Args:
            name: Name of dictionary to load
            force_reload: Whether to bypass cache and force reload

        Returns:
            LoadResult: Result of loading operation

        Raises:
            DictionaryLoadError: If loading fails or sourcedata is not available
        """
        if name not in self.DICTIONARIES:
            available = list(self.DICTIONARIES.keys())
            raise DictionaryLoadError(
                f"Unknown dictionary '{name}'. Available: {available}"
            )

        dict_info = self.DICTIONARIES[name]
        start_time = time.time()

        # Check if already loaded in memory (unless forcing reload)
        if not force_reload and name in self._loaded_data:
            cached_data = self._loaded_data[name]
            load_time = time.time() - start_time
            result = LoadResult(
                name=name,
                data=cached_data,
                load_time=load_time,
                row_count=len(cached_data),
                validation_errors=[],
                warnings=["Data loaded from in-memory cache"],
            )
            self._load_results[name] = result
            return result

        # Load from sourcedata module
        #
        # CACHING NOTE: Sourcedata loading uses in-memory caching via _loaded_data.
        # This is appropriate because:
        # 1. Sourcedata is immutable at runtime (compiled into the module)
        # 2. No file I/O means no need for mtime-based invalidation
        # 3. The _loaded_data cache provides the same deduplication benefit
        sourcedata_result = self._load_from_sourcedata(dict_info)
        if sourcedata_result is not None:
            validated_data, validation_errors, warnings = sourcedata_result
            load_time = time.time() - start_time

            result = LoadResult(
                name=name,
                data=validated_data,
                load_time=load_time,
                row_count=len(validated_data),
                validation_errors=validation_errors,
                warnings=warnings,
            )

            self._loaded_data[name] = validated_data
            self._load_results[name] = result

            logger.info(
                f"Loaded {name} from sourcedata: {len(validated_data)} rows, "
                f"{len(validation_errors)} errors "
                f"in {load_time:.2f}s"
            )

            return result

        # Sourcedata is not available or dictionary not found
        load_time = time.time() - start_time
        error_msg = (
            f"Failed to load dictionary '{name}'. "
            f"Sourcedata module is not available or does not contain "
            f"'{dict_info.sourcedata_attr}'. "
            f"Please ensure the retail_datagen.sourcedata package is "
            f"properly installed."
        )
        raise DictionaryLoadError(error_msg)

    def load_all_dictionaries(
        self, parallel: bool = True, fail_on_error: bool = True
    ) -> dict[str, LoadResult]:
        """
        Load all required dictionaries.

        Args:
            parallel: Whether to load dictionaries in parallel
            fail_on_error: Whether to stop on first error or continue

        Returns:
            Dict[str, LoadResult]: Results for each dictionary

        Raises:
            DictionaryLoadError: If loading fails and fail_on_error=True
        """
        logger.info("Starting to load all dictionaries")
        start_time = time.time()

        results = {}
        errors = []

        if parallel and self.max_workers > 1:
            # Parallel loading
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_name = {
                    executor.submit(self.load_dictionary, name): name
                    for name in self.DICTIONARIES.keys()
                }

                for future in as_completed(future_to_name):
                    name = future_to_name[future]
                    try:
                        result = future.result()
                        results[name] = result
                    except Exception as e:
                        errors.append((name, e))
                        if fail_on_error:
                            # Cancel remaining futures
                            for f in future_to_name:
                                f.cancel()
                            break
        else:
            # Sequential loading
            for name in self.DICTIONARIES.keys():
                try:
                    result = self.load_dictionary(name)
                    results[name] = result
                except Exception as e:
                    errors.append((name, e))
                    if fail_on_error:
                        break

        # Check for errors
        if errors and fail_on_error:
            error_msg = "; ".join([f"{name}: {error}" for name, error in errors])
            raise DictionaryLoadError(f"Failed to load dictionaries: {error_msg}")

        # Perform consistency checks if we have data
        if results and not errors:
            try:
                self._check_data_consistency()
            except DictionaryConsistencyError as e:
                if fail_on_error:
                    raise
                else:
                    logger.warning(f"Data consistency check failed: {e}")

        total_time = time.time() - start_time
        logger.info(
            f"Loaded {len(results)} dictionaries in {total_time:.2f}s "
            f"({len(errors)} errors)"
        )

        return results

    def get_data(self, name: str) -> list[BaseModel]:
        """
        Get loaded data for a specific dictionary.

        Args:
            name: Name of dictionary

        Returns:
            List[BaseModel]: Loaded data

        Raises:
            DictionaryLoadError: If dictionary not loaded
        """
        if name not in self._loaded_data:
            raise DictionaryLoadError(f"Dictionary '{name}' not loaded")

        return self._loaded_data[name]

    def get_load_result(self, name: str) -> LoadResult:
        """
        Get load result for a specific dictionary.

        Args:
            name: Name of dictionary

        Returns:
            LoadResult: Load result information

        Raises:
            DictionaryLoadError: If dictionary not loaded
        """
        if name not in self._load_results:
            raise DictionaryLoadError(f"Dictionary '{name}' not loaded")

        return self._load_results[name]

    def is_loaded(self, name: str) -> bool:
        """Check if a dictionary is loaded."""
        return name in self._loaded_data

    def get_loaded_dictionaries(self) -> list[str]:
        """Get list of loaded dictionary names."""
        return list(self._loaded_data.keys())

    def clear_cache(self) -> None:
        """Clear the in-memory dictionary cache."""
        self._loaded_data.clear()
        self._load_results.clear()
        logger.debug("Dictionary cache cleared")

    def get_summary(self) -> dict[str, Any]:
        """
        Get summary information about loaded dictionaries.

        Returns:
            Dict with summary information
        """
        summary = {
            "total_dictionaries": len(self.DICTIONARIES),
            "loaded_dictionaries": len(self._loaded_data),
            "dictionaries": {},
        }

        for name, result in self._load_results.items():
            summary["dictionaries"][name] = {
                "rows": result.row_count,
                "load_time": result.load_time,
                "errors": len(result.validation_errors),
                "warnings": len(result.warnings),
            }

        return summary

    # Convenience methods for common dictionary access patterns
    def load_geographies(self) -> list[GeographyDict]:
        """Load and return geography dictionary data."""
        if not self.is_loaded("geographies"):
            self.load_dictionary("geographies")
        return self.get_data("geographies")

    def load_first_names(self) -> list[FirstNameDict]:
        """Load and return first names dictionary data."""
        if not self.is_loaded("first_names"):
            self.load_dictionary("first_names")
        return self.get_data("first_names")

    def load_last_names(self) -> list[LastNameDict]:
        """Load and return last names dictionary data."""
        if not self.is_loaded("last_names"):
            self.load_dictionary("last_names")
        return self.get_data("last_names")

    def load_products(self) -> list[ProductDict]:
        """Load and return products dictionary data."""
        if not self.is_loaded("products"):
            self.load_dictionary("products")
        return self.get_data("products")

    def load_product_brands(self) -> list[ProductBrandDict]:
        """Load and return product brands dictionary data."""
        if not self.is_loaded("product_brands"):
            self.load_dictionary("product_brands")
        return self.get_data("product_brands")

    def load_tax_rates(self) -> list[TaxJurisdiction]:
        """Load and return tax rates dictionary data."""
        if not self.is_loaded("tax_rates"):
            self.load_dictionary("tax_rates")
        return self.get_data("tax_rates")
