"""
Dictionary data loading system for the retail data generator.

This module provides a comprehensive system for loading CSV dictionary files
from the data/dictionaries directory and validating them against Pydantic models.
It includes caching, error handling, and performance optimizations.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ValidationError

from ..config.models import RetailConfig
from ..config.settings import load_config_with_fallback
from .exceptions import (
    DictionaryConsistencyError,
    DictionaryEncodingError,
    DictionaryFileNotFoundError,
    DictionaryLoadError,
    DictionaryParsingError,
    DictionarySchemaError,
    DictionaryValidationError,
)
from .models import (
    FirstNameDict,
    GeographyDict,
    LastNameDict,
    ProductBrandDict,
    ProductCompanyDict,
    ProductDict,
    ProductTagDict,
    TaxJurisdiction,
)

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


@dataclass
class LoadResult:
    """Result of loading a dictionary file."""

    name: str
    data: list[BaseModel]
    load_time: float
    row_count: int
    validation_errors: list[str]
    warnings: list[str]


class DictionaryCache:
    """Simple in-memory cache for loaded dictionary data."""

    def __init__(self, max_age_seconds: int = 3600):  # 1 hour default
        self.max_age_seconds = max_age_seconds
        self._cache: dict[str, dict[str, Any]] = {}

    def _get_cache_key(self, file_path: Path, file_mtime: float) -> str:
        """Generate cache key based on file path and modification time."""
        return f"{file_path}:{file_mtime}"

    def get(self, file_path: Path) -> list[BaseModel] | None:
        """Get cached data if available and not expired."""
        try:
            file_mtime = file_path.stat().st_mtime
            cache_key = self._get_cache_key(file_path, file_mtime)

            if cache_key in self._cache:
                cache_entry = self._cache[cache_key]

                # Check if cache entry is still valid
                if time.time() - cache_entry["timestamp"] < self.max_age_seconds:
                    logger.debug(f"Cache hit for {file_path}")
                    return cache_entry["data"]
                else:
                    # Remove expired entry
                    del self._cache[cache_key]
                    logger.debug(f"Cache expired for {file_path}")

            return None

        except OSError:
            # File doesn't exist or can't be accessed
            return None

    def set(self, file_path: Path, data: list[BaseModel]) -> None:
        """Cache the loaded data."""
        try:
            file_mtime = file_path.stat().st_mtime
            cache_key = self._get_cache_key(file_path, file_mtime)

            self._cache[cache_key] = {"data": data, "timestamp": time.time()}

            logger.debug(f"Cached data for {file_path}")

        except OSError as e:
            logger.warning(f"Could not cache data for {file_path}: {e}")

    def clear(self) -> None:
        """Clear all cached data."""
        self._cache.clear()
        logger.debug("Dictionary cache cleared")


class DictionaryLoader:
    """
    Main dictionary loading system that reads CSV files and validates them
    against Pydantic models.
    """

    # Dictionary definitions
    DICTIONARIES = {
        "geographies": DictionaryInfo(
            name="geographies",
            filename="geographies.csv",
            model_class=GeographyDict,
            expected_rows=1000,
            description="Geographic locations with synthetic addresses",
        ),
        "product_tags": DictionaryInfo(
            name="product_tags",
            filename="product_tags.csv",
            model_class=ProductTagDict,
            required=False,
            expected_rows=None,
            description="Optional product tag overlay (ProductName, Tags)",
        ),
        "first_names": DictionaryInfo(
            name="first_names",
            filename="first_names.csv",
            model_class=FirstNameDict,
            expected_rows=250,
            description="Synthetic first names",
        ),
        "last_names": DictionaryInfo(
            name="last_names",
            filename="last_names.csv",
            model_class=LastNameDict,
            expected_rows=250,
            description="Synthetic last names",
        ),
        "product_companies": DictionaryInfo(
            name="product_companies",
            filename="product_companies.csv",
            model_class=ProductCompanyDict,
            expected_rows=100,
            description="Synthetic product companies",
        ),
        "product_brands": DictionaryInfo(
            name="product_brands",
            filename="product_brands.csv",
            model_class=ProductBrandDict,
            expected_rows=500,
            description="Product brands with company relationships",
        ),
        "products": DictionaryInfo(
            name="products",
            filename="products.csv",
            model_class=ProductDict,
            expected_rows=10000,
            description="Product catalog with base pricing",
        ),
        "tax_rates": DictionaryInfo(
            name="tax_rates",
            filename="tax_rates.csv",
            model_class=TaxJurisdiction,
            expected_rows=164,
            description="Tax rates by jurisdiction (state, county, city)",
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
            config: Configuration object (will be loaded if not provided)
            use_cache: Whether to use caching for loaded data
            encoding: File encoding to use
            chunk_size: Size of chunks for processing large files
            max_workers: Number of threads for parallel loading
        """
        self.config = config or load_config_with_fallback()

        # Set dictionary path from config or parameter
        if dictionary_path:
            self.dictionary_path = Path(dictionary_path)
        else:
            self.dictionary_path = Path(self.config.paths["dict"])

        self.encoding = encoding
        self.chunk_size = chunk_size
        self.max_workers = max_workers

        # Initialize cache
        self.cache = DictionaryCache() if use_cache else None

        # Storage for loaded data
        self._loaded_data: dict[str, list[BaseModel]] = {}
        self._load_results: dict[str, LoadResult] = {}

        logger.info(f"DictionaryLoader initialized with path: {self.dictionary_path}")

    def _find_file(self, filename: str) -> Path:
        """
        Find dictionary file, trying multiple paths and extensions.

        Args:
            filename: Name of the file to find

        Returns:
            Path: Full path to the found file

        Raises:
            DictionaryFileNotFoundError: If file cannot be found
        """
        search_paths = [
            self.dictionary_path / filename,
            Path.cwd() / "data" / "dictionaries" / filename,
            Path.cwd() / filename,
        ]
        # Also try repo-relative fallback to datagen/data/dictionaries
        try:
            repo_datagen = Path(__file__).resolve().parents[3]
            search_paths.append(repo_datagen / "data" / "dictionaries" / filename)
        except Exception:
            pass

        # Also try without .csv extension if filename doesn't have it
        if not filename.endswith(".csv"):
            search_paths.extend(
                [path.parent / (path.stem + ".csv") for path in search_paths]
            )

        for path in search_paths:
            if path.exists() and path.is_file():
                return path

        raise DictionaryFileNotFoundError(self.dictionary_path / filename, search_paths)

    def _read_csv_with_encoding(
        self, file_path: Path, encodings: list[str] = None
    ) -> pd.DataFrame:
        """
        Read CSV file, trying multiple encodings if needed.

        Args:
            file_path: Path to CSV file
            encodings: List of encodings to try

        Returns:
            pd.DataFrame: Loaded data

        Raises:
            DictionaryEncodingError: If file cannot be read with any encoding
        """
        encodings = encodings or [self.encoding, "utf-8", "latin-1", "cp1252"]

        last_error = None
        for encoding in encodings:
            try:
                logger.debug(f"Trying to read {file_path} with encoding {encoding}")

                # Preserve leading zeros and avoid implicit numeric coercion
                df = pd.read_csv(
                    file_path,
                    encoding=encoding,
                    skipinitialspace=True,
                    na_filter=False,  # Don't convert empty strings to NaN
                    dtype=str,
                )

                # Remove any completely empty rows
                df = df.dropna(how="all").reset_index(drop=True)

                # Strip whitespace from string columns
                for col in df.select_dtypes(include=["object"]).columns:
                    df[col] = df[col].astype(str).str.strip()

                logger.debug(f"Successfully read {file_path} with encoding {encoding}")
                return df

            except (UnicodeDecodeError, UnicodeError) as e:
                last_error = e
                continue
            except Exception as e:
                raise DictionaryParsingError(file_path, str(e), original_error=e)

        raise DictionaryEncodingError(file_path, encodings, last_error)

    def _validate_schema(
        self, df: pd.DataFrame, dict_info: DictionaryInfo, file_path: Path
    ) -> list[str]:
        """
        Validate that DataFrame has expected columns.

        Args:
            df: DataFrame to validate
            dict_info: Dictionary information
            file_path: Path to the file

        Returns:
            List[str]: List of warnings

        Raises:
            DictionarySchemaError: If required columns are missing
        """
        # Get expected columns from the model
        model_fields = dict_info.model_class.model_fields
        # Only require fields that are truly required (no default provided)
        required_fields: list[str] = []
        for name, fld in model_fields.items():
            # Pydantic v2: is_required() method
            is_req = False
            is_required_method = getattr(fld, "is_required", None)
            if callable(is_required_method):
                try:
                    is_req = bool(is_required_method())
                except Exception:
                    is_req = False
            # Pydantic v1: 'required' attribute
            if not is_req:
                is_req = bool(getattr(fld, "required", False))
            # If still unknown, fall back to checking for explicit default
            if not is_req:
                # If default attribute exists and is not None, treat as optional
                if hasattr(fld, "default"):
                    if getattr(fld, "default") is None:
                        # Consider optional with default None
                        is_req = False
                    else:
                        # Some fields have no default attribute in v1;
                        # leave as optional by default
                        pass
            if is_req:
                required_fields.append(name)
        actual_columns = list(df.columns)

        warnings = []

        # Check for missing required columns only
        missing_columns = set(required_fields) - set(actual_columns)
        if missing_columns:
            raise DictionarySchemaError(file_path, required_fields, actual_columns)

        # Check for extra columns
        extra_columns = set(actual_columns) - set(model_fields)
        if extra_columns:
            warnings.append(f"Extra columns found: {list(extra_columns)}")

        return warnings

    def _validate_data(
        self, df: pd.DataFrame, dict_info: DictionaryInfo, file_path: Path
    ) -> tuple[list[BaseModel], list[str], list[str]]:
        """
        Validate DataFrame data against Pydantic model.

        Args:
            df: DataFrame to validate
            dict_info: Dictionary information
            file_path: Path to the file

        Returns:
            tuple: (validated_data, validation_errors, warnings)
        """
        validated_data = []
        validation_errors = []
        warnings = []

        for idx, row in df.iterrows():
            try:
                # Convert row to dict for model validation
                row_dict = row.to_dict()

                # Create and validate model instance
                instance = dict_info.model_class(**row_dict)
                validated_data.append(instance)

            except ValidationError as e:
                error_details = []
                for error in e.errors():
                    field = error.get("loc", ["unknown"])[0]
                    msg = error.get("msg", "Unknown validation error")
                    error_details.append(f"{field}: {msg}")

                # +2 for 1-based index + header row
                validation_errors.append(
                    f"Row {idx + 2}: {'; '.join(error_details)}"
                )

            except Exception as e:
                validation_errors.append(
                    f"Row {idx + 2}: Unexpected error: {e}"
                )

        # Check expected row count
        if dict_info.expected_rows and len(validated_data) != dict_info.expected_rows:
            warnings.append(
                f"Expected {dict_info.expected_rows} rows, got {len(validated_data)}"
            )

        return validated_data, validation_errors, warnings

    def _check_data_consistency(self) -> None:
        """
        Check consistency across loaded dictionaries.

        Raises:
            DictionaryConsistencyError: If consistency checks fail
        """
        if not self._loaded_data:
            return

        # Check brand-company consistency
        if (
            "product_brands" in self._loaded_data
            and "product_companies" in self._loaded_data
        ):
            companies = {
                item.Company for item in self._loaded_data["product_companies"]
            }
            brand_companies = {
                item.Company for item in self._loaded_data["product_brands"]
            }

            missing_companies = brand_companies - companies
            if missing_companies:
                raise DictionaryConsistencyError(
                    "Brand references companies not found in product_companies",
                    {"missing_companies": list(missing_companies)},
                )

        logger.debug("Data consistency checks passed")

    def load_dictionary(self, name: str, force_reload: bool = False) -> LoadResult:
        """
        Load a single dictionary file.

        Args:
            name: Name of dictionary to load
            force_reload: Whether to bypass cache and force reload

        Returns:
            LoadResult: Result of loading operation

        Raises:
            DictionaryLoadError: If loading fails
        """
        if name not in self.DICTIONARIES:
            available = list(self.DICTIONARIES.keys())
            raise DictionaryLoadError(
                f"Unknown dictionary '{name}'. Available: {available}"
            )

        dict_info = self.DICTIONARIES[name]
        file_path = self._find_file(dict_info.filename)

        start_time = time.time()

        try:
            # Try cache first (if enabled and not forcing reload)
            if self.cache and not force_reload:
                cached_data = self.cache.get(file_path)
                if cached_data is not None:
                    load_time = time.time() - start_time
                    result = LoadResult(
                        name=name,
                        data=cached_data,
                        load_time=load_time,
                        row_count=len(cached_data),
                        validation_errors=[],
                        warnings=["Data loaded from cache"],
                    )
                    self._loaded_data[name] = cached_data
                    self._load_results[name] = result
                    return result

            logger.info(f"Loading dictionary: {name} from {file_path}")

            # Read CSV file
            df = self._read_csv_with_encoding(file_path)

            # Validate schema
            schema_warnings = self._validate_schema(df, dict_info, file_path)

            # Validate data
            validated_data, validation_errors, data_warnings = self._validate_data(
                df, dict_info, file_path
            )

            # Combine warnings
            all_warnings = schema_warnings + data_warnings

            # Cache the results if caching is enabled
            if self.cache:
                self.cache.set(file_path, validated_data)

            # Store results
            load_time = time.time() - start_time
            result = LoadResult(
                name=name,
                data=validated_data,
                load_time=load_time,
                row_count=len(validated_data),
                validation_errors=validation_errors,
                warnings=all_warnings,
            )

            self._loaded_data[name] = validated_data
            self._load_results[name] = result

            logger.info(
                f"Loaded {name}: {len(validated_data)} rows, "
                f"{len(validation_errors)} errors, "
                f"{len(all_warnings)} warnings "
                f"in {load_time:.2f}s"
            )

            return result

        except Exception as e:
            load_time = time.time() - start_time
            if isinstance(e, (DictionaryLoadError, DictionaryValidationError)):
                raise
            else:
                raise DictionaryLoadError(
                    f"Failed to load dictionary {name}", file_path, e
                )

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
        """Clear the dictionary cache."""
        if self.cache:
            self.cache.clear()

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

    def load_product_companies(self) -> list[ProductCompanyDict]:
        """Load and return product companies dictionary data."""
        if not self.is_loaded("product_companies"):
            self.load_dictionary("product_companies")
        return self.get_data("product_companies")

    def load_tax_rates(self) -> list[TaxJurisdiction]:
        """Load and return tax rates dictionary data."""
        if not self.is_loaded("tax_rates"):
            self.load_dictionary("tax_rates")
        return self.get_data("tax_rates")
