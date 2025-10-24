"""
Configuration models for the retail data generator.

These models define the structure and validation for the config.json file
as specified in AGENTS.md.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)


class VolumeConfig(BaseModel):
    """Configuration for data generation volume settings."""

    stores: int = Field(..., gt=0, description="Number of stores to generate")
    dcs: int = Field(
        ..., gt=0, description="Number of distribution centers to generate"
    )
    refrigerated_trucks: int = Field(
        8, gt=0, description="Number of refrigerated trucks to generate"
    )
    non_refrigerated_trucks: int = Field(
        12, gt=0, description="Number of non-refrigerated trucks to generate"
    )
    total_geographies: int = Field(
        100, gt=0, description="Total number of geographies to generate"
    )
    total_customers: int = Field(
        500000, gt=0, description="Total number of customers to generate"
    )
    total_products: int = Field(
        10000, gt=0, description="Total number of products to generate"
    )
    customers_per_day: int = Field(
        ..., gt=0, description="Average number of customers per day per store"
    )
    items_per_ticket_mean: float = Field(
        ..., gt=0.0, description="Average number of items per customer receipt"
    )

    # Inventory configuration parameters
    dc_initial_inventory_min: int = Field(
        500, gt=0, description="Minimum initial inventory per product at DCs"
    )
    dc_initial_inventory_max: int = Field(
        5000, gt=0, description="Maximum initial inventory per product at DCs"
    )
    store_initial_inventory_min: int = Field(
        20, gt=0, description="Minimum initial inventory per product at stores"
    )
    store_initial_inventory_max: int = Field(
        200, gt=0, description="Maximum initial inventory per product at stores"
    )
    dc_reorder_point_min: int = Field(
        50, gt=0, description="Minimum reorder point for DC inventory"
    )
    dc_reorder_point_max: int = Field(
        500, gt=0, description="Maximum reorder point for DC inventory"
    )
    store_reorder_point_min: int = Field(
        5, gt=0, description="Minimum reorder point for store inventory"
    )
    store_reorder_point_max: int = Field(
        50, gt=0, description="Maximum reorder point for store inventory"
    )

    # Supplier-to-DC truck configuration
    supplier_refrigerated_trucks: int = Field(
        12,
        gt=0,
        description="Number of refrigerated trucks for supplier-to-DC transport",
    )
    supplier_non_refrigerated_trucks: int = Field(
        18,
        gt=0,
        description="Number of non-refrigerated trucks for supplier-to-DC transport",
    )


class RealtimeConfig(BaseModel):
    """Configuration for real-time streaming settings."""

    emit_interval_ms: int = Field(
        ..., gt=0, description="Interval between event emissions in milliseconds"
    )
    burst: int = Field(..., gt=0, description="Number of events to emit in each burst")
    azure_connection_string: str = Field(
        default="",
        description="Azure Event Hub connection string (prefer AZURE_EVENTHUB_CONNECTION_STRING env var)",
    )
    max_batch_size: int = Field(
        default=256, gt=0, description="Maximum events per batch sent to Event Hub"
    )
    batch_timeout_ms: int = Field(
        default=1000,
        gt=0,
        description="Maximum time to wait for batch completion in milliseconds",
    )
    retry_attempts: int = Field(
        default=3,
        ge=0,
        description="Number of retry attempts for failed Event Hub operations",
    )
    backoff_multiplier: float = Field(
        default=2.0,
        gt=0.0,
        description="Multiplier for exponential backoff between retries",
    )
    circuit_breaker_enabled: bool = Field(
        default=True, description="Enable circuit breaker pattern for failure handling"
    )
    monitoring_interval: int = Field(
        default=30,
        gt=0,
        description="Monitoring and statistics update interval in seconds",
    )
    max_buffer_size: int = Field(
        default=10000, gt=0, description="Maximum size of internal event buffer"
    )
    enable_dead_letter_queue: bool = Field(
        default=True, description="Enable dead letter queue for failed events"
    )

    # Optional Azure Key Vault configuration
    use_keyvault: bool = Field(
        default=False, description="Load connection string from Azure Key Vault"
    )
    keyvault_url: str | None = Field(
        default=None, description="Azure Key Vault URL (e.g., https://your-vault.vault.azure.net/)"
    )
    keyvault_secret_name: str = Field(
        default="eventhub-connection-string",
        description="Secret name in Azure Key Vault",
    )

    @field_validator("azure_connection_string", mode="before")
    @classmethod
    def load_connection_string_from_env(cls, v: str | None) -> str:
        """Load connection string from environment variable if not provided in config."""
        # If value is provided in config and is not empty, use it
        if v and v.strip():
            return v

        # Otherwise, try to load from environment variable
        env_value = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING", "")
        return env_value

    @field_validator("azure_connection_string")
    @classmethod
    def validate_connection_string_format(cls, v: str) -> str:
        """Validate connection string format if provided."""
        if not v or v == "":
            # Empty is OK (will be loaded from env var)
            return v

        # Check for test mode environment variable
        test_mode = os.getenv("RETAIL_DATAGEN_TEST_MODE", "false").lower() == "true"

        # Skip validation for test/mock connections or in test mode
        if v.startswith(("mock://", "test://")) or test_mode:
            return v

        # Import here to avoid circular dependency
        from ..shared.credential_utils import validate_eventhub_connection_string

        # Use strict validation only in production mode (not in test mode)
        strict = not test_mode

        is_valid, error = validate_eventhub_connection_string(
            v, strict=strict, allow_mock=True
        )

        if not is_valid:
            if strict:
                raise ValueError(f"Invalid Event Hub connection string: {error}")
            else:
                # In non-strict mode, just log warning
                logger.warning(f"Connection string validation warning: {error}")

        return v

    @model_validator(mode="after")
    def validate_fabric_rti_config(self):
        """Additional validation for Fabric RTI connections."""
        # Import here to avoid circular dependency
        from ..shared.credential_utils import validate_fabric_rti_specific

        conn_str = self.get_connection_string()

        if conn_str and not conn_str.startswith(("mock://", "test://", "")):
            is_valid, message, metadata = validate_fabric_rti_specific(conn_str)

            if not is_valid:
                logger.warning(f"Fabric RTI validation: {message}")
            elif metadata.get("is_fabric_rti"):
                logger.info(
                    f"Detected Fabric RTI connection to: {metadata.get('entity_path')}"
                )

        return self

    def get_connection_string(self) -> str:
        """
        Get connection string from the appropriate source.

        Priority order:
        1. Azure Key Vault (if use_keyvault=True)
        2. Environment variable (AZURE_EVENTHUB_CONNECTION_STRING)
        3. Configuration file value

        Returns:
            str: Connection string

        Raises:
            ValueError: If Key Vault is enabled but URL is not provided
            ImportError: If Key Vault libraries are not installed
        """
        # Load from Key Vault if enabled
        if self.use_keyvault:
            if not self.keyvault_url:
                raise ValueError("keyvault_url required when use_keyvault=true")
            return self._load_from_keyvault()

        # Try environment variable first (already loaded by validator)
        env_conn = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
        if env_conn:
            return env_conn

        # Fall back to config value
        return self.azure_connection_string or ""

    def _load_from_keyvault(self) -> str:
        """
        Load connection string from Azure Key Vault.

        Returns:
            str: Connection string from Key Vault

        Raises:
            ImportError: If azure-keyvault-secrets or azure-identity not installed
        """
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient

            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=self.keyvault_url, credential=credential)
            secret = client.get_secret(self.keyvault_secret_name)
            return secret.value
        except ImportError as e:
            raise ImportError(
                "Azure Key Vault support requires: "
                "pip install azure-keyvault-secrets azure-identity"
            ) from e


class PathsConfig(BaseModel):
    """Configuration for data file paths."""

    model_config = {"populate_by_name": True}

    dictionaries: str = Field(
        ..., min_length=1, description="Path to dictionary CSV files", alias="dict"
    )
    master: str = Field(
        ..., min_length=1, description="Path to master data output files"
    )
    facts: str = Field(..., min_length=1, description="Path to fact data output files")

    @field_validator("dictionaries", "master", "facts")
    @classmethod
    def validate_non_empty_paths(cls, v: str) -> str:
        """Validate that paths are not empty or whitespace only."""
        if not v or not v.strip():
            raise ValueError("Path cannot be empty or whitespace only")
        return v.strip()


class StreamConfig(BaseModel):
    """Configuration for Azure Event Hub streaming."""

    hub: str = Field(..., min_length=1, description="Azure Event Hub name")

    @field_validator("hub")
    @classmethod
    def validate_hub_name(cls, v: str) -> str:
        """Validate that hub name is not empty or whitespace only."""
        if not v or not v.strip():
            raise ValueError("Hub name cannot be empty or whitespace only")
        return v.strip()


class HistoricalConfig(BaseModel):
    """Configuration for historical data generation."""

    start_date: str = Field(
        "2024-01-01",
        description="Start date for historical data generation (YYYY-MM-DD format)",
    )

    @field_validator("start_date")
    @classmethod
    def validate_start_date(cls, v: str) -> str:
        """Validate start date format and ensure it's not in the future."""
        try:
            parsed_date = datetime.strptime(v, "%Y-%m-%d")
            if parsed_date > datetime.now():
                raise ValueError("Start date cannot be in the future")
            return v
        except ValueError as e:
            if "time data" in str(e):
                raise ValueError("Start date must be in YYYY-MM-DD format")
            raise e


class PerformanceConfig(BaseModel):
    """Configuration for performance and resource usage settings."""

    max_cpu_percent: float = Field(
        100.0,
        gt=0.0,
        le=100.0,
        description="Maximum CPU usage percentage (0-100). Limits parallel workers.",
    )
    max_workers: int | None = Field(
        None,
        gt=0,
        description="Override maximum number of parallel workers. If None, calculated from max_cpu_percent.",
    )

    def get_max_workers(self) -> int:
        """Calculate maximum workers based on CPU limit."""
        import os

        # If explicitly set, use that
        if self.max_workers is not None:
            return self.max_workers

        # Otherwise calculate from CPU percentage
        cpu_count = os.cpu_count() or 1
        max_workers = max(1, int(cpu_count * (self.max_cpu_percent / 100.0)))
        return max_workers


class RetailConfig(BaseModel):
    """Main configuration model for the retail data generator."""

    seed: int = Field(
        ...,
        ge=0,
        le=2**32 - 1,
        description="Random seed for reproducible data generation",
    )
    volume: VolumeConfig = Field(..., description="Volume settings for data generation")
    realtime: RealtimeConfig = Field(
        ..., description="Real-time streaming configuration"
    )
    paths: PathsConfig = Field(..., description="File path configuration")
    stream: StreamConfig = Field(..., description="Event Hub streaming configuration")
    historical: HistoricalConfig = Field(
        default_factory=HistoricalConfig,
        description="Historical data generation configuration",
    )
    performance: PerformanceConfig = Field(
        default_factory=PerformanceConfig,
        description="Performance and resource usage configuration",
    )

    @classmethod
    def from_file(cls, file_path: str | Path) -> "RetailConfig":
        """
        Load configuration from a JSON file.

        Args:
            file_path: Path to the configuration JSON file

        Returns:
            RetailConfig instance

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the JSON is invalid or doesn't match the schema
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        try:
            with path.open("r") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")

        return cls(**data)

    def to_file(self, file_path: str | Path) -> None:
        """
        Save configuration to a JSON file.

        Args:
            file_path: Path where to save the configuration file
        """
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w") as f:
            json.dump(self.model_dump(), f, indent=2)


# Alias for backward compatibility and test imports
Config = RetailConfig
