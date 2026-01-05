"""
Configuration models for the retail data generator.

These models define the structure and validation for the config.json file
as specified in AGENTS.md.
"""

import json
import logging
import os
from datetime import UTC, datetime
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

    # Omnichannel / Online orders volume
    online_orders_per_day: int = Field(
        2500,
        ge=0,
        description="Average number of online orders per day across the network",
    )

    # Marketing impressions per day (global cap)
    marketing_impressions_per_day: int = Field(
        10000,
        ge=0,
        description="Approximate cap for daily marketing impressions (for performance)",
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

    # Truck-to-DC assignment configuration
    truck_dc_assignment_rate: float = Field(
        0.85,
        gt=0.0,
        le=1.0,
        description="Percentage of trucks assigned to specific DCs (0.0-1.0). "
        "Remaining trucks represent 'pool' or 'rental' trucks with DCID=NULL.",
    )
    trucks_per_dc: int | None = Field(
        None,
        gt=0,
        description="Optional: Override automatic truck assignment calculation. "
        "If specified, exactly this many trucks will be assigned to each DC. "
        "If None, trucks are distributed based on truck_dc_assignment_rate.",
    )


class RealtimeConfig(BaseModel):
    """Configuration for real-time streaming settings.

    SECURITY NOTE: Prefer environment variable AZURE_EVENTHUB_CONNECTION_STRING
    over storing connection strings in config.json to avoid accidental exposure
    in version control.
    """

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

        # Security warning: Only warn if connection string appears to be hardcoded in config
        # (not loaded from environment variable). Compare against env var to detect source.
        if self.azure_connection_string:
            env_value = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING", "")
            # Only warn if the value differs from env var (meaning it came from config)
            # If env var is empty, the value must have come from config
            # If env var matches, the value was safely loaded from env var
            if not env_value or self.azure_connection_string != env_value:
                logger.warning(
                    "SECURITY WARNING: Azure Event Hub connection string is configured in the model. "
                    "If this value comes from config.json, it may be accidentally committed to version control. "
                    "Recommended: Use environment variable AZURE_EVENTHUB_CONNECTION_STRING instead. "
                    "To suppress this warning, leave azure_connection_string empty in config.json "
                    "and set the AZURE_EVENTHUB_CONNECTION_STRING environment variable."
                )

        return self

    def get_connection_string(self) -> str:
        """
        Get connection string from the appropriate source.

        Priority order:
        1. Environment variable (AZURE_EVENTHUB_CONNECTION_STRING)
        2. Configuration file value

        Returns:
            str: Connection string
        """
        # Try environment variable first
        env_conn = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
        if env_conn:
            return env_conn

        # Fall back to config value
        return self.azure_connection_string or ""


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


class StorageConfig(BaseModel):
    """Configuration for Azure Storage (Blob/Data Lake) access.

    These fields are optional and not required for local CSV/Parquet export.
    If provided, they can be used by services that upload generated data to
    Azure Storage. Values can be supplied via config file or environment vars.

    SECURITY NOTE: Prefer environment variables (AZURE_STORAGE_ACCOUNT_KEY)
    over storing keys in config.json to avoid accidental exposure in version control.

    Environment overrides (checked if field empty/None):
    - account_uri: AZURE_STORAGE_ACCOUNT_URI or AZURE_STORAGE_ACCOUNT_URL
    - account_key: AZURE_STORAGE_ACCOUNT_KEY
    """

    account_uri: str | None = Field(
        default=None,
        description="Azure Storage account URI (e.g., https://<account>.blob.core.windows.net)",
    )
    account_key: str | None = Field(
        default=None,
        description="Azure Storage account key (sensitive - prefer environment variable)",
    )

    @field_validator("account_uri", mode="before")
    @classmethod
    def load_account_uri_from_env(cls, v: str | None) -> str | None:
        # Prefer explicit value if provided
        if v and str(v).strip():
            return str(v).strip()

        # Environment fallbacks
        env_uri = os.getenv("AZURE_STORAGE_ACCOUNT_URI") or os.getenv(
            "AZURE_STORAGE_ACCOUNT_URL"
        )
        return env_uri.strip() if env_uri else None

    @field_validator("account_key", mode="before")
    @classmethod
    def load_account_key_from_env(cls, v: str | None) -> str | None:
        if v and str(v).strip():
            return str(v).strip()
        env_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        return env_key.strip() if env_key else None

    @model_validator(mode="after")
    def validate_uri_format(self) -> "StorageConfig":
        # Basic sanity checks only; support various Azure clouds/domains
        if self.account_uri is not None:
            uri = self.account_uri.strip()
            if not (uri.startswith("https://") or uri.startswith("http://")):
                raise ValueError(
                    "Storage account URI must start with http:// or https://"
                )
            # Prevent obviously invalid bare values
            if "." not in uri and "/" not in uri:
                raise ValueError("Storage account URI appears invalid")

        # Security warning: Only warn if account_key appears to be hardcoded in config
        # (not loaded from environment variable). Compare against env var to detect source.
        if self.account_key:
            env_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
            # Only warn if the value differs from env var (meaning it came from config)
            # If env var is empty, the value must have come from config
            # If env var matches, the value was safely loaded from env var
            if not env_key or self.account_key != env_key:
                logger.warning(
                    "SECURITY WARNING: Azure Storage account key is configured in the model. "
                    "If this value comes from config.json, it may be accidentally committed to version control. "
                    "Recommended: Use environment variable AZURE_STORAGE_ACCOUNT_KEY instead. "
                    "To suppress this warning, leave account_key empty in config.json "
                    "and set the AZURE_STORAGE_ACCOUNT_KEY environment variable."
                )
        return self


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
            parsed_date = datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=UTC)
            if parsed_date > datetime.now(UTC):
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
    batch_hours: int = Field(
        1,
        ge=1,
        le=24,
        description="Batch N hours of data before inserting to DuckDB (1 = no batching)",
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


class MarketingCostConfig(BaseModel):
    """Configuration for marketing impression costs.

    Costs are per impression in USD. Actual cost is calculated by:
    base_cost * device_multiplier, where base_cost is randomly selected
    from the [min, max] range for the channel.
    """

    # Channel-specific cost ranges (min, max) in dollars
    # Based on industry averages for digital marketing channels

    email_cost_min: float = Field(
        0.10,
        ge=0.0,
        description="Minimum cost per EMAIL channel impression in USD",
    )
    email_cost_max: float = Field(
        0.25,
        ge=0.0,
        description="Maximum cost per EMAIL channel impression in USD",
    )

    display_cost_min: float = Field(
        0.50,
        ge=0.0,
        description="Minimum cost per DISPLAY channel impression in USD",
    )
    display_cost_max: float = Field(
        2.00,
        ge=0.0,
        description="Maximum cost per DISPLAY channel impression in USD",
    )

    social_cost_min: float = Field(
        0.75,
        ge=0.0,
        description="Minimum cost per SOCIAL channel impression in USD",
    )
    social_cost_max: float = Field(
        3.00,
        ge=0.0,
        description="Maximum cost per SOCIAL channel impression in USD",
    )

    search_cost_min: float = Field(
        1.00,
        ge=0.0,
        description="Minimum cost per SEARCH channel impression in USD",
    )
    search_cost_max: float = Field(
        5.00,
        ge=0.0,
        description="Maximum cost per SEARCH channel impression in USD",
    )

    video_cost_min: float = Field(
        1.50,
        ge=0.0,
        description="Minimum cost per VIDEO channel impression in USD",
    )
    video_cost_max: float = Field(
        5.25,
        ge=0.0,
        description="Maximum cost per VIDEO channel impression in USD",
    )

    facebook_cost_min: float = Field(
        0.25,
        ge=0.0,
        description="Minimum cost per FACEBOOK channel impression in USD",
    )
    facebook_cost_max: float = Field(
        3.00,
        ge=0.0,
        description="Maximum cost per FACEBOOK channel impression in USD",
    )

    google_cost_min: float = Field(
        1.00,
        ge=0.0,
        description="Minimum cost per GOOGLE channel impression in USD",
    )
    google_cost_max: float = Field(
        5.00,
        ge=0.0,
        description="Maximum cost per GOOGLE channel impression in USD",
    )

    instagram_cost_min: float = Field(
        0.20,
        ge=0.0,
        description="Minimum cost per INSTAGRAM channel impression in USD",
    )
    instagram_cost_max: float = Field(
        3.00,
        ge=0.0,
        description="Maximum cost per INSTAGRAM channel impression in USD",
    )

    youtube_cost_min: float = Field(
        1.50,
        ge=0.0,
        description="Minimum cost per YOUTUBE channel impression in USD",
    )
    youtube_cost_max: float = Field(
        5.25,
        ge=0.0,
        description="Maximum cost per YOUTUBE channel impression in USD",
    )

    # Device-specific multipliers
    mobile_multiplier: float = Field(
        1.0,
        gt=0.0,
        description="Cost multiplier for MOBILE devices (1.0 = baseline)",
    )
    tablet_multiplier: float = Field(
        1.2,
        gt=0.0,
        description="Cost multiplier for TABLET devices (higher engagement)",
    )
    desktop_multiplier: float = Field(
        1.5,
        gt=0.0,
        description="Cost multiplier for DESKTOP devices (highest engagement)",
    )

    @model_validator(mode="after")
    def validate_cost_ranges(self) -> "MarketingCostConfig":
        """Validate that min costs are less than or equal to max costs."""
        cost_pairs = [
            ("email", self.email_cost_min, self.email_cost_max),
            ("display", self.display_cost_min, self.display_cost_max),
            ("social", self.social_cost_min, self.social_cost_max),
            ("search", self.search_cost_min, self.search_cost_max),
            ("video", self.video_cost_min, self.video_cost_max),
            ("facebook", self.facebook_cost_min, self.facebook_cost_max),
            ("google", self.google_cost_min, self.google_cost_max),
            ("instagram", self.instagram_cost_min, self.instagram_cost_max),
            ("youtube", self.youtube_cost_min, self.youtube_cost_max),
        ]

        for channel, min_cost, max_cost in cost_pairs:
            if min_cost > max_cost:
                raise ValueError(
                    f"{channel} min cost ({min_cost}) must be <= max cost ({max_cost})"
                )

        return self


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
    marketing_cost: MarketingCostConfig = Field(
        default_factory=MarketingCostConfig,
        description="Marketing impression cost configuration",
    )
    storage: StorageConfig = Field(
        default_factory=StorageConfig,
        description="Azure Storage configuration (optional)",
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
