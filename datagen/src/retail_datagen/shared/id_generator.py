"""
Standardized ID generation for all business entities.

Provides collision-resistant ID generation using a format that ensures uniqueness
across all fact tables while remaining deterministic and debuggable.

Format: {PREFIX}{entity_id}{YYYYMMDDHHmmssffffff}{seq:04d}

Examples:
    Receipt ID: RCP00120260128142530123456789
    Return ID: RET00120260128152030234567890001
    Trace ID: TRC20260128142530123456789
"""

from datetime import UTC, datetime
from threading import Lock


class EntityIdGenerator:
    """
    Thread-safe ID generator for business entities.

    Generates unique IDs using a standardized format that includes:
    - Entity type prefix (e.g., RCP, RET, SHIP)
    - Optional contextual entity ID (e.g., store_id, dc_id)
    - High-resolution timestamp (microseconds)
    - Sequential counter within the same context and second

    The format ensures:
    - No collisions even at high throughput
    - Chronological ordering
    - Debuggability (timestamp visible in ID)
    - Uniqueness across multiple runs

    Attributes:
        prefix: Entity type identifier (e.g., "RCP", "RET", "SHIP")
        entity_id_width: Fixed width for entity ID padding (0 = no entity ID)
        sequence_width: Width for sequential counter (default: 4)

    Thread Safety:
        This class uses a lock to ensure thread-safe counter increments.
    """

    def __init__(
        self,
        prefix: str,
        entity_id_width: int = 0,
        sequence_width: int = 4,
    ) -> None:
        """
        Initialize the ID generator.

        Args:
            prefix: Entity type prefix (e.g., "RCP", "RET", "SHIP")
            entity_id_width: Fixed width for entity ID (0 = no entity ID)
            sequence_width: Width for sequential counter (default: 4)

        Raises:
            ValueError: If prefix is empty or widths are negative
        """
        if not prefix:
            raise ValueError("Prefix cannot be empty")
        if entity_id_width < 0:
            raise ValueError("entity_id_width must be >= 0")
        if sequence_width < 1:
            raise ValueError("sequence_width must be >= 1")

        self.prefix = prefix
        self.entity_id_width = entity_id_width
        self.sequence_width = sequence_width

        # Track counters per entity per second
        self._counters: dict[tuple[int | None, str], int] = {}
        self._lock = Lock()

    def generate(
        self,
        timestamp: datetime | None = None,
        entity_id: int | None = None,
    ) -> str:
        """
        Generate a unique ID.

        Args:
            timestamp: Timestamp for the ID (defaults to current UTC time)
            entity_id: Optional entity context (e.g., store_id, dc_id)

        Returns:
            Formatted ID string

        Raises:
            ValueError: If entity_id is required but not provided, or if
                       timestamp is not timezone-aware

        Examples:
            >>> gen = EntityIdGenerator("RCP", entity_id_width=3)
            >>> gen.generate(datetime(2026, 1, 28, 14, 25, 30, 123456), entity_id=1)
            'RCP00120260128142530123456001'

            >>> gen = EntityIdGenerator("TRC")
            >>> gen.generate(datetime(2026, 1, 28, 14, 25, 30, 123456))
            'TRC202601281425301234560001'
        """
        if timestamp is None:
            timestamp = datetime.now(UTC)
        elif timestamp.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware")

        if self.entity_id_width > 0 and entity_id is None:
            raise ValueError(
                f"entity_id is required for {self.prefix} IDs "
                f"(entity_id_width={self.entity_id_width})"
            )

        # Format timestamp to microsecond precision
        ts_str = timestamp.strftime("%Y%m%d%H%M%S%f")

        # Build entity component
        entity_str = ""
        if self.entity_id_width > 0 and entity_id is not None:
            entity_str = f"{entity_id:0{self.entity_id_width}d}"

        # Get sequential counter for this entity+second combination
        # Key is (entity_id, second_timestamp) to reset counter each second
        second_key = timestamp.strftime("%Y%m%d%H%M%S")
        counter_key = (entity_id, second_key)

        with self._lock:
            counter = self._counters.get(counter_key, 0) + 1
            self._counters[counter_key] = counter

            # Clean up old entries to prevent memory growth
            # Keep only current second's counters
            keys_to_remove = [
                key for key in self._counters.keys() if key[1] != second_key
            ]
            for key in keys_to_remove:
                del self._counters[key]

        sequence_str = f"{counter:0{self.sequence_width}d}"

        return f"{self.prefix}{entity_str}{ts_str}{sequence_str}"

    def reset_counters(self) -> None:
        """
        Reset all counters.

        Useful for testing or when starting a new generation batch.
        Thread-safe.
        """
        with self._lock:
            self._counters.clear()
