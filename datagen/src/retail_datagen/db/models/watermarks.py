"""
Watermark tracking model for incremental fact data processing.

Tracks the state of fact data publication to Azure Event Hub, enabling:
- Incremental streaming (resume from last published timestamp)
- Efficient purging of published data
- Monitoring and auditing of data pipeline

Watermarks are stored in facts.db alongside the fact tables they track.
"""

from datetime import datetime

from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from retail_datagen.db.models.base import Base


class FactDataWatermark(Base):
    """
    Fact data watermark tracking table.

    Maintains high-water marks for each fact table to support:
    1. Incremental streaming - Identify unpublished data
    2. Data purging - Remove published data safely
    3. Pipeline monitoring - Track publication progress

    Workflow:
    1. Historical generation writes to fact tables
    2. Streaming reads unpublished data (event_ts >= earliest_unpublished_ts)
    3. After successful publication, update latest_published_ts
    4. Periodically purge data where event_ts <= latest_published_ts

    Business Rules:
    - One row per fact table (unique on fact_table_name)
    - earliest_unpublished_ts: Oldest timestamp not yet published (null if all published)
    - latest_published_ts: Newest timestamp successfully published (null if none published)
    - last_purge_ts: When published data was last removed
    - Auto-update created_at and updated_at timestamps

    Example:
        # After publishing receipts up to 2025-01-15 12:00:00
        watermark = FactDataWatermark(
            fact_table_name='fact_receipts',
            earliest_unpublished_ts=datetime(2025, 1, 15, 12, 0, 1),  # Next unpublished
            latest_published_ts=datetime(2025, 1, 15, 12, 0, 0),      # Last published
            last_purge_ts=datetime(2025, 1, 15, 12, 5, 0)             # Purge timestamp
        )
    """

    __tablename__ = "fact_data_watermarks"

    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Fact table identifier (unique constraint)
    fact_table_name: Mapped[str] = mapped_column(
        String(100), nullable=False, unique=True, index=True
    )

    # Watermark timestamps
    earliest_unpublished_ts: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, index=True
    )
    latest_published_ts: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, index=True
    )
    last_purge_ts: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Audit timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Composite index for watermark queries
    __table_args__ = (
        Index(
            "ix_watermark_table_unpublished",
            "fact_table_name",
            "earliest_unpublished_ts",
        ),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<FactDataWatermark(id={self.id}, table={self.fact_table_name}, "
            f"earliest_unpublished={self.earliest_unpublished_ts}, "
            f"latest_published={self.latest_published_ts}, "
            f"last_purge={self.last_purge_ts})>"
        )

    def is_fully_published(self) -> bool:
        """
        Check if all data in the fact table has been published.

        Returns:
            True if earliest_unpublished_ts is None (no unpublished data)
        """
        return self.earliest_unpublished_ts is None

    def needs_purge(self, purge_interval_hours: int = 24) -> bool:
        """
        Check if the fact table needs purging based on last purge time.

        Args:
            purge_interval_hours: Minimum hours between purges (default: 24)

        Returns:
            True if purge is needed (last_purge_ts is old or None)
        """
        if self.last_purge_ts is None:
            return True

        hours_since_purge = (
            datetime.utcnow() - self.last_purge_ts
        ).total_seconds() / 3600
        return hours_since_purge >= purge_interval_hours

    def get_publication_lag_seconds(self) -> float | None:
        """
        Calculate lag between oldest unpublished data and now.

        Returns:
            Lag in seconds, or None if no unpublished data
        """
        if self.earliest_unpublished_ts is None:
            return None

        return (datetime.utcnow() - self.earliest_unpublished_ts).total_seconds()
