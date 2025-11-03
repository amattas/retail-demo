"""
SQLAlchemy ORM models for fact tables.

Contains all 9 fact tables used for historical transaction data:
1. DCInventoryTransaction - DC inventory movements
2. TruckMove - Truck logistics and movements
3. StoreInventoryTransaction - Store inventory changes
4. Receipt - Receipt headers (transaction summary)
5. ReceiptLine - Receipt line items (individual products)
6. FootTraffic - In-store traffic sensor data
7. BLEPing - BLE beacon proximity data
8. MarketingImpression - Marketing impressions and ad tracking
9. OnlineOrder - Online order transactions

All tables use INTEGER primary keys and indexed event_ts columns for
efficient time-range queries. Foreign keys reference master tables but
are not enforced at the database level (cross-database references).

Data Flow:
- Historical generation writes to these tables
- Streaming reads from these tables to publish to Azure Event Hub
- After successful publication, data is purged (facts.db is temporary storage)
"""

from datetime import datetime

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from retail_datagen.db.models.base import Base


class DCInventoryTransaction(Base):
    """
    Distribution center inventory transaction fact table.

    Tracks all inventory movements at distribution centers including:
    - Inbound receipts from suppliers
    - Outbound shipments to stores
    - Inventory adjustments and corrections

    Business Rules:
    - QtyDelta represents net change (can be positive or negative)
    - Balance tracking maintained by application logic
    - Txn_type values: 'receipt', 'shipment', 'adjustment'
    """

    __tablename__ = "fact_dc_inventory_txn"

    # Primary key
    txn_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys (references master.db tables - not enforced)
    dc_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    product_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Event metadata
    event_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    # Transaction details
    txn_type: Mapped[str] = mapped_column(String(50), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    balance: Mapped[int] = mapped_column(Integer, nullable=False)

    # Composite indexes for common query patterns
    __table_args__ = (
        Index("ix_dc_inv_event_dc", "event_ts", "dc_id"),
        Index("ix_dc_inv_event_product", "event_ts", "product_id"),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<DCInventoryTransaction(txn_id={self.txn_id}, dc_id={self.dc_id}, "
            f"product_id={self.product_id}, event_ts={self.event_ts}, "
            f"type={self.txn_type}, qty={self.quantity}, balance={self.balance})>"
        )


class TruckMove(Base):
    """
    Truck movement fact table.

    Tracks truck logistics including loading, transit, and delivery:
    - Loading at distribution centers
    - Transit to stores
    - Delivery at stores

    Business Rules:
    - Each shipment has multiple status updates (SCHEDULED, LOADING, IN_TRANSIT, etc.)
    - Status values: 'SCHEDULED', 'LOADING', 'IN_TRANSIT', 'DELIVERED', 'DELAYED'
    - Links to both DC (origin) and Store (destination)
    - ETA/ETD nullable (may not be set for all status updates)
    """

    __tablename__ = "fact_truck_moves"

    # Primary key
    move_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys (references master.db tables - not enforced)
    truck_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    dc_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    store_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    product_id: Mapped[int] = mapped_column(Integer, nullable=True, index=True)

    # Event metadata
    event_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    # Logistics fields (what generator produces)
    status: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    shipment_id: Mapped[str | None] = mapped_column(
        String(100), nullable=True, index=True
    )
    eta: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    etd: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Composite indexes for common query patterns
    __table_args__ = (
        Index("ix_truck_event_truck", "event_ts", "truck_id"),
        Index("ix_truck_event_store", "event_ts", "store_id"),
        Index("ix_truck_shipment_status", "shipment_id", "status"),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<TruckMove(move_id={self.move_id}, truck_id={self.truck_id}, "
            f"dc_id={self.dc_id}, store_id={self.store_id}, event_ts={self.event_ts}, "
            f"status={self.status}, shipment_id={self.shipment_id})>"
        )


class StoreInventoryTransaction(Base):
    """
    Store inventory transaction fact table.

    Tracks all inventory movements at retail stores including:
    - Deliveries from distribution centers
    - Sales to customers (in-store and online)
    - Shrinkage (theft, damage, expiration)
    - Inventory adjustments

    Business Rules:
    - QtyDelta represents net change (negative for sales/shrinkage)
    - Balance tracking maintained by application logic
    - Txn_type values: 'delivery', 'sale', 'shrinkage', 'adjustment'
    - Source tracks origin (truck ID, 'ONLINE', 'MANUAL', etc.)
    """

    __tablename__ = "fact_store_inventory_txn"

    # Primary key
    txn_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys (references master.db tables - not enforced)
    store_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    product_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Event metadata
    event_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    # Transaction details
    txn_type: Mapped[str] = mapped_column(String(50), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    balance: Mapped[int] = mapped_column(Integer, nullable=False)

    # Composite indexes for common query patterns
    __table_args__ = (
        Index("ix_store_inv_event_store", "event_ts", "store_id"),
        Index("ix_store_inv_event_product", "event_ts", "product_id"),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<StoreInventoryTransaction(txn_id={self.txn_id}, store_id={self.store_id}, "
            f"product_id={self.product_id}, event_ts={self.event_ts}, "
            f"type={self.txn_type}, qty={self.quantity}, balance={self.balance})>"
        )


class Receipt(Base):
    """
    Receipt header fact table.

    Represents the transaction summary for each customer purchase:
    - Total amount calculation
    - Tax and discount tracking
    - Payment method
    - Customer association (nullable for anonymous purchases)

    Business Rules:
    - Total = Subtotal + Tax - Discount
    - CustomerID nullable for cash transactions without loyalty card
    - Receipt lines stored separately in ReceiptLine table
    """

    __tablename__ = "fact_receipts"

    # Primary key
    receipt_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    # External business key (string), used to relate receipt_lines to receipts
    receipt_id_ext: Mapped[str | None] = mapped_column(
        String(100), nullable=True, index=True
    )

    # Foreign keys (references master.db tables - not enforced)
    store_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    customer_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    # Event metadata
    event_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    # Receipt totals
    total_amount: Mapped[float] = mapped_column(Float, nullable=False)
    tax_amount: Mapped[float] = mapped_column(Float, nullable=False)
    discount_amount: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Payment information
    payment_method: Mapped[str] = mapped_column(String(50), nullable=False)

    # Composite indexes for common query patterns
    __table_args__ = (
        Index("ix_receipt_event_store", "event_ts", "store_id"),
        Index("ix_receipt_event_customer", "event_ts", "customer_id"),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<Receipt(receipt_id={self.receipt_id}, store_id={self.store_id}, "
            f"customer_id={self.customer_id}, event_ts={self.event_ts}, "
            f"total={self.total_amount}, payment={self.payment_method})>"
        )


class ReceiptLine(Base):
    """
    Receipt line item fact table.

    Represents individual products purchased on a receipt:
    - Product and quantity information
    - Unit price at time of sale
    - Line total calculation
    - Promotional code tracking

    Business Rules:
    - Line_total = Unit_price * Quantity (after discount)
    - Links to Receipt via receipt_id (same database, can be FK)
    - Product pricing may differ from master due to promotions
    """

    __tablename__ = "fact_receipt_lines"

    # Primary key
    line_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys
    receipt_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("fact_receipts.receipt_id"), nullable=False, index=True
    )
    product_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Line details
    line_num: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[float] = mapped_column(Float, nullable=False)
    ext_price: Mapped[float] = mapped_column(Float, nullable=False)  # Also known as line_total
    promo_code: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Alias for backward compatibility
    @property
    def line_total(self) -> float:
        """Alias for ext_price for backward compatibility."""
        return self.ext_price

    # Index for receipt lookups
    __table_args__ = (
        Index("ix_receipt_line_receipt_product", "receipt_id", "product_id"),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<ReceiptLine(line_id={self.line_id}, receipt_id={self.receipt_id}, "
            f"product_id={self.product_id}, line={self.line_num}, qty={self.quantity}, "
            f"unit_price={self.unit_price}, ext_price={self.ext_price}, promo={self.promo_code})>"
        )


class FootTraffic(Base):
    """
    Foot traffic sensor fact table.

    Tracks customer movement through store zones using sensor data:
    - Individual sensor readings with zone and dwell time
    - Person detection counts
    - Zone-specific traffic patterns

    Business Rules:
    - Sensor_id identifies physical sensor location
    - Zone values: 'ENTRANCE', 'AISLES_A', 'AISLES_B', 'CHECKOUT', 'EXIT'
    - Dwell_seconds calculated from sensor detection time
    - Count typically 1 (single person detection per record)
    - Used for heat mapping and flow analysis
    """

    __tablename__ = "fact_foot_traffic"

    # Primary key
    traffic_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    # Foreign keys (references master.db tables - not enforced)
    store_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Event metadata
    event_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    # Sensor-level fields (what generator produces)
    sensor_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    zone: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    dwell_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False)

    # Composite indexes for common query patterns
    __table_args__ = (
        Index("ix_foot_traffic_event_store", "event_ts", "store_id"),
        Index("ix_foot_traffic_zone_event", "zone", "event_ts"),
        Index("ix_foot_traffic_sensor_event", "sensor_id", "event_ts"),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<FootTraffic(traffic_id={self.traffic_id}, store_id={self.store_id}, "
            f"event_ts={self.event_ts}, sensor={self.sensor_id}, zone={self.zone}, "
            f"dwell={self.dwell_seconds}, count={self.count})>"
        )


class BLEPing(Base):
    """
    BLE beacon ping fact table.

    Tracks customer proximity to in-store beacons using Bluetooth Low Energy:
    - Customer identification via BLE ID (anonymous identifier)
    - Beacon location tracking
    - RSSI signal strength measurement

    Business Rules:
    - Customer_ble_id is anonymous identifier (not linked to customer_id by default)
    - Customer_id nullable (requires separate lookup from BLE ID to customer)
    - Beacon_id identifies physical beacon location
    - RSSI measured in dBm (signal strength: -30 = close, -80 = far)
    - Zone tracks general store area
    - Used for proximity marketing and zone analytics
    """

    __tablename__ = "fact_ble_pings"

    # Primary key
    ping_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys (references master.db tables - not enforced)
    store_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Event metadata
    event_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    # BLE fields (what generator produces)
    customer_ble_id: Mapped[str] = mapped_column(
        String(100), nullable=False, index=True
    )
    beacon_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    rssi: Mapped[int] = mapped_column(Integer, nullable=False)  # Signal strength in dBm
    zone: Mapped[str] = mapped_column(String(50), nullable=True)

    # Optional customer lookup (if BLE ID is registered)
    customer_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    # Composite indexes for common query patterns
    __table_args__ = (
        Index("ix_ble_event_store", "event_ts", "store_id"),
        Index("ix_ble_event_customer", "event_ts", "customer_id"),
        Index("ix_ble_beacon_event", "beacon_id", "event_ts"),
        Index("ix_ble_customer_ble_id", "customer_ble_id", "event_ts"),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<BLEPing(ping_id={self.ping_id}, store_id={self.store_id}, "
            f"customer_ble_id={self.customer_ble_id}, beacon_id={self.beacon_id}, "
            f"event_ts={self.event_ts}, rssi={self.rssi})>"
        )


class MarketingImpression(Base):
    """
    Marketing impression fact table.

    Tracks individual ad impressions across channels:
    - Multi-channel attribution (email, SMS, push, social)
    - Campaign and creative tracking
    - Cost per impression tracking
    - Device and targeting information

    Business Rules:
    - Customer_id nullable (requires lookup from customer_ad_id)
    - Customer_ad_id is anonymous advertising identifier
    - Channel values: 'email', 'sms', 'push', 'social', 'display'
    - Campaign_id links impressions to campaigns
    - Creative_id identifies specific ad creative
    - Cost tracked in dollars per impression
    - Device values: 'mobile', 'desktop', 'tablet'
    """

    __tablename__ = "fact_marketing"

    # Primary key
    impression_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    # Event metadata
    event_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    # Ad impression fields (what generator produces)
    campaign_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    creative_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    impression_id_ext: Mapped[str | None] = mapped_column(
        String(100), nullable=True, index=True
    )
    customer_ad_id: Mapped[str | None] = mapped_column(
        String(100), nullable=True, index=True
    )
    channel: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    device: Mapped[str | None] = mapped_column(String(50), nullable=True)
    cost: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Optional customer lookup (if ad ID is registered)
    customer_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    # Composite indexes for common query patterns
    __table_args__ = (
        Index("ix_marketing_event_customer", "event_ts", "customer_id"),
        Index("ix_marketing_channel_event", "channel", "event_ts"),
        Index("ix_marketing_campaign_event", "campaign_id", "event_ts"),
        Index("ix_marketing_customer_ad_id", "customer_ad_id", "event_ts"),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<MarketingImpression(impression_id={self.impression_id}, "
            f"campaign_id={self.campaign_id}, creative_id={self.creative_id}, "
            f"event_ts={self.event_ts}, channel={self.channel}, "
            f"customer_ad_id={self.customer_ad_id}, cost={self.cost})>"
        )


class OnlineOrder(Base):
    """
    Online order fact table.

    Tracks e-commerce orders with fulfillment details:
    - Customer and product information
    - Order totals and pricing
    - Fulfillment status tracking

    Business Rules:
    - Fulfillment_status values: 'created', 'picked', 'shipped', 'delivered'
    - Total_amount includes all taxes and fees
    - Customer_id references registered online customers
    - Each order creates corresponding inventory transactions
    """

    __tablename__ = "fact_online_orders"

    # Primary key
    order_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Foreign keys (references master.db tables - not enforced)
    customer_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    product_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Event metadata
    event_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    # Order details
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    total_amount: Mapped[float] = mapped_column(Float, nullable=False)
    fulfillment_status: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True
    )

    # Fulfillment details (optional - may be null for older records)
    fulfillment_mode: Mapped[str | None] = mapped_column(
        String(50), nullable=True, index=True
    )
    node_type: Mapped[str | None] = mapped_column(String(10), nullable=True)
    node_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    # Composite indexes for common query patterns
    __table_args__ = (
        Index("ix_online_order_event_customer", "event_ts", "customer_id"),
        Index("ix_online_order_event_product", "event_ts", "product_id"),
        Index("ix_online_order_status_event", "fulfillment_status", "event_ts"),
        Index("ix_online_order_mode_node", "fulfillment_mode", "node_type", "node_id"),
        {"extend_existing": True},
    )

    def __repr__(self) -> str:
        return (
            f"<OnlineOrder(order_id={self.order_id}, customer_id={self.customer_id}, "
            f"product_id={self.product_id}, event_ts={self.event_ts}, "
            f"qty={self.quantity}, total={self.total_amount}, "
            f"status={self.fulfillment_status})>"
        )
