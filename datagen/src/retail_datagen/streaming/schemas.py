"""
Event envelope schemas for real-time streaming.

This module defines Pydantic models for event envelopes and event types
used in the real-time streaming system.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Event types supported by the streaming system."""

    # Transaction Events
    RECEIPT_CREATED = "receipt_created"
    RECEIPT_LINE_ADDED = "receipt_line_added"
    PAYMENT_PROCESSED = "payment_processed"

    # Inventory Events
    INVENTORY_UPDATED = "inventory_updated"
    STOCKOUT_DETECTED = "stockout_detected"
    REORDER_TRIGGERED = "reorder_triggered"

    # Customer Events
    CUSTOMER_ENTERED = "customer_entered"
    CUSTOMER_ZONE_CHANGED = "customer_zone_changed"
    BLE_PING_DETECTED = "ble_ping_detected"

    # Operational Events
    TRUCK_ARRIVED = "truck_arrived"
    TRUCK_DEPARTED = "truck_departed"
    STORE_OPENED = "store_opened"
    STORE_CLOSED = "store_closed"

    # Marketing Events
    AD_IMPRESSION = "ad_impression"
    PROMOTION_APPLIED = "promotion_applied"

    # Omnichannel / Online
    ONLINE_ORDER_CREATED = "online_order_created"
    ONLINE_ORDER_PICKED = "online_order_picked"
    ONLINE_ORDER_SHIPPED = "online_order_shipped"


class EventEnvelope(BaseModel):
    """
    Event envelope for real-time streaming.

    This envelope wraps all events with metadata required for proper
    event processing and tracing.
    """

    event_type: EventType = Field(..., description="Type of event being streamed")

    payload: dict[str, Any] = Field(..., description="Actual event data")

    trace_id: str = Field(
        ..., min_length=1, description="Unique trace identifier for event tracking"
    )

    ingest_timestamp: datetime = Field(
        ..., description="Timestamp when event was created for ingestion"
    )

    schema_version: str = Field(
        default="1.0", description="Version of the event schema"
    )

    source: str = Field(
        default="retail-datagen", description="Source system generating the event"
    )

    correlation_id: str | None = Field(
        None, description="Optional correlation ID for linking related events"
    )

    partition_key: str | None = Field(
        None, description="Optional partition key for Event Hub partitioning strategy"
    )

    session_id: str | None = Field(
        None, description="Session ID for tracking related events in a session"
    )

    parent_event_id: str | None = Field(
        None, description="Parent event ID for event causality tracking"
    )


class ReceiptCreatedPayload(BaseModel):
    """Payload for receipt_created events."""

    store_id: int = Field(..., gt=0)
    customer_id: int = Field(..., gt=0)
    receipt_id: str = Field(..., min_length=1)
    subtotal: float = Field(..., ge=0)
    tax: float = Field(..., ge=0)
    total: float = Field(..., ge=0)
    tender_type: str = Field(..., min_length=1)
    item_count: int = Field(..., gt=0)
    # TODO: Populate campaign_id in MarketingCampaignSimulator when receipt is
    # generated during an active campaign (see GitHub issue for tracking)
    campaign_id: str | None = Field(
        None, description="Optional campaign ID for attribution tracking"
    )


class ReceiptLineAddedPayload(BaseModel):
    """Payload for receipt_line_added events."""

    receipt_id: str = Field(..., min_length=1)
    line_number: int = Field(..., gt=0)
    product_id: int = Field(..., gt=0)
    quantity: int = Field(..., gt=0)
    unit_price: float = Field(..., gt=0)
    extended_price: float = Field(..., gt=0)
    promo_code: str | None = None


class InventoryUpdatedPayload(BaseModel):
    """Payload for inventory_updated events."""

    store_id: int | None = Field(None, gt=0)
    dc_id: int | None = Field(None, gt=0)
    product_id: int = Field(..., gt=0)
    quantity_delta: int = Field(description="Quantity change, cannot be zero")
    reason: str = Field(..., min_length=1)
    source: str | None = None


class CustomerEnteredPayload(BaseModel):
    """Payload for customer_entered events."""

    store_id: int = Field(..., gt=0)
    sensor_id: str = Field(..., min_length=1)
    zone: str = Field(..., min_length=1)
    customer_count: int = Field(..., ge=0)
    dwell_time: int = Field(default=0, ge=0)


class CustomerZoneChangedPayload(BaseModel):
    """Payload for customer_zone_changed events."""

    store_id: int = Field(..., gt=0)
    customer_ble_id: str = Field(..., min_length=1)
    from_zone: str = Field(..., min_length=1)
    to_zone: str = Field(..., min_length=1)
    timestamp: datetime


class BLEPingDetectedPayload(BaseModel):
    """Payload for ble_ping_detected events."""

    store_id: int = Field(..., gt=0)
    beacon_id: str = Field(..., min_length=1)
    customer_ble_id: str = Field(..., min_length=1)
    rssi: int = Field(..., ge=-120, le=0)
    zone: str = Field(..., min_length=1)


class TruckArrivedPayload(BaseModel):
    """Payload for truck_arrived events."""

    truck_id: str = Field(..., min_length=1)
    dc_id: int | None = Field(None, gt=0)
    store_id: int | None = Field(None, gt=0)
    shipment_id: str = Field(..., min_length=1)
    arrival_time: datetime
    estimated_unload_duration: int = Field(..., gt=0)


class TruckDepartedPayload(BaseModel):
    """Payload for truck_departed events."""

    truck_id: str = Field(..., min_length=1)
    dc_id: int | None = Field(None, gt=0)
    store_id: int | None = Field(None, gt=0)
    shipment_id: str = Field(..., min_length=1)
    departure_time: datetime
    actual_unload_duration: int = Field(..., gt=0)


class StoreOperationPayload(BaseModel):
    """Payload for store_opened/store_closed events."""

    store_id: int = Field(..., gt=0)
    operation_time: datetime
    operation_type: str = Field(..., min_length=1)  # "opened" or "closed"


class AdImpressionPayload(BaseModel):
    """Payload for ad_impression events."""

    channel: str = Field(..., min_length=1)
    campaign_id: str = Field(..., min_length=1)
    creative_id: str = Field(..., min_length=1)
    customer_ad_id: str = Field(..., min_length=1)
    impression_id: str = Field(..., min_length=1)
    cost: float = Field(..., ge=0)
    device_type: str = Field(..., min_length=1)


class PromotionAppliedPayload(BaseModel):
    """Payload for promotion_applied events."""

    receipt_id: str = Field(..., min_length=1)
    promo_code: str = Field(..., min_length=1)
    discount_amount: float = Field(..., gt=0)
    discount_type: str = Field(..., min_length=1)  # "percentage" or "fixed"
    product_ids: list[int] = Field(..., min_length=1)


class StockoutDetectedPayload(BaseModel):
    """Payload for stockout_detected events."""

    store_id: int | None = Field(None, gt=0)
    dc_id: int | None = Field(None, gt=0)
    product_id: int = Field(..., gt=0)
    last_known_quantity: int = Field(..., ge=0)
    detection_time: datetime


class ReorderTriggeredPayload(BaseModel):
    """Payload for reorder_triggered events."""

    store_id: int | None = Field(None, gt=0)
    dc_id: int | None = Field(None, gt=0)
    product_id: int = Field(..., gt=0)
    current_quantity: int = Field(..., ge=0)
    reorder_quantity: int = Field(..., gt=0)
    reorder_point: int = Field(..., ge=0)
    priority: str = Field(default="NORMAL")  # NORMAL, HIGH, URGENT


class PaymentProcessedPayload(BaseModel):
    """Payload for payment_processed events."""

    receipt_id: str = Field(..., min_length=1)
    payment_method: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0)
    transaction_id: str = Field(..., min_length=1)
    processing_time: datetime
    status: str = Field(default="APPROVED")  # APPROVED, DECLINED, PENDING


class OnlineOrderCreatedPayload(BaseModel):
    """Payload for online_order_created events."""

    order_id: str = Field(..., min_length=1)
    customer_id: int = Field(..., gt=0)
    fulfillment_mode: str = Field(
        ..., min_length=1
    )  # SHIP_FROM_STORE / SHIP_FROM_DC / BOPIS
    node_type: str = Field(..., min_length=1)  # STORE or DC
    node_id: int = Field(..., gt=0)
    item_count: int = Field(..., gt=0)
    subtotal: float = Field(..., ge=0)
    tax: float = Field(..., ge=0)
    total: float = Field(..., ge=0)
    tender_type: str = Field(..., min_length=1)


class OnlineOrderPickedPayload(BaseModel):
    """Payload for online_order_picked events."""

    order_id: str = Field(..., min_length=1)
    node_type: str = Field(..., min_length=1)
    node_id: int = Field(..., gt=0)
    fulfillment_mode: str = Field(..., min_length=1)
    picked_time: datetime


class OnlineOrderShippedPayload(BaseModel):
    """Payload for online_order_shipped events."""

    order_id: str = Field(..., min_length=1)
    node_type: str = Field(..., min_length=1)
    node_id: int = Field(..., gt=0)
    fulfillment_mode: str = Field(..., min_length=1)
    shipped_time: datetime
