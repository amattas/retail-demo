"""Entity models used by the omnichannel data generator."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Iterable, Sequence


class NodeType(str, Enum):
    """Supported node types."""

    STORE = "STORE"
    DC = "DC"


class FulfillmentMode(str, Enum):
    """Supported fulfillment modes."""

    SHIP_FROM_STORE = "SHIP_FROM_STORE"
    SHIP_FROM_DC = "SHIP_FROM_DC"
    BOPIS = "BOPIS"
    CURBSIDE = "CURBSIDE"


class FulfillmentEventType(str, Enum):
    """Fulfillment event taxonomy."""

    PICK_CONFIRMED = "PICK_CONFIRMED"
    PICK_FAILED = "PICK_FAILED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    PICKED_UP = "PICKED_UP"
    NO_SHOW = "NO_SHOW"
    REROUTED = "REROUTED"


@dataclass(slots=True)
class NodeCapabilities:
    """Fulfillment capabilities for a node."""

    ship: bool
    bopis: bool
    curbside: bool


@dataclass(slots=True)
class NodeCapacity:
    """Operational capacity for a node."""

    node_id: str
    time: datetime
    picks_per_hour: float
    backlog: int


@dataclass(slots=True)
class StoreHours:
    """Store hours representation."""

    node_id: str
    day_of_week: int
    open_time: datetime
    close_time: datetime


@dataclass(slots=True)
class Node:
    """Represents a store or distribution center."""

    node_id: str
    type: NodeType
    timezone: str
    lat: float
    lon: float
    capabilities: NodeCapabilities
    capacity: NodeCapacity
    service_area: dict[str, list[str]]
    accuracy_score: float
    ship_base_cost: float
    per_km_rate: float
    per_kg_rate: float
    handling_cost: float
    safety_stock: int


@dataclass(slots=True)
class InventoryRecord:
    """Inventory state for a node/sku combination."""

    sku: str
    on_hand_true: int
    allocated: int
    safety_stock: int
    last_cycle_count: datetime
    observed_on_hand: int


@dataclass(slots=True)
class InventorySnapshot:
    """Snapshot emitted for downstream analytics."""

    time: datetime
    node_id: str
    sku: str
    on_hand_true: int
    on_hand: int
    allocated: int
    safety_stock: int
    last_cycle_count: datetime
    accuracy_applied: float


@dataclass(slots=True)
class InboundShipment:
    """Inbound shipment state."""

    shipment_id: str
    dest_node_id: str
    sku: str
    qty: int
    eta_start: datetime
    eta_end: datetime
    source_node_id: str


@dataclass(slots=True)
class OrderLine:
    """Line item for an order."""

    sku: str
    qty: int
    weight_kg: float = 1.0


@dataclass(slots=True)
class OrderConstraints:
    """Order routing constraints."""

    mode_allow: Sequence[FulfillmentMode]
    allow_split: bool
    promise_by: datetime
    max_nodes: int


@dataclass(slots=True)
class OrderDestination:
    """Order destination metadata."""

    lat: float
    lon: float
    postal: str
    country: str


@dataclass(slots=True)
class Order:
    """Order representation used by omnichannel routing."""

    order_id: str
    created_at: datetime
    ship_to: OrderDestination
    lines: Sequence[OrderLine]
    constraints: OrderConstraints


@dataclass(slots=True)
class QuoteCandidate:
    """Fulfillment candidate for an order."""

    node_id: str
    mode: FulfillmentMode
    eta: datetime
    fill_rate: float
    cost_breakdown: dict[str, float]
    total_cost: float
    reasons: list[str]
    line_qtys: list[dict[str, Any]]


@dataclass(slots=True)
class AllocationSelection:
    """Selection data for allocation."""

    node_id: str
    mode: FulfillmentMode
    lines: Sequence[OrderLine]


@dataclass(slots=True)
class QuoteBundle:
    """Quote output for an order."""

    order_id: str
    generated_at: datetime
    candidates: list[QuoteCandidate]
    recommendation: dict[str, Any]
    decision_trail: dict[str, Any]
    candidate_selections: list[list[AllocationSelection]]


@dataclass(slots=True)
class Allocation:
    """Allocation decision representation."""

    allocation_id: str
    order_id: str
    selection: Sequence[AllocationSelection]
    reserved_at: datetime
    expires_at: datetime
    status: str


@dataclass(slots=True)
class AllocationBundle:
    """Allocation result with metadata."""

    allocation: Allocation
    reserved: bool
    attempted_fallback: bool
    attempted_selection: Sequence[AllocationSelection]


@dataclass(slots=True)
class FulfillmentEvent:
    """Event emitted during fulfillment lifecycle."""

    event_id: str
    order_id: str
    allocation_id: str
    node_id: str
    mode: FulfillmentMode
    event_type: FulfillmentEventType
    event_time: datetime
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NodeCandidate:
    """Internal representation of a routing candidate."""

    node: Node
    available_qty: int
    eta_hours: float
    distance_km: float
    mode: FulfillmentMode


@dataclass(slots=True)
class DecisionFeatureVector:
    """Decision trail entry for analytics."""

    node_ids: tuple[str, ...]
    score: float
    sla_ok: bool
    feature_vector: dict[str, float]


def flatten_candidates(features: Iterable[DecisionFeatureVector]) -> list[dict[str, Any]]:
    """Convert feature vectors into serialisable dictionaries."""

    return [
        {
            "node_ids": list(feature.node_ids),
            "score": feature.score,
            "sla_ok": feature.sla_ok,
            "feature_vector": dict(feature.feature_vector),
        }
        for feature in features
    ]


@dataclass(slots=True)
class PerturbationResult:
    """Result bundle used by the perturb hook."""

    records: list[dict[str, Any]]
    applied_noise: dict[str, Any]


@dataclass(slots=True)
class CustomerArrival:
    """Used to model drive time for pickup flows."""

    eta_minutes: float
    pickup_window_start: datetime
    pickup_window_end: datetime
    curbside_vehicle: str | None = None
    curbside_parking_bay: str | None = None


def combine_lines(lines: Sequence[OrderLine]) -> list[dict[str, Any]]:
    """Convert order lines to a serialisable structure."""

    return [{"sku": line.sku, "qty": line.qty} for line in lines]


def combine_selection(selection: Sequence[AllocationSelection]) -> list[dict[str, Any]]:
    """Serialise allocation selections."""

    return [
        {
            "node_id": item.node_id,
            "mode": item.mode.value,
            "lines": combine_lines(item.lines),
        }
        for item in selection
    ]


@dataclass(slots=True)
class InboundWindow:
    """Inbound aggregation helper."""

    qty: int
    eta_start: datetime
    eta_end: datetime

    def overlaps(self, window_start: datetime, window_end: datetime) -> bool:
        """Return True if the inbound window overlaps the supplied time range."""

        return not (self.eta_end < window_start or self.eta_start > window_end)


@dataclass(slots=True)
class NodeInventoryState:
    """Lightweight structure for inventory computations."""

    on_hand: int
    allocated: int
    safety_stock: int
    inbound: list[InboundWindow] = field(default_factory=list)

    def atp(self) -> int:
        return self.on_hand - self.allocated - self.safety_stock

    def atp_window(self, window_end: datetime) -> int:
        base = self.atp()
        inbound_qty = 0
        for window in self.inbound:
            if window.eta_end <= window_end:
                inbound_qty += window.qty
        return base + inbound_qty


@dataclass(slots=True)
class OrderContext:
    """Bundle used internally during routing."""

    order: Order
    now: datetime
