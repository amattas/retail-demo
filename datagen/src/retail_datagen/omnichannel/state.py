"""State container for the omnichannel hooks."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable

from .config import OmniConfig
from .models import (
    Allocation,
    FulfillmentMode,
    InboundShipment,
    InventoryRecord,
    Node,
    NodeInventoryState,
    StoreHours,
    OrderLine,
)


@dataclass(slots=True)
class OmniState:
    """State required across omnichannel hooks."""

    config: OmniConfig
    nodes: Dict[str, Node]
    inventory: Dict[str, Dict[str, InventoryRecord]]
    inbound: Dict[str, list[InboundShipment]]
    store_hours: Dict[str, list[StoreHours]]
    active_allocations: Dict[str, Allocation] = field(default_factory=dict)

    def iter_nodes(self) -> Iterable[Node]:
        """Iterate over known nodes."""

        return self.nodes.values()

    def inventory_record(self, node_id: str, sku: str) -> InventoryRecord | None:
        """Retrieve an inventory record if present."""

        return self.inventory.get(node_id, {}).get(sku)

    def inbound_for(self, node_id: str) -> list[InboundShipment]:
        """Return inbound shipments for a node."""

        return self.inbound.setdefault(node_id, [])

    def summarise_inventory(
        self, node_id: str, sku: str, promise_by: datetime
    ) -> NodeInventoryState | None:
        """Return a lightweight snapshot used for ATP calculations."""

        record = self.inventory_record(node_id, sku)
        if record is None:
            return None

        from .models import InboundWindow

        inbound_objs: list[InboundWindow] = []
        if self.config.routing.use_inbound_windowing:
            for shipment in self.inbound_for(node_id):
                if shipment.sku != sku:
                    continue
                inbound_objs.append(
                    InboundWindow(
                        qty=shipment.qty,
                        eta_start=shipment.eta_start,
                        eta_end=shipment.eta_end,
                    )
                )

        return NodeInventoryState(
            on_hand=record.on_hand_true,
            allocated=record.allocated,
            safety_stock=record.safety_stock,
            inbound=inbound_objs,
        )

    def update_inventory(
        self,
        node_id: str,
        sku: str,
        delta_on_hand: int = 0,
        delta_allocated: int = 0,
    ) -> InventoryRecord | None:
        """Apply mutations to inventory levels."""

        record = self.inventory_record(node_id, sku)
        if record is None:
            return None

        record.on_hand_true += delta_on_hand
        record.allocated += delta_allocated
        if record.on_hand_true < 0:
            record.on_hand_true = 0
        if record.allocated < 0:
            record.allocated = 0
        record.observed_on_hand = max(record.on_hand_true, 0)
        return record

    def ensure_inventory(self, node: Node, sku: str) -> InventoryRecord:
        """Ensure an inventory record exists for the node/sku pair."""

        node_inventory = self.inventory.setdefault(node.node_id, {})
        record = node_inventory.get(sku)
        if record is None:
            record = InventoryRecord(
                sku=sku,
                on_hand_true=node.safety_stock,
                allocated=0,
                safety_stock=node.safety_stock,
                last_cycle_count=datetime.utcnow(),
                observed_on_hand=node.safety_stock,
            )
            node_inventory[sku] = record
        return record

    def can_fulfil(self, node_id: str, line: OrderLine, promise_by: datetime) -> bool:
        """Return ``True`` if node can satisfy the order line within the promise window."""

        summary = self.summarise_inventory(node_id, line.sku, promise_by)
        if summary is None:
            return False
        available = summary.atp_window(promise_by)
        return available >= line.qty

    def track_allocation(self, allocation: Allocation) -> None:
        """Persist an allocation for later fulfilment."""

        self.active_allocations[allocation.allocation_id] = allocation

    def resolve_mode(self, node: Node) -> FulfillmentMode:
        """Infer the preferred shipping mode for a node."""

        if node.type == node.type.DC:
            return FulfillmentMode.SHIP_FROM_DC
        if node.capabilities.bopis:
            return FulfillmentMode.BOPIS
        return FulfillmentMode.SHIP_FROM_STORE
