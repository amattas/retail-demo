"""Helper functions used across omnichannel hooks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import product
from math import asin, cos, radians, sin, sqrt
from typing import Sequence

from .config import OmniConfig
from .models import (
    AllocationSelection,
    FulfillmentMode,
    Node,
    NodeCandidate,
    NodeType,
    Order,
    OrderConstraints,
    OrderLine,
)
from .state import OmniState

EARTH_RADIUS_KM = 6371.0
DEFAULT_LINE_WEIGHT_KG = 1.0
SHIPPING_SPEED_KMH = 55.0


@dataclass(slots=True)
class Destination:
    """Simplified destination structure used in helper functions."""

    lat: float
    lon: float


def distance_km(node: Node, dest: Destination) -> float:
    """Compute haversine distance between a node and the order destination."""

    lat1 = radians(node.lat)
    lon1 = radians(node.lon)
    lat2 = radians(dest.lat)
    lon2 = radians(dest.lon)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return EARTH_RADIUS_KM * c


def eta_hours(
    node: Node,
    dest: Destination,
    mode: FulfillmentMode,
    now: datetime,
    picks_per_hour: float,
    backlog: int,
) -> float:
    """Compute travel + handling time in hours for the candidate."""

    travel_hours = 0.0
    if mode in (FulfillmentMode.SHIP_FROM_STORE, FulfillmentMode.SHIP_FROM_DC):
        travel_hours = distance_km(node, dest) / SHIPPING_SPEED_KMH
    else:
        # pickup flows leverage customer drive time, approximated as half travel duration
        travel_hours = distance_km(node, dest) / (SHIPPING_SPEED_KMH * 1.5)

    queue_delay = backlog / picks_per_hour if picks_per_hour > 0 else 0.0

    if mode in (FulfillmentMode.BOPIS, FulfillmentMode.CURBSIDE):
        # Add a small buffer for staging
        queue_delay += 0.5

    return max(0.0, travel_hours + queue_delay)


def shipping_cost(node: Node, lines: Sequence[OrderLine], distance: float) -> float:
    """Return the shipping cost for the candidate."""

    total_weight = sum(line.weight_kg or DEFAULT_LINE_WEIGHT_KG for line in lines)
    return node.ship_base_cost + node.per_km_rate * distance + node.per_kg_rate * total_weight


def handling_cost(node: Node, lines: Sequence[OrderLine]) -> float:
    """Return handling cost for the node."""

    line_count = sum(line.qty for line in lines)
    return node.handling_cost * max(1, line_count)


def sla_risk_penalty(eta: datetime, promise_by: datetime, cfg: OmniConfig) -> float:
    """Calculate SLA risk penalty based on lateness."""

    if eta <= promise_by:
        return 0.0
    delta_hours = (eta - promise_by).total_seconds() / 3600.0
    return cfg.routing.sla_penalty_lambda * max(0.0, delta_hours)


def opportunity_cost(node: Node, post_allocation_stock: int, cfg: OmniConfig) -> float:
    """Opportunity cost grows as stock falls below safety stock."""

    if post_allocation_stock >= node.safety_stock:
        return 0.0
    slack = max(1, node.safety_stock)
    shortage_ratio = (node.safety_stock - post_allocation_stock) / slack
    accuracy_penalty = 1.0 - node.accuracy_score
    return shortage_ratio * (1.0 + accuracy_penalty) * cfg.routing.split_penalty


def total_cost(
    node_set: Sequence[NodeCandidate],
    order: Order,
    state: OmniState,
    cfg: OmniConfig,
) -> float:
    """Aggregate total cost across the node set."""

    if len(node_set) != len(order.lines):
        raise ValueError("Node set must align with order lines for cost calculation")

    total = 0.0
    lines_by_node: dict[str, list[OrderLine]] = {}
    candidate_by_node: dict[str, NodeCandidate] = {}

    for candidate, line in zip(node_set, order.lines):
        bucket = lines_by_node.setdefault(candidate.node.node_id, [])
        bucket.append(line)
        candidate_by_node.setdefault(candidate.node.node_id, candidate)

    split_penalty = cfg.routing.split_penalty if len(lines_by_node) > 1 else 0.0

    for node_id, lines in lines_by_node.items():
        candidate = candidate_by_node[node_id]
        distance = candidate.distance_km
        total += shipping_cost(candidate.node, lines, distance)
        total += handling_cost(candidate.node, lines)
        eta = order.created_at + timedelta(hours=candidate.eta_hours)
        total += sla_risk_penalty(eta, order.constraints.promise_by, cfg)
        for line in lines:
            inventory_state = state.summarise_inventory(
                node_id,
                line.sku,
                order.constraints.promise_by,
            )
            if inventory_state is None:
                continue
            post_allocation = inventory_state.on_hand - line.qty
            total += opportunity_cost(candidate.node, post_allocation, cfg)

    return total + split_penalty


def _allowed_modes(constraints: OrderConstraints) -> set[FulfillmentMode]:
    return set(constraints.mode_allow)


def shortlist(
    order: Order,
    line: OrderLine,
    state: OmniState,
    cfg: OmniConfig,
) -> list[NodeCandidate]:
    """Return ranked shortlist of feasible nodes for the order line."""

    allowed_modes = _allowed_modes(order.constraints)
    dest = Destination(lat=order.ship_to.lat, lon=order.ship_to.lon)
    candidates: list[NodeCandidate] = []

    for node in state.iter_nodes():
        if line.sku not in state.inventory.get(node.node_id, {}):
            continue

        potential_modes: list[FulfillmentMode] = []
        if node.type == NodeType.DC and FulfillmentMode.SHIP_FROM_DC in allowed_modes:
            potential_modes.append(FulfillmentMode.SHIP_FROM_DC)
        if node.capabilities.ship and FulfillmentMode.SHIP_FROM_STORE in allowed_modes:
            potential_modes.append(FulfillmentMode.SHIP_FROM_STORE)
        if node.capabilities.bopis and FulfillmentMode.BOPIS in allowed_modes:
            potential_modes.append(FulfillmentMode.BOPIS)
        if node.capabilities.curbside and FulfillmentMode.CURBSIDE in allowed_modes:
            potential_modes.append(FulfillmentMode.CURBSIDE)
        if not potential_modes:
            continue

        summary = state.summarise_inventory(node.node_id, line.sku, order.constraints.promise_by)
        if summary is None:
            continue

        atp_window = summary.atp_window(order.constraints.promise_by)
        if atp_window < line.qty:
            continue

        dist = distance_km(node, dest)
        for mode in potential_modes:
            eta = eta_hours(
                node,
                dest,
                mode,
                now=order.created_at,
                picks_per_hour=node.capacity.picks_per_hour,
                backlog=node.capacity.backlog,
            )

            candidates.append(
                NodeCandidate(
                    node=node,
                    available_qty=atp_window,
                    eta_hours=eta,
                    distance_km=dist,
                    mode=mode,
                )
            )

    candidates.sort(key=lambda c: (c.distance_km, c.eta_hours))
    return candidates[: cfg.routing.shortlist_k]


def meets_sla(
    node_set: Sequence[NodeCandidate],
    order: Order,
    state: OmniState,
    cfg: OmniConfig,
) -> bool:
    """Return True if the combination satisfies promise constraints."""

    promise_by = order.constraints.promise_by
    for candidate in node_set:
        eta = order.created_at + timedelta(hours=candidate.eta_hours)
        if eta > promise_by:
            return False
    return True


def try_reserve(selection: Sequence[AllocationSelection], state: OmniState) -> bool:
    """Attempt to reserve inventory for the provided selection."""

    # First pass: ensure availability
    for choice in selection:
        for line in choice.lines:
            record = state.inventory_record(choice.node_id, line.sku)
            if record is None:
                return False
            if record.on_hand_true - record.allocated - record.safety_stock < line.qty:
                return False

    # Second pass: commit reservations
    for choice in selection:
        for line in choice.lines:
            state.update_inventory(choice.node_id, line.sku, delta_allocated=line.qty)

    return True


def node_combinations(
    per_line: Sequence[Sequence[NodeCandidate]],
    allow_split: bool,
    max_nodes: int,
) -> list[tuple[NodeCandidate, ...]]:
    """Utility to generate feasible node combinations."""

    if not per_line:
        return []

    if not allow_split:
        combos: list[tuple[NodeCandidate, ...]] = []
        for candidate in per_line[0]:
            node_id = candidate.node.node_id
            sequence: list[NodeCandidate] = [candidate]
            feasible = True
            for candidates in per_line[1:]:
                match = next(
                    (item for item in candidates if item.node.node_id == node_id),
                    None,
                )
                if match is None:
                    feasible = False
                    break
                sequence.append(match)
            if feasible:
                combos.append(tuple(sequence))
        return combos

    combos = []
    for combo in product(*per_line):
        nodes = {candidate.node.node_id for candidate in combo}
        if len(nodes) > max_nodes:
            continue
        combos.append(combo)
    return combos


__all__ = [
    "Destination",
    "distance_km",
    "eta_hours",
    "shipping_cost",
    "handling_cost",
    "sla_risk_penalty",
    "opportunity_cost",
    "total_cost",
    "shortlist",
    "meets_sla",
    "try_reserve",
    "node_combinations",
]
