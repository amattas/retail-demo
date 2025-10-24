"""Hook implementations for omnichannel data generation."""

from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence
from uuid import uuid4

from .config import OmniConfig
from .helpers import (
    handling_cost,
    meets_sla,
    node_combinations,
    opportunity_cost,
    shortlist,
    shipping_cost,
    sla_risk_penalty,
    total_cost,
    try_reserve,
)
from .models import (
    Allocation,
    AllocationBundle,
    AllocationSelection,
    DecisionFeatureVector,
    FulfillmentEvent,
    FulfillmentEventType,
    FulfillmentMode,
    InboundShipment,
    InventorySnapshot,
    Node,
    NodeCapabilities,
    NodeCapacity,
    NodeCandidate,
    NodeType,
    Order,
    OrderLine,
    PerturbationResult,
    QuoteBundle,
    QuoteCandidate,
    StoreHours,
    combine_lines,
    flatten_candidates,
)
from .state import OmniState

TIMEZONES = [
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
]


def _ensure_config(config: OmniConfig | Mapping[str, Any]) -> OmniConfig:
    if isinstance(config, OmniConfig):
        return config
    return OmniConfig.from_dict(dict(config))


def _ensure_rng(rng: random.Random | None, seed: int) -> random.Random:
    if rng is None:
        return random.Random(seed)
    return rng


def _random_coordinate(rng: random.Random) -> tuple[float, float]:
    lat = rng.uniform(25.0, 48.5)
    lon = rng.uniform(-124.0, -67.0)
    return lat, lon


def _sample_postal_prefix(rng: random.Random) -> str:
    return f"{rng.randint(100, 999)}"


def _generate_store_hours(
    node_id: str,
    cfg: OmniConfig,
    rng: random.Random,
) -> list[StoreHours]:
    base_time = cfg.time.start
    hours: list[StoreHours] = []
    for day in range(7):
        open_time = base_time + timedelta(days=day, hours=9)
        close_time = base_time + timedelta(days=day, hours=21)
        hours.append(
            StoreHours(
                node_id=node_id,
                day_of_week=day,
                open_time=open_time,
                close_time=close_time,
            )
        )
    return hours


def _create_node(
    node_id: str,
    node_type: NodeType,
    cfg: OmniConfig,
    rng: random.Random,
) -> Node:
    lat, lon = _random_coordinate(rng)
    timezone = rng.choice(TIMEZONES)
    ship_base = cfg.costs.ship_base_dc if node_type == NodeType.DC else cfg.costs.ship_base_store
    handling = cfg.costs.handling_dc if node_type == NodeType.DC else cfg.costs.handling_store
    accuracy = 0.98 if node_type == NodeType.DC else max(
        0.0,
        min(1.0, rng.gauss(cfg.network.store_accuracy_mean, cfg.network.store_accuracy_std)),
    )
    capabilities = NodeCapabilities(
        ship=True,
        bopis=node_type == NodeType.STORE,
        curbside=node_type == NodeType.STORE,
    )
    capacity = NodeCapacity(
        node_id=node_id,
        time=cfg.time.start,
        picks_per_hour=max(
            10.0,
            rng.gauss(cfg.capacity.pick_rate_store_mean, cfg.capacity.pick_rate_store_std),
        ) if node_type == NodeType.STORE else 120.0,
        backlog=int(rng.random() * 20 if node_type == NodeType.STORE else rng.random() * 100),
    )
    return Node(
        node_id=node_id,
        type=node_type,
        timezone=timezone,
        lat=lat,
        lon=lon,
        capabilities=capabilities,
        capacity=capacity,
        service_area={"postal_prefixes": [_sample_postal_prefix(rng)]},
        accuracy_score=accuracy,
        ship_base_cost=ship_base,
        per_km_rate=cfg.costs.per_km,
        per_kg_rate=cfg.costs.per_kg,
        handling_cost=handling,
        safety_stock=cfg.network.default_safety_stock,
    )


def _initial_inventory(
    node: Node,
    sku: str,
    cfg: OmniConfig,
    rng: random.Random,
) -> InventoryRecord:
    if node.type == NodeType.DC:
        on_hand = rng.randint(100, 500)
    else:
        on_hand = rng.randint(10, 80)
    safety = max(node.safety_stock, 1)
    last_cycle = cfg.time.start - timedelta(hours=rng.randint(1, 72))
    return InventoryRecord(
        sku=sku,
        on_hand_true=on_hand,
        allocated=0,
        safety_stock=safety,
        last_cycle_count=last_cycle,
        observed_on_hand=on_hand,
    )


def _generate_inbound(
    node: Node,
    sku: str,
    cfg: OmniConfig,
    rng: random.Random,
) -> list[InboundShipment]:
    shipments: list[InboundShipment] = []
    for _ in range(rng.randint(0, 2)):
        start_offset = rng.randint(1, 5)
        eta_start = cfg.time.start + timedelta(days=start_offset)
        eta_end = eta_start + timedelta(days=rng.randint(0, 2))
        shipment = InboundShipment(
            shipment_id=f"SHP-{uuid4().hex[:8]}",
            dest_node_id=node.node_id,
            sku=sku,
            qty=rng.randint(5, 40),
            eta_start=eta_start,
            eta_end=eta_end,
            source_node_id="DC" if node.type == NodeType.STORE else "VENDOR",
        )
        shipments.append(shipment)
    return shipments


def prepare(
    config: OmniConfig | Mapping[str, Any],
    rng: random.Random | None,
    catalogs: Mapping[str, Sequence[str]] | None = None,
) -> OmniState:
    """Initialise the omnichannel state."""

    cfg = _ensure_config(config)
    rand = _ensure_rng(rng, cfg.reproducibility.base_seed)
    skus = list((catalogs or {}).get("skus", [])) or [f"SKU-{i:05d}" for i in range(1, 51)]

    nodes: dict[str, Node] = {}
    inventory: dict[str, dict[str, InventoryRecord]] = {}
    inbound: dict[str, list[InboundShipment]] = {}
    store_hours: dict[str, list[NodeCapacity]] = {}

    for dc_index in range(cfg.network.dcs):
        node = _create_node(f"dc_{dc_index:03d}", NodeType.DC, cfg, rand)
        nodes[node.node_id] = node
        inventory[node.node_id] = {
            sku: _initial_inventory(node, sku, cfg, rand) for sku in skus
        }
        inbound[node.node_id] = []
        store_hours[node.node_id] = _generate_store_hours(node.node_id, cfg, rand)

    for store_index in range(cfg.network.stores):
        node = _create_node(f"store_{store_index:04d}", NodeType.STORE, cfg, rand)
        nodes[node.node_id] = node
        inventory[node.node_id] = {
            sku: _initial_inventory(node, sku, cfg, rand) for sku in skus
        }
        inbound[node.node_id] = []
        store_hours[node.node_id] = _generate_store_hours(node.node_id, cfg, rand)
        for sku in skus:
            inbound[node.node_id].extend(_generate_inbound(node, sku, cfg, rand))

    return OmniState(
        config=cfg,
        nodes=nodes,
        inventory=inventory,
        inbound=inbound,
        store_hours=store_hours,
    )


def emit_supply(time: datetime, state: OmniState) -> dict[str, list[Any]]:
    """Emit supply-side records for the snapshot cadence."""

    inventory_snapshots: list[InventorySnapshot] = []
    inbound_shipments: list[InboundShipment] = []
    capacities: list[NodeCapacity] = []
    hours_records: list[StoreHours] = []

    for node in state.iter_nodes():
        capacities.append(
            NodeCapacity(
                node_id=node.node_id,
                time=time,
                picks_per_hour=node.capacity.picks_per_hour,
                backlog=node.capacity.backlog,
            )
        )
        for sku, record in state.inventory[node.node_id].items():
            inventory_snapshots.append(
                InventorySnapshot(
                    time=time,
                    node_id=node.node_id,
                    sku=sku,
                    on_hand_true=record.on_hand_true,
                    on_hand=record.observed_on_hand,
                    allocated=record.allocated,
                    safety_stock=record.safety_stock,
                    last_cycle_count=record.last_cycle_count,
                    accuracy_applied=node.accuracy_score,
                )
            )
        for shipment in list(state.inbound_for(node.node_id)):
            inbound_shipments.append(shipment)
            if shipment.eta_start <= time <= shipment.eta_end:
                state.update_inventory(node.node_id, shipment.sku, delta_on_hand=shipment.qty)
                state.inbound[node.node_id].remove(shipment)

    for entries in state.store_hours.values():
        hours_records.extend(entries)

    return {
        "inventory_snapshots": inventory_snapshots,
        "inbound_shipments": inbound_shipments,
        "node_capacities": capacities,
        "store_hours": hours_records,
    }


def _candidate_to_selection(
    candidate_tuple: Sequence[NodeCandidate],
    order: Order,
) -> list[AllocationSelection]:
    grouped: dict[str, list[OrderLine]] = defaultdict(list)
    for candidate, line in zip(candidate_tuple, order.lines):
        grouped[candidate.node.node_id].append(line)
    selections: list[AllocationSelection] = []
    for node_id, lines in grouped.items():
        mode = candidate_tuple[0].mode
        for candidate in candidate_tuple:
            if candidate.node.node_id == node_id:
                mode = candidate.mode
                break
        selections.append(
            AllocationSelection(
                node_id=node_id,
                mode=mode,
                lines=tuple(lines),
            )
        )
    return selections


def _build_quote_candidate(
    combo: Sequence[NodeCandidate],
    order: Order,
    state: OmniState,
    cfg: OmniConfig,
) -> tuple[QuoteCandidate, DecisionFeatureVector, list[AllocationSelection]]:
    node_ids = [candidate.node.node_id for candidate in combo]
    combined_id = "+".join(node_ids)
    eta_hours = max(candidate.eta_hours for candidate in combo)
    eta = order.created_at + timedelta(hours=eta_hours)

    lines_by_node: dict[str, list[OrderLine]] = defaultdict(list)
    candidate_by_node: dict[str, NodeCandidate] = {}
    for candidate, line in zip(combo, order.lines):
        lines_by_node[candidate.node.node_id].append(line)
        candidate_by_node.setdefault(candidate.node.node_id, candidate)

    cost_breakdown = {
        "shipping": 0.0,
        "handling": 0.0,
        "sla_risk": 0.0,
        "opportunity": 0.0,
        "split_penalty": cfg.routing.split_penalty if len(lines_by_node) > 1 else 0.0,
    }

    for node_id, lines in lines_by_node.items():
        candidate = candidate_by_node[node_id]
        distance = candidate.distance_km
        cost_breakdown["shipping"] += shipping_cost(candidate.node, lines, distance)
        cost_breakdown["handling"] += handling_cost(candidate.node, lines)
        eta_candidate = order.created_at + timedelta(hours=candidate.eta_hours)
        cost_breakdown["sla_risk"] += sla_risk_penalty(
            eta_candidate,
            order.constraints.promise_by,
            cfg,
        )
        for line in lines:
            summary = state.summarise_inventory(
                node_id,
                line.sku,
                order.constraints.promise_by,
            )
            if summary is None:
                continue
            post_allocation = summary.on_hand - line.qty
            cost_breakdown["opportunity"] += opportunity_cost(
                candidate.node,
                post_allocation,
                cfg,
            )

    total = total_cost(combo, order, state, cfg)
    candidate = QuoteCandidate(
        node_id=combined_id,
        mode=combo[0].mode if len(set(node_ids)) == 1 else FulfillmentMode.SHIP_FROM_DC,
        eta=eta,
        fill_rate=1.0,
        cost_breakdown=cost_breakdown,
        total_cost=total,
        reasons=["Feasible", "Meets SLA"],
        line_qtys=combine_lines(order.lines),
    )
    feature = DecisionFeatureVector(
        node_ids=tuple(node_ids),
        score=total,
        sla_ok=meets_sla(combo, order, state, cfg),
        feature_vector={
            "distance_km": sum(c.distance_km for c in combo) / len(combo),
            "ship_cost": cost_breakdown["shipping"],
            "handling": cost_breakdown["handling"],
            "risk": cost_breakdown["sla_risk"],
            "opportunity": cost_breakdown["opportunity"],
            "backlog": sum(c.node.capacity.backlog for c in combo) / len(combo),
        },
    )
    selections = _candidate_to_selection(combo, order)
    return candidate, feature, selections


def quote(order: Order, time: datetime, state: OmniState) -> QuoteBundle:
    """Generate ranked quotes for an order."""

    cfg = state.config
    per_line_candidates = [shortlist(order, line, state, cfg) for line in order.lines]
    combos = node_combinations(
        per_line_candidates,
        allow_split=order.constraints.allow_split and cfg.routing.allow_split,
        max_nodes=min(order.constraints.max_nodes, cfg.routing.max_nodes),
    )

    ranked: list[tuple[QuoteCandidate, DecisionFeatureVector, list[AllocationSelection]]] = []
    for combo in combos:
        if not meets_sla(combo, order, state, cfg):
            continue
        candidate, feature, selections = _build_quote_candidate(combo, order, state, cfg)
        ranked.append((candidate, feature, selections))

    ranked.sort(key=lambda item: item[0].total_cost)

    if not ranked:
        raise ValueError(f"No feasible fulfillment candidates for order {order.order_id}")

    candidates = [item[0] for item in ranked]
    decision_vectors = [item[1] for item in ranked]
    selections = [item[2] for item in ranked]

    recommendation = {
        "node_id": candidates[0].node_id,
        "mode": candidates[0].mode.value if isinstance(candidates[0].mode, FulfillmentMode) else candidates[0].mode,
    }

    decision_trail = {
        "order_id": order.order_id,
        "ranked": flatten_candidates(decision_vectors),
        "chosen_index": 0,
    }

    return QuoteBundle(
        order_id=order.order_id,
        generated_at=time,
        candidates=candidates,
        recommendation=recommendation,
        decision_trail=decision_trail,
        candidate_selections=selections,
    )


def allocate(
    order: Order,
    selected: Sequence[AllocationSelection],
    time: datetime,
    state: OmniState,
) -> AllocationBundle:
    """Reserve inventory for the selected candidate."""

    cfg = state.config
    success = try_reserve(selected, state)
    attempted_fallback = False
    attempted_selection = list(selected)

    if not success and cfg.noise.reroute_enable:
        attempted_fallback = True
        quote_bundle = quote(order, time, state)
        for alt_selection in quote_bundle.candidate_selections[1:]:
            if try_reserve(alt_selection, state):
                selected = alt_selection
                success = True
                break

    allocation = Allocation(
        allocation_id=f"A-{uuid4().hex[:10]}",
        order_id=order.order_id,
        selection=tuple(selected),
        reserved_at=time,
        expires_at=time + timedelta(minutes=30),
        status="RESERVED" if success else "FAILED",
    )
    if success:
        state.track_allocation(allocation)

    return AllocationBundle(
        allocation=allocation,
        reserved=success,
        attempted_fallback=attempted_fallback,
        attempted_selection=attempted_selection,
    )


def _simulate_pick_times(
    allocation: Allocation,
    state: OmniState,
    rng: random.Random,
) -> list[tuple[AllocationSelection, datetime]]:
    results: list[tuple[AllocationSelection, datetime]] = []
    for selection in allocation.selection:
        node = state.nodes[selection.node_id]
        base_hours = sum(line.qty for line in selection.lines) / max(node.capacity.picks_per_hour, 1.0)
        jitter = rng.random() * 0.5
        ready_time = allocation.reserved_at + timedelta(hours=base_hours + jitter)
        results.append((selection, ready_time))
    return results


def realize(allocation: Allocation, clock: datetime, state: OmniState) -> list[FulfillmentEvent]:
    """Simulate downstream fulfillment events for the allocation."""

    events: list[FulfillmentEvent] = []
    cfg = state.config
    rand = random.Random(cfg.reproducibility.base_seed ^ hash(allocation.allocation_id))
    pick_schedule = _simulate_pick_times(allocation, state, rand)

    for selection, pick_time in pick_schedule:
        node = state.nodes[selection.node_id]
        fail_probability = cfg.noise.pick_fail_rate * (1.0 - node.accuracy_score)
        if rand.random() < fail_probability:
            events.append(
                FulfillmentEvent(
                    event_id=f"EV-{uuid4().hex[:8]}",
                    order_id=allocation.order_id,
                    allocation_id=allocation.allocation_id,
                    node_id=selection.node_id,
                    mode=selection.mode,
                    event_type=FulfillmentEventType.PICK_FAILED,
                    event_time=pick_time,
                    details={"reason": "inventory_shortage"},
                )
            )
            for line in selection.lines:
                state.update_inventory(selection.node_id, line.sku, delta_allocated=-line.qty)
            continue

        events.append(
            FulfillmentEvent(
                event_id=f"EV-{uuid4().hex[:8]}",
                order_id=allocation.order_id,
                allocation_id=allocation.allocation_id,
                node_id=selection.node_id,
                mode=selection.mode,
                event_type=FulfillmentEventType.PICK_CONFIRMED,
                event_time=pick_time,
                details={},
            )
        )
        for line in selection.lines:
            state.update_inventory(
                selection.node_id,
                line.sku,
                delta_on_hand=-line.qty,
                delta_allocated=-line.qty,
            )

        if selection.mode in (FulfillmentMode.SHIP_FROM_STORE, FulfillmentMode.SHIP_FROM_DC):
            ship_time = pick_time + timedelta(hours=1)
            deliver_time = ship_time + timedelta(days=2)
            events.append(
                FulfillmentEvent(
                    event_id=f"EV-{uuid4().hex[:8]}",
                    order_id=allocation.order_id,
                    allocation_id=allocation.allocation_id,
                    node_id=selection.node_id,
                    mode=selection.mode,
                    event_type=FulfillmentEventType.SHIPPED,
                    event_time=ship_time,
                    details={"carrier": "OmniCarrier"},
                )
            )
            events.append(
                FulfillmentEvent(
                    event_id=f"EV-{uuid4().hex[:8]}",
                    order_id=allocation.order_id,
                    allocation_id=allocation.allocation_id,
                    node_id=selection.node_id,
                    mode=selection.mode,
                    event_type=FulfillmentEventType.DELIVERED,
                    event_time=deliver_time,
                    details={},
                )
            )
        else:
            ready_time = pick_time + timedelta(hours=0.5)
            pickup_time = ready_time + timedelta(hours=2)
            events.append(
                FulfillmentEvent(
                    event_id=f"EV-{uuid4().hex[:8]}",
                    order_id=allocation.order_id,
                    allocation_id=allocation.allocation_id,
                    node_id=selection.node_id,
                    mode=selection.mode,
                    event_type=FulfillmentEventType.READY_FOR_PICKUP,
                    event_time=ready_time,
                    details={},
                )
            )
            events.append(
                FulfillmentEvent(
                    event_id=f"EV-{uuid4().hex[:8]}",
                    order_id=allocation.order_id,
                    allocation_id=allocation.allocation_id,
                    node_id=selection.node_id,
                    mode=selection.mode,
                    event_type=FulfillmentEventType.PICKED_UP,
                    event_time=pickup_time,
                    details={},
                )
            )

    return events


def perturb(
    batch: Sequence[dict[str, Any]],
    config: OmniConfig,
    rng: random.Random | None = None,
) -> PerturbationResult:
    """Apply observational noise to a batch of records."""

    rand = _ensure_rng(rng, config.reproducibility.base_seed + 1)
    noisy: list[dict[str, Any]] = []
    applied = {
        "inventory_miscount": 0,
        "event_latency": 0,
        "ooo_events": 0,
    }

    for record in batch:
        mutated = dict(record)
        if "on_hand" in mutated and rand.random() < config.noise.inventory_miscount_rate:
            delta = rand.choice([-1, 1])
            mutated["on_hand"] = max(0, mutated["on_hand"] + delta)
            applied["inventory_miscount"] += 1
        if "event_time" in mutated and isinstance(mutated["event_time"], datetime):
            latency = rand.expovariate(1.0 / max(config.noise.event_latency_seconds_p95, 1))
            mutated["event_time"] = mutated["event_time"] + timedelta(seconds=latency)
            applied["event_latency"] += 1
        noisy.append(mutated)

    if batch and rand.random() < config.noise.ooo_events_probability:
        noisy.reverse()
        applied["ooo_events"] = len(batch)

    return PerturbationResult(records=noisy, applied_noise=applied)
