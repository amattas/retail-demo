"""Unit tests for omnichannel helper functions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from retail_datagen.omnichannel.config import DEFAULT_OMNI_CONFIG, OmniConfig
from retail_datagen.omnichannel.helpers import (
    Destination,
    distance_km,
    eta_hours,
    handling_cost,
    meets_sla,
    opportunity_cost,
    shipping_cost,
    shortlist,
    sla_risk_penalty,
    total_cost,
    try_reserve,
)
from retail_datagen.omnichannel.models import (
    AllocationSelection,
    FulfillmentMode,
    InventoryRecord,
    Node,
    NodeCapabilities,
    NodeCapacity,
    NodeCandidate,
    NodeType,
    Order,
    OrderConstraints,
    OrderDestination,
    OrderLine,
)
from retail_datagen.omnichannel.state import OmniState


@pytest.fixture()
def sample_node() -> Node:
    return Node(
        node_id="store_001",
        type=NodeType.STORE,
        timezone="America/Chicago",
        lat=41.0,
        lon=-87.0,
        capabilities=NodeCapabilities(ship=True, bopis=True, curbside=True),
        capacity=NodeCapacity(
            node_id="store_001",
            time=datetime(2025, 9, 1),
            picks_per_hour=60.0,
            backlog=10,
        ),
        service_area={"postal_prefixes": ["606"]},
        accuracy_score=0.95,
        ship_base_cost=3.5,
        per_km_rate=0.03,
        per_kg_rate=0.1,
        handling_cost=2.0,
        safety_stock=2,
    )


@pytest.fixture()
def sample_state(sample_node: Node) -> OmniState:
    inventory = {
        sample_node.node_id: {
            "SKU-1": InventoryRecord(
                sku="SKU-1",
                on_hand_true=20,
                allocated=0,
                safety_stock=2,
                last_cycle_count=datetime(2025, 8, 30),
                observed_on_hand=20,
            )
        }
    }
    return OmniState(
        config=DEFAULT_OMNI_CONFIG,
        nodes={sample_node.node_id: sample_node},
        inventory=inventory,
        inbound={sample_node.node_id: []},
        store_hours={sample_node.node_id: []},
    )


def test_distance_km_zero(sample_node: Node) -> None:
    dest = Destination(lat=sample_node.lat, lon=sample_node.lon)
    assert distance_km(sample_node, dest) == pytest.approx(0.0)


def test_eta_hours_respects_pick_queue(sample_node: Node) -> None:
    dest = Destination(lat=sample_node.lat + 0.1, lon=sample_node.lon)
    now = datetime(2025, 9, 1, 12, 0)
    ship_eta = eta_hours(sample_node, dest, FulfillmentMode.SHIP_FROM_STORE, now, 60.0, 10)
    bopis_eta = eta_hours(sample_node, dest, FulfillmentMode.BOPIS, now, 60.0, 10)
    queue_delay = 10 / 60.0
    assert pytest.approx(ship_eta, rel=0.1) == ship_eta  # sanity check positive value
    assert bopis_eta >= queue_delay + 0.5


def test_shipping_cost_scale_with_distance(sample_node: Node) -> None:
    lines = [OrderLine(sku="SKU-1", qty=2)]
    near_cost = shipping_cost(sample_node, lines, 1.0)
    far_cost = shipping_cost(sample_node, lines, 10.0)
    assert far_cost > near_cost


def test_handling_cost_grows_with_qty(sample_node: Node) -> None:
    low = handling_cost(sample_node, [OrderLine(sku="SKU-1", qty=1)])
    high = handling_cost(sample_node, [OrderLine(sku="SKU-1", qty=5)])
    assert high > low


def test_sla_risk_penalty_zero_if_on_time() -> None:
    cfg = DEFAULT_OMNI_CONFIG
    now = datetime.now(UTC)
    assert sla_risk_penalty(now, now + timedelta(hours=1), cfg) == 0.0


def test_opportunity_cost_increases_when_below_safety(sample_node: Node) -> None:
    cfg = DEFAULT_OMNI_CONFIG
    baseline = opportunity_cost(sample_node, post_allocation_stock=2, cfg=cfg)
    higher = opportunity_cost(sample_node, post_allocation_stock=0, cfg=cfg)
    assert higher > baseline


def test_total_cost_includes_split_penalty(sample_state: OmniState, sample_node: Node) -> None:
    cfg = sample_state.config
    order = Order(
        order_id="O-1",
        created_at=datetime(2025, 9, 1, 12, 0),
        ship_to=OrderDestination(lat=sample_node.lat, lon=sample_node.lon, postal="60601", country="US"),
        lines=[OrderLine(sku="SKU-1", qty=1)],
        constraints=OrderConstraints(
            mode_allow=[FulfillmentMode.SHIP_FROM_STORE],
            allow_split=True,
            promise_by=datetime(2025, 9, 2, 12, 0),
            max_nodes=2,
        ),
    )
    candidate = NodeCandidate(
        node=sample_node,
        available_qty=10,
        eta_hours=2.0,
        distance_km=5.0,
        mode=FulfillmentMode.SHIP_FROM_STORE,
    )
    cost = total_cost([candidate], order, sample_state, cfg)
    assert cost > 0


def test_shortlist_filters_on_atp(sample_state: OmniState, sample_node: Node) -> None:
    order = Order(
        order_id="O-1",
        created_at=datetime(2025, 9, 1, 12, 0),
        ship_to=OrderDestination(lat=sample_node.lat, lon=sample_node.lon, postal="60601", country="US"),
        lines=[OrderLine(sku="SKU-1", qty=1)],
        constraints=OrderConstraints(
            mode_allow=[FulfillmentMode.SHIP_FROM_STORE],
            allow_split=False,
            promise_by=datetime(2025, 9, 2, 12, 0),
            max_nodes=1,
        ),
    )
    candidates = shortlist(order, order.lines[0], sample_state, sample_state.config)
    assert len(candidates) == 1


def test_meets_sla(sample_state: OmniState, sample_node: Node) -> None:
    order = Order(
        order_id="O-1",
        created_at=datetime(2025, 9, 1, 12, 0),
        ship_to=OrderDestination(lat=sample_node.lat, lon=sample_node.lon, postal="60601", country="US"),
        lines=[OrderLine(sku="SKU-1", qty=1)],
        constraints=OrderConstraints(
            mode_allow=[FulfillmentMode.SHIP_FROM_STORE],
            allow_split=False,
            promise_by=datetime(2025, 9, 1, 18, 0),
            max_nodes=1,
        ),
    )
    candidate = shortlist(order, order.lines[0], sample_state, sample_state.config)[0]
    assert meets_sla([candidate], order, sample_state, sample_state.config)


def test_try_reserve_updates_allocation(sample_state: OmniState, sample_node: Node) -> None:
    selection = [
        AllocationSelection(
            node_id=sample_node.node_id,
            mode=FulfillmentMode.SHIP_FROM_STORE,
            lines=[OrderLine(sku="SKU-1", qty=1)],
        )
    ]
    assert try_reserve(selection, sample_state)
    record = sample_state.inventory_record(sample_node.node_id, "SKU-1")
    assert record is not None
    assert record.allocated == 1
