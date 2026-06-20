"""
Supply chain disruption simulation endpoints.

This module handles creating, managing, and applying supply chain disruptions
that affect streaming events for demo purposes.
"""

import logging
import random
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, status

from ...api.models import (
    ActiveDisruptionsResponse,
    DisruptionRequest,
    DisruptionResponse,
    OperationResult,
)
from ...shared.dependencies import rate_limit
from .state import active_disruptions

logger = logging.getLogger(__name__)

router = APIRouter()


def _cleanup_expired_disruptions():
    """Remove expired disruptions from active list."""
    now = datetime.now(UTC)
    expired_keys = [
        disruption_id
        for disruption_id, data in active_disruptions.items()
        if data["active_until"] <= now
    ]
    for key in expired_keys:
        del active_disruptions[key]


@router.post(
    "/disruption/create",
    response_model=DisruptionResponse,
    summary="Create supply chain disruption",
    description="Create a supply chain disruption that affects streaming events",
)
@rate_limit(max_requests=20, window_seconds=60)
async def create_disruption(request: DisruptionRequest):
    """Create a new supply chain disruption."""
    # Clean up expired disruptions first
    _cleanup_expired_disruptions()

    # Generate disruption ID
    disruption_id = f"disruption_{uuid4().hex[:8]}"
    active_until = datetime.now(UTC) + timedelta(minutes=request.duration_minutes)

    # Create disruption data
    disruption_data = {
        "disruption_id": disruption_id,
        "type": request.disruption_type,
        "target_id": request.target_id,
        "severity": request.severity,
        "product_ids": request.product_ids or [],
        "created_at": datetime.now(UTC),
        "active_until": active_until,
        "events_affected": 0,
        "status": "active",
    }

    # Store in global state
    active_disruptions[disruption_id] = disruption_data

    logger.info(
        f"Created disruption {disruption_id}: {request.disruption_type} "
        f"affecting target {request.target_id}"
    )

    return DisruptionResponse(
        success=True,
        disruption_id=disruption_id,
        message=(
            f"Created {request.disruption_type} disruption "
            f"for target {request.target_id}"
        ),
        active_until=active_until,
    )


@router.get(
    "/disruption/list",
    response_model=ActiveDisruptionsResponse,
    summary="List active disruptions",
    description="Get list of all currently active supply chain disruptions",
)
async def list_active_disruptions():
    """Get list of all active disruptions."""
    # Clean up expired disruptions first
    _cleanup_expired_disruptions()

    # Format disruptions for response
    disruptions = []
    for disruption_id, data in active_disruptions.items():
        disruption_info = {
            "disruption_id": disruption_id,
            "type": data["type"],
            "target_id": data["target_id"],
            "severity": data["severity"],
            "created_at": data["created_at"],
            "active_until": data["active_until"],
            "time_remaining_minutes": max(
                0,
                (data["active_until"] - datetime.now(UTC)).total_seconds() / 60,
            ),
            "events_affected": data.get("events_affected", 0),
            "status": data["status"],
        }
        disruptions.append(disruption_info)

    return ActiveDisruptionsResponse(
        disruptions=disruptions,
        count=len(disruptions),
        timestamp=datetime.now(UTC),
    )


@router.delete(
    "/disruption/{disruption_id}",
    response_model=OperationResult,
    summary="Cancel disruption",
    description="Cancel an active supply chain disruption",
)
async def cancel_disruption(disruption_id: str):
    """Cancel an active disruption."""
    if disruption_id not in active_disruptions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Disruption {disruption_id} not found or already expired",
        )

    # Remove the disruption
    active_disruptions.pop(disruption_id)

    logger.info(f"Cancelled disruption {disruption_id}")

    return OperationResult(
        success=True,
        message=f"Cancelled disruption {disruption_id}",
        operation_id=disruption_id,
    )


@router.post(
    "/disruption/clear-all",
    response_model=OperationResult,
    summary="Clear all disruptions",
    description="Cancel all active supply chain disruptions",
)
async def clear_all_disruptions():
    """Cancel all active disruptions."""
    count = len(active_disruptions)
    active_disruptions.clear()

    logger.info(f"Cleared all {count} active disruptions")

    return OperationResult(success=True, message=f"Cleared {count} active disruptions")


def get_active_disruptions_for_target(
    target_type: str, target_id: int
) -> list[dict[str, Any]]:
    """Get active disruptions affecting a specific target."""
    _cleanup_expired_disruptions()

    matching_disruptions = []
    for disruption_data in active_disruptions.values():
        if disruption_data["target_id"] == target_id:
            # Check if disruption type matches target type
            disruption_type = disruption_data["type"]
            if (
                (
                    target_type == "dc"
                    and disruption_type in ["dc_outage", "inventory_shortage"]
                )
                or (
                    target_type == "store" and disruption_type in ["inventory_shortage"]
                )
                or (
                    target_type == "truck"
                    and disruption_type in ["truck_breakdown", "weather_delay"]
                )
            ):
                matching_disruptions.append(disruption_data)

    return matching_disruptions


def apply_disruption_effects(event_data: dict[str, Any]) -> dict[str, Any]:
    """Apply disruption effects to an event if applicable."""
    # Clean up expired disruptions
    _cleanup_expired_disruptions()

    if not active_disruptions:
        return event_data

    event_type = event_data.get("event_type")
    payload = event_data.get("payload", {})

    # Apply disruptions based on event type and target
    for disruption_id, disruption_data in active_disruptions.items():
        disruption_type = disruption_data["type"]
        target_id = disruption_data["target_id"]
        severity = disruption_data["severity"]

        modified = False

        # DC inventory disruptions
        if (
            event_type == "dc_inventory_txn"
            and disruption_type in ["dc_outage", "inventory_shortage"]
            and payload.get("DCID") == target_id
        ):
            if disruption_type == "dc_outage":
                # Cancel all DC operations
                if random.random() < severity:
                    payload["QtyDelta"] = 0
                    payload["Reason"] = "OUTAGE"
                    modified = True

            elif disruption_type == "inventory_shortage":
                # Reduce inventory quantities
                if payload.get("QtyDelta", 0) > 0:  # Only affect positive inventory
                    reduction_factor = severity * random.uniform(0.5, 1.0)
                    payload["QtyDelta"] = max(
                        0, int(payload["QtyDelta"] * (1 - reduction_factor))
                    )
                    modified = True

        # Store inventory disruptions
        elif (
            event_type == "store_inventory_txn"
            and disruption_type == "inventory_shortage"
            and payload.get("StoreID") == target_id
        ):
            # Reduce store deliveries
            if (
                payload.get("QtyDelta", 0) > 0
                and payload.get("Reason") == "INBOUND_SHIPMENT"
            ):
                reduction_factor = severity * random.uniform(0.3, 0.8)
                payload["QtyDelta"] = max(
                    0, int(payload["QtyDelta"] * (1 - reduction_factor))
                )
                modified = True

        # Truck movement disruptions
        elif (
            event_type == "truck_move"
            and disruption_type in ["truck_breakdown", "weather_delay"]
            and payload.get("TruckId") == str(target_id)
        ):
            if disruption_type == "truck_breakdown":
                # Set truck to delayed status
                if random.random() < severity:
                    payload["Status"] = "DELAYED"
                    # Extend ETA
                    if "ETA" in payload:
                        original_eta = datetime.fromisoformat(
                            payload["ETA"].replace("Z", "+00:00")
                        )
                        delay_hours = int(severity * 8)  # Up to 8 hour delay
                        payload["ETA"] = (
                            original_eta + timedelta(hours=delay_hours)
                        ).isoformat()
                    modified = True

            elif disruption_type == "weather_delay":
                # Weather affects all trucks in area - add delays
                if random.random() < severity * 0.7:  # 70% of severity probability
                    payload["Status"] = "DELAYED"
                    delay_hours = int(severity * 4)  # Up to 4 hour delay
                    if "ETA" in payload:
                        original_eta = datetime.fromisoformat(
                            payload["ETA"].replace("Z", "+00:00")
                        )
                        payload["ETA"] = (
                            original_eta + timedelta(hours=delay_hours)
                        ).isoformat()
                    modified = True

        if modified:
            # Increment events affected counter
            disruption_data["events_affected"] += 1
            # Add disruption marker to event
            event_data["disruption_applied"] = {
                "disruption_id": disruption_id,
                "type": disruption_type,
                "severity": severity,
            }

    return event_data
