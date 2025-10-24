"""Utilities for computing reference omnichannel metrics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

MODE_TARGETS = {
    "SHIP_FROM_DC": (0.55, 0.65),
    "SHIP_FROM_STORE": (0.20, 0.30),
    "BOPIS": (0.10, 0.20),
    "CURBSIDE": (0.10, 0.20),
}


def _rate(bounds: tuple[float, float], value: float) -> dict[str, Any]:
    return {
        "value": round(value, 4),
        "lower": bounds[0],
        "upper": bounds[1],
        "within_bounds": bounds[0] <= value <= bounds[1],
    }


def compute_mode_mix(allocations: pd.DataFrame) -> dict[str, Any]:
    """Compute the share of fulfillment modes."""

    if allocations.empty:
        return {}
    mode_counts = allocations.groupby("mode")[["order_id"]].count()
    total = float(mode_counts["order_id"].sum())
    ratios = {
        mode: count["order_id"] / total for mode, count in mode_counts.iterrows()
    }
    return {mode: _rate(MODE_TARGETS.get(mode, (0.0, 1.0)), ratio) for mode, ratio in ratios.items()}


def compute_split_rate(allocations: pd.DataFrame) -> float:
    """Percentage of orders fulfilled by more than one node."""

    if allocations.empty:
        return 0.0
    per_order = allocations.groupby("order_id")["node_id"].nunique()
    split_orders = float((per_order > 1).sum())
    return split_orders / float(len(per_order))


def compute_on_time_rate(events: pd.DataFrame) -> float:
    """Calculate on-time delivery rate from fulfillment events."""

    delivered = events[events["event_type"] == "DELIVERED"].copy()
    if delivered.empty:
        return 0.0
    on_time = delivered[delivered["event_time"] <= delivered["promise_by"]]
    return float(len(on_time)) / float(len(delivered))


def compute_pick_fail_rate(events: pd.DataFrame) -> float:
    """Compute pick failure rate."""

    picks = events[events["event_type"].isin(["PICK_CONFIRMED", "PICK_FAILED"])]
    if picks.empty:
        return 0.0
    failed = picks[picks["event_type"] == "PICK_FAILED"]
    return float(len(failed)) / float(len(picks))


def compute_inventory_accuracy(snapshots: pd.DataFrame) -> float:
    """Calculate average absolute error relative to ground truth."""

    if snapshots.empty:
        return 0.0
    diff = (snapshots["on_hand_true"] - snapshots["on_hand"]).abs()
    return float(1.0 - (diff.mean() / snapshots["on_hand_true"].replace(0, 1).mean()))


def evaluate_metrics(base_path: Path) -> dict[str, Any]:
    """Load sample CSV outputs and compute tracking metrics."""

    allocations = pd.read_csv(base_path / "allocations.csv", parse_dates=["reserved_at", "expires_at"])
    events = pd.read_csv(base_path / "fulfillment_events.csv", parse_dates=["event_time", "promise_by"])
    snapshots = pd.read_csv(base_path / "inventory_snapshots.csv", parse_dates=["time", "last_cycle_count"])

    return {
        "mode_mix": compute_mode_mix(allocations),
        "split_rate": compute_split_rate(allocations),
        "on_time_rate": compute_on_time_rate(events),
        "pick_fail_rate": compute_pick_fail_rate(events),
        "inventory_accuracy": compute_inventory_accuracy(snapshots),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute omnichannel metrics for generated data")
    parser.add_argument("path", type=Path, help="Path to directory containing omnichannel CSV outputs")
    args = parser.parse_args()
    metrics = evaluate_metrics(args.path)
    print(json.dumps(metrics, default=float, indent=2))


if __name__ == "__main__":
    main()
