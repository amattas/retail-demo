"""
Online order generation with complete lifecycle support.

This module provides the enhanced online order generation logic with:
- Multi-line order support (already exists via basket)
- Status progression (created -> picked -> shipped -> delivered)
- Proper tax calculation based on fulfillment location
- Realistic tender type distribution
"""

from datetime import datetime, timedelta
from decimal import Decimal

from retail_datagen.shared.models import (
    InventoryReason,
    ProductTaxability,
    TenderType,
)


def generate_online_orders_with_lifecycle(
    date: datetime,
    config,
    customers: list,
    geographies: list,
    stores: list,
    distribution_centers: list,
    customer_journey_sim,
    inventory_flow_sim,
    temporal_patterns,
    rng,
    generate_trace_id_func,
    basket_adjuster=None,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Generate online orders for the given date with complete lifecycle and corresponding inventory effects.

    Creates orders with status progression (created -> picked -> shipped -> delivered)
    and proper financial calculations based on fulfillment location tax rates.

    Args:
        date: Date to generate orders for
        config: Configuration object
        customers: List of customer objects
        geographies: List of geography objects
        stores: List of store objects
        distribution_centers: List of DC objects
        customer_journey_sim: Customer journey simulator
        inventory_flow_sim: Inventory flow simulator
        temporal_patterns: Temporal patterns object
        rng: Random number generator
        generate_trace_id_func: Function to generate trace IDs

    Returns:
        (orders, store_inventory_txn, dc_inventory_txn)
    """
    orders: list[dict] = []
    store_txn: list[dict] = []
    dc_txn: list[dict] = []
    order_lines: list[dict] = []

    base_per_day = max(0, int(config.volume.online_orders_per_day))
    if base_per_day == 0 or not customers:
        return orders, store_txn, dc_txn, order_lines

    # Seasonality/holiday multiplier, not bounded by store hours
    seasonal_mult = temporal_patterns.seasonal.get_seasonal_multiplier(date)
    # Smooth out extremes
    seasonal_mult = max(0.5, min(seasonal_mult, 2.5))
    total_orders = max(0, int(base_per_day * seasonal_mult))

    for i in range(total_orders):
        # Random event time during the day for order creation
        hour = rng.randint(0, 23)
        minute = rng.randint(0, 59)
        second = rng.randint(0, 59)
        created_ts = datetime(date.year, date.month, date.day, hour, minute, second)

        customer = rng.choice(customers)

        # Generate a basket using the same simulator
        basket = customer_journey_sim.generate_shopping_basket(customer.ID)
        if callable(basket_adjuster):
            try:
                basket_adjuster(created_ts, basket)
            except Exception:
                pass

        # Online-specific downsizing: customers typically buy fewer distinct items online.
        # Target distribution: 60% → 1–3 lines, 30% → 2–5 lines, 10% → 5–8 lines.
        items = list(basket.items)
        r = rng.random()
        if r < 0.60:
            tmin, tmax = 1, 3
        elif r < 0.90:
            tmin, tmax = 2, 5
        else:
            tmin, tmax = 5, 8
        target_lines = max(1, rng.randint(tmin, tmax))
        if len(items) > target_lines:
            items = rng.sample(items, target_lines)
        # Cap per-line quantity for online orders (bias toward single-unit)
        adjusted_items: list[tuple] = []
        for product, qty in items:
            if qty > 3:
                qty = rng.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
            adjusted_items.append((product, qty))
        items = adjusted_items

        # Choose header-level mode/node for pricing (tax base), but allow per-line routing below
        # Distribution: 60% DC, 30% Store, 10% BOPIS
        mode = rng.choices(["SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"], weights=[0.60, 0.30, 0.10])[0]

        if mode in ("SHIP_FROM_STORE", "BOPIS") and stores:
            node_type = "STORE"
            store = rng.choice(stores)
            node_id = store.ID
            # Get store tax rate with fallback
            fulfillment_tax_rate = store.tax_rate if store.tax_rate is not None else Decimal("0.07407")
        else:
            node_type = "DC"
            dc = rng.choice(distribution_centers) if distribution_centers else None
            if not dc:
                node_type = "STORE"
                store = rng.choice(stores)
                node_id = store.ID
                fulfillment_tax_rate = store.tax_rate if store.tax_rate is not None else Decimal("0.07407")
            else:
                node_id = dc.ID
                # Approximate DC taxation using customer's local rate (store in same geo if available)
                customer_geo = next((g for g in geographies if g.ID == customer.GeographyID), None)
                if customer_geo:
                    customer_stores = [s for s in stores if s.GeographyID == customer_geo.ID]
                    if customer_stores:
                        fulfillment_tax_rate = customer_stores[0].tax_rate or Decimal("0.07407")
                    else:
                        fulfillment_tax_rate = Decimal("0.07407")
                else:
                    fulfillment_tax_rate = Decimal("0.07407")

        # Select tender type with realistic online distribution
        # Online orders: 60% Credit, 25% Debit, 10% PayPal, 5% Other
        tender_weights = {
            TenderType.CREDIT_CARD: 0.60,
            TenderType.DEBIT_CARD: 0.25,
            TenderType.PAYPAL: 0.10,
            TenderType.OTHER: 0.05,
        }
        tender_options = list(tender_weights.keys())
        weights = list(tender_weights.values())
        tender_type = rng.choices(tender_options, weights=weights)[0]

        order_id = f"ONL{date.strftime('%Y%m%d')}{i:05d}{rng.randint(100, 999)}"

        # Calculate status progression timestamps
        # created (T+0) -> picked (T+15-30min) -> shipped (T+2-4hrs) -> delivered (T+1-3days)
        picked_minutes = rng.randint(15, 30)
        picked_ts = created_ts + timedelta(minutes=picked_minutes)

        shipped_hours = rng.randint(2, 4)
        shipped_ts = created_ts + timedelta(hours=shipped_hours)

        delivered_days = rng.randint(1, 3)
        delivered_ts = created_ts + timedelta(days=delivered_days)

        # Calculate order totals with proper tax handling
        order_subtotal = Decimal("0.00")
        order_tax = Decimal("0.00")

        for product, qty in items:
            # Get product taxability
            taxability = getattr(product, "taxability", ProductTaxability.TAXABLE)

            if taxability == ProductTaxability.TAXABLE:
                taxability_multiplier = Decimal("1.0")
            elif taxability == ProductTaxability.REDUCED_RATE:
                taxability_multiplier = Decimal("0.5")
            else:  # NON_TAXABLE
                taxability_multiplier = Decimal("0.0")

            # Calculate line subtotal and tax
            line_subtotal = (product.SalePrice * qty).quantize(Decimal("0.01"))
            line_tax = (line_subtotal * fulfillment_tax_rate * taxability_multiplier).quantize(Decimal("0.01"))

            order_subtotal += line_subtotal
            order_tax += line_tax

        order_total = (order_subtotal + order_tax).quantize(Decimal("0.01"))

        # Per-line routing assignments (omnichannel can split across nodes)
        line_routing = []  # (product_id, node_type, node_id, line_mode, line_status)

        # Create order line items at creation time; include per-line lifecycle
        line_num = 0
        for product, qty in items:
            line_num += 1
            unit_price = product.SalePrice
            ext_price = (unit_price * qty).quantize(Decimal("0.01"))

            # Choose per-line routing (can differ from header)
            line_mode = rng.choices(["SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"], weights=[0.60, 0.30, 0.10])[0]
            if line_mode in ("SHIP_FROM_STORE", "BOPIS") and stores:
                ln_type = "STORE"
                ln_store = rng.choice(stores)
                ln_node_id = ln_store.ID
            else:
                ln_type = "DC"
                ln_dc = rng.choice(distribution_centers) if distribution_centers else None
                if not ln_dc:
                    ln_type = "STORE"
                    ln_store = rng.choice(stores)
                    ln_node_id = ln_store.ID
                else:
                    ln_node_id = ln_dc.ID

            # Compute per-line lifecycle consistent with SLA assumptions
            # Determine availability at node to simulate backorders
            if ln_type == "STORE":
                current_balance_ln = inventory_flow_sim.get_store_balance(ln_node_id, product.ID)
            else:
                current_balance_ln = inventory_flow_sim.get_dc_balance(ln_node_id, product.ID)
            is_backordered = (current_balance_ln or 0) < qty

            # Holiday factor increases delays during peaks
            holiday_factor = max(1.0, min(2.0, 0.8 + seasonal_mult * 0.6))

            # Helper: triangular hours (min, mode, max)
            def _tri_h(min_h: int, mode_h: int, max_h: int) -> float:
                import random as _r
                return _r.triangular(min_h, max_h, mode_h)

            if line_mode == "BOPIS":
                pickup_h = max(4.0, _tri_h(4, 12, 24) * float(holiday_factor))
                _picked_ts = created_ts + timedelta(hours=max(1, int(pickup_h // 2)))
                _shipped_ts = None
                _delivered_ts = created_ts + timedelta(hours=int(pickup_h))
                line_status = "DELIVERED"
            else:
                if is_backordered:
                    ship_delay_h = int(rng.randint(72, 120) * float(holiday_factor))  # 3–5 days
                else:
                    ship_delay_h = int(_tri_h(8, 18, 48) * float(holiday_factor))  # most within 24h, worst 48h

                pick_min = rng.randint(30, 240)
                _picked_ts = created_ts + timedelta(minutes=min(pick_min, max(1, ship_delay_h * 60 - 30)))
                _shipped_ts = created_ts + timedelta(hours=ship_delay_h)

                # Transit time: DC 1–3 days, Store 0.5–2 days; scale by holidays
                if ln_type == "DC":
                    transit_days = rng.randint(1, 3)
                else:
                    transit_days = max(1, int(rng.uniform(0.5, 2.0)))
                transit_days = int(max(1, transit_days * float(holiday_factor)))
                _delivered_ts = _shipped_ts + timedelta(days=transit_days)

                line_status = "DELIVERED" if not is_backordered else "SHIPPED"

            line_routing.append((product.ID, ln_type, ln_node_id, line_mode, line_status))
            order_lines.append(
                {
                    "OrderId": order_id,
                    "ProductID": product.ID,
                    "Line": line_num,
                    "Qty": qty,
                    "UnitPrice": str(unit_price.quantize(Decimal("0.01"))),
                    "ExtPrice": str(ext_price),
                    "PromoCode": None,
                    # Per-line fulfillment lifecycle
                    "PickedTS": _picked_ts,
                    "ShippedTS": _shipped_ts,
                    "DeliveredTS": _delivered_ts,
                    "FulfillmentStatus": line_status,
                    "FulfillmentMode": line_mode,
                    "NodeType": ln_type,
                    "NodeID": ln_node_id,
                }
            )

        # Compute header status from line statuses
        all_statuses = [ls for (_, _, _, _, ls) in line_routing]
        if all(s == "DELIVERED" for s in all_statuses):
            header_status = "COMPLETE"
        elif any(s in {"PICKED", "SHIPPED"} for s in all_statuses):
            header_status = "OPEN"
        else:
            header_status = "NEW"

        # Status 1: created - Order placed, payment processed (header row only)
        trace_id_created = generate_trace_id_func()
        # Compute completed_ts when all lines delivered
        completed_ts = None
        if all_statuses and all(s == "DELIVERED" for s in all_statuses):
            # Use the latest delivered timestamp among lines
            delivered_times = [
                dt for dt in (
                    (_delivered_ts if 'delivered_ts' in locals() else None),
                ) if dt is not None
            ]
            # Fallback: use delivered_ts computed above for lines; recompute from order_lines list
            if not delivered_times:
                from datetime import datetime as _dt
                delivered_times = [
                    ol.get("DeliveredTS") for ol in order_lines if ol.get("OrderId") == order_id and ol.get("DeliveredTS") is not None
                ]
            if delivered_times:
                completed_ts = max(delivered_times)

        orders.append(
            {
                "TraceId": trace_id_created,
                "EventTS": created_ts,
                "OrderId": order_id,
                "CustomerID": customer.ID,
                "Subtotal": str(order_subtotal),
                "Tax": str(order_tax),
                "Total": str(order_total),
                "TenderType": tender_type.value,
                # Header completion timestamp; leave other lifecycle on lines
                "CompletedTS": completed_ts,
            }
        )

        # Status 2: picked - no longer creates an additional order row (avoid multi-status rows)
        trace_id_picked = generate_trace_id_func()

        # Create inventory effects at picked stage (when items leave shelf)
        for product, qty in items:
            # Look up per-line routing and status for inventory movement
            ln_type, inv_node_id, line_status = None, None, None
            for pid, lt, lnid, _, ls in line_routing:
                if pid == product.ID:
                    ln_type, inv_node_id, line_status = lt, lnid, ls
                    break

            # Only decrement inventory if at least PICKED
            if line_status not in {"PICKED", "SHIPPED", "DELIVERED"}:
                continue

            if (ln_type or node_type) == "STORE":
                # Update store inventory and get balance
                key = ((inv_node_id or node_id), product.ID)
                current_balance = inventory_flow_sim._store_inventory.get(key, 0)
                new_balance = max(0, current_balance - qty)
                inventory_flow_sim._store_inventory[key] = new_balance

                # Get balance after transaction
                balance = inventory_flow_sim.get_store_balance((inv_node_id or node_id), product.ID)

                store_txn.append(
                    {
                        "TraceId": trace_id_picked,
                        "EventTS": picked_ts,
                        "StoreID": (inv_node_id or node_id),
                        "ProductID": product.ID,
                        "QtyDelta": -qty,
                        "Reason": InventoryReason.SALE.value,
                        "Source": order_id,
                        "Balance": balance,
                    }
                )
            else:  # DC
                # Update DC inventory and get balance
                key = ((inv_node_id or node_id), product.ID)
                current_balance = inventory_flow_sim._dc_inventory.get(key, 0)
                new_balance = max(0, current_balance - qty)
                inventory_flow_sim._dc_inventory[key] = new_balance

                # Get balance after transaction
                balance = inventory_flow_sim.get_dc_balance((inv_node_id or node_id), product.ID)

                dc_txn.append(
                    {
                        "TraceId": trace_id_picked,
                        "EventTS": picked_ts,
                        "DCID": (inv_node_id or node_id),
                        "ProductID": product.ID,
                        "QtyDelta": -qty,
                        "Reason": InventoryReason.SALE.value,
                        "Source": order_id,
                        "Balance": balance,
                    }
                )

        # Status 3: shipped - no additional order row emitted
        trace_id_shipped = generate_trace_id_func()

        # Status 4: delivered - no additional order row emitted
        trace_id_delivered = generate_trace_id_func()

    return orders, store_txn, dc_txn, order_lines
