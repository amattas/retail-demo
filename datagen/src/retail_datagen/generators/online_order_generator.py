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

        # Generate a small basket using the same simulator
        basket = customer_journey_sim.generate_shopping_basket(customer.ID)
        if callable(basket_adjuster):
            try:
                basket_adjuster(created_ts, basket)
            except Exception:
                pass

        # Choose fulfillment mode and node
        # Distribution: 60% DC, 30% Store, 10% BOPIS
        mode = rng.choices(
            ["SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"],
            weights=[0.60, 0.30, 0.10],
        )[0]

        if mode in ("SHIP_FROM_STORE", "BOPIS") and stores:
            node_type = "STORE"
            store = rng.choice(stores)
            node_id = store.ID
            # Get store tax rate with fallback
            fulfillment_tax_rate = (
                store.tax_rate if store.tax_rate is not None else Decimal("0.07407")
            )
        else:
            node_type = "DC"
            dc = (
                rng.choice(distribution_centers)
                if distribution_centers
                else None
            )
            if not dc:
                # Fallback to store if no DCs
                node_type = "STORE"
                store = rng.choice(stores)
                node_id = store.ID
                fulfillment_tax_rate = (
                    store.tax_rate if store.tax_rate is not None else Decimal("0.07407")
                )
            else:
                node_id = dc.ID
                # For DC fulfillment, use customer's home geography tax rate
                # Find customer's geography to get their tax rate
                customer_geo = next((g for g in geographies if g.ID == customer.GeographyID), None)
                if customer_geo:
                    # Try to find a store in customer's geography to get tax rate
                    customer_stores = [s for s in stores if s.GeographyID == customer_geo.ID]
                    if customer_stores:
                        fulfillment_tax_rate = (
                            customer_stores[0].tax_rate if customer_stores[0].tax_rate is not None
                            else Decimal("0.07407")
                        )
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

        for product, qty in basket.items:
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

        # Create order line items at creation time for composition (single snapshot)
        line_num = 0
        for product, qty in basket.items:
            line_num += 1
            unit_price = product.SalePrice
            ext_price = (unit_price * qty).quantize(Decimal("0.01"))
            order_lines.append(
                {
                    "OrderId": order_id,
                    "ProductID": product.ID,
                    "Line": line_num,
                    "Qty": qty,
                    "UnitPrice": str(unit_price.quantize(Decimal("0.01"))),
                    "ExtPrice": str(ext_price),
                    "PromoCode": None,
                }
            )

        # Create order records for each status in the lifecycle
        # Status 1: created - Order placed, payment processed
        trace_id_created = generate_trace_id_func()
        for product, qty in basket.items:
            orders.append(
                {
                    "TraceId": trace_id_created,
                    "EventTS": created_ts,
                    "OrderId": order_id,
                    "CustomerID": customer.ID,
                    "ProductID": product.ID,
                    "Qty": qty,
                    "Subtotal": str(order_subtotal),
                    "Tax": str(order_tax),
                    "Total": str(order_total),
                    "TenderType": tender_type.value,
                    "FulfillmentStatus": "created",
                    "FulfillmentMode": mode,
                    "NodeType": node_type,
                    "NodeID": node_id,
                }
            )

        # Status 2: picked - Items picked from inventory
        trace_id_picked = generate_trace_id_func()
        for product, qty in basket.items:
            orders.append(
                {
                    "TraceId": trace_id_picked,
                    "EventTS": picked_ts,
                    "OrderId": order_id,
                    "CustomerID": customer.ID,
                    "ProductID": product.ID,
                    "Qty": qty,
                    "Subtotal": str(order_subtotal),
                    "Tax": str(order_tax),
                    "Total": str(order_total),
                    "TenderType": tender_type.value,
                    "FulfillmentStatus": "picked",
                    "FulfillmentMode": mode,
                    "NodeType": node_type,
                    "NodeID": node_id,
                }
            )

        # Create inventory effects at picked stage (when items leave shelf)
        for product, qty in basket.items:
            if node_type == "STORE":
                # Update store inventory and get balance
                key = (node_id, product.ID)
                current_balance = inventory_flow_sim._store_inventory.get(key, 0)
                new_balance = max(0, current_balance - qty)
                inventory_flow_sim._store_inventory[key] = new_balance

                # Get balance after transaction
                balance = inventory_flow_sim.get_store_balance(node_id, product.ID)

                store_txn.append(
                    {
                        "TraceId": trace_id_picked,
                        "EventTS": picked_ts,
                        "StoreID": node_id,
                        "ProductID": product.ID,
                        "QtyDelta": -qty,
                        "Reason": InventoryReason.SALE.value,
                        "Source": order_id,
                        "Balance": balance,
                    }
                )
            else:  # DC
                # Update DC inventory and get balance
                key = (node_id, product.ID)
                current_balance = inventory_flow_sim._dc_inventory.get(key, 0)
                new_balance = max(0, current_balance - qty)
                inventory_flow_sim._dc_inventory[key] = new_balance

                # Get balance after transaction
                balance = inventory_flow_sim.get_dc_balance(node_id, product.ID)

                dc_txn.append(
                    {
                        "TraceId": trace_id_picked,
                        "EventTS": picked_ts,
                        "DCID": node_id,
                        "ProductID": product.ID,
                        "QtyDelta": -qty,
                        "Reason": InventoryReason.SALE.value,
                        "Source": order_id,
                        "Balance": balance,
                    }
                )

        # Status 3: shipped - Package handed to carrier
        trace_id_shipped = generate_trace_id_func()
        for product, qty in basket.items:
            orders.append(
                {
                    "TraceId": trace_id_shipped,
                    "EventTS": shipped_ts,
                    "OrderId": order_id,
                    "CustomerID": customer.ID,
                    "ProductID": product.ID,
                    "Qty": qty,
                    "Subtotal": str(order_subtotal),
                    "Tax": str(order_tax),
                    "Total": str(order_total),
                    "TenderType": tender_type.value,
                    "FulfillmentStatus": "shipped",
                    "FulfillmentMode": mode,
                    "NodeType": node_type,
                    "NodeID": node_id,
                }
            )

        # Status 4: delivered - Package delivered to customer
        trace_id_delivered = generate_trace_id_func()
        for product, qty in basket.items:
            orders.append(
                {
                    "TraceId": trace_id_delivered,
                    "EventTS": delivered_ts,
                    "OrderId": order_id,
                    "CustomerID": customer.ID,
                    "ProductID": product.ID,
                    "Qty": qty,
                    "Subtotal": str(order_subtotal),
                    "Tax": str(order_tax),
                    "Total": str(order_total),
                    "TenderType": tender_type.value,
                    "FulfillmentStatus": "delivered",
                    "FulfillmentMode": mode,
                    "NodeType": node_type,
                    "NodeID": node_id,
                }
            )

    return orders, store_txn, dc_txn, order_lines
