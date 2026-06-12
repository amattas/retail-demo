"""Authoritative output schemas for generated tables (Plan 2a + 2b scope).

Spark simple type strings. Dimension columns keep the legacy PascalCase names
because the semantic model TMDL binds sourceColumn to them (e.g. StoreNumber,
Cost, MSRP). Most fact tables are snake_case, but several 2b tables have
TMDL-bound PascalCase or mixed-case columns documented per table below.

The TMDL contract test (tests/generation/test_schema_contract.py) is the
arbiter: every sourceColumn in a table's TMDL must be present here with a
compatible type. Extra columns not in TMDL are allowed.

Columns added vs plan (from TMDL audit 2026-06-12):
  fact_receipts: added ("Subtotal", "string") — legacy trace/aggregate string
    column bound as sourceColumn: Subtotal in the semantic model. Not a
    snake_case column; appears to be a pre-existing legacy column the model
    still references.

  --- Plan 2b TMDL deltas (per-table, added to reconcile TMDL bindings) ---

  fact_ble_pings:
    - ("CustomerId", "double") — TMDL sourceColumn is CustomerId (PascalCase),
      not customer_id. Plan listed customer_id (kept as extra); TMDL column added.
    - ("__index_level_0__", "long") — TMDL-bound legacy pandas-index column.

  fact_customer_zone_changes:
    - TMDL uses PascalCase for all data columns: StoreID, CustomerBLEId,
      FromZone, ToZone (plan listed snake_case equivalents, kept as extras).
    - ("StoreID", "long"), ("CustomerBLEId", "string"), ("FromZone", "string"),
      ("ToZone", "string") — TMDL-bound columns added.
    - ("__index_level_0__", "long") — TMDL-bound.

  fact_marketing:
    - ("CustomerId", "double") — TMDL sourceColumn is CustomerId; plan's
      customer_id kept as extra.
    - ("CostCents", "long") — TMDL sourceColumn is CostCents; plan's cost_cents
      kept as extra.
    - ("__index_level_0__", "long") — TMDL-bound.

  fact_promo_lines:
    - TMDL uses PascalCase: ReceiptId, PromoCode, LineNumber, ProductID, Qty,
      DiscountAmount, DiscountCents (plan listed snake_case equivalents, kept
      as extras).
    - ("ReceiptId", "string"), ("PromoCode", "string"), ("LineNumber", "long"),
      ("ProductID", "long"), ("Qty", "long"), ("DiscountAmount", "string"),
      ("DiscountCents", "long") — TMDL-bound columns added.
    - ("__index_level_0__", "long") — TMDL-bound.

  fact_online_order_lines:
    - ("__index_level_0__", "long") — TMDL-bound.

  fact_reorders:
    - ("__index_level_0__", "long") — TMDL-bound.

  fact_truck_moves:
    - ("__index_level_0__", "long") — TMDL-bound.

  fact_truck_inventory:
    - ("__index_level_0__", "long") — TMDL-bound.

  fact_dc_inventory_txn:
    - ("Source", "string") — TMDL sourceColumn is PascalCase Source; plan's
      source (lowercase) kept as extra.
    - ("__index_level_0__", "long") — TMDL-bound.

  fact_store_inventory_txn:
    - ("__index_level_0__", "long") — TMDL-bound.

  fact_stockouts:
    - ("__index_level_0__", "long") — TMDL-bound.

Columns in plan but NOT in TMDL (allowed — Direct Lake ignores extra columns):
  fact_receipts: trace_id, tender_type
  fact_payments: tender_type (not in payments TMDL at all)
  fact_ble_pings: customer_id (TMDL uses CustomerId), trace_id, event_ts,
    event_date
  fact_customer_zone_changes: store_id, customer_ble_id, from_zone, to_zone,
    event_ts, trace_id, event_date
  fact_marketing: customer_id, cost_cents, event_ts, trace_id, event_date,
    channel (extra beyond TMDL-bound channel — already in TMDL, fine)
  fact_promo_lines: receipt_id_ext, promo_code, line_number, product_id,
    quantity, discount_amount, discount_cents, event_ts, trace_id, event_date
  fact_dc_inventory_txn: source (lowercase), trace_id, event_ts, event_date
"""

# table -> list of (column, spark_type)
TABLES: dict[str, list[tuple[str, str]]] = {
    "dim_geographies": [
        ("ID", "long"), ("City", "string"), ("State", "string"), ("ZipCode", "string"),
        ("District", "string"), ("Region", "string"),
    ],
    "dim_stores": [
        ("ID", "long"), ("StoreNumber", "string"), ("Address", "string"),
        ("GeographyID", "long"), ("tax_rate", "double"), ("volume_class", "string"),
        ("store_format", "string"), ("operating_hours", "string"),
        ("daily_traffic_multiplier", "double"),
    ],
    "dim_distribution_centers": [
        ("ID", "long"), ("DCNumber", "string"), ("Address", "string"),
        ("GeographyID", "long"),
    ],
    "dim_trucks": [
        ("ID", "long"), ("LicensePlate", "string"), ("Refrigeration", "boolean"),
        # double, not long: NULL for pool trucks + Direct Lake nullability
        ("DCID", "double"),
    ],
    "dim_customers": [
        ("ID", "long"), ("FirstName", "string"), ("LastName", "string"),
        ("Address", "string"), ("GeographyID", "long"), ("LoyaltyCard", "string"),
        ("Phone", "string"), ("BLEId", "string"), ("AdId", "string"),
    ],
    "dim_products": [
        ("ID", "long"), ("ProductName", "string"), ("Brand", "string"),
        ("Company", "string"), ("Department", "string"), ("Category", "string"),
        ("Subcategory", "string"), ("Cost", "double"), ("MSRP", "double"),
        ("SalePrice", "double"), ("RequiresRefrigeration", "boolean"),
        ("LaunchDate", "timestamp"), ("taxability", "string"), ("Tags", "string"),
    ],
    "dim_date": [
        ("date_key", "long"), ("date", "date"), ("year", "long"), ("quarter", "long"),
        ("month", "long"), ("month_name", "string"), ("day", "long"),
        ("day_of_week", "long"), ("day_name", "string"), ("week_of_year", "long"),
        ("is_weekend", "long"), ("fiscal_year", "long"), ("fiscal_quarter", "long"),
    ],
    "fact_receipts": [
        ("receipt_id_ext", "string"), ("trace_id", "string"), ("event_ts", "timestamp"),
        ("event_date", "date"), ("store_id", "long"), ("customer_id", "long"),
        ("receipt_type", "string"), ("tender_type", "string"),
        ("subtotal_cents", "long"), ("discount_amount", "string"),
        ("tax_cents", "long"), ("total_cents", "long"),
        ("subtotal_amount", "string"), ("tax_amount", "string"),
        ("total_amount", "string"), ("payment_method", "string"),
        # Legacy column bound by the semantic model (sourceColumn: Subtotal).
        # Present in fact_receipts.tmdl as a string column; added here to
        # satisfy the TMDL contract test (TMDL arbiter rule).
        ("Subtotal", "string"),
    ],
    "fact_receipt_lines": [
        ("receipt_id_ext", "string"), ("event_ts", "timestamp"), ("event_date", "date"),
        ("line_num", "int"), ("product_id", "long"), ("quantity", "int"),
        ("unit_price", "string"), ("unit_cents", "long"),
        ("ext_price", "string"), ("ext_cents", "long"), ("promo_code", "string"),
    ],
    "fact_payments": [
        ("receipt_id_ext", "string"), ("order_id_ext", "string"),
        ("event_ts", "timestamp"), ("event_date", "date"),
        ("payment_method", "string"), ("amount_cents", "long"), ("amount", "string"),
        ("transaction_id", "string"), ("status", "string"),
        ("decline_reason", "string"), ("processing_time_ms", "long"),
        ("store_id", "long"), ("customer_id", "long"),
    ],
    # -----------------------------------------------------------------------
    # Plan 2b: 15 remaining fact tables
    # -----------------------------------------------------------------------
    "fact_store_ops": [
        # TMDL-bound: trace_id, store_id (int64→long), operation_type, event_date
        # Extra (not in TMDL, allowed): event_ts
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("operation_type", "string"), ("event_date", "date"),
    ],
    "fact_foot_traffic": [
        # TMDL-bound: count (int64→long), zone, dwell_seconds (int64→long),
        #   store_id (int64→long), sensor_id, event_date
        # Extra (not in TMDL, allowed): event_ts, trace_id
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("sensor_id", "string"), ("zone", "string"), ("dwell_seconds", "long"),
        ("count", "long"), ("event_date", "date"),
    ],
    "fact_ble_pings": [
        # TMDL-bound: beacon_id, rssi (int64→long), customer_ble_id, zone,
        #   store_id (int64→long), CustomerId (double), __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, event_date,
        #   customer_id (plan name; TMDL uses CustomerId)
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("beacon_id", "string"), ("customer_ble_id", "string"),
        ("customer_id", "double"),
        # TMDL-bound PascalCase column (sourceColumn: CustomerId)
        ("CustomerId", "double"),
        ("rssi", "long"), ("zone", "string"),
        ("event_date", "date"),
        # TMDL-bound legacy pandas-index column
        ("__index_level_0__", "long"),
    ],
    "fact_customer_zone_changes": [
        # TMDL-bound PascalCase: StoreID (int64→long), CustomerBLEId (string),
        #   FromZone (string), ToZone (string), __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, event_date,
        #   store_id, customer_ble_id, from_zone, to_zone (snake_case plan names)
        ("event_ts", "timestamp"), ("trace_id", "string"),
        # snake_case plan columns (extras)
        ("store_id", "long"), ("customer_ble_id", "string"),
        ("from_zone", "string"), ("to_zone", "string"),
        # TMDL-bound PascalCase columns
        ("StoreID", "long"), ("CustomerBLEId", "string"),
        ("FromZone", "string"), ("ToZone", "string"),
        ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    "fact_marketing": [
        # TMDL-bound: device, cost, customer_ad_id, impression_id_ext, campaign_id,
        #   creative_id, channel, CustomerId (double), CostCents (int64→long),
        #   __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, event_date,
        #   customer_id (plan name; TMDL uses CustomerId),
        #   cost_cents (plan name; TMDL uses CostCents)
        ("event_ts", "timestamp"), ("trace_id", "string"), ("channel", "string"),
        ("campaign_id", "string"), ("creative_id", "string"),
        ("customer_ad_id", "string"),
        # snake_case plan columns (extras)
        ("customer_id", "double"), ("cost_cents", "long"),
        # TMDL-bound PascalCase columns (sourceColumn: CustomerId, CostCents)
        ("CustomerId", "double"), ("CostCents", "long"),
        ("impression_id_ext", "string"), ("cost", "string"),
        ("device", "string"), ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    "fact_promotions": [
        # TMDL-bound: receipt_id_ext, promo_code, discount_amount, discount_cents
        #   (int64→long), discount_type, product_count (int64→long), product_ids,
        #   store_id (int64→long), customer_id (int64→long), event_date
        # Extra (not in TMDL, allowed): event_ts, trace_id
        ("event_ts", "timestamp"), ("trace_id", "string"), ("receipt_id_ext", "string"),
        ("promo_code", "string"), ("discount_amount", "string"),
        ("discount_cents", "long"), ("discount_type", "string"),
        ("product_count", "long"), ("product_ids", "string"), ("store_id", "long"),
        ("customer_id", "long"), ("event_date", "date"),
    ],
    "fact_promo_lines": [
        # TMDL-bound PascalCase: ReceiptId, PromoCode, LineNumber (int64→long),
        #   ProductID (int64→long), Qty (int64→long), DiscountAmount (string),
        #   DiscountCents (int64→long), __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, event_date,
        #   receipt_id_ext, promo_code, line_number, product_id, quantity,
        #   discount_amount, discount_cents (snake_case plan names)
        ("event_ts", "timestamp"), ("trace_id", "string"),
        # snake_case plan columns (extras)
        ("receipt_id_ext", "string"), ("promo_code", "string"),
        ("line_number", "long"), ("product_id", "long"),
        ("quantity", "long"), ("discount_amount", "string"),
        ("discount_cents", "long"),
        # TMDL-bound PascalCase columns
        ("ReceiptId", "string"), ("PromoCode", "string"),
        ("LineNumber", "long"), ("ProductID", "long"),
        ("Qty", "long"), ("DiscountAmount", "string"), ("DiscountCents", "long"),
        ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    "fact_online_order_headers": [
        # TMDL-bound: customer_id (int64→long), subtotal_cents (int64→long),
        #   order_id_ext, payment_method, total_amount, total_cents (int64→long),
        #   tax_amount, subtotal_amount, tax_cents (int64→long), event_date
        # Extra (not in TMDL, allowed): event_ts
        ("order_id_ext", "string"), ("customer_id", "long"),
        ("subtotal_cents", "long"), ("tax_cents", "long"), ("total_cents", "long"),
        ("subtotal_amount", "string"), ("tax_amount", "string"),
        ("total_amount", "string"), ("payment_method", "string"),
        ("event_ts", "timestamp"), ("event_date", "date"),
    ],
    "fact_online_order_lines": [
        # TMDL-bound: order_id, unit_cents (int64→long), fulfillment_mode,
        #   promo_code, node_type, product_id (int64→long), ext_price, ext_cents
        #   (int64→long), line_num (int64→long), fulfillment_status, unit_price,
        #   quantity (int64→long), node_id (int64→long), __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): picked_ts, shipped_ts, delivered_ts,
        #   event_ts, event_date
        ("order_id", "string"), ("product_id", "long"), ("line_num", "long"),
        ("quantity", "long"), ("unit_price", "string"), ("unit_cents", "long"),
        ("ext_price", "string"), ("ext_cents", "long"), ("promo_code", "string"),
        ("fulfillment_mode", "string"), ("fulfillment_status", "string"),
        ("node_type", "string"), ("node_id", "long"),
        ("picked_ts", "timestamp"), ("shipped_ts", "timestamp"),
        ("delivered_ts", "timestamp"), ("event_ts", "timestamp"),
        ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    "fact_reorders": [
        # TMDL-bound: store_id (int64→long), dc_id (int64→long), product_id
        #   (int64→long), current_quantity (int64→long), reorder_quantity
        #   (int64→long), reorder_point (int64→long), priority,
        #   __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, event_date
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("dc_id", "long"), ("product_id", "long"), ("current_quantity", "long"),
        ("reorder_quantity", "long"), ("reorder_point", "long"), ("priority", "string"),
        ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    "fact_truck_moves": [
        # TMDL-bound: status, shipment_id, dc_id (int64→long), truck_id
        #   (int64→long), store_id (int64→long), actual_unload_duration (double),
        #   __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, eta, etd,
        #   departure_time, event_date
        ("event_ts", "timestamp"), ("trace_id", "string"), ("truck_id", "long"),
        ("dc_id", "long"), ("store_id", "long"), ("shipment_id", "string"),
        ("status", "string"), ("eta", "timestamp"), ("etd", "timestamp"),
        ("departure_time", "timestamp"), ("actual_unload_duration", "double"),
        ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    "fact_truck_inventory": [
        # TMDL-bound: truck_id (int64→long), shipment_id, product_id (int64→long),
        #   quantity (int64→long), action, location_id (int64→long), location_type,
        #   __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, event_date
        ("event_ts", "timestamp"), ("trace_id", "string"), ("truck_id", "long"),
        ("shipment_id", "string"), ("product_id", "long"), ("quantity", "long"),
        ("action", "string"), ("location_id", "long"), ("location_type", "string"),
        ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    "fact_dc_inventory_txn": [
        # TMDL-bound: txn_type, quantity (int64→long), dc_id (int64→long),
        #   balance (int64→long), product_id (int64→long), Source (PascalCase!),
        #   __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, event_date,
        #   source (lowercase snake_case plan name)
        ("event_ts", "timestamp"), ("trace_id", "string"), ("dc_id", "long"),
        ("product_id", "long"), ("quantity", "long"), ("balance", "long"),
        ("txn_type", "string"),
        # snake_case plan column (extra)
        ("source", "string"),
        # TMDL-bound PascalCase column (sourceColumn: Source)
        ("Source", "string"),
        ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    "fact_store_inventory_txn": [
        # TMDL-bound: txn_type, quantity (int64→long), balance (int64→long),
        #   product_id (int64→long), store_id (int64→long), source (lowercase OK),
        #   __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, event_date
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("product_id", "long"), ("quantity", "long"), ("balance", "long"),
        ("txn_type", "string"), ("source", "string"), ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    "fact_stockouts": [
        # PascalCase IS the contract: TMDL binds StoreID (double), DCID (double),
        #   ProductID (int64→long), LastKnownQuantity (int64→long),
        #   __index_level_0__ (int64→long)
        # Extra (not in TMDL, allowed): event_ts, trace_id, event_date
        ("event_ts", "timestamp"), ("trace_id", "string"),
        ("StoreID", "double"), ("DCID", "double"),
        ("ProductID", "long"), ("LastKnownQuantity", "long"),
        ("event_date", "date"),
        ("__index_level_0__", "long"),
    ],
    # -----------------------------------------------------------------------
    # Plan 2c: 9 Gold (au) aggregate tables
    # TMDL audit 2026-06-12: bindings match exactly; `computed_at` and `as_of`
    # are produced by the legacy transforms but unbound in TMDL (extras OK).
    # TMDL `day`/`ts` are dateTime — TYPE_COMPAT accepts timestamp|date.
    # -----------------------------------------------------------------------
    "sales_minute_store": [
        ("store_id", "long"), ("ts", "timestamp"), ("total_sales", "double"),
        ("receipts", "long"), ("avg_basket", "double"),
    ],
    "top_products_15m": [
        ("product_id", "long"), ("revenue", "double"), ("units", "long"),
        ("computed_at", "timestamp"),  # produced by legacy code, unbound in TMDL
    ],
    "inventory_position_current": [
        ("store_id", "long"), ("product_id", "long"), ("on_hand", "long"),
        ("as_of", "timestamp"),
    ],
    "dc_inventory_position_current": [
        ("dc_id", "long"), ("product_id", "long"), ("on_hand", "long"),
        ("as_of", "timestamp"),
    ],
    "truck_dwell_daily": [
        ("site", "string"), ("day", "date"), ("avg_dwell_min", "double"),
        ("trucks", "long"),
    ],
    "online_sales_daily": [
        ("day", "date"), ("orders", "long"), ("subtotal", "double"),
        ("tax", "double"), ("total", "double"), ("avg_order_value", "double"),
    ],
    "zone_dwell_minute": [
        ("store_id", "long"), ("zone", "string"), ("ts", "timestamp"),
        ("avg_dwell", "double"), ("customers", "long"),
    ],
    "marketing_cost_daily": [
        ("campaign_id", "string"), ("day", "date"), ("impressions", "long"),
        ("cost", "double"),
    ],
    "tender_mix_daily": [
        ("day", "date"), ("payment_method", "string"), ("transactions", "long"),
        ("total_amount", "double"),
    ],
}


_SPARK_TYPE_MAP = None


def _type_map():
    """Lazy-import PySpark type map (avoids import cost when pyspark not installed)."""
    global _SPARK_TYPE_MAP
    if _SPARK_TYPE_MAP is None:
        from pyspark.sql.types import (
            BooleanType, DateType, DoubleType, IntegerType,
            LongType, StringType, TimestampType,
        )
        _SPARK_TYPE_MAP = {
            "long": LongType(),
            "int": IntegerType(),
            "string": StringType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
        }
    return _SPARK_TYPE_MAP


def spark_schema(table: str):
    """Build a StructType for createDataFrame with explicit types."""
    from pyspark.sql.types import StructField, StructType

    tmap = _type_map()
    fields = [
        StructField(name, tmap[typ], nullable=True)
        for name, typ in TABLES[table]
    ]
    return StructType(fields)


def column_names(table: str) -> list[str]:
    return [name for name, _ in TABLES[table]]
