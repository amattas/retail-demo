"""Authoritative output schemas for generated tables (Plan 2a scope).

Spark simple type strings. Dimension columns keep the legacy PascalCase names
because the semantic model TMDL binds sourceColumn to them (e.g. StoreNumber,
Cost, MSRP). Fact tables are snake_case. The TMDL contract test verifies this
module against fabric/powerbi/retail_model.SemanticModel.

Columns added vs plan (from TMDL audit 2026-06-12):
  fact_receipts: added ("Subtotal", "string") — legacy trace/aggregate string
    column bound as sourceColumn: Subtotal in the semantic model. Not a
    snake_case column; appears to be a pre-existing legacy column the model
    still references.

Columns in plan but NOT in TMDL (allowed — Direct Lake ignores extra columns):
  fact_receipts: trace_id, tender_type
  fact_payments: tender_type (not in payments TMDL at all)
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
