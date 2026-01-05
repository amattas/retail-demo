"""
DuckDB master table reader.

Loads master dimension tables from DuckDB into Pydantic models used by the
generator and streaming modules.
"""

from __future__ import annotations

from datetime import UTC, datetime
from datetime import date as dt_date
from datetime import time as dt_time
from decimal import Decimal
from typing import Any

import duckdb

from retail_datagen.db.duckdb_engine import get_duckdb_conn
from retail_datagen.shared.models import (
    Customer,
    DistributionCenter,
    GeographyMaster,
    ProductMaster,
    Store,
    Truck,
)


def _fetch_all(conn: duckdb.DuckDBPyConnection, table: str) -> tuple[list[str], list[tuple]]:
    cur = conn.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    cols = [d[0] for d in (cur.description or [])]
    return cols, rows


def _to_decimal(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _to_datetime_from_date(d: dt_date | datetime | None) -> datetime:
    if d is None:
        return datetime.now(UTC)
    if isinstance(d, datetime):
        return d
    return datetime.combine(d, dt_time(0, 0))


def read_geographies() -> list[GeographyMaster]:
    conn = get_duckdb_conn()
    cols, rows = _fetch_all(conn, "dim_geographies")
    idx = {c.lower(): i for i, c in enumerate(cols)}
    out: list[GeographyMaster] = []
    for r in rows:
        out.append(
            GeographyMaster(
                ID=int(r[idx["id"]]),
                City=str(r[idx["city"]]),
                State=str(r[idx["state"]]),
                ZipCode=str(r[idx.get("zipcode", idx.get("zip_code", "zipcode"))]),
                District=str(r[idx["district"]]),
                Region=str(r[idx["region"]]),
            )
        )
    return out


def read_stores() -> list[Store]:
    conn = get_duckdb_conn()
    cols, rows = _fetch_all(conn, "dim_stores")
    idx = {c.lower(): i for i, c in enumerate(cols)}
    out: list[Store] = []
    for r in rows:
        out.append(
            Store(
                ID=int(r[idx["id"]]),
                StoreNumber=str(r[idx["storenumber"]]),
                Address=str(r[idx["address"]]),
                GeographyID=int(r[idx["geographyid"]]),
                tax_rate=_to_decimal(r[idx.get("tax_rate", idx.get("taxrate", "tax_rate"))]),
                volume_class=r[idx.get("volume_class")],
                store_format=r[idx.get("store_format")],
                operating_hours=r[idx.get("operating_hours")],
                daily_traffic_multiplier=_to_decimal(r[idx.get("daily_traffic_multiplier")]),
            )
        )
    return out


def read_distribution_centers() -> list[DistributionCenter]:
    conn = get_duckdb_conn()
    cols, rows = _fetch_all(conn, "dim_distribution_centers")
    idx = {c.lower(): i for i, c in enumerate(cols)}
    out: list[DistributionCenter] = []
    for r in rows:
        out.append(
            DistributionCenter(
                ID=int(r[idx["id"]]),
                DCNumber=str(r[idx["dcnumber"]]),
                Address=str(r[idx["address"]]),
                GeographyID=int(r[idx["geographyid"]]),
            )
        )
    return out


def read_trucks() -> list[Truck]:
    conn = get_duckdb_conn()
    cols, rows = _fetch_all(conn, "dim_trucks")
    idx = {c.lower(): i for i, c in enumerate(cols)}
    out: list[Truck] = []
    for r in rows:
        out.append(
            Truck(
                ID=int(r[idx["id"]]),
                LicensePlate=str(r[idx["licenseplate"]]),
                Refrigeration=bool(r[idx["refrigeration"]]),
                DCID=int(r[idx.get("dcid", idx.get("dc_id", "dcid"))]) if r[idx.get("dcid", idx.get("dc_id", "dcid"))] is not None else None,
            )
        )
    return out


def read_customers() -> list[Customer]:
    conn = get_duckdb_conn()
    cols, rows = _fetch_all(conn, "dim_customers")
    idx = {c.lower(): i for i, c in enumerate(cols)}
    out: list[Customer] = []
    for r in rows:
        out.append(
            Customer(
                ID=int(r[idx["id"]]),
                FirstName=str(r[idx["firstname"]]),
                LastName=str(r[idx["lastname"]]),
                Address=str(r[idx["address"]]),
                GeographyID=int(r[idx["geographyid"]]),
                LoyaltyCard=str(r[idx["loyaltycard"]]),
                Phone=str(r[idx["phone"]]),
                BLEId=str(r[idx["bleid"]]),
                AdId=str(r[idx["adid"]]),
            )
        )
    return out


def read_products() -> list[ProductMaster]:
    conn = get_duckdb_conn()
    cols, rows = _fetch_all(conn, "dim_products")
    idx = {c.lower(): i for i, c in enumerate(cols)}
    out: list[ProductMaster] = []
    for r in rows:
        launch = r[idx.get("launchdate", idx.get("launch_date", "launchdate"))]
        launch_dt = _to_datetime_from_date(launch)
        out.append(
            ProductMaster(
                ID=int(r[idx["id"]]),
                ProductName=str(r[idx["productname"]]),
                Brand=str(r[idx["brand"]]) if r[idx["brand"]] is not None else None,
                Company=str(r[idx["company"]]) if r[idx["company"]] is not None else None,
                Department=str(r[idx["department"]]),
                Category=str(r[idx["category"]]),
                Subcategory=str(r[idx["subcategory"]]),
                Cost=_to_decimal(r[idx["cost"]]) or Decimal("0.00"),
                MSRP=_to_decimal(r[idx["msrp"]]) or Decimal("0.00"),
                SalePrice=_to_decimal(r[idx["saleprice"]]) or Decimal("0.00"),
                RequiresRefrigeration=bool(r[idx.get("requiresrefrigeration", idx.get("requires_refrigeration", "requiresrefrigeration"))]),
                LaunchDate=launch_dt,
                taxability=r[idx.get("taxability")],
                Tags=r[idx.get("tags")],
            )
        )
    return out


def read_all_masters() -> tuple[
    list[GeographyMaster],
    list[Store],
    list[DistributionCenter],
    list[Customer],
    list[ProductMaster],
    list[Truck],
]:
    geos = read_geographies()
    stores = read_stores()
    dcs = read_distribution_centers()
    customers = read_customers()
    products = read_products()
    trucks = read_trucks()
    return geos, stores, dcs, customers, products, trucks

