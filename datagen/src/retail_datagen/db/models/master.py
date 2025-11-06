"""
SQLAlchemy ORM models for master dimension tables.

This module defines the schema for all dimension tables stored in master.db:
- Geography (dim_geographies)
- Store (dim_stores)
- DistributionCenter (dim_distribution_centers)
- Truck (dim_trucks)
- Customer (dim_customers)
- Product (dim_products)

All models include proper relationships, indexes, and constraints aligned with
the existing Pydantic models in shared/models.py.
"""

from datetime import date

from sqlalchemy import (
    Boolean,
    Date,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from retail_datagen.db.models.base import Base


class Geography(Base):
    """
    Geography dimension table (dim_geographies).

    Stores geographic information including city, state, postal code, and region.
    This is the master geographic reference used by stores, DCs, and customers.

    Corresponds to GeographyMaster Pydantic model.
    """

    __tablename__ = "dim_geographies"

    # Primary key
    geography_id: Mapped[int] = mapped_column(
        "ID", Integer, primary_key=True, index=True, comment="Primary key"
    )

    # Geographic attributes
    city: Mapped[str] = mapped_column(
        "City", String(100), nullable=False, index=True, comment="City name"
    )
    state: Mapped[str] = mapped_column(
        "State",
        String(2),
        nullable=False,
        index=True,
        comment="Two-letter state code",
    )
    postal_code: Mapped[str] = mapped_column(
        "ZipCode", String(10), nullable=False, comment="ZIP code"
    )
    district: Mapped[str] = mapped_column(
        "District", String(100), nullable=False, comment="District name"
    )
    region: Mapped[str] = mapped_column(
        "Region", String(100), nullable=False, index=True, comment="Region name"
    )

    # Relationships (back-populated from related tables)
    stores: Mapped[list["Store"]] = relationship(
        "Store", back_populates="geography", lazy="selectin"
    )
    distribution_centers: Mapped[list["DistributionCenter"]] = relationship(
        "DistributionCenter", back_populates="geography", lazy="selectin"
    )
    customers: Mapped[list["Customer"]] = relationship(
        "Customer", back_populates="geography", lazy="selectin"
    )

    # Indexes for common query patterns
    __table_args__ = (
        Index("idx_geography_state_city", "State", "City"),
        Index("idx_geography_region", "Region"),
        {"extend_existing": True, "comment": "Master geography dimension table"},
    )

    def __repr__(self) -> str:
        return f"<Geography(id={self.geography_id}, city='{self.city}', state='{self.state}')>"


class Store(Base):
    """
    Store dimension table (dim_stores).

    Stores retail location information including store number, address, and
    geographic reference. Linked to Geography via foreign key.

    Corresponds to Store Pydantic model.
    """

    __tablename__ = "dim_stores"

    # Primary key
    store_id: Mapped[int] = mapped_column(
        "ID", Integer, primary_key=True, index=True, comment="Primary key"
    )

    # Store attributes
    store_number: Mapped[str] = mapped_column(
        "StoreNumber",
        String(20),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique store identifier",
    )
    address: Mapped[str] = mapped_column(
        "Address", Text, nullable=False, comment="Store street address"
    )

    # Foreign key to Geography
    geography_id: Mapped[int] = mapped_column(
        "GeographyID",
        Integer,
        ForeignKey("dim_geographies.ID", ondelete="RESTRICT"),
        nullable=False,
        index=True,
        comment="Foreign key to Geography dimension",
    )

    # Jurisdiction-based tax rate for store location (0-20%)
    # Note: This column mirrors the Pydantic Store model's tax_rate field and
    # is used by historical and streaming generation for accurate tax calcs.
    tax_rate: Mapped[float | None] = mapped_column(
        "tax_rate",
        Float,
        nullable=True,
        comment="Combined tax rate for store jurisdiction (e.g., 0.0825)",
    )

    # Store profile fields for realistic variability
    volume_class: Mapped[str | None] = mapped_column(
        "volume_class",
        String(50),
        nullable=True,
        comment="Store volume classification (flagship, high_volume, etc.)",
    )
    store_format: Mapped[str | None] = mapped_column(
        "store_format",
        String(50),
        nullable=True,
        comment="Store format (hypermarket, superstore, standard, etc.)",
    )
    operating_hours: Mapped[str | None] = mapped_column(
        "operating_hours",
        String(50),
        nullable=True,
        comment="Operating hours pattern (24/7, extended, standard, etc.)",
    )
    daily_traffic_multiplier: Mapped[float | None] = mapped_column(
        "daily_traffic_multiplier",
        Float,
        nullable=True,
        comment="Traffic multiplier relative to baseline (0.3-3.0 typical)",
    )

    # Relationships
    geography: Mapped["Geography"] = relationship(
        "Geography", back_populates="stores", lazy="joined"
    )

    # Indexes
    __table_args__ = (
        Index("idx_store_geography", "GeographyID"),
        Index("idx_store_number", "StoreNumber"),
        {"extend_existing": True, "comment": "Store dimension table"},
    )

    def __repr__(self) -> str:
        return f"<Store(id={self.store_id}, store_number='{self.store_number}')>"


class DistributionCenter(Base):
    """
    Distribution Center dimension table (dim_distribution_centers).

    Stores DC information including DC number, address, and geographic reference.
    DCs are the source for truck shipments to stores.

    Corresponds to DistributionCenter Pydantic model.
    """

    __tablename__ = "dim_distribution_centers"

    # Primary key
    dc_id: Mapped[int] = mapped_column(
        "ID", Integer, primary_key=True, index=True, comment="Primary key"
    )

    # DC attributes
    dc_number: Mapped[str] = mapped_column(
        "DCNumber",
        String(20),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique DC identifier",
    )
    address: Mapped[str] = mapped_column(
        "Address", Text, nullable=False, comment="DC street address"
    )

    # Foreign key to Geography
    geography_id: Mapped[int] = mapped_column(
        "GeographyID",
        Integer,
        ForeignKey("dim_geographies.ID", ondelete="RESTRICT"),
        nullable=False,
        index=True,
        comment="Foreign key to Geography dimension",
    )

    # Relationships
    geography: Mapped["Geography"] = relationship(
        "Geography", back_populates="distribution_centers", lazy="joined"
    )
    trucks: Mapped[list["Truck"]] = relationship(
        "Truck", back_populates="distribution_center", lazy="selectin"
    )

    # Indexes
    __table_args__ = (
        Index("idx_dc_geography", "GeographyID"),
        Index("idx_dc_number", "DCNumber"),
        {"extend_existing": True, "comment": "Distribution center dimension table"},
    )

    def __repr__(self) -> str:
        return f"<DistributionCenter(id={self.dc_id}, dc_number='{self.dc_number}')>"


class Truck(Base):
    """
    Truck dimension table (dim_trucks).

    Stores truck information including license plate, refrigeration capability,
    and home DC. Used for supply chain logistics tracking.

    Corresponds to Truck Pydantic model.

    Note: DCID can be 0 for supplier trucks (not home-based at any DC).
    """

    __tablename__ = "dim_trucks"

    # Primary key
    truck_id: Mapped[int] = mapped_column(
        "ID", Integer, primary_key=True, index=True, comment="Primary key"
    )

    # Truck attributes
    license_plate: Mapped[str] = mapped_column(
        "LicensePlate",
        String(20),
        nullable=False,
        unique=True,
        index=True,
        comment="Truck license plate identifier",
    )
    refrigeration: Mapped[bool] = mapped_column(
        "Refrigeration",
        Boolean,
        nullable=False,
        default=False,
        comment="Whether truck has refrigeration capability",
    )

    # Foreign key to DistributionCenter (nullable for supplier trucks with DCID=0)
    dc_id: Mapped[int] = mapped_column(
        "DCID",
        Integer,
        ForeignKey("dim_distribution_centers.ID", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Home distribution center (NULL for supplier trucks)",
    )

    # Relationships
    distribution_center: Mapped["DistributionCenter"] = relationship(
        "DistributionCenter", back_populates="trucks", lazy="joined"
    )

    # Indexes
    __table_args__ = (
        Index("idx_truck_dc", "DCID"),
        Index("idx_truck_license_plate", "LicensePlate"),
        Index("idx_truck_refrigeration", "Refrigeration"),
        {
            "extend_existing": True,
            "comment": "Truck dimension table for supply chain logistics",
        },
    )

    def __repr__(self) -> str:
        return f"<Truck(id={self.truck_id}, license_plate='{self.license_plate}', refrigeration={self.refrigeration})>"


class Customer(Base):
    """
    Customer dimension table (dim_customers).

    Stores customer information including name, contact details, loyalty info,
    and device identifiers (BLE, Ad ID). All data is synthetic.

    Corresponds to Customer Pydantic model.

    Important: All customer data is 100% synthetic. No real PII.
    """

    __tablename__ = "dim_customers"

    # Primary key
    customer_id: Mapped[int] = mapped_column(
        "ID", Integer, primary_key=True, index=True, comment="Primary key"
    )

    # Customer personal information (all synthetic)
    first_name: Mapped[str] = mapped_column(
        "FirstName", String(50), nullable=False, comment="Synthetic first name"
    )
    last_name: Mapped[str] = mapped_column(
        "LastName",
        String(50),
        nullable=False,
        index=True,
        comment="Synthetic last name",
    )
    address: Mapped[str] = mapped_column(
        "Address", Text, nullable=False, comment="Synthetic street address"
    )
    phone: Mapped[str] = mapped_column(
        "Phone", String(20), nullable=False, comment="Synthetic phone number"
    )

    # Foreign key to Geography
    geography_id: Mapped[int] = mapped_column(
        "GeographyID",
        Integer,
        ForeignKey("dim_geographies.ID", ondelete="RESTRICT"),
        nullable=False,
        index=True,
        comment="Foreign key to Geography dimension",
    )

    # Customer identifiers
    loyalty_card: Mapped[str] = mapped_column(
        "LoyaltyCard",
        String(50),
        nullable=False,
        unique=True,
        index=True,
        comment="Loyalty card number (synthetic)",
    )
    ble_id: Mapped[str] = mapped_column(
        "BLEId",
        String(50),
        nullable=False,
        unique=True,
        index=True,
        comment="BLE beacon identifier (synthetic)",
    )
    ad_id: Mapped[str] = mapped_column(
        "AdId",
        String(50),
        nullable=False,
        unique=True,
        index=True,
        comment="Advertising identifier (synthetic)",
    )

    # Relationships
    geography: Mapped["Geography"] = relationship(
        "Geography", back_populates="customers", lazy="joined"
    )

    # Indexes for common query patterns
    __table_args__ = (
        Index("idx_customer_geography", "GeographyID"),
        Index("idx_customer_last_name", "LastName"),
        Index("idx_customer_loyalty_card", "LoyaltyCard"),
        Index("idx_customer_ble_id", "BLEId"),
        Index("idx_customer_ad_id", "AdId"),
        {
            "extend_existing": True,
            "comment": "Customer dimension table (100% synthetic data)",
        },
    )

    def __repr__(self) -> str:
        return f"<Customer(id={self.customer_id}, name='{self.first_name} {self.last_name}', loyalty='{self.loyalty_card}')>"


class Product(Base):
    """
    Product dimension table (dim_products).

    Stores product master data including name, brand, category hierarchy,
    pricing (cost, MSRP, sale price), refrigeration requirements, and launch date.

    Corresponds to ProductMaster Pydantic model.

    Pricing Constraints (enforced at application level):
    - Cost < SalePrice <= MSRP
    - Cost is 50-85% of SalePrice

    Important: All product names, brands, and companies are synthetic.
    """

    __tablename__ = "dim_products"

    # Primary key
    product_id: Mapped[int] = mapped_column(
        "ID", Integer, primary_key=True, index=True, comment="Primary key"
    )

    # Product identification
    product_name: Mapped[str] = mapped_column(
        "ProductName", String(200), nullable=False, index=True, comment="Product name"
    )
    brand: Mapped[str] = mapped_column(
        "Brand", String(100), nullable=False, index=True, comment="Brand name"
    )
    company: Mapped[str] = mapped_column(
        "Company", String(100), nullable=False, index=True, comment="Company name"
    )

    # Category hierarchy
    department: Mapped[str] = mapped_column(
        "Department",
        String(100),
        nullable=False,
        index=True,
        comment="Product department",
    )
    category: Mapped[str] = mapped_column(
        "Category", String(100), nullable=False, index=True, comment="Product category"
    )
    subcategory: Mapped[str] = mapped_column(
        "Subcategory", String(100), nullable=False, comment="Product subcategory"
    )

    # Pricing (stored as REAL/Float in SQLite)
    # Constraints: Cost < SalePrice <= MSRP, Cost is 50-85% of SalePrice
    cost: Mapped[float] = mapped_column(
        "Cost", Float, nullable=False, comment="Product cost price"
    )
    msrp: Mapped[float] = mapped_column(
        "MSRP", Float, nullable=False, comment="Manufacturer suggested retail price"
    )
    sale_price: Mapped[float] = mapped_column(
        "SalePrice", Float, nullable=False, index=True, comment="Actual sale price"
    )

    # Product attributes
    requires_refrigeration: Mapped[bool] = mapped_column(
        "RequiresRefrigeration",
        Boolean,
        nullable=False,
        default=False,
        index=True,
        comment="Whether product requires refrigerated transportation",
    )
    launch_date: Mapped[date] = mapped_column(
        "LaunchDate",
        Date,
        nullable=False,
        comment="Product launch date (when available in stores)",
    )
    taxability: Mapped[str] = mapped_column(
        "taxability",
        String(20),
        nullable=False,
        default="TAXABLE",
        index=True,
        comment="Product tax classification (TAXABLE, NON_TAXABLE, REDUCED_RATE)",
    )

    # Optional product tags (e.g., holiday/seasonal keywords) for generation-time affinity
    tags: Mapped[str | None] = mapped_column(
        "tags",
        Text,
        nullable=True,
        comment="Optional semicolon-separated product tags for simulation",
    )

    # Indexes for common query patterns
    __table_args__ = (
        Index("idx_product_brand", "Brand"),
        Index("idx_product_company", "Company"),
        Index("idx_product_department", "Department"),
        Index("idx_product_category", "Category"),
        Index("idx_product_dept_cat", "Department", "Category"),
        Index("idx_product_refrigeration", "RequiresRefrigeration"),
        Index("idx_product_sale_price", "SalePrice"),
        Index("idx_product_launch_date", "LaunchDate"),
        Index("idx_product_taxability", "taxability"),
        {
            "extend_existing": True,
            "comment": "Product master dimension table (100% synthetic data)",
        },
    )

    def __repr__(self) -> str:
        return f"<Product(id={self.product_id}, name='{self.product_name}', brand='{self.brand}', sale_price={self.sale_price})>"
