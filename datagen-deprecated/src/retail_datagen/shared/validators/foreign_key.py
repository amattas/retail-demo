"""
Foreign key validator.

Validates foreign key relationships between tables.
Ensures referential integrity across all dimension and fact tables.
"""


class ForeignKeyValidator:
    """
    Validates foreign key relationships between tables.

    Ensures referential integrity across all dimension and fact tables.
    """

    def __init__(self) -> None:
        """Initialize with empty reference collections."""
        self._geography_ids: set[int] = set()
        self._store_ids: set[int] = set()
        self._dc_ids: set[int] = set()
        self._truck_ids: set[int] = set()
        self._customer_ids: set[int] = set()
        self._product_ids: set[int] = set()
        self._receipt_ids: set[str] = set()

    def register_geography_ids(self, geography_ids: list[int]) -> None:
        """Register valid geography IDs."""
        self._geography_ids.update(geography_ids)

    def register_store_ids(self, store_ids: list[int]) -> None:
        """Register valid store IDs."""
        self._store_ids.update(store_ids)

    def register_dc_ids(self, dc_ids: list[int]) -> None:
        """Register valid distribution center IDs."""
        self._dc_ids.update(dc_ids)

    def register_truck_ids(self, truck_ids: list[int]) -> None:
        """Register valid truck IDs."""
        self._truck_ids.update(truck_ids)

    def register_customer_ids(self, customer_ids: list[int]) -> None:
        """Register valid customer IDs."""
        self._customer_ids.update(customer_ids)

    def register_product_ids(self, product_ids: list[int]) -> None:
        """Register valid product IDs."""
        self._product_ids.update(product_ids)

    def register_receipt_ids(self, receipt_ids: list[str]) -> None:
        """Register valid receipt IDs."""
        self._receipt_ids.update(receipt_ids)

    def validate_geography_fk(self, geography_id: int) -> bool:
        """Validate geography foreign key."""
        return geography_id in self._geography_ids

    def validate_store_fk(self, store_id: int) -> bool:
        """Validate store foreign key."""
        return store_id in self._store_ids

    def validate_dc_fk(self, dc_id: int) -> bool:
        """Validate distribution center foreign key."""
        return dc_id in self._dc_ids

    def validate_truck_fk(self, truck_id: int) -> bool:
        """Validate truck foreign key."""
        return truck_id in self._truck_ids

    def validate_customer_fk(self, customer_id: int) -> bool:
        """Validate customer foreign key."""
        return customer_id in self._customer_ids

    def validate_product_fk(self, product_id: int) -> bool:
        """Validate product foreign key."""
        return product_id in self._product_ids

    def validate_receipt_fk(self, receipt_id: str) -> bool:
        """Validate receipt foreign key."""
        return receipt_id in self._receipt_ids

    def get_validation_summary(self) -> dict[str, int]:
        """Get summary of registered IDs for validation."""
        return {
            "geographies": len(self._geography_ids),
            "stores": len(self._store_ids),
            "dcs": len(self._dc_ids),
            "trucks": len(self._truck_ids),
            "customers": len(self._customer_ids),
            "products": len(self._product_ids),
            "receipts": len(self._receipt_ids),
        }
