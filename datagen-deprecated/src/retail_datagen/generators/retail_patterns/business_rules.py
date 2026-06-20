"""
Business rules engine for data validation and consistency checks.

This module enforces business rules and constraints across fact table generation,
ensuring data consistency, validating business logic, and maintaining
referential integrity between different fact tables.
"""

from decimal import Decimal


class BusinessRulesEngine:
    """
    Enforces business rules and constraints across fact table generation.

    Ensures data consistency, validates business logic, and maintains
    referential integrity between different fact tables.
    """

    def __init__(self):
        """Initialize business rules engine."""
        self._validation_errors: list[str] = []
        self._warnings: list[str] = []

    def validate_receipt_totals(
        self, receipt_lines: list[dict], receipt_total: Decimal
    ) -> bool:
        """
        Validate that receipt line totals match receipt header total.

        Args:
            receipt_lines: List of receipt line records
            receipt_total: Expected receipt total

        Returns:
            True if totals match within tolerance
        """
        calculated_total = sum(
            Decimal(str(line["UnitPrice"])) * line["Qty"] for line in receipt_lines
        )

        tolerance = Decimal("0.01")
        if abs(calculated_total - receipt_total) > tolerance:
            self._validation_errors.append(
                f"Receipt total mismatch: calculated={calculated_total}, "
                f"expected={receipt_total}"
            )
            return False

        return True

    def validate_inventory_consistency(
        self,
        inventory_transactions: list[dict],
        starting_inventory: dict[tuple[int, int], int],
    ) -> bool:
        """
        Validate that inventory transactions don't result in negative inventory.

        Args:
            inventory_transactions: List of inventory transaction records
            starting_inventory: Starting inventory levels

        Returns:
            True if inventory remains non-negative
        """
        current_inventory = starting_inventory.copy()

        for transaction in inventory_transactions:
            if transaction.get("StoreID"):
                key = (transaction["StoreID"], transaction["ProductID"])
            else:
                key = (transaction["DCID"], transaction["ProductID"])

            current_level = current_inventory.get(key, 0)
            new_level = current_level + transaction["QtyDelta"]

            if new_level < 0 and transaction["Reason"] not in ["ADJUSTMENT", "LOST"]:
                self._validation_errors.append(
                    f"Negative inventory: {key} would have {new_level} units"
                )
                return False

            current_inventory[key] = max(0, new_level)

        return True

    def validate_truck_timing(self, truck_moves: list[dict]) -> bool:
        """
        Validate truck movement timing logic.

        Args:
            truck_moves: List of truck movement records

        Returns:
            True if timing is logical
        """
        for move in truck_moves:
            eta = move["ETA"]
            etd = move["ETD"]

            if etd < eta:
                self._validation_errors.append(
                    f"Truck {move['TruckId']}: ETD ({etd}) before ETA ({eta})"
                )
                return False

        return True

    def validate_geographic_consistency(
        self, records: list[dict], geography_mapping: dict[int, int]
    ) -> bool:
        """
        Validate geographic consistency in transactions.

        Args:
            records: Records to validate
            geography_mapping: Mapping of entity IDs to geography IDs

        Returns:
            True if geographically consistent
        """
        for record in records:
            if "StoreID" in record and "CustomerID" in record:
                store_geo = geography_mapping.get(record["StoreID"])
                customer_geo = geography_mapping.get(record["CustomerID"])

                if store_geo and customer_geo and store_geo != customer_geo:
                    # This is a warning, not an error (customers can travel)
                    self._warnings.append(
                        f"Customer {record['CustomerID']} "
                        "shopping outside home geography"
                    )

        return True

    def get_validation_summary(self) -> dict[str, any]:
        """
        Get summary of validation results.

        Returns:
            Dictionary with validation results
        """
        return {
            "errors": self._validation_errors.copy(),
            "warnings": self._warnings.copy(),
            "error_count": len(self._validation_errors),
            "warning_count": len(self._warnings),
            "passed": len(self._validation_errors) == 0,
        }

    def clear_validation_results(self):
        """Clear accumulated validation results."""
        self._validation_errors.clear()
        self._warnings.clear()
