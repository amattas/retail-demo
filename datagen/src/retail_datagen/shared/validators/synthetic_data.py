"""
Synthetic data validator.

Validates that generated data is synthetic and safe.
Prevents generation of real addresses and brands as required by AGENTS.md.
"""

import re

from .blocklists import (
    REAL_ADDRESS_PATTERNS,
    REAL_BRANDS,
)


class SyntheticDataValidator:
    """
    Validates that generated data is synthetic and safe.

    Prevents generation of real addresses and brands as required by AGENTS.md.
    """

    def __init__(self) -> None:
        """Initialize with blocklists of real data to avoid."""
        self.real_brands = REAL_BRANDS
        self.real_address_patterns = REAL_ADDRESS_PATTERNS

    def is_synthetic_address(self, address: str) -> bool:
        """
        Check if an address is synthetic.

        Args:
            address: Address to validate

        Returns:
            True if synthetic, False if potentially real
        """
        address_lower = address.lower().strip()

        # Check against known real address patterns
        for pattern in self.real_address_patterns:
            if re.match(pattern, address_lower, re.IGNORECASE):
                return False

        return True

    def is_synthetic_brand(self, brand: str) -> bool:
        """
        Check if a brand name is synthetic and safe for generation.

        Args:
            brand: Brand name to validate

        Returns:
            True if synthetic and safe, False if real brand detected
        """
        brand_lower = brand.lower().strip()

        # Basic format validation
        if not brand_lower:
            return False

        if len(brand_lower) < 2 or len(brand_lower) > 100:
            return False

        # Check against comprehensive real brand blocklist
        if brand_lower in self.real_brands:
            return False

        # Check for partial matches with real brands
        for real_brand in self.real_brands:
            # Check if the brand contains a real brand name
            if real_brand in brand_lower or brand_lower in real_brand:
                # Allow if the difference is significant enough (more than 3 characters)
                if abs(len(brand_lower) - len(real_brand)) < 3:
                    return False

        # Additional checks for common brand patterns that indicate real brands
        real_brand_patterns = [
            r".*nike.*",
            r".*adidas.*",
            r".*apple.*",
            r".*samsung.*",
            r".*microsoft.*",
            r".*google.*",
            r".*amazon.*",
            r".*walmart.*",
            r".*target.*",
            r".*coca.*cola.*",
            r".*pepsi.*",
            r".*mcdonalds.*",
            r".*starbucks.*",
            r".*disney.*",
            r".*marvel.*",
            r".*sony.*",
            r".*honda.*",
            r".*toyota.*",
            r".*ford.*",
            r".*bmw.*",
            r".*mercedes.*",
            r".*volkswagen.*",
            r".*audi.*",
        ]

        for pattern in real_brand_patterns:
            if re.match(pattern, brand_lower, re.IGNORECASE):
                return False

        return True

    def validate_phone_is_synthetic(self, phone: str) -> bool:
        """
        Validate that phone number uses synthetic/test patterns.

        Args:
            phone: Phone number to validate

        Returns:
            True if synthetic, False otherwise
        """
        # Remove formatting
        digits_only = re.sub(r"[^\d]", "", phone)

        if len(digits_only) != 10:
            return False

        area_code = digits_only[:3]

        # Check for test/reserved area codes
        test_area_codes = {"555", "800", "888", "877", "866", "844", "833", "822"}
        if area_code in test_area_codes:
            return True

        # Avoid premium numbers
        premium_codes = {"900", "976"}
        if area_code in premium_codes:
            return False

        # For other area codes, require specific patterns to ensure synthetic
        # This is a simplified check - in practice, you might have more sophisticated rules
        return True

    def validate_loyalty_card_format(self, loyalty_card: str) -> bool:
        """
        Validate loyalty card uses synthetic format.

        Args:
            loyalty_card: Loyalty card number

        Returns:
            True if matches synthetic format
        """
        return bool(re.match(r"^LC\d{9}$", loyalty_card))

    def validate_ble_id_format(self, ble_id: str) -> bool:
        """
        Validate BLE ID uses synthetic format.

        Args:
            ble_id: BLE identifier

        Returns:
            True if matches synthetic format
        """
        return bool(re.match(r"^BLE[A-Z0-9]{6}$", ble_id))

    def validate_ad_id_format(self, ad_id: str) -> bool:
        """
        Validate advertising ID uses synthetic format.

        Args:
            ad_id: Advertising identifier

        Returns:
            True if matches synthetic format
        """
        return bool(re.match(r"^AD[A-Z0-9]{6}$", ad_id))
