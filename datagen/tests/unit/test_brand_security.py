"""
Unit test for brand security validation.
"""

from retail_datagen.shared.validators import SyntheticDataValidator


def test_brand_security_blocklist_and_allowlist():
    validator = SyntheticDataValidator()

    real_brands_to_block = [
        "Nike",
        "Adidas",
        "Apple",
        "Samsung",
        "Microsoft",
        "Google",
        "Walmart",
        "Target",
        "Coca Cola",
        "Pepsi",
    ]
    for brand in real_brands_to_block:
        assert not validator.is_synthetic_brand(brand), f"Real brand allowed: {brand}"

    synthetic_brands_to_allow = [
        "Fresh Select",
        "Quality Choice",
        "Natural Best",
        "Pure Simple",
        "Golden Choice",
        "Tech Pro",
        "Style Pro",
    ]
    for brand in synthetic_brands_to_allow:
        assert validator.is_synthetic_brand(brand), f"Synthetic brand blocked: {brand}"
