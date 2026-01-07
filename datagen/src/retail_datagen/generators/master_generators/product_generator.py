"""
Product master data generation with brands, companies, and pricing.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import numpy as np

from retail_datagen.shared.models import (
    ProductBrandDict,
    ProductCompanyDict,
    ProductDict,
    ProductMaster,
    ProductTaxability,
)
from retail_datagen.shared.validators import PricingCalculator

from ..utils import ProgressReporter

logger = logging.getLogger(__name__)

# Constants for product generation
_MAX_COMBINATION_BATCH_SIZE = 1000
_COMBINATION_SAFETY_MULTIPLIER = 2
_MAX_GENERATION_ATTEMPTS_MULTIPLIER = 10


@dataclass
class ProductCategoryData:
    """Organized product and brand data by category."""

    companies_by_category: dict[str, list[str]]
    company_names: list[str]
    brands_by_category: dict[str, list[tuple[int, Any]]]
    products_by_category: dict[str, list[tuple[int, Any]]]


class ProductGeneratorMixin:
    """Mixin for product master data generation."""

    def _organize_products_and_brands_by_category(
        self,
        product_data: list[ProductDict],
        brand_data: list[ProductBrandDict],
        company_data: list[ProductCompanyDict],
    ) -> ProductCategoryData:
        """Organize products, brands, and companies by category for smart matching."""
        # Group companies by category
        companies_by_category: dict[str, list[str]] = {}
        for company in company_data:
            category = company.Category
            if category not in companies_by_category:
                companies_by_category[category] = []
            companies_by_category[category].append(company.Company)

        company_names = [company.Company for company in company_data]

        # Group brands by category
        brands_by_category: dict[str, list[tuple[int, Any]]] = {}
        for brand_idx, brand in enumerate(brand_data):
            category = brand.Category
            if category not in brands_by_category:
                brands_by_category[category] = []
            brands_by_category[category].append((brand_idx, brand))

        # Group products by category
        products_by_category: dict[str, list[tuple[int, Any]]] = {}
        for product_idx, product in enumerate(product_data):
            product_category = self._map_product_to_brand_category(
                product.Category, product.Department
            )
            if product_category not in products_by_category:
                products_by_category[product_category] = []
            products_by_category[product_category].append((product_idx, product))

        print(f"Brand categories: {sorted(brands_by_category.keys())}")
        print(f"Product categories mapped to: {sorted(products_by_category.keys())}")

        return ProductCategoryData(
            companies_by_category=companies_by_category,
            company_names=company_names,
            brands_by_category=brands_by_category,
            products_by_category=products_by_category,
        )

    def _create_valid_brand_product_combinations(
        self,
        category_data: ProductCategoryData,
    ) -> list[tuple[int, int]]:
        """Create valid category-matched brand-product combinations."""
        valid_combinations: list[tuple[int, int]] = []

        for category in category_data.brands_by_category.keys():
            if category in category_data.products_by_category:
                category_brands = category_data.brands_by_category[category]
                category_products = category_data.products_by_category[category]

                for product_idx, _ in category_products:
                    for brand_idx, _ in category_brands:
                        valid_combinations.append((product_idx, brand_idx))

                print(
                    f"Category '{category}': {len(category_brands)} brands × "
                    f"{len(category_products)} products = {len(category_brands) * len(category_products)} combinations"
                )

        print(f"Total valid category-matched combinations: {len(valid_combinations):,}")
        return valid_combinations

    def _generate_single_product(
        self,
        product_id: int,
        product_idx: int,
        brand_idx: int,
        category_data: ProductCategoryData,
        product_data: list[ProductDict],
        brand_data: list[ProductBrandDict],
        target_product_count: int,
        pricing_calculator: PricingCalculator,
        product_tags_overlay: dict[str, str],
        historical_start: datetime,
        rng: Any,
    ) -> ProductMaster | None:
        """Generate a single product with validation retry logic."""
        product = product_data[product_idx]
        brand = brand_data[brand_idx]

        # Match company to brand by category
        brand_category = brand.Category
        if (
            brand_category in category_data.companies_by_category
            and category_data.companies_by_category[brand_category]
        ):
            company = rng.choice(category_data.companies_by_category[brand_category])
        else:
            company = rng.choice(category_data.company_names)

        base_price = float(product.BasePrice)
        max_retries = 5

        for retry in range(max_retries):
            # Calculate pricing with variation
            if retry == 0:
                price_variation = float(np.random.uniform(0.85, 1.15))
            else:
                price_variation = rng.uniform(0.9, 1.1)

            adjusted_base_price = base_price * price_variation
            pricing = pricing_calculator.calculate_full_pricing(Decimal(str(adjusted_base_price)))

            try:
                # Determine tags
                _tags = getattr(product, "Tags", None)
                if not _tags:
                    _tags = product_tags_overlay.get(product.ProductName)

                return ProductMaster(
                    ID=product_id,
                    ProductName=product.ProductName,
                    Brand=brand.Brand,
                    Company=company,
                    Department=product.Department,
                    Category=product.Category,
                    Subcategory=product.Subcategory,
                    Cost=pricing["Cost"],
                    MSRP=pricing["MSRP"],
                    SalePrice=pricing["SalePrice"],
                    RequiresRefrigeration=self._requires_refrigeration(
                        product.Category, product.Subcategory
                    ),
                    LaunchDate=self._calculate_product_launch_date(
                        product_id, target_product_count, historical_start, rng
                    ),
                    taxability=self._determine_product_taxability(
                        product.Department, product.Category
                    ),
                    Tags=_tags,
                )
            except ValueError as e:
                if retry == max_retries - 1:
                    print(
                        f"Warning: Failed to generate valid pricing for "
                        f"{product.ProductName} + {brand.Brand} after {max_retries} attempts: {e}"
                    )

        return None

    def _print_product_generation_summary(
        self,
        target_count: int,
        combinations_processed: int,
        successful: int,
        failed: int,
        final_count: int,
    ) -> None:
        """Print detailed product generation summary."""
        print("\n=== Product Generation Summary ===")
        print(f"Target products: {target_count}")
        print(f"Total combinations processed: {combinations_processed}")
        print(f"Successful products: {successful}")
        print(f"Failed validations: {failed}")
        print(f"Final product count: {final_count}")

    def generate_products_master(
        self,
        target_product_count: int,
        product_data: list[ProductDict],
        brand_data: list[ProductBrandDict],
        company_data: list[ProductCompanyDict],
        product_tags_overlay: dict[str, str],
        pricing_calculator: PricingCalculator,
        historical_start_date: str,
        rng: Any,
        np_rng: Any,
    ) -> list[ProductMaster]:
        """
        Generate products with realistic pricing and brand combinations.

        Args:
            target_product_count: Number of products to generate
            product_data: Product dictionary data
            brand_data: Brand dictionary data
            company_data: Company dictionary data
            product_tags_overlay: Optional product tags
            pricing_calculator: Pricing calculator utility
            historical_start_date: Start date for launch date calculation
            rng: Random number generator
            np_rng: NumPy random generator

        Returns:
            List of ProductMaster records
        """
        print("Generating product master data with brand combinations...")

        if not product_data or not brand_data or not company_data:
            raise ValueError("Product dictionary data not loaded")

        print(f"Target products: {target_product_count}")
        print(f"Available base products: {len(product_data)}")
        print(f"Available brands: {len(brand_data)}")
        print(f"Available companies: {len(company_data)}")

        # Parse historical start date
        historical_start = datetime.strptime(historical_start_date, "%Y-%m-%d")

        # Organize data by category
        print("Creating category-aware brand-product combinations...")
        category_data = self._organize_products_and_brands_by_category(
            product_data, brand_data, company_data
        )

        # Create valid combinations
        valid_combinations = self._create_valid_brand_product_combinations(category_data)

        # Sample combinations
        vc_count = len(valid_combinations)
        if vc_count == 0:
            selected_combinations: list[tuple[int, int]] = []
        else:
            replace = vc_count < target_product_count
            idx = np_rng.choice(vc_count, size=target_product_count, replace=replace)
            selected_combinations = [valid_combinations[i] for i in idx]

        print(f"Selected {len(selected_combinations)} category-matched combinations")

        # Initialize generation
        products_master = []
        progress_reporter = ProgressReporter(target_product_count, "Generating product combinations")
        product_id = 1
        combination_idx = 0
        successful_products = 0
        failed_validations = 0

        # Guard clause
        if len(valid_combinations) == 0:
            raise ValueError(
                "No valid brand-product combinations available. "
                "Check that brand and product data have matching categories."
            )

        # Max attempts to prevent infinite loop
        max_total_attempts = target_product_count * _MAX_GENERATION_ATTEMPTS_MULTIPLIER

        # Generate products until we reach target count
        while successful_products < target_product_count:
            # Safety check
            if combination_idx >= max_total_attempts:
                raise RuntimeError(
                    f"Failed to generate {target_product_count} products after "
                    f"{max_total_attempts} attempts. Generated {successful_products} products, "
                    f"{failed_validations} failed validations."
                )

            # Replenish combinations if exhausted
            if combination_idx >= len(selected_combinations):
                remaining_needed = target_product_count - successful_products
                batch_size = min(
                    _MAX_COMBINATION_BATCH_SIZE,
                    remaining_needed * _COMBINATION_SAFETY_MULTIPLIER,
                )
                print(
                    f"Exhausted combinations at {combination_idx} with "
                    f"{successful_products}/{target_product_count} successful. "
                    f"Generating {batch_size} more..."
                )
                add_idx = np_rng.integers(0, len(valid_combinations), size=batch_size)
                selected_combinations.extend([valid_combinations[i] for i in add_idx])

            product_idx, brand_idx = selected_combinations[combination_idx]
            combination_idx += 1

            # Generate single product
            product_master = self._generate_single_product(
                product_id,
                product_idx,
                brand_idx,
                category_data,
                product_data,
                brand_data,
                target_product_count,
                pricing_calculator,
                product_tags_overlay,
                historical_start,
                rng,
            )

            if product_master:
                products_master.append(product_master)
                successful_products += 1
            else:
                failed_validations += 1

            product_id += 1

            # Progress update every 500 products
            if successful_products % 500 == 0 and successful_products > 0:
                progress_reporter.update(500)

        # Final progress update
        remaining_progress = target_product_count - (successful_products // 500) * 500
        if remaining_progress > 0:
            progress_reporter.update(remaining_progress)
        progress_reporter.complete()

        # Print summary and validate
        self._print_product_generation_summary(
            target_product_count, combination_idx, successful_products, failed_validations, len(products_master)
        )

        if successful_products != target_product_count:
            raise ValueError(f"Expected {target_product_count} products, got {successful_products}")
        if len(products_master) != target_product_count:
            raise ValueError(
                f"Expected {target_product_count} products in list, got {len(products_master)}"
            )

        print(f"Generated {len(products_master)} product master records with brand combinations")
        return products_master

    def _map_product_to_brand_category(self, product_category: str, product_department: str) -> str:
        """Map product categories/departments to appropriate brand categories."""
        category_mapping = {
            # Food-related mappings
            "Fresh Produce": "Food",
            "Meat & Seafood": "Food",
            "Dairy & Eggs": "Food",
            "Frozen Foods": "Food",
            "Pantry Staples": "Food",
            "Beverages": "Food",
            "Snacks & Candy": "Food",
            "Bakery": "Food",
            "International Foods": "Food",
            "Organic & Natural": "Food",
            # Electronics mappings
            "Consumer Electronics": "Electronics",
            "Computers & Accessories": "Electronics",
            "Mobile Devices": "Electronics",
            "Gaming": "Electronics",
            "Audio & Video": "Electronics",
            "Smart Home": "Electronics",
            "Wearable Tech": "Electronics",
            # Clothing mappings
            "Men's Apparel": "Clothing",
            "Women's Apparel": "Clothing",
            "Kids' Clothing": "Clothing",
            "Athletic Wear": "Clothing",
            "Footwear": "Clothing",
            "Accessories": "Clothing",
            # Health & Personal Care mappings
            "Health & Wellness": "Health",
            "Personal Care": "Health",
            "Beauty": "Health",
            "Pharmacy": "Health",
            "Baby Care": "Health",
            # Pet mappings
            "Pet Food": "Pet",
            "Pet Supplies": "Pet",
            "Pet Care": "Pet",
            # Automotive mappings
            "Automotive": "Automotive",
            "Car Care": "Automotive",
            # Office mappings
            "Office Supplies": "Office",
            "School & Office": "Office",
            "Stationery": "Office",
            "Business Supplies": "Office",
            # Sports mappings
            "Sports & Outdoors": "Sports",
            "Exercise & Fitness": "Sports",
            "Outdoor Recreation": "Sports",
            "Team Sports": "Sports",
            # Home mappings
            "Home & Garden": "Home",
            "Household Essentials": "Home",
            "Home Improvement": "Home",
            "Furniture": "Home",
            "Kitchen & Dining": "Home",
            "Home Decor": "Home",
        }

        # First try exact category match
        if product_category in category_mapping:
            return category_mapping[product_category]

        # Then try department match
        if product_department in category_mapping:
            return category_mapping[product_department]

        # For unmapped categories, make intelligent guesses
        category_lower = product_category.lower()
        department_lower = product_department.lower()

        # Food-related keywords
        if any(
            keyword in category_lower or keyword in department_lower
            for keyword in [
                "food",
                "grocery",
                "fresh",
                "frozen",
                "meat",
                "dairy",
                "produce",
                "beverage",
                "snack",
                "bakery",
            ]
        ):
            return "Food"
        # Electronics keywords
        elif any(
            keyword in category_lower or keyword in department_lower
            for keyword in [
                "electronic",
                "computer",
                "tech",
                "digital",
                "mobile",
                "phone",
                "gaming",
                "audio",
                "video",
            ]
        ):
            return "Electronics"
        # Clothing keywords
        elif any(
            keyword in category_lower or keyword in department_lower
            for keyword in [
                "apparel",
                "clothing",
                "fashion",
                "wear",
                "shoe",
                "footwear",
                "accessory",
            ]
        ):
            return "Clothing"
        # Default to Home
        else:
            return "Home"

    def _calculate_product_launch_date(
        self, product_id: int, total_products: int, historical_start: datetime, rng: Any
    ) -> datetime:
        """Calculate product launch date for realistic product introduction over time."""
        # Define product introduction windows
        established_products_pct = 0.60  # 60% already established
        early_launch_pct = 0.30  # 30% launch in first 6 months

        established_count = int(total_products * established_products_pct)
        early_launch_count = int(total_products * early_launch_pct)

        if product_id <= established_count:
            # Already established - launched 6 months to 2 years before
            days_before = rng.randint(180, 730)
            return historical_start - timedelta(days=days_before)
        elif product_id <= established_count + early_launch_count:
            # Early launch - launched in first 6 months
            days_after = rng.randint(0, 180)
            return historical_start + timedelta(days=days_after)
        else:
            # Late launch - months 6-12
            days_after = rng.randint(180, 365)
            return historical_start + timedelta(days=days_after)

    def _requires_refrigeration(self, category: str, subcategory: str) -> bool:
        """Determine if a product requires refrigeration based on category."""
        refrigerated_categories = {"Dairy & Alternatives", "Meat & Poultry", "Seafood"}

        if category == "Produce":
            return True

        if category == "Baby Food":
            return True

        if category == "Medicine" and "refrigerated" in subcategory.lower():
            return True

        non_refrigerated_departments = {
            "Electronics",
            "Clothing",
            "Health & Beauty",
            "Baby & Kids",
            "Home & Garden",
            "Pet Supplies",
            "Automotive",
            "Office Supplies",
            "Seasonal",
            "Sports & Recreation",
        }

        for dept in non_refrigerated_departments:
            if category.startswith(dept) or dept in category:
                return False

        return category in refrigerated_categories

    def _determine_product_taxability(self, department: str, category: str) -> ProductTaxability:
        """Determine product taxability based on department and category."""
        food_keywords = {
            "food",
            "grocery",
            "fresh",
            "frozen",
            "meat",
            "dairy",
            "produce",
            "beverage",
            "snack",
            "bakery",
            "pantry",
            "organic",
        }

        department_lower = department.lower()
        category_lower = category.lower()

        # Check if it's food/grocery
        if any(
            keyword in department_lower or keyword in category_lower for keyword in food_keywords
        ):
            return ProductTaxability.NON_TAXABLE

        # Clothing items have reduced tax rate
        clothing_keywords = {
            "apparel",
            "clothing",
            "fashion",
            "wear",
            "shoe",
            "footwear",
        }

        if any(
            keyword in department_lower or keyword in category_lower for keyword in clothing_keywords
        ):
            return ProductTaxability.REDUCED_RATE

        # Small portion of non-food, non-clothing items receive reduced rate
        try:
            key = f"{department_lower}:{category_lower}"
            h = hash(key) % 100
            if h < 10:
                return ProductTaxability.REDUCED_RATE
        except Exception as e:
            logger.debug(f"Failed to compute hash for taxability: {e}")

        return ProductTaxability.TAXABLE
