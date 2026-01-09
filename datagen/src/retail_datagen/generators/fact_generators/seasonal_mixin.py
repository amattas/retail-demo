"""
Seasonal patterns and holiday-specific logic for product promotions
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from retail_datagen.shared.models import ProductMaster

from .base_types import FactGeneratorBase

logger = logging.getLogger(__name__)


class SeasonalMixin(FactGeneratorBase):
    """Seasonal patterns and holiday-specific logic for product promotions"""

    def _thanksgiving_date(self, year: int) -> datetime:
        # 4th Thursday in November
        d = datetime(year, 11, 1)
        # weekday(): Mon=0..Sun=6; Thursday=3
        first_thu = d + timedelta(days=(3 - d.weekday() + 7) % 7)
        return first_thu + timedelta(weeks=3)

    def _memorial_day(self, year: int) -> datetime:
        # Last Monday of May
        d = datetime(year, 5, 31)
        return d - timedelta(days=(d.weekday() - 0) % 7)

    def _labor_day(self, year: int) -> datetime:
        # First Monday of September
        d = datetime(year, 9, 1)
        return d + timedelta(days=(0 - d.weekday()) % 7)

    def _in_window(
        self, date: datetime, center: datetime, lead_days: int, lag_days: int
    ) -> bool:
        start = center - timedelta(days=lead_days)
        end = center + timedelta(days=lag_days)
        return start.date() <= date.date() <= end.date()

    def _product_has_keywords(
        self, product: ProductMaster, keywords: list[str]
    ) -> bool:
        t = getattr(product, "Tags", None) or getattr(product, "tags", None) or ""
        hay = " ".join(
            [
                str(product.ProductName),
                str(product.Department),
                str(product.Category),
                str(product.Subcategory),
                str(t or ""),
            ]
        ).lower()
        return any(k in hay for k in keywords)

    def _get_product_multiplier(self, date: datetime, product: ProductMaster) -> float:
        year = date.year
        tg = self._thanksgiving_date(year)
        bf = tg.replace(day=tg.day) + timedelta(days=1)
        xmas = datetime(year, 12, 25)
        # Thanksgiving lead core foods
        if self._in_window(date, tg, 10, 1):
            core = [
                "thanksgiving",
                "turkey",
                "stuffing",
                "cranberry",
                "cranberries",
                "pie",
                "pumpkin",
                "rolls",
                "casserole",
                "green bean",
                "cream of mushroom",
                "fried onion",
                "gravy",
                "yams",
                "sweet potato",
                "baking",
            ]
            baking = [
                "baking",
                "flour",
                "sugar",
                "spice",
                "cinnamon",
                "nutmeg",
                "clove",
            ]
            if self._product_has_keywords(product, core):
                return 3.5
            if self._product_has_keywords(product, baking):
                return 1.8
            # general grocery light bump
            if self._product_has_keywords(
                product, ["grocery", "produce", "meat", "beverage", "snack"]
            ):
                return 1.3
        # Black Friday (non-food)
        if date.date() == bf.date():
            if self._product_has_keywords(
                product,
                ["electronics", "tv", "laptop", "headphone", "gaming", "appliance"],
            ):
                return 5.0
            if self._product_has_keywords(
                product, ["toy", "lego", "action figure", "doll"]
            ):
                return 3.0
            if self._product_has_keywords(
                product, ["home", "home goods", "cookware", "small appliance"]
            ):
                return 2.2
            if self._product_has_keywords(
                product, ["apparel", "clothing", "shoe", "footwear"]
            ):
                return 2.3
        # Christmas ramp
        if self._in_window(date, xmas, 14, 0):
            if self._product_has_keywords(
                product,
                [
                    "ham",
                    "roast",
                    "cookie",
                    "baking",
                    "candy",
                    "cider",
                    "eggnog",
                    "hot chocolate",
                    "hot beverage",
                ],
            ):
                return 1.8
            if self._product_has_keywords(
                product, ["electronics", "toy", "apparel", "home"]
            ):
                return 1.6
        # Grill-out windows
        mem = self._memorial_day(year)
        lab = self._labor_day(year)
        jul4 = datetime(year, 7, 4)
        grill_tags = [
            "grill",
            "hot dog",
            "hotdog",
            "sausage",
            "burger",
            "ground beef",
            "steak",
            "chicken breast",
            "bun",
            "buns",
            "ketchup",
            "mustard",
            "relish",
            "bbq sauce",
            "charcoal",
            "chips",
            "soda",
            "ice",
        ]
        if (
            self._in_window(date, mem, 2, 2)
            or self._in_window(date, lab, 2, 2)
            or self._in_window(date, jul4, 2, 2)
        ):
            if self._product_has_keywords(product, grill_tags):
                return 2.5
        return 1.0

    def _apply_holiday_overlay_to_basket(self, date: datetime, basket) -> None:
        """Adjust basket in-place based on holiday overlay.

        Applies qty boosts and occasional extra lines.
        """
        if not getattr(basket, "items", None):
            return
        # Increase quantity for existing targeted items
        new_items = []
        targeted_candidates = []
        for product, qty in basket.items:
            m = self._get_product_multiplier(date, product)
            if m > 1.0:
                # Rough qty bump: +1 for ~each full +0.8 in multiplier
                bump = 0
                if m >= 3.0:
                    bump = self._rng.choice([1, 2])
                elif m >= 1.5:
                    bump = self._rng.choice([0, 1])
                qty = max(1, qty + bump)
                targeted_candidates.append(product)
            new_items.append((product, qty))

        # Basket size bump for some holidays
        basket_mult = 1.0
        year = date.year
        tg = self._thanksgiving_date(year)
        xmas = datetime(year, 12, 25)
        if self._in_window(date, tg, 10, 1):
            basket_mult = 1.2
        elif self._in_window(date, xmas, 2, 0):
            basket_mult = 1.2

        extra = 0
        if basket_mult > 1.0:
            base_count = sum(q for _, q in new_items)
            extra = max(0, int(base_count * (basket_mult - 1.0) * 0.5))

        # Add a few extra targeted items if needed
        if extra > 0:
            # Choose from strong targets first; otherwise random
            pool = targeted_candidates or [p for p, _ in new_items]
            for _ in range(extra):
                product = self._rng.choice(pool)
                new_items.append((product, 1))

        basket.items = new_items
