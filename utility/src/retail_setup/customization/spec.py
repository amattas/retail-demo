"""Custom business layouts: the AI draft contract and the validated, saved spec.

A custom layout is produced in two steps:

1. A Copilot model fills a :class:`CustomizationDraft` (structured output) for a
   business name. Drafts are untrusted: :func:`draft_problems` checks bounds,
   internal consistency, and that no real brand name the model drew on appears
   in generated brands or product titles.
2. :func:`build_spec` deterministically turns an accepted draft into a
   :class:`CustomizationSpec`, the per-deployment artifact every later step reads
   (dictionaries, report/app themes). Nothing after this point calls a model.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from retail_setup.customization.logo import LogoInfo
from retail_setup.dictionaries.models import StoreTypeProfile

SPEC_VERSION = 1
CUSTOM_PREFIX = "custom-"
HEX_PATTERN = r"^#[0-9A-Fa-f]{6}$"
# Display text reaches report JSON, CSS, HTML titles, and logs. Keep it to
# letters, digits, spaces, and common business-name punctuation.
_TEXT = re.compile(r"^[\w &'’.,!+\-/()%]+$")
_ZONE = re.compile(r"^[a-z][a-z0-9-]{0,39}$")

MIN_PRODUCTS = 200
MAX_PRODUCTS = 6000
MIN_DEPARTMENTS = 3
MAX_DEPARTMENTS = 16

Taxability = Literal["TAXABLE", "NON_TAXABLE", "REDUCED_RATE"]


def _text(value: str, *, field: str, max_length: int) -> str:
    value = " ".join(value.split())
    if not value or len(value) > max_length or not _TEXT.fullmatch(value):
        raise ValueError(
            f"{field} must be 1-{max_length} characters of letters, digits, spaces, "
            "or & ' . , ! + - / ( ) %"
        )
    return value


class _Closed(BaseModel):
    # Closed objects keep provider structured-output schemas strict.
    model_config = ConfigDict(extra="forbid")


class SubcategoryDraft(_Closed):
    name: str = Field(description="Subcategory name, e.g. 'Fresh Fruit'.")
    price_min: float = Field(gt=0, le=100000, description="Lowest typical shelf price, USD.")
    price_max: float = Field(gt=0, le=100000, description="Highest typical shelf price, USD.")
    nouns: list[str] = Field(
        min_length=3, max_length=40, description="Generic item nouns, e.g. 'Gala Apples'."
    )
    modifiers: list[str] = Field(
        min_length=2, max_length=12, description="Generic descriptors, e.g. 'Organic'."
    )


class CategoryDraft(_Closed):
    category: str
    brand_share: float = Field(
        ge=0, le=1, description="Share of products whose title starts with a brand."
    )
    refrigerated: bool = Field(description="True when products need cold storage.")
    subcategories: list[SubcategoryDraft] = Field(min_length=1, max_length=8)


class DepartmentDraft(_Closed):
    name: str
    weight: float = Field(gt=0, description="Relative share of basket lines.")
    taxability: Taxability
    categories: list[CategoryDraft] = Field(min_length=1, max_length=8)


class BrandDraft(_Closed):
    brand: str = Field(description="Fictional brand name, a light variation of a real one.")
    department: str = Field(description="Exact name of the department this brand sells in.")


class ProfileDraft(_Closed):
    basket_lambda: float = Field(gt=0, le=40, description="Mean basket line ceiling.")
    avg_ticket_target: float = Field(gt=0, le=20000, description="Average ticket, USD.")
    hourly_weights: list[float] = Field(min_length=24, max_length=24)
    daily_weights: list[float] = Field(min_length=7, max_length=7, description="Monday first.")
    monthly_weights: list[float] = Field(min_length=12, max_length=12, description="Jan first.")
    promo_rate: float = Field(ge=0, le=0.6)
    online_order_share: float = Field(ge=0, le=0.8)
    zones: list[str] = Field(
        min_length=3, max_length=16, description="Lowercase hyphenated store zones."
    )


class PaletteDraft(_Closed):
    primary: str = Field(pattern=HEX_PATTERN, description="Main brand color.")
    secondary: str = Field(pattern=HEX_PATTERN, description="Second brand color.")
    accent: str = Field(pattern=HEX_PATTERN, description="Highlight color.")
    dark: str = Field(pattern=HEX_PATTERN, description="Dark brand neutral for headings.")
    light: str = Field(pattern=HEX_PATTERN, description="Light brand neutral for surfaces.")


class CustomizationDraft(_Closed):
    display_name: str = Field(description="The business name as customers know it.")
    summary: str = Field(description="One sentence on what the business sells.")
    base_preset: str = Field(description="Closest preset layout name.")
    profile: ProfileDraft
    departments: list[DepartmentDraft] = Field(min_length=1, max_length=MAX_DEPARTMENTS)
    brands: list[BrandDraft] = Field(min_length=4, max_length=48)
    inspiration_brands: list[str] = Field(
        max_length=80,
        description=(
            "Every real brand, house brand, or trademark you varied to create the "
            "fictional brands, plus the business's own name."
        ),
    )
    palette: PaletteDraft


class Palette(_Closed):
    primary: str = Field(pattern=HEX_PATTERN)
    secondary: str = Field(pattern=HEX_PATTERN)
    accent: str = Field(pattern=HEX_PATTERN)
    dark: str = Field(pattern=HEX_PATTERN)
    light: str = Field(pattern=HEX_PATTERN)

    @field_validator("*")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()


class GeneratorInfo(_Closed):
    sdk: str
    model: str | None = None


class _TreeSubcategory(_Closed):
    name: str
    price_min: float
    price_max: float
    nouns: list[str]
    modifiers: list[str]


class _TreeCategory(_Closed):
    category: str
    brand_share: float
    refrigerated: bool
    subcategories: list[_TreeSubcategory]


class _TreeDepartment(_Closed):
    name: str
    taxability: Taxability
    categories: list[_TreeCategory]


class _TreeBrand(_Closed):
    Brand: str
    Company: str
    Category: str


class CatalogTree(_Closed):
    """The catalog tree a custom spec feeds to ``dictionaries.catalog.build_catalog``."""

    store_type: str
    brand_styles: list[str]
    brands: list[_TreeBrand]
    departments: list[_TreeDepartment]


class CustomizationSpec(_Closed):
    """The saved per-deployment layout. Every downstream artifact derives from it.

    Validation re-applies every draft check, so a hand-edited or shared spec
    (``customize --from-spec``) is held to the same rules as a fresh draft.
    """

    version: Literal[1] = 1
    business_name: str
    display_name: str
    summary: str
    slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,40}$")
    base_preset: str
    profile: StoreTypeProfile
    catalog_tree: dict[str, Any]
    inspiration_brands: list[str]
    palette: Palette
    generator: GeneratorInfo
    logo: LogoInfo | None = Field(
        default=None, description="Provenance of the saved logo.svg beside this spec."
    )

    @property
    def store_type(self) -> str:
        return self.profile.store_type

    def content_hash(self) -> str:
        """SHA-256 of the canonical spec JSON (stable across saves)."""

        payload = json.dumps(self.model_dump(mode="json"), sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @model_validator(mode="after")
    def _consistent(self) -> CustomizationSpec:
        expected = f"{CUSTOM_PREFIX}{self.slug}"
        if slugify(validate_business_name(self.business_name)) != self.slug:
            raise ValueError("slug must be derived from business_name")
        tree = CatalogTree.model_validate(self.catalog_tree)
        if self.profile.store_type != expected or tree.store_type != expected:
            raise ValueError(f"profile and catalog store_type must be {expected!r}")
        if tree.brand_styles != [b.Brand for b in tree.brands] or any(
            b.Company != f"{b.Brand} Co." for b in tree.brands
        ):
            raise ValueError("catalog brands must match brand_styles with '<Brand> Co.' companies")
        problems = draft_problems(
            self._as_draft(tree), business_name=self.business_name, presets=[self.base_preset]
        )
        if problems:
            raise ValueError("invalid customization spec:\n- " + "\n- ".join(problems[:20]))
        return self

    def _as_draft(self, tree: CatalogTree) -> CustomizationDraft:
        weights = self.profile.department_weights
        if set(weights) != {d.name for d in tree.departments}:
            raise ValueError("profile department_weights must name every catalog department")
        p = self.profile
        return CustomizationDraft(
            display_name=self.display_name,
            summary=self.summary,
            base_preset=self.base_preset,
            profile=ProfileDraft(
                basket_lambda=p.basket_lambda,
                avg_ticket_target=p.avg_ticket_target,
                hourly_weights=p.hourly_weights,
                daily_weights=p.daily_weights,
                monthly_weights=p.monthly_weights,
                promo_rate=p.promo_rate,
                online_order_share=p.online_order_share,
                zones=p.zones,
            ),
            departments=[
                DepartmentDraft(
                    name=d.name,
                    weight=weights[d.name],
                    taxability=d.taxability,
                    categories=[CategoryDraft.model_validate(c.model_dump()) for c in d.categories],
                )
                for d in tree.departments
            ],
            brands=[BrandDraft(brand=b.Brand, department=b.Category) for b in tree.brands],
            inspiration_brands=self.inspiration_brands,
            palette=PaletteDraft.model_validate(self.palette.model_dump()),
        )


def slugify(name: str) -> str:
    """Return a lowercase ASCII slug for a business name (``Lowe's`` -> ``lowes``)."""

    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    ascii_name = re.sub(r"['’]", "", ascii_name.lower())
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name).strip("-")[:40].strip("-")
    if not slug:
        raise ValueError(f"business name {name!r} has no ASCII letters or digits")
    return slug


def validate_business_name(name: str) -> str:
    """Normalize and validate an operator-entered business name."""

    name = _text(name, field="business name", max_length=60)
    slugify(name)
    return name


def _normalize(value: str) -> str:
    # Drop apostrophes first so "Lowe's" and the common variant "Lowes" compare equal.
    value = re.sub(r"['’]", "", value.casefold())
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def _contains_phrase(text: str, phrase: str) -> bool:
    return f" {phrase} " in f" {text} "


def _fictionalization_problems(draft: CustomizationDraft, business_name: str) -> list[str]:
    names = [business_name, draft.display_name, *draft.inspiration_brands]
    denied = sorted({n for n in (_normalize(name) for name in names) if n})
    problems: list[str] = []

    def check(label: str, value: str) -> None:
        normalized = _normalize(value)
        for name in denied:
            # Short names ("A&P") are only rejected as exact matches; longer ones are
            # also rejected when they appear as a phrase inside a title.
            if normalized == name or (len(name) >= 4 and _contains_phrase(normalized, name)):
                problems.append(
                    f"{label} {value!r} uses the real name {name!r}; vary it so it is "
                    "clearly fictional"
                )
                return

    for brand in draft.brands:
        check("brand", brand.brand)
    for department in draft.departments:
        for category in department.categories:
            for sub in category.subcategories:
                for word in (*sub.nouns, *sub.modifiers):
                    check(f"{sub.name} product word", word)
    return problems


def _profile(draft: CustomizationDraft, store_type: str) -> StoreTypeProfile:
    p = draft.profile
    return StoreTypeProfile.model_validate(
        {
            "store_type": store_type,
            "display_name": draft.display_name,
            "basket_lambda": p.basket_lambda,
            "avg_ticket_target": p.avg_ticket_target,
            "hourly_weights": p.hourly_weights,
            "daily_weights": p.daily_weights,
            "monthly_weights": p.monthly_weights,
            "department_weights": {d.name: d.weight for d in draft.departments},
            "promo_rate": p.promo_rate,
            "online_order_share": p.online_order_share,
            "zones": list(dict.fromkeys(p.zones)),
        }
    )


def draft_problems(
    draft: CustomizationDraft, *, business_name: str, presets: list[str]
) -> list[str]:
    """Return every reason ``draft`` cannot become a spec (empty when acceptable)."""

    problems: list[str] = []

    def text(value: str, field: str, max_length: int = 60) -> None:
        try:
            _text(value, field=field, max_length=max_length)
        except ValueError as exc:
            problems.append(f"{exc} (got {value!r})")

    text(draft.display_name, "display_name")
    text(draft.summary, "summary", 300)
    if draft.base_preset not in presets:
        problems.append(f"base_preset must be one of {presets}, got {draft.base_preset!r}")

    departments = [d.name for d in draft.departments]
    if not MIN_DEPARTMENTS <= len(departments) <= MAX_DEPARTMENTS:
        problems.append(
            f"provide {MIN_DEPARTMENTS}-{MAX_DEPARTMENTS} departments, got {len(departments)}"
        )
    if len({d.casefold() for d in departments}) != len(departments):
        problems.append("department names must be unique")
    product_count = 0
    for department in draft.departments:
        text(department.name, "department name", 40)
        for category in department.categories:
            text(category.category, "category name", 40)
            for sub in category.subcategories:
                text(sub.name, "subcategory name", 40)
                if sub.price_max < sub.price_min:
                    problems.append(f"{sub.name}: price_max is below price_min")
                for word in (*sub.nouns, *sub.modifiers):
                    text(word, f"{sub.name} word", 40)
                product_count += len(set(sub.nouns)) * len(set(sub.modifiers))
    if not MIN_PRODUCTS <= product_count <= MAX_PRODUCTS:
        problems.append(
            f"nouns x modifiers must yield {MIN_PRODUCTS}-{MAX_PRODUCTS} products "
            f"in total, got {product_count}"
        )

    brand_names = [b.brand for b in draft.brands]
    if len({b.casefold() for b in brand_names}) != len(brand_names):
        problems.append("brand names must be unique")
    known = {d.casefold() for d in departments}
    covered = set()
    for brand in draft.brands:
        text(brand.brand, "brand", 40)
        if brand.department.casefold() not in known:
            problems.append(f"brand {brand.brand!r} names unknown department {brand.department!r}")
        covered.add(brand.department.casefold())
    uncovered = sorted(d for d in departments if d.casefold() not in covered)
    if uncovered:
        problems.append(f"every department needs at least one brand; missing: {uncovered}")

    for zone in draft.profile.zones:
        if not _ZONE.fullmatch(zone):
            problems.append(f"zone {zone!r} must be lowercase letters, digits, and hyphens")
    for name in draft.inspiration_brands:
        text(name, "inspiration brand", 60)

    try:
        _profile(draft, f"{CUSTOM_PREFIX}validation")
    except ValueError as exc:
        problems.append(f"profile: {exc}")

    problems.extend(_fictionalization_problems(draft, business_name))
    return problems


def build_spec(
    draft: CustomizationDraft,
    *,
    business_name: str,
    presets: list[str],
    generator: GeneratorInfo,
) -> CustomizationSpec:
    """Turn an accepted draft into the saved spec. Raises ValueError when invalid."""

    business_name = validate_business_name(business_name)
    problems = draft_problems(draft, business_name=business_name, presets=presets)
    if problems:
        raise ValueError("customization draft is invalid:\n- " + "\n- ".join(problems))
    slug = slugify(business_name)
    store_type = f"{CUSTOM_PREFIX}{slug}"
    # Map each brand to its department name so generation.dims matches brand
    # pools by department and never puts a produce brand on a pharmacy item.
    brands = [
        {"Brand": b.brand, "Company": f"{b.brand} Co.", "Category": _department(draft, b)}
        for b in draft.brands
    ]
    tree = {
        "store_type": store_type,
        "brand_styles": [b.brand for b in draft.brands],
        "brands": brands,
        "departments": [
            {
                "name": d.name,
                "taxability": d.taxability,
                "categories": [
                    {
                        "category": c.category,
                        "brand_share": c.brand_share,
                        "refrigerated": c.refrigerated,
                        "subcategories": [
                            {
                                "name": s.name,
                                "price_min": s.price_min,
                                "price_max": s.price_max,
                                "nouns": list(dict.fromkeys(s.nouns)),
                                "modifiers": list(dict.fromkeys(s.modifiers)),
                            }
                            for s in c.subcategories
                        ],
                    }
                    for c in d.categories
                ],
            }
            for d in draft.departments
        ],
    }
    return CustomizationSpec(
        business_name=business_name,
        display_name=_text(draft.display_name, field="display_name", max_length=60),
        summary=_text(draft.summary, field="summary", max_length=300),
        slug=slug,
        base_preset=draft.base_preset,
        profile=_profile(draft, store_type),
        catalog_tree=tree,
        inspiration_brands=sorted({" ".join(n.split()) for n in draft.inspiration_brands}),
        palette=Palette.model_validate(draft.palette.model_dump()),
        generator=generator,
    )


def _department(draft: CustomizationDraft, brand: BrandDraft) -> str:
    by_key = {d.name.casefold(): d.name for d in draft.departments}
    return by_key[brand.department.casefold()]
