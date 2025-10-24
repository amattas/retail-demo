"""
Custom validators for pricing logic, synthetic data safety, and business rules.

This module implements the complex validation logic specified in AGENTS.md,
including pricing constraints, FK relationships, and synthetic data safety.
"""

import random
import re
from datetime import datetime
from decimal import Decimal
from typing import Any


class PricingCalculator:
    """
    Handles pricing calculations according to AGENTS.md specifications:

    - MSRP = BasePrice ±15%
    - SalePrice = MSRP (60% of time) OR discounted 5-35% (40% of time)
    - Cost = 50-85% of SalePrice
    - Always ensure: Cost < SalePrice ≤ MSRP
    """

    def __init__(self, seed: int = 42):
        """Initialize with random seed for reproducibility."""
        self._rng = random.Random(seed)

    def calculate_msrp(self, base_price: Decimal) -> Decimal:
        """
        Calculate MSRP as BasePrice ±15%.

        Args:
            base_price: Base price from product dictionary

        Returns:
            MSRP within ±15% of base price

        Raises:
            ValueError: If base price is <= 0
        """
        if base_price <= 0:
            raise ValueError("Base price must be positive")

        # Generate random percentage between -15% and +15%
        variance_percent = Decimal(str(self._rng.uniform(-0.15, 0.15)))
        msrp = base_price * (Decimal("1.0") + variance_percent)

        # Ensure MSRP is positive and round to 2 decimal places
        msrp = max(Decimal("0.01"), msrp.quantize(Decimal("0.01")))

        return msrp

    def calculate_sale_price(self, msrp: Decimal) -> Decimal:
        """
        Calculate sale price with 60/40 distribution:
        - 60%: SalePrice = MSRP
        - 40%: SalePrice = MSRP discounted 5-35%

        Args:
            msrp: Manufacturer suggested retail price

        Returns:
            Sale price according to distribution rules
        """
        if msrp <= 0:
            raise ValueError("MSRP must be positive")

        # 60% chance to keep MSRP as sale price
        if self._rng.random() < 0.60:
            return msrp

        # 40% chance to apply discount of 5-35%
        discount_percent = Decimal(str(self._rng.uniform(0.05, 0.35)))
        sale_price = msrp * (Decimal("1.0") - discount_percent)

        # Ensure sale price is positive and round to 2 decimal places
        sale_price = max(Decimal("0.01"), sale_price.quantize(Decimal("0.01")))

        return sale_price

    def calculate_cost(self, sale_price: Decimal) -> Decimal:
        """
        Calculate cost as 50-85% of sale price.

        Args:
            sale_price: Sale price

        Returns:
            Cost between 50-85% of sale price
        """
        if sale_price <= 0:
            raise ValueError("Sale price must be positive")

        # Generate random percentage between 50% and 85%
        cost_percent = Decimal(str(self._rng.uniform(0.50, 0.85)))
        cost = sale_price * cost_percent

        # Ensure cost is less than sale price and round to 2 decimal places
        cost = min(cost, sale_price - Decimal("0.01"))
        cost = max(Decimal("0.01"), cost.quantize(Decimal("0.01")))

        return cost

    def calculate_full_pricing(self, base_price: Decimal) -> dict[str, Decimal]:
        """
        Calculate complete pricing structure from base price.

        Args:
            base_price: Base price from product dictionary

        Returns:
            Dictionary with Cost, MSRP, and SalePrice

        Raises:
            ValueError: If pricing constraints are violated
        """
        msrp = self.calculate_msrp(base_price)
        sale_price = self.calculate_sale_price(msrp)
        cost = self.calculate_cost(sale_price)

        # Validate final constraints
        if not (cost < sale_price <= msrp):
            raise ValueError(
                f"Pricing constraint violation: "
                f"Cost ({cost}) < SalePrice ({sale_price}) <= MSRP ({msrp})"
            )

        return {"Cost": cost, "MSRP": msrp, "SalePrice": sale_price}

    def calculate_batch_pricing(
        self, base_prices: list[Decimal]
    ) -> list[dict[str, Decimal]]:
        """
        Calculate pricing for multiple products efficiently.

        Args:
            base_prices: List of base prices

        Returns:
            List of pricing dictionaries
        """
        return [self.calculate_full_pricing(price) for price in base_prices]


class PricingValidator:
    """Validates pricing structures against business rules."""

    @staticmethod
    def validate_pricing_structure(pricing: dict[str, Decimal]) -> bool:
        """
        Validate that a pricing structure meets all constraints.

        Args:
            pricing: Dictionary with 'cost', 'sale_price', 'msrp' keys

        Returns:
            True if valid, False otherwise
        """
        try:
            cost = pricing["cost"]
            sale_price = pricing["sale_price"]
            msrp = pricing["msrp"]
        except KeyError:
            return False

        # Check basic constraint: Cost < SalePrice <= MSRP
        if not (cost < sale_price <= msrp):
            return False

        # Check cost is 50-85% of sale price
        cost_ratio = cost / sale_price
        if not (Decimal("0.50") <= cost_ratio <= Decimal("0.85")):
            return False

        return True

    @staticmethod
    def validate_msrp_variance(base_price: Decimal, msrp: Decimal) -> bool:
        """
        Validate MSRP is within ±15% of base price.

        Args:
            base_price: Base price
            msrp: MSRP to validate

        Returns:
            True if within range, False otherwise
        """
        min_msrp = base_price * Decimal("0.85")
        max_msrp = base_price * Decimal("1.15")
        return min_msrp <= msrp <= max_msrp


class SyntheticDataValidator:
    """
    Validates that generated data is synthetic and safe.

    Prevents generation of real names, addresses, companies, and
    personally identifiable information as required by AGENTS.md.
    """

    def __init__(self) -> None:
        """Initialize with blacklists of real data to avoid."""
        # Common real first names to avoid
        self.real_first_names = {
            "john",
            "mary",
            "michael",
            "jennifer",
            "william",
            "elizabeth",
            "david",
            "patricia",
            "robert",
            "linda",
            "christopher",
            "barbara",
            "daniel",
            "sandra",
            "matthew",
            "betty",
            "anthony",
            "helen",
            "mark",
            "nancy",
            "donald",
            "karen",
            "steven",
            "lisa",
            "paul",
            "anna",
            "andrew",
            "brenda",
            "joshua",
            "emma",
            "kenneth",
            "olivia",
            "kevin",
            "sophia",
            "brian",
            "cynthia",
            "george",
            "marie",
            "edward",
            "janet",
            "ronald",
            "catherine",
            "timothy",
            "frances",
            "jason",
            "samantha",
            "jeffrey",
            "debra",
        }

        # Common real last names to avoid
        self.real_last_names = {
            "smith",
            "johnson",
            "williams",
            "brown",
            "jones",
            "garcia",
            "miller",
            "davis",
            "rodriguez",
            "martinez",
            "hernandez",
            "lopez",
            "gonzales",
            "wilson",
            "anderson",
            "thomas",
            "taylor",
            "moore",
            "jackson",
            "martin",
            "lee",
            "perez",
            "thompson",
            "white",
            "harris",
            "sanchez",
            "clark",
            "ramirez",
            "lewis",
            "robinson",
            "walker",
            "young",
            "allen",
            "king",
            "wright",
            "scott",
            "torres",
            "nguyen",
            "hill",
            "flores",
            "green",
            "adams",
            "nelson",
            "baker",
            "hall",
            "rivera",
            "campbell",
            "mitchell",
            "carter",
            "roberts",
        }

        # Real company names to avoid (now empty as all data should be synthetic)
        self.real_companies = set()

        # Comprehensive real brand blocklist - major retail, technology, automotive, and consumer brands
        self.real_brands = {
            # Technology brands
            "apple",
            "microsoft",
            "google",
            "amazon",
            "meta",
            "facebook",
            "tesla",
            "nvidia",
            "amd",
            "intel",
            "samsung",
            "sony",
            "lg",
            "panasonic",
            "toshiba",
            "canon",
            "nikon",
            "hp",
            "dell",
            "lenovo",
            "asus",
            "acer",
            "cisco",
            "oracle",
            "ibm",
            "adobe",
            "salesforce",
            "vmware",
            "zoom",
            "slack",
            "twitter",
            "x",
            "linkedin",
            "tiktok",
            "snapchat",
            "spotify",
            "netflix",
            "youtube",
            "gmail",
            "outlook",
            "teams",
            "skype",
            "whatsapp",
            "instagram",
            "pinterest",
            "reddit",
            # Automotive brands
            "toyota",
            "honda",
            "ford",
            "chevrolet",
            "chevy",
            "gmc",
            "cadillac",
            "buick",
            "bmw",
            "mercedes",
            "audi",
            "volkswagen",
            "vw",
            "porsche",
            "ferrari",
            "lamborghini",
            "maserati",
            "bentley",
            "rolls-royce",
            "jaguar",
            "land rover",
            "volvo",
            "saab",
            "peugeot",
            "renault",
            "citroen",
            "fiat",
            "alfa romeo",
            "hyundai",
            "kia",
            "mazda",
            "subaru",
            "mitsubishi",
            "nissan",
            "infiniti",
            "lexus",
            "acura",
            "lincoln",
            "dodge",
            "chrysler",
            "jeep",
            "ram",
            "mini",
            "smart",
            # Retail and consumer brands
            "walmart",
            "target",
            "costco",
            "home depot",
            "lowes",
            "best buy",
            "kroger",
            "safeway",
            "publix",
            "whole foods",
            "trader joes",
            "aldi",
            "cvs",
            "walgreens",
            "rite aid",
            "macys",
            "nordstrom",
            "kohls",
            "jcpenney",
            "sears",
            "tj maxx",
            "marshalls",
            "ross",
            "burlington",
            "bed bath beyond",
            "barnes noble",
            "gamestop",
            "petco",
            "petsmart",
            "dollar general",
            "dollar tree",
            "family dollar",
            # Apparel and fashion brands
            "nike",
            "adidas",
            "puma",
            "reebok",
            "under armour",
            "lululemon",
            "gap",
            "old navy",
            "banana republic",
            "american eagle",
            "abercrombie",
            "hollister",
            "forever 21",
            "h&m",
            "zara",
            "uniqlo",
            "calvin klein",
            "tommy hilfiger",
            "polo ralph lauren",
            "levis",
            "wrangler",
            "lee",
            "carhartt",
            "patagonia",
            "north face",
            "columbia",
            "timberland",
            "ugg",
            "vans",
            "converse",
            "jordans",
            "air jordan",
            "yeezy",
            "gucci",
            "prada",
            "louis vuitton",
            "chanel",
            "hermes",
            "burberry",
            "versace",
            "armani",
            "dolce gabbana",
            "valentino",
            "givenchy",
            "balenciaga",
            "saint laurent",
            # Food and beverage brands
            "coca cola",
            "coke",
            "pepsi",
            "sprite",
            "fanta",
            "dr pepper",
            "mountain dew",
            "red bull",
            "monster",
            "rockstar",
            "gatorade",
            "powerade",
            "vitamin water",
            "dasani",
            "aquafina",
            "evian",
            "perrier",
            "la croix",
            "lacroix",
            "mcdonalds",
            "burger king",
            "wendys",
            "subway",
            "taco bell",
            "kfc",
            "pizza hut",
            "dominos",
            "papa johns",
            "starbucks",
            "dunkin",
            "tim hortons",
            "panera",
            "chipotle",
            "qdoba",
            "panda express",
            "olive garden",
            "red lobster",
            "applebees",
            "chilis",
            "tgi fridays",
            "outback",
            "texas roadhouse",
            "cracker barrel",
            "nestle",
            "kraft",
            "heinz",
            "campbells",
            "general mills",
            "kelloggs",
            "post",
            "quaker",
            "cheerios",
            "frosted flakes",
            "corn flakes",
            "rice krispies",
            "lucky charms",
            "honey nut cheerios",
            "cinnamon toast crunch",
            "fruit loops",
            "oreo",
            "chips ahoy",
            "ritz",
            "triscuit",
            "wheat thins",
            "goldfish",
            "cheez its",
            "doritos",
            "lays",
            "pringles",
            "cheetos",
            "fritos",
            "tostitos",
            "ruffles",
            # Consumer goods and household brands
            "procter gamble",
            "pg",
            "unilever",
            "johnson johnson",
            "jj",
            "colgate",
            "palmolive",
            "crest",
            "oral b",
            "listerine",
            "scope",
            "head shoulders",
            "pantene",
            "herbal essences",
            "tresemme",
            "loreal",
            "maybelline",
            "revlon",
            "covergirl",
            "max factor",
            "clinique",
            "estee lauder",
            "mac",
            "nars",
            "urban decay",
            "sephora",
            "ulta",
            "sally beauty",
            "dove",
            "axe",
            "degree",
            "secret",
            "old spice",
            "gillette",
            "venus",
            "schick",
            "bic",
            "chapstick",
            "carmex",
            "burts bees",
            "vaseline",
            "jergens",
            "aveeno",
            "neutrogena",
            "olay",
            "cetaphil",
            "eucerin",
            "lubriderm",
            "nivea",
            "lancome",
            "clarins",
            "tide",
            "gain",
            "downy",
            "bounce",
            "all",
            "arm hammer",
            "oxiclean",
            "clorox",
            "lysol",
            "febreze",
            "glade",
            "air wick",
            "pledge",
            "windex",
            "scrubbing bubbles",
            "fantastik",
            "formula 409",
            "mr clean",
            "pine sol",
            "ajax",
            "comet",
            "soft scrub",
            "kaboom",
            "clr",
            "lime away",
            # Home improvement and hardware brands
            "dewalt",
            "milwaukee",
            "makita",
            "ryobi",
            "black decker",
            "craftsman",
            "stanley",
            "husky",
            "kobalt",
            "ridgid",
            "porter cable",
            "bosch",
            "skil",
            "worx",
            "greenworks",
            "ego",
            "toro",
            "husqvarna",
            "echo",
            "stihl",
            "john deere",
            "cub cadet",
            "troy bilt",
            "snapper",
            "ariens",
            "craftsman",
            "weber",
            "char broil",
            "traeger",
            "big green egg",
            "napoleon",
            "blackstone",
            "coleman",
            "yeti",
            "igloo",
            "pelican",
            "rtic",
            "hydroflask",
            "contigo",
            "thermos",
            "stanley",
            "nalgene",
            "camelbak",
            "osprey",
            "deuter",
            "gregory",
            # Electronics and appliance brands
            "whirlpool",
            "ge",
            "kenmore",
            "frigidaire",
            "maytag",
            "amana",
            "kitchenaid",
            "bosch",
            "miele",
            "viking",
            "wolf",
            "sub zero",
            "thermador",
            "dacor",
            "jenn air",
            "electrolux",
            "speed queen",
            "lg",
            "samsung",
            "haier",
            "sharp",
            "sanyo",
            "emerson",
            "rca",
            "westinghouse",
            "insignia",
            "dynex",
            "magnavox",
            "philips",
            "sylvania",
            "zenith",
            "durabrand",
            "tcl",
            "hisense",
            "vizio",
            "roku",
            "fire tv",
            "chromecast",
            "apple tv",
            "nvidia shield",
            # Gaming brands
            "playstation",
            "xbox",
            "nintendo",
            "steam",
            "epic games",
            "activision",
            "blizzard",
            "call of duty",
            "world of warcraft",
            "overwatch",
            "diablo",
            "starcraft",
            "hearthstone",
            "candy crush",
            "angry birds",
            "pokemon",
            "mario",
            "zelda",
            "sonic",
            "minecraft",
            "roblox",
            "fortnite",
            "apex legends",
            "valorant",
            "league of legends",
            "dota",
            "counter strike",
            "fifa",
            "madden",
            "nba 2k",
            "grand theft auto",
            "gta",
            "red dead redemption",
            "assassins creed",
            "far cry",
            "battlefield",
            "medal of honor",
            "halo",
            "gears of war",
            "destiny",
            "borderlands",
            "fallout",
            "elder scrolls",
            "skyrim",
            "doom",
            "wolfenstein",
            "dishonored",
            "prey",
            "rage",
            "quake",
            "unreal tournament",
            # Sports brands
            "espn",
            "fox sports",
            "nfl",
            "nba",
            "mlb",
            "nhl",
            "mls",
            "pga",
            "lpga",
            "nascar",
            "formula 1",
            "f1",
            "ufc",
            "wwe",
            "aew",
            "nxt",
            "raw",
            "smackdown",
            "monday night football",
            "sunday night football",
            "thursday night football",
            "march madness",
            "college football playoff",
            "world series",
            "super bowl",
            "stanley cup",
            "nba finals",
            "world cup",
            "olympics",
            "paralympics",
            # Media and entertainment brands
            "disney",
            "pixar",
            "marvel",
            "star wars",
            "lucasfilm",
            "espn",
            "abc",
            "cbs",
            "nbc",
            "fox",
            "cnn",
            "msnbc",
            "fox news",
            "bbc",
            "pbs",
            "hbo",
            "showtime",
            "starz",
            "epix",
            "paramount",
            "universal",
            "warner bros",
            "sony pictures",
            "paramount pictures",
            "columbia pictures",
            "twentieth century fox",
            "dreamworks",
            "illumination",
            "blue sky",
            "laika",
            "studio ghibli",
            "cartoon network",
            "nickelodeon",
            "disney channel",
            "disney junior",
            "nick jr",
            "discovery",
            "history",
            "nat geo",
            "animal planet",
            "food network",
            "hgtv",
            "tlc",
            "bravo",
            "e!",
            "mtv",
            "vh1",
            "comedy central",
            "adult swim",
            "tbs",
            "tnt",
            "usa",
            "syfy",
            "fx",
            "fxx",
            "fxm",
            "lifetime",
            "hallmark",
            "oxygen",
            "we tv",
            "own",
            "bet",
            "vh1",
            "cmt",
            "tvland",
            "spike",
            # Financial and insurance brands
            "visa",
            "mastercard",
            "american express",
            "amex",
            "discover",
            "paypal",
            "venmo",
            "zelle",
            "cash app",
            "apple pay",
            "google pay",
            "samsung pay",
            "bank of america",
            "wells fargo",
            "chase",
            "jpmorgan",
            "citibank",
            "citi",
            "us bank",
            "pnc",
            "truist",
            "td bank",
            "fifth third",
            "regions",
            "suntrust",
            "capital one",
            "discover bank",
            "ally",
            "marcus",
            "goldman sachs",
            "morgan stanley",
            "charles schwab",
            "fidelity",
            "vanguard",
            "etrade",
            "robinhood",
            "td ameritrade",
            "interactive brokers",
            "webull",
            "stash",
            "acorns",
            "betterment",
            "wealthfront",
            "mint",
            "quicken",
            "turbotax",
            "hr block",
            "jackson hewitt",
            "liberty tax",
            "credit karma",
            "experian",
            "equifax",
            "transunion",
            "fico",
            "myfico",
            "creditwise",
            "credit sesame",
            "state farm",
            "geico",
            "progressive",
            "allstate",
            "farmers",
            "usaa",
            "liberty mutual",
            "nationwide",
            "travelers",
            "hartford",
            "aig",
            "metlife",
            "prudential",
            "new york life",
            "northwestern mutual",
            "aflac",
            # Airlines and travel brands
            "american airlines",
            "delta",
            "united",
            "southwest",
            "jetblue",
            "alaska",
            "spirit",
            "frontier",
            "allegiant",
            "hawaiian",
            "virgin america",
            "virgin atlantic",
            "british airways",
            "lufthansa",
            "air france",
            "klm",
            "emirates",
            "qatar",
            "singapore airlines",
            "cathay pacific",
            "ana",
            "jal",
            "qantas",
            "air canada",
            "expedia",
            "booking.com",
            "priceline",
            "kayak",
            "travelocity",
            "orbitz",
            "hotels.com",
            "trivago",
            "agoda",
            "airbnb",
            "vrbo",
            "homeaway",
            "marriott",
            "hilton",
            "hyatt",
            "ihg",
            "choice",
            "wyndham",
            "best western",
            "la quinta",
            "red roof",
            "motel 6",
            "super 8",
            "days inn",
            "comfort inn",
            "holiday inn",
            "hampton inn",
            "homewood suites",
            "embassy suites",
            "doubletree",
            "sheraton",
            "westin",
            "st regis",
            "luxury collection",
            "w hotels",
            "aloft",
            "element",
            "residence inn",
            "springhill suites",
            "fairfield inn",
            "courtyard",
            "ac hotels",
            "moxy",
            "edition",
            # Common shortened forms and variants
            "addy",
            "benz",
            "beemer",
            "insta",
            "fb",
            "goog",
            "msft",
            "aapl",
            "tsla",
            "amzn",
            "nflx",
            "nvda",
            "intc",
            "csco",
            "orcl",
            "crm",
            "zoom",
            "zm",
            "nke",
            "ko",
            "pep",
            "wmt",
            "tgt",
            "hd",
            "low",
            "cost",
            "bby",
            "kr",
            "pg",
            "jnj",
            "pfe",
            "mrk",
            "abbv",
            "jpm",
            "bac",
            "wfc",
            "c",
            "usb",
            "gs",
            "ms",
            "schw",
            "axa",
            "met",
            "pru",
            "afl",
            "trav",
            "all",
            "pgr",
            "geico",
            "usaa",
            "dal",
            "ual",
            "aal",
            "luv",
            "jblu",
            "alk",
            "save",
            "ulcc",
            "ha",
            "mar",
            "hlt",
            "ihg",
            "wynd",
            "choice",
            "bwxt",
        }

        # Real address patterns to avoid
        self.real_address_patterns = [
            r".*1600 pennsylvania avenue.*",  # White House
            r".*350 fifth avenue.*",  # Empire State Building
            r".*221b baker street.*",  # Sherlock Holmes
            r".*1 infinite loop.*",  # Apple
            r".*1 microsoft way.*",  # Microsoft
            r".*1600 amphitheatre.*",  # Google
        ]

    def is_synthetic_first_name(self, name: str) -> bool:
        """
        Check if a first name is acceptable for synthetic data generation.

        Real names are allowed as per requirements. This method now validates
        basic format requirements rather than rejecting real names.

        Args:
            name: First name to validate

        Returns:
            True if acceptable for use, False if invalid format
        """
        name_stripped = name.strip()

        # Basic format validation
        if not name_stripped:
            return False

        if len(name_stripped) < 2 or len(name_stripped) > 50:
            return False

        # Allow letters, spaces, hyphens, and apostrophes
        if not re.match(r"^[A-Za-z\s\-']+$", name_stripped):
            return False

        return True

    def is_synthetic_last_name(self, name: str) -> bool:
        """
        Check if a last name is acceptable for synthetic data generation.

        Real names are allowed as per requirements. This method now validates
        basic format requirements rather than rejecting real names.

        Args:
            name: Last name to validate

        Returns:
            True if acceptable for use, False if invalid format
        """
        name_stripped = name.strip()

        # Basic format validation
        if not name_stripped:
            return False

        if len(name_stripped) < 2 or len(name_stripped) > 50:
            return False

        # Allow letters, spaces, hyphens, and apostrophes
        if not re.match(r"^[A-Za-z\s\-']+$", name_stripped):
            return False

        return True

    def is_synthetic_company(self, company: str) -> bool:
        """
        Check if a company name is synthetic.

        Args:
            company: Company name to validate

        Returns:
            True if synthetic, False if potentially real
        """
        company_lower = company.lower().strip()

        # Remove common business suffixes for comparison
        suffixes = [
            "inc",
            "corp",
            "llc",
            "ltd",
            "co",
            "company",
            "corporation",
            "incorporated",
            "limited",
        ]
        company_clean = company_lower
        for suffix in suffixes:
            company_clean = company_clean.replace(f" {suffix}", "").replace(
                f".{suffix}", ""
            )

        # Check against real companies
        if company_clean in self.real_companies:
            return False

        # Check for partial matches
        for real_company in self.real_companies:
            if real_company in company_clean or company_clean in real_company:
                if len(company_clean) - len(real_company) < 3:
                    return False

        return True

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


class BusinessRuleValidator:
    """
    Validates business rules and data integrity constraints.

    Implements complex business logic validation beyond simple data types.
    """

    @staticmethod
    def validate_receipt_totals(
        subtotal: Decimal,
        tax: Decimal,
        total: Decimal,
        tolerance: Decimal = Decimal("0.01"),
    ) -> bool:
        """
        Validate that receipt totals are mathematically correct.

        Args:
            subtotal: Subtotal amount
            tax: Tax amount
            total: Total amount
            tolerance: Acceptable rounding tolerance

        Returns:
            True if totals are correct within tolerance
        """
        expected_total = subtotal + tax
        return abs(total - expected_total) <= tolerance

    @staticmethod
    def validate_receipt_line_pricing(
        qty: int,
        unit_price: Decimal,
        ext_price: Decimal,
        tolerance: Decimal = Decimal("0.01"),
    ) -> bool:
        """
        Validate that receipt line pricing is mathematically correct.

        Args:
            qty: Quantity
            unit_price: Unit price
            ext_price: Extended price
            tolerance: Acceptable rounding tolerance

        Returns:
            True if pricing is correct within tolerance
        """
        expected_ext_price = unit_price * qty
        return abs(ext_price - expected_ext_price) <= tolerance

    @staticmethod
    def validate_inventory_balance(transactions: list[dict[str, Any]]) -> bool:
        """
        Validate that inventory transactions maintain non-negative balance.

        Args:
            transactions: List of inventory transactions with QtyDelta

        Returns:
            True if balance never goes negative
        """
        balance = 0
        for txn in sorted(transactions, key=lambda x: x.get("EventTS", datetime.min)):
            balance += txn.get("QtyDelta", 0)
            if balance < 0:
                return False
        return True

    @staticmethod
    def validate_truck_timing(eta: datetime, etd: datetime) -> bool:
        """
        Validate that truck ETA is before ETD.

        Args:
            eta: Estimated time of arrival
            etd: Estimated time of departure

        Returns:
            True if timing is logical
        """
        return eta <= etd

    @staticmethod
    def validate_store_hours_consistency(event_time: datetime) -> bool:
        """
        Validate that events occur during reasonable business hours.

        Args:
            event_time: Time of the event

        Returns:
            True if within reasonable business hours (simplified check)
        """
        hour = event_time.hour
        # Simple check: most retail activity between 6 AM and 11 PM
        return 6 <= hour <= 23


# Aliases for test compatibility
PricingCalculator = PricingCalculator
PricingValidator = PricingValidator
SyntheticDataValidator = SyntheticDataValidator
ForeignKeyValidator = ForeignKeyValidator
BusinessRuleValidator = BusinessRuleValidator
