"""Marketing impressions fact, Spark-native.

Ports datagen's marketing_campaign.py constants: four campaign archetypes,
each with its own channel mix and base impressions/day. Daily volumes are
scaled by `cfg.store_count / 86` — 86 is the legacy datagen fleet size, so a
fleet of 86 stores reproduces datagen's nominal volumes. `flash_sale` runs
only ~1 day in 7 (a per-day uniform gate, u < 1/7).

Per impression: channel uniform over the archetype's channels; device
MOBILE/DESKTOP/TABLET at 60/30/10 with cost multipliers 1.2/0.8/0.9; cost a
uniform draw within the channel's dollar band times the device multiplier;
a uniformly-assigned customer supplies `customer_ad_id` (dim_customers.AdId),
with `customer_id` populated for ~5% of impressions (CRM match) else NULL.
All draws via `runtime.seeded_draws`, so output is deterministic for a
(config, seed) pair regardless of cluster shape.

Schema note: fact_marketing is TMDL-bound with DUAL columns — PascalCase
`CustomerId`/`CostCents` (the TMDL sourceColumns) are exact mirrors of the
snake_case plan columns `customer_id`/`cost_cents`, and `__index_level_0__`
is the legacy pandas index (hash-derived via legacy_index).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.runtime import legacy_index, seeded_draws
from retail_setup.generation.schemas import column_names

# (archetype, channels, base impressions/day at the legacy 86-store fleet)
ARCHETYPES: list[tuple[str, list[str], int]] = [
    ("seasonal_sale", ["FACEBOOK", "GOOGLE", "EMAIL"], 1000),
    ("product_launch", ["INSTAGRAM", "YOUTUBE", "DISPLAY"], 2000),
    ("loyalty_program", ["EMAIL", "SOCIAL"], 500),
    ("flash_sale", ["SOCIAL", "SEARCH"], 5000),  # runs ~1 day in 7
]

# channel -> (lo, hi) cost-per-impression band in dollars (uniform draw)
CHANNEL_COSTS: dict[str, tuple[float, float]] = {
    "EMAIL": (0.005, 0.05),
    "DISPLAY": (0.10, 0.50),
    "SOCIAL": (0.08, 0.25),
    "SEARCH": (0.15, 0.75),
    "FACEBOOK": (0.10, 0.40),
    "GOOGLE": (0.25, 1.50),
    "INSTAGRAM": (0.08, 0.35),
    "YOUTUBE": (0.30, 1.50),
}

LEGACY_FLEET_SIZE = 86  # datagen's store count; volumes scale store_count/86

DEVICE_WEIGHTS = [("MOBILE", 0.6), ("DESKTOP", 0.3), ("TABLET", 0.1)]
DEVICE_MULTIPLIERS = {"MOBILE": 1.2, "DESKTOP": 0.8, "TABLET": 0.9}


def generate_marketing(
    spark: SparkSession,
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
) -> DataFrame:
    """Ad-impression facts: day x archetype grid exploded to impressions."""

    d = seeded_draws(cfg.seed)
    scale = cfg.store_count / LEGACY_FLEET_SIZE

    # --- day x archetype grid (driver-side; days x 4 rows)
    rows = [
        (idx + 1, name, channels, base)
        for idx, (name, channels, base) in enumerate(ARCHETYPES)
    ]
    grid = spark.createDataFrame(
        [
            (day_offset, idx, name, channels, base)
            for day_offset in range((cfg.end_date - cfg.start_date).days + 1)
            for (idx, name, channels, base) in rows
        ],
        "day_offset int, archetype_idx int, archetype string, "
        "channels array<string>, base_impressions int",
    ).withColumn(
        "day", F.date_add(F.lit(cfg.start_date), F.col("day_offset"))
    )

    # flash_sale runs ~1 day in 7 (uniform gate per day)
    grid = grid.filter(
        (F.col("archetype") != "flash_sale")
        | (d.u([F.col("day")], "mk_flash_gate") < F.lit(1.0 / 7.0))
    )

    # per-day impression count: clamped normal around the scaled base
    lam = F.col("base_impressions") * F.lit(scale)
    n_imps = F.greatest(
        F.lit(1),
        F.round(lam + d.gauss(["day", "archetype"], "mk_n") * F.sqrt(lam)),
    ).cast("int")
    grid = grid.withColumn("n_impressions", n_imps)

    # --- explode to impressions; build IDs
    imps = (
        grid
        .withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n_impressions"))))
        .withColumn(
            "campaign_id",
            F.concat(
                F.lit("CAMP"),
                F.date_format("day", "yyyyMMdd"),
                F.lpad(F.col("archetype_idx").cast("string"), 2, "0"),
            ),
        )
        .withColumn(
            "impression_id_ext",
            F.concat(
                F.lit("IMP"), F.col("campaign_id"),
                F.lpad(F.col("seq").cast("string"), 7, "0"),
            ),
        )
        # drops the 'IMP' prefix: CREAT + campaign_id + seq
        .withColumn(
            "creative_id",
            F.concat(F.lit("CREAT"), F.substring("impression_id_ext", 4, 30)),
        )
    )

    key = [F.col("impression_id_ext")]

    # --- channel uniform over the archetype's channels
    ch_idx = F.floor(d.u(key, "mk_channel") * F.size("channels")).cast("int")
    imps = imps.withColumn("channel", F.element_at("channels", ch_idx + 1))

    # --- device pick (60/30/10) and cost multiplier
    imps = imps.withColumn(
        "device", d.pick_by_weights(key, "mk_device", DEVICE_WEIGHTS)
    )
    mult = None
    for device, m in DEVICE_MULTIPLIERS.items():
        cond = F.col("device") == device
        mult = F.when(cond, m) if mult is None else mult.when(cond, m)
    imps = imps.withColumn("device_mult", mult)

    # --- cost: uniform within the channel band, times the device multiplier
    lo, hi = None, None
    for channel, (c_lo, c_hi) in CHANNEL_COSTS.items():
        cond = F.col("channel") == channel
        lo = F.when(cond, c_lo) if lo is None else lo.when(cond, c_lo)
        hi = F.when(cond, c_hi) if hi is None else hi.when(cond, c_hi)
    cost_dollars = (lo + d.u(key, "mk_cost") * (hi - lo)) * F.col("device_mult")
    imps = (
        imps
        .withColumn("cost_cents", F.round(cost_dollars * 100).cast("long"))
        .withColumn("cost", F.format_string("%.2f", cost_dollars))
    )

    # --- uniform customer per impression; ~5% CRM-matched (customer_id set)
    imps = imps.withColumn(
        "customer_pick",
        (d.h64(key, "mk_customer") % F.lit(cfg.customer_count) + 1).cast("long"),
    )
    customers = dims["dim_customers"].select(
        F.col("ID").alias("customer_pick"), F.col("AdId").alias("customer_ad_id")
    )
    imps = imps.join(customers, "customer_pick", "left")
    imps = imps.withColumn(
        "customer_id",
        F.when(
            d.u(key, "mk_crm") < F.lit(0.05),
            F.col("customer_pick").cast("double"),
        ),
    )

    # --- event_ts uniform within the day
    day_ts = F.unix_timestamp(F.col("day").cast("timestamp"))
    event_ts = F.timestamp_seconds(
        day_ts + F.floor(d.u(key, "mk_ts") * F.lit(86400))
    )

    out = imps.select(
        event_ts.alias("event_ts"),
        F.concat(F.lit("TRC-MKT-"), F.col("impression_id_ext")).alias("trace_id"),
        "channel",
        "campaign_id",
        "creative_id",
        "customer_ad_id",
        "customer_id",
        "cost_cents",
        # TMDL-bound PascalCase duals: exact mirrors of the snake_case columns
        F.col("customer_id").alias("CustomerId"),
        F.col("cost_cents").alias("CostCents"),
        "impression_id_ext",
        "cost",
        "device",
        F.col("day").alias("event_date"),
    ).withColumn(
        "__index_level_0__",
        legacy_index("impression_id_ext"),
    )
    return out.select(*column_names("fact_marketing"))
