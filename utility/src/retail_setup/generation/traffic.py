"""Visit-first store traffic shared by receipt generation and footfall facts.

Visitors per store, local day and local hour follow the profile's hourly,
daily and monthly weights, the holiday calendar of the store type's class
(closed days have no hours), the geography's weather, the volume trend and the
store's traffic multiplier (``stream_calendar``, shared with the live stream).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.pricing import volume_growth_column
from retail_setup.generation.runtime import date_grid, seeded_draws
from retail_setup.generation.stream_calendar import (
    VISIT_CONVERSION_CALIBRATION,
    daily_factor_column,
    holiday_factor_column,
    monthly_factor_column,
    store_class,
    visit_conversion,
    weather_factor_column,
)


def generate_hourly_visits(
    spark: SparkSession,
    stores: DataFrame,
    profile: StoreTypeProfile,
    cfg: GenerationConfig,
    geographies: DataFrame | None = None,
) -> DataFrame:
    """``store_id, day, hour, hour_ts, visitors, n_receipts`` per open store-hour.

    ``geographies`` (``dim_geographies``) gives each store's state for its
    weather climate; without it every store has the temperate climate.
    """
    draws = seeded_draws(cfg.seed)
    if geographies is None:
        stores = stores.withColumn("State", F.lit(None).cast("string"))
    else:
        stores = stores.join(
            geographies.select(F.col("ID").alias("GeographyID"), "State"), "GeographyID", "left"
        )
    hours = stores.select(
        F.col("ID").alias("store_id"),
        "daily_traffic_multiplier",
        "operating_hours",
    )
    is_24 = F.col("operating_hours") == "24h"
    parts = F.split("operating_hours", "-")
    hours = hours.withColumn(
        "hour",
        F.explode(
            F.sequence(
                F.when(is_24, 0).otherwise(parts.getItem(0).cast("int")),
                F.when(is_24, 24).otherwise(parts.getItem(1).cast("int")) - 1,
            )
        ),
    ).withColumn(
        "_hour_weight",
        F.element_at(F.array(*[F.lit(w) for w in profile.hourly_weights]), F.col("hour") + 1),
    )
    hours = hours.withColumn(
        "_hour_share",
        F.col("_hour_weight") / F.sum("_hour_weight").over(Window.partitionBy("store_id")),
    )
    # Calendar and weather factors are evaluated once per day and store-day,
    # not per hour; closed days have no hours at all.
    days = (
        date_grid(spark, cfg.start_date, cfg.end_date)
        .withColumn("_holiday", holiday_factor_column(F.col("day"), store_class(profile)))
        .filter(F.col("_holiday") > 0)
        .withColumn(
            "_day_factor",
            F.col("_holiday")
            * daily_factor_column(profile, F.col("day"))
            * monthly_factor_column(profile, F.col("day"))
            * volume_growth_column(cfg.annual_volume_growth, cfg.anchor_date, F.col("day")),
        )
        .drop("_holiday")
    )
    # Nearby stores (one geography) share weather; store/hour conversion
    # remains heterogeneous.
    store_days = (
        stores.select(F.col("ID").alias("store_id"), "GeographyID", "State")
        .crossJoin(days)
        .withColumn(
            "_weather",
            weather_factor_column(draws, F.col("GeographyID"), F.col("State"), F.col("day")),
        )
        .select("store_id", "day", (F.col("_day_factor") * F.col("_weather")).alias("_factor"))
    )
    grid = hours.join(store_days, "store_id")
    conversion = visit_conversion(draws, F.col("store_id"), F.col("day"), F.col("hour"))
    grid = grid.withColumn(
        "_visits_mean",
        F.lit(cfg.transactions_per_store_day / VISIT_CONVERSION_CALIBRATION)
        * F.col("_factor")
        * F.col("daily_traffic_multiplier")
        * F.col("_hour_share"),
    ).withColumn("_conversion", conversion)
    grid = grid.withColumn(
        "visitors",
        draws.negative_binomial(["store_id", "day", "hour"], "visitors", F.col("_visits_mean"), 24),
    )
    visitor_ids = F.when(
        F.col("visitors") > 0, F.sequence(F.lit(1).cast("long"), F.col("visitors"))
    ).otherwise(F.array().cast("array<long>"))
    grid = grid.withColumn(
        "n_receipts",
        F.aggregate(
            visitor_ids,
            F.lit(0).cast("long"),
            lambda count, visitor: (
                count
                + (
                    draws.u(["store_id", "day", "hour", visitor], "visit-conversion")
                    < F.col("_conversion")
                ).cast("long")
            ),
        ),
    )
    return grid.withColumn(
        "hour_ts",
        F.timestamp_seconds(
            F.unix_timestamp(F.col("day").cast("timestamp")) + F.col("hour") * 3600
        ),
    ).select("store_id", "day", "hour", "hour_ts", "visitors", "n_receipts")
