"""Visit-first store traffic shared by receipt generation and footfall facts."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.runtime import date_grid, seeded_draws


def generate_hourly_visits(
    spark: SparkSession,
    stores: DataFrame,
    profile: StoreTypeProfile,
    cfg: GenerationConfig,
) -> DataFrame:
    draws = seeded_draws(cfg.seed)
    hours = stores.select(
        F.col("ID").alias("store_id"),
        "GeographyID",
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
    days = date_grid(spark, cfg.start_date, cfg.end_date).filter(
        ~((F.month("day") == 12) & (F.dayofmonth("day") == 25))
    )
    grid = hours.crossJoin(days)
    daily = F.element_at(
        F.array(*[F.lit(w * 7 / sum(profile.daily_weights)) for w in profile.daily_weights]),
        F.pmod(F.dayofweek("day") + 5, F.lit(7)) + 1,
    )
    monthly = F.element_at(
        F.array(*[F.lit(w * 12 / sum(profile.monthly_weights)) for w in profile.monthly_weights]),
        F.month("day"),
    )
    # Nearby stores share weather; store/hour conversion remains heterogeneous.
    weather = F.exp(0.15 * draws.gauss(["GeographyID", "day"], "weather") - 0.15**2 / 2)
    conversion = 1.0 / (
        1.0
        + F.exp(
            F.lit(1.2657)
            - 0.25 * draws.gauss(["store_id"], "conversion-store")
            - 0.20 * draws.gauss(["store_id", "day", "hour"], "conversion-hour")
        )
    )
    grid = grid.withColumn(
        "_visits_mean",
        F.lit(cfg.transactions_per_store_day / 0.225)
        * daily
        * monthly
        * weather
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
