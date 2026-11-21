"""Store time zones and the local-to-UTC publication convention.

Historical generation works in each store's local wall-clock time: opening
hours, hourly traffic curves and the inventory schedule are all local. Silver
stores every event timestamp as true UTC, the same convention the live stream
uses, and ``event_date`` is the UTC date of ``event_ts``.

Each store's IANA time zone (``dim_stores.TimeZone``) comes from its
geography's state. The generation stages convert their published outputs with
the helpers below:

- store events (receipts, payments, promotions, store ops, foot traffic, BLE,
  store inventory, reorders) use their store's zone
- receipt and promo lines use the store encoded in ``receipt_id_ext``
- a shipment's whole lifecycle (truck moves, truck loads and unloads, and the
  DC ledger rows that mirror the loads) uses the destination store's zone, so
  one shipment never mixes zones
- online orders and marketing impressions are not tied to store hours and are
  generated directly in UTC; they are not converted

``fact_receipts.local_hour`` keeps the store-local hour for hour-of-day
reporting.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Primary IANA zone per US state (plus DC and PR). States that span two zones
# use the zone of most of their population.
STATE_TIME_ZONES: dict[str, str] = {
    "AL": "America/Chicago",
    "AK": "America/Anchorage",
    "AZ": "America/Phoenix",
    "AR": "America/Chicago",
    "CA": "America/Los_Angeles",
    "CO": "America/Denver",
    "CT": "America/New_York",
    "DC": "America/New_York",
    "DE": "America/New_York",
    "FL": "America/New_York",
    "GA": "America/New_York",
    "HI": "Pacific/Honolulu",
    "IA": "America/Chicago",
    "ID": "America/Boise",
    "IL": "America/Chicago",
    "IN": "America/Indiana/Indianapolis",
    "KS": "America/Chicago",
    "KY": "America/New_York",
    "LA": "America/Chicago",
    "MA": "America/New_York",
    "MD": "America/New_York",
    "ME": "America/New_York",
    "MI": "America/Detroit",
    "MN": "America/Chicago",
    "MO": "America/Chicago",
    "MS": "America/Chicago",
    "MT": "America/Denver",
    "NC": "America/New_York",
    "ND": "America/Chicago",
    "NE": "America/Chicago",
    "NH": "America/New_York",
    "NJ": "America/New_York",
    "NM": "America/Denver",
    "NV": "America/Los_Angeles",
    "NY": "America/New_York",
    "OH": "America/New_York",
    "OK": "America/Chicago",
    "OR": "America/Los_Angeles",
    "PA": "America/New_York",
    "PR": "America/Puerto_Rico",
    "RI": "America/New_York",
    "SC": "America/New_York",
    "SD": "America/Chicago",
    "TN": "America/Chicago",
    "TX": "America/Chicago",
    "UT": "America/Denver",
    "VA": "America/New_York",
    "VT": "America/New_York",
    "WA": "America/Los_Angeles",
    "WI": "America/Chicago",
    "WV": "America/New_York",
    "WY": "America/Denver",
}

TIME_ZONE_COLUMN = "_tz"
_TZ_STORE = "_tz_store_id"


def state_time_zone(state: str) -> str:
    """IANA time zone of a US state code."""
    try:
        return STATE_TIME_ZONES[state.strip().upper()]
    except KeyError:
        raise ValueError(f"no time zone is defined for state {state!r}") from None


def receipt_store_id(receipt_id_ext: Column) -> Column:
    """Store ID encoded in a receipt ID (``RCP``/``RET`` + yyyyMMddHHmm + store4 + seq6)."""
    return F.substring(receipt_id_ext, 16, 4).cast("long")


def with_store_time_zone(
    df: DataFrame,
    dims: dict[str, DataFrame],
    store_id: Column,
    *,
    required: Column | None = None,
) -> DataFrame:
    """Add the store's time zone as ``_tz``.

    Rows matching ``required`` (default: every row) fail when their store has
    no time zone; other rows get a NULL zone.
    """
    zones = dims["dim_stores"].select(
        F.col("ID").cast("long").alias(_TZ_STORE), F.col("TimeZone").alias(TIME_ZONE_COLUMN)
    )
    joined = df.join(F.broadcast(zones), store_id == F.col(_TZ_STORE), "left").drop(_TZ_STORE)
    zone = F.col(TIME_ZONE_COLUMN)
    missing = F.raise_error(F.lit("event references a store without a time zone"))
    checked = F.when(zone.isNotNull(), zone)
    if required is not None:
        checked = checked.when(~required, F.lit(None).cast("string"))
    return joined.withColumn(TIME_ZONE_COLUMN, checked.otherwise(missing))


def local_to_utc(df: DataFrame, columns: list[str], *, convert: Column | None = None) -> DataFrame:
    """Convert local wall-clock ``columns`` to UTC using ``_tz``.

    ``event_date`` is re-derived from the converted ``event_ts``. ``convert``
    limits the conversion to matching rows (other rows are already UTC).
    """
    for name in columns:
        utc = F.to_utc_timestamp(F.col(name), F.col(TIME_ZONE_COLUMN))
        df = df.withColumn(
            name, utc if convert is None else F.when(convert, utc).otherwise(F.col(name))
        )
    if "event_ts" in columns and "event_date" in df.columns:
        df = df.withColumn("event_date", F.to_date("event_ts"))
    return df


def localize_by_store(
    df: DataFrame,
    dims: dict[str, DataFrame],
    columns: list[str],
    *,
    store_id: Column | None = None,
    convert: Column | None = None,
) -> DataFrame:
    """Convert a frame of store-local timestamps to UTC, keeping its columns."""
    original = df.columns
    store = F.col("store_id") if store_id is None else store_id
    zoned = with_store_time_zone(df, dims, store, required=convert)
    return local_to_utc(zoned, columns, convert=convert).select(*original)


def localize_receipt_lines(df: DataFrame, dims: dict[str, DataFrame]) -> DataFrame:
    """Convert lines keyed by ``receipt_id_ext`` (receipt or promo lines) to UTC."""
    return localize_by_store(
        df, dims, ["event_ts"], store_id=receipt_store_id(F.col("receipt_id_ext"))
    )


def local_hour(event_ts: Column, time_zone: Column) -> Column:
    """Store-local hour (0-23) of a UTC timestamp."""
    return F.hour(F.from_utc_timestamp(event_ts, time_zone)).cast("int")


def with_local_hour(df: DataFrame, dims: dict[str, DataFrame]) -> DataFrame:
    """Set ``local_hour`` of a UTC store frame from its store's time zone."""
    original = df.columns
    zoned = with_store_time_zone(df, dims, F.col("store_id"))
    return zoned.withColumn(
        "local_hour", local_hour(F.col("event_ts"), F.col(TIME_ZONE_COLUMN))
    ).select(*original)
