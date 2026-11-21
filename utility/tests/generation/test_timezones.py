"""Store time zones: local generation, UTC storage, local reporting hour."""

from __future__ import annotations

import json
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.dictionaries.loader import default_dictionary_root
from retail_setup.generation.timezones import (
    STATE_TIME_ZONES,
    localize_by_store,
    receipt_store_id,
    state_time_zone,
    with_local_hour,
)


def test_every_dictionary_state_has_a_time_zone() -> None:
    geographies = json.loads(
        (default_dictionary_root() / "_shared" / "geographies.json").read_text(encoding="utf-8")
    )
    states = {row["State"] for row in geographies}
    assert states <= set(STATE_TIME_ZONES)
    assert state_time_zone(" ca ") == "America/Los_Angeles"
    assert state_time_zone("AZ") == "America/Phoenix"
    with pytest.raises(ValueError, match="ZZ"):
        state_time_zone("ZZ")


# Local wall-clock hours on the days around both 2025 US DST changes
# (spring forward Sunday 9 March, fall back Sunday 2 November).
DAYS = [date(2025, 3, 8), date(2025, 3, 9), date(2025, 3, 10)] + [
    date(2025, 11, 1),
    date(2025, 11, 2),
    date(2025, 11, 3),
]
HOURS = [9, 13, 17, 21]
_FMT = "yyyy-MM-dd HH:mm:ss"
STORES = {1: "America/Los_Angeles", 2: "America/New_York"}


@pytest.fixture(scope="module")
def localized(spark):
    stores = spark.createDataFrame(list(STORES.items()), "ID long, TimeZone string")
    dims = {"dim_stores": stores}
    rows = [
        (store, f"{day.isoformat()} {hour:02d}:30:00", day)
        for store in STORES
        for day in DAYS
        for hour in HOURS
    ]
    local = (
        spark.createDataFrame(rows, "store_id long, event_ts string, event_date date")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("local_hour", F.lit(None).cast("int"))
        .withColumn("_local", F.date_format("event_ts", _FMT))
    )
    utc = with_local_hour(localize_by_store(local, dims, ["event_ts"]), dims).select(
        "store_id",
        "_local",
        "local_hour",
        "event_date",
        F.date_format("event_ts", _FMT).alias("utc"),
        F.unix_timestamp("event_ts").alias("epoch"),
    )
    # Timestamps are compared as UTC strings: collected datetimes would be
    # shifted into the test machine's local zone.
    return {(r.store_id, r._local): r for r in utc.collect()}


def test_stores_share_local_hours_but_not_utc_hours(localized) -> None:
    for day in DAYS:
        for hour in HOURS:
            local_ts = f"{day.isoformat()} {hour:02d}:30:00"
            west = localized[(1, local_ts)]
            east = localized[(2, local_ts)]
            assert west.local_hour == east.local_hour == hour
            assert west.epoch - east.epoch == 3 * 3600


def test_utc_offsets_follow_daylight_saving(localized) -> None:
    east = {ts: row.utc for (store, ts), row in localized.items() if store == 2}
    # 09:30 New York is 14:30 UTC in winter time and 13:30 UTC in summer time.
    assert east["2025-03-08 09:30:00"] == "2025-03-08 14:30:00"
    assert east["2025-03-10 09:30:00"] == "2025-03-10 13:30:00"
    assert east["2025-11-01 09:30:00"] == "2025-11-01 13:30:00"
    assert east["2025-11-03 09:30:00"] == "2025-11-03 14:30:00"


def test_event_date_is_the_utc_date(localized) -> None:
    evening = localized[(1, "2025-03-08 21:30:00")]
    assert evening.utc == "2025-03-09 05:30:00"
    assert evening.event_date == date(2025, 3, 9)


def test_receipt_ids_carry_their_store(spark) -> None:
    rows = [("RCP2025010209150012000034",), ("RET2025010312000007000001",)]
    parsed = (
        spark.createDataFrame(rows, "receipt_id_ext string")
        .select(receipt_store_id(F.col("receipt_id_ext")).alias("store_id"))
        .collect()
    )
    assert [row.store_id for row in parsed] == [12, 7]


def test_unknown_store_fails_instead_of_dropping_the_zone(spark) -> None:
    dims = {"dim_stores": spark.createDataFrame([(1, "UTC")], "ID long, TimeZone string")}
    frame = spark.createDataFrame([(9, "2025-01-01 12:00:00")], "store_id long, event_ts string")
    frame = frame.withColumn("event_ts", F.to_timestamp("event_ts"))
    with pytest.raises(Exception, match="time zone"):
        localize_by_store(frame, dims, ["event_ts"]).collect()
