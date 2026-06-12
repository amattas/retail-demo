"""Thin write layer. Notebooks call write_to_lakehouse; tests use write_table
with a format/location override (no delta-spark dependency locally)."""

from pyspark.sql import DataFrame


def write_table(df: DataFrame, table: str, location: str, fmt: str = "delta") -> None:
    df.write.format(fmt).mode("overwrite").save(location)


def write_to_lakehouse(df: DataFrame, lakehouse: str, schema: str, table: str) -> None:
    """Overwrite-by-design, matching 02-historical-data-load semantics."""
    df.write.format("delta").mode("overwrite").saveAsTable(f"{lakehouse}.{schema}.{table}")
