"""Gold aggregates for setup-11, built from the shared ``gold_rules`` module.

Setup-11 (``ml-required``) and notebook 04 (``streaming-data-load``) publish
the same ten Gold tables. Both use ``gold_rules`` (notebook 04 embeds its
source; ``scripts/build_notebooks.py --check`` enforces that), so the tables
don't change depending on which pipeline ran last. This module only adds the
contract column order from ``schemas.TABLES``.
"""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from retail_setup.generation.gold_rules import GOLD_TABLES, build_gold_tables
from retail_setup.generation.schemas import column_names

__all__ = ["GOLD_TABLES", "generate_gold"]


def generate_gold(
    spark: SparkSession, tables: dict[str, DataFrame], cutoff: datetime
) -> dict[str, DataFrame]:
    """Build the 10 Gold aggregate frames from the Silver fact tables.

    ``cutoff`` is the exclusive upper bound from ``gold_rules.gold_cutoff``.
    """
    gold = build_gold_tables(tables, cutoff)
    return {name: gold[name].select(*column_names(name)) for name in GOLD_TABLES}
