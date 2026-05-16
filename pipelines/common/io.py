"""Delta / Spark table helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession


def table_exists(spark: SparkSession, fqn: str) -> bool:
    return spark.catalog.tableExists(fqn)
