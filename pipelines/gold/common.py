"""Shared helpers for gold incremental MERGE jobs."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.io import table_exists
from pipelines.config import LakehouseConfig


def gold_fqn(cfg: LakehouseConfig, name: str) -> str:
    return f"{cfg.catalog}.{cfg.gold_schema}.{name}"


def read_silver_events(
    spark: SparkSession,
    cfg: LakehouseConfig,
    *,
    for_event_dates: Sequence[date] | None = None,
) -> DataFrame:
    """
    Read silver.events, optionally scoped to specific ``event_date`` values.

    Passing ``for_event_dates`` recomputes metrics only for those partitions while
    still using the canonical silver layer (correct counts for those dates).
    """
    df = spark.read.table(cfg.silver_fqn)
    if for_event_dates is not None and len(for_event_dates) > 0:
        df = df.where(F.col("event_date").isin(*for_event_dates))
    return df


def merge_or_create(
    spark: SparkSession,
    cfg: LakehouseConfig,
    table_name: str,
    incoming: DataFrame,
    *,
    merge_condition: str,
    partition_cols: list[str] | None = None,
) -> None:
    fqn = gold_fqn(cfg, table_name)
    if not table_exists(spark, fqn):
        writer = incoming.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(fqn)
        return

    incoming.createOrReplaceTempView(f"_gold_{table_name}_incoming")
    spark.sql(
        f"""
        MERGE INTO {fqn} AS t
        USING _gold_{table_name}_incoming AS s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
