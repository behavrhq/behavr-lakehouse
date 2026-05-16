"""
Gold: search analytics — searches per day, zero-result searches, query volume.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.config import LakehouseConfig
from pipelines.gold.common import merge_or_create, read_silver_events


def build_search_metrics(
    spark: SparkSession,
    cfg: LakehouseConfig,
    *,
    for_event_dates: Sequence[date] | None = None,
):
    e = read_silver_events(spark, cfg, for_event_dates=for_event_dates).where(
        (F.col("search_query").isNotNull()) & (F.length(F.trim(F.col("search_query"))) > 0)
    )
    return (
        e.groupBy("site_id", "event_date", "search_query")
        .agg(
            F.count("*").alias("search_count"),
            F.sum(F.when(F.col("zero_results"), F.lit(1)).otherwise(F.lit(0))).alias("zero_result_searches"),
            F.countDistinct("session_id").alias("distinct_sessions"),
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )


def run_gold_search_metrics(
    spark: SparkSession,
    config: LakehouseConfig | None = None,
    *,
    for_event_dates: Sequence[date] | None = None,
) -> None:
    cfg = config or LakehouseConfig.from_env()
    agg = build_search_metrics(spark, cfg, for_event_dates=for_event_dates)
    merge_or_create(
        spark,
        cfg,
        "search_metrics",
        agg,
        merge_condition="""
            t.site_id = s.site_id
            AND t.event_date = s.event_date
            AND t.search_query <=> s.search_query
        """,
        partition_cols=["event_date"],
    )
