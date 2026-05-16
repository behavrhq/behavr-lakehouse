"""
Gold: page popularity by site, date, and URL.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.config import LakehouseConfig
from pipelines.gold.common import merge_or_create, read_silver_events


def build_page_metrics(
    spark: SparkSession,
    cfg: LakehouseConfig,
    *,
    for_event_dates: Sequence[date] | None = None,
):
    e = read_silver_events(spark, cfg, for_event_dates=for_event_dates).where(
        (F.col("page_url").isNotNull()) & (F.length(F.trim(F.col("page_url"))) > 0)
    )
    return (
        e.groupBy("site_id", "event_date", "page_url")
        .agg(
            F.count("*").alias("page_views"),
            F.countDistinct("session_id").alias("distinct_sessions"),
            F.countDistinct("user_id").alias("distinct_users"),
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )


def run_gold_page_metrics(
    spark: SparkSession,
    config: LakehouseConfig | None = None,
    *,
    for_event_dates: Sequence[date] | None = None,
) -> None:
    cfg = config or LakehouseConfig.from_env()
    agg = build_page_metrics(spark, cfg, for_event_dates=for_event_dates)
    merge_or_create(
        spark,
        cfg,
        "page_metrics",
        agg,
        merge_condition="""
            t.site_id = s.site_id
            AND t.event_date = s.event_date
            AND t.page_url <=> s.page_url
        """,
        partition_cols=["event_date"],
    )
