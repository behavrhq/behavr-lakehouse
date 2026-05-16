"""
Gold: funnel-style conversion counts by site and date (step = event_type).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.config import LakehouseConfig
from pipelines.gold.common import merge_or_create, read_silver_events


def build_funnel_metrics(
    spark: SparkSession,
    cfg: LakehouseConfig,
    *,
    for_event_dates: Sequence[date] | None = None,
):
    e = read_silver_events(spark, cfg, for_event_dates=for_event_dates)
    return (
        e.groupBy("site_id", "event_date", "event_type")
        .agg(F.count("*").alias("event_count"), F.countDistinct("session_id").alias("distinct_sessions"))
        .withColumnRenamed("event_type", "funnel_step")
        .withColumn("_gold_updated_at", F.current_timestamp())
    )


def run_gold_funnel_metrics(
    spark: SparkSession,
    config: LakehouseConfig | None = None,
    *,
    for_event_dates: Sequence[date] | None = None,
) -> None:
    cfg = config or LakehouseConfig.from_env()
    agg = build_funnel_metrics(spark, cfg, for_event_dates=for_event_dates)
    merge_or_create(
        spark,
        cfg,
        "funnel_metrics",
        agg,
        merge_condition="""
            t.site_id = s.site_id
            AND t.event_date = s.event_date
            AND t.funnel_step <=> s.funnel_step
        """,
        partition_cols=["event_date"],
    )
