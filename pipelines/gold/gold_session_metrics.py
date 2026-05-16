"""
Gold: session analytics — session counts, duration, bounce proxy.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.config import LakehouseConfig
from pipelines.gold.common import merge_or_create, read_silver_events


def build_session_metrics(
    spark: SparkSession,
    cfg: LakehouseConfig,
    *,
    for_event_dates: Sequence[date] | None = None,
):
    e = read_silver_events(spark, cfg, for_event_dates=for_event_dates).where(F.col("session_id").isNotNull())
    per_session = (
        e.groupBy("site_id", "event_date", "session_id")
        .agg(
            F.min("occurred_at_utc").alias("session_start"),
            F.max("occurred_at_utc").alias("session_end"),
            F.count("*").alias("events_in_session"),
            F.countDistinct("page_url").alias("distinct_pages"),
        )
        .withColumn(
            "duration_seconds",
            F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start")),
        )
        .withColumn(
            "is_bounce",
            F.when((F.col("events_in_session") == 1) | (F.col("distinct_pages") == 1), F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
    )
    return (
        per_session.groupBy("site_id", "event_date")
        .agg(
            F.count("*").alias("sessions"),
            F.avg("duration_seconds").alias("avg_duration_seconds"),
            F.sum(F.when(F.col("is_bounce"), F.lit(1)).otherwise(F.lit(0))).alias("bounce_sessions"),
        )
        .withColumn(
            "bounce_rate",
            F.when(F.col("sessions") > 0, F.col("bounce_sessions") / F.col("sessions")).otherwise(F.lit(None)),
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )


def run_gold_session_metrics(
    spark: SparkSession,
    config: LakehouseConfig | None = None,
    *,
    for_event_dates: Sequence[date] | None = None,
) -> None:
    cfg = config or LakehouseConfig.from_env()
    agg = build_session_metrics(spark, cfg, for_event_dates=for_event_dates)
    merge_or_create(
        spark,
        cfg,
        "session_metrics",
        agg,
        merge_condition="t.site_id = s.site_id AND t.event_date = s.event_date",
        partition_cols=["event_date"],
    )
