"""
Databricks job task entrypoints.

Wire each function as a separate task, or call from a notebook with ``getActiveSession()``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime

from pyspark.sql import SparkSession

from pipelines.bronze.bronze_raw_events import start_bronze_stream
from pipelines.config import LakehouseConfig
from pipelines.gold import (
    run_gold_funnel_metrics,
    run_gold_page_metrics,
    run_gold_product_metrics,
    run_gold_search_metrics,
    run_gold_session_metrics,
)
from pipelines.silver.silver_events import run_silver_merge


def task_bronze_raw_events(spark: SparkSession, *, trigger_once: bool = True) -> None:
    start_bronze_stream(spark, trigger_once=trigger_once)


def task_silver_events(
    spark: SparkSession,
    *,
    since_ingested_at: datetime | None = None,
) -> None:
    run_silver_merge(spark, since_ingested_at=since_ingested_at)


def task_gold_all(
    spark: SparkSession,
    *,
    for_event_dates: Sequence[date] | None = None,
) -> None:
    run_gold_search_metrics(spark, for_event_dates=for_event_dates)
    run_gold_product_metrics(spark, for_event_dates=for_event_dates)
    run_gold_session_metrics(spark, for_event_dates=for_event_dates)
    run_gold_page_metrics(spark, for_event_dates=for_event_dates)
    run_gold_funnel_metrics(spark, for_event_dates=for_event_dates)


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    cfg = LakehouseConfig.from_env()
    # Minimal default: run silver then gold (bronze is typically long-lived streaming).
    run_silver_merge(spark, cfg)
    task_gold_all(spark)


if __name__ == "__main__":
    main()
