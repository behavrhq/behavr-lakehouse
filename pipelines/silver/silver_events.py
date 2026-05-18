"""
Silver: MERGE incremental load from bronze with deduplication on event_id (latest occurred_at).

Bronze columns are SDK-normalized (``event_site_id``, ``anonymous_id``, ``received_at``, etc.);
``normalize_silver_events`` maps them to the canonical silver schema (``site_id``, ``user_id``, …).
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipelines.common.io import table_exists
from pipelines.config import LakehouseConfig
from pipelines.silver.transforms import normalize_silver_events


def run_silver_merge(
    spark: SparkSession,
    config: LakehouseConfig | None = None,
    *,
    since_ingested_at: datetime | None = None,
    since_received_at: datetime | None = None,
) -> None:
    """
    Merge normalized rows from bronze into silver.

    When ``since_ingested_at`` is set, only bronze rows with ``_ingested_at`` after that
    timestamp are read (pipeline ingest watermark).

    When ``since_received_at`` is set and bronze exposes ``received_at``, only rows with
    ``received_at`` after that value are read (collector / SDK receive time).

    When both are set, both predicates apply (intersection). When neither is set, the full
    bronze table is scanned (backfills).
    """
    cfg = config or LakehouseConfig.from_env()
    bronze = spark.read.table(cfg.bronze_fqn)
    if since_ingested_at is not None:
        bronze = bronze.filter(F.col("_ingested_at") > F.lit(since_ingested_at))
    if since_received_at is not None and "received_at" in bronze.columns:
        bronze = bronze.filter(F.col("received_at") > F.lit(since_received_at))
    incoming = normalize_silver_events(bronze).alias("src")
    if not table_exists(spark, cfg.silver_fqn):
        incoming.write.format("delta").mode("overwrite").partitionBy("event_date").saveAsTable(
            cfg.silver_fqn
        )
        return

    target = cfg.silver_fqn
    incoming.createOrReplaceTempView("_silver_incoming")
    spark.sql(
        f"""
        MERGE INTO {target} AS t
        USING _silver_incoming AS s
        ON t.event_id = s.event_id
        WHEN MATCHED AND s.occurred_at_utc > t.occurred_at_utc THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
