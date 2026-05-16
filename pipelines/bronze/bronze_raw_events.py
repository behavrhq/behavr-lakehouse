"""
Bronze: incremental Auto Loader ingestion from raw JSONL into Delta.

Append-only, minimal transform: ingestion metadata + event_date partition key.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.config import LakehouseConfig


def build_bronze_stream(spark: SparkSession, config: LakehouseConfig | None = None) -> DataFrame:
    """
    Streaming DataFrame: cloudFiles over JSON with schema inference and evolution.
    """
    cfg = config or LakehouseConfig.from_env()
    schema_location = f"{cfg.checkpoint_base}/schemas/{cfg.bronze_table}"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(cfg.raw_events_path)
    )


def _with_bronze_columns(df: DataFrame) -> DataFrame:
    ts = F.coalesce(
        F.to_timestamp(F.col("occurred_at")),
        F.to_timestamp(F.col("occurred_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(F.col("occurred_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("occurred_at_ts", ts)
        .withColumn("event_date", F.to_date(F.col("occurred_at_ts")))
    )


def start_bronze_stream(
    spark: SparkSession,
    config: LakehouseConfig | None = None,
    *,
    trigger_once: bool = False,
) -> None:
    """
    Start streaming write to Unity Catalog bronze table.

    Checkpointing is required for incremental Auto Loader progress.
    """
    cfg = config or LakehouseConfig.from_env()
    checkpoint = f"{cfg.checkpoint_base}/checkpoints/{cfg.bronze_table}"
    df = _with_bronze_columns(build_bronze_stream(spark, cfg))
    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .partitionBy("event_date")
    )
    if trigger_once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="1 minute")
    query = writer.toTable(cfg.bronze_fqn)
    query.awaitTermination()
