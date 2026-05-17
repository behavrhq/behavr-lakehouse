"""
Bronze: incremental Auto Loader ingestion from raw JSONL into Delta.

Append-only, minimal transform:
- ingestion metadata
- event_date partition key
- schema evolution support
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.config import LakehouseConfig


def build_bronze_stream(
        spark: SparkSession,
        config: LakehouseConfig | None = None,
) -> DataFrame:
    """
    Build streaming DataFrame using Databricks Auto Loader.
    """
    cfg = config or LakehouseConfig.from_env()

    schema_location = (
        f"/Volumes/behavr/bronze/pipeline_state/schemas/{cfg.bronze_table}"
    )

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(cfg.raw_events_path)
    )


def with_bronze_columns(df: DataFrame) -> DataFrame:
    """
    Add ingestion metadata and normalized event_date partition column.
    """

    df = (
        df
        .withColumnRenamed("eventId", "event_id")
        .withColumnRenamed("eventType", "event_type")
        .withColumnRenamed("occurredAt", "occurred_at")
        .withColumnRenamed("receivedAt", "received_at")
        .withColumnRenamed("sessionId", "session_id")
        .withColumnRenamed("anonymousId", "anonymous_id")
        .withColumnRenamed("siteId", "site_id")
        .withColumnRenamed("userAgent", "user_agent")
        .withColumnRenamed("deviceType", "device_type")
        .withColumnRenamed("browserLanguage", "browser_language")
        .withColumnRenamed("sdkVersion", "sdk_version")
    )

    occurred_at_ts = F.coalesce(
        F.to_timestamp(F.col("occurred_at")),
        F.to_timestamp(
            F.col("occurred_at"),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        ),
        F.to_timestamp(
            F.col("occurred_at"),
            "yyyy-MM-dd'T'HH:mm:ss'Z'"
        ),
    )

    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("occurred_at_ts", occurred_at_ts)
        .withColumn(
            "event_date",
            F.to_date(F.col("occurred_at_ts"))
        )
    )


def start_bronze_stream(
        spark: SparkSession,
        config: LakehouseConfig | None = None,
        *,
        trigger_once: bool = True,
) -> None:
    """
    Start Bronze ingestion stream into Delta Lake.
    """

    cfg = config or LakehouseConfig.from_env()

    checkpoint_location = (
        f"/Volumes/behavr/bronze/pipeline_state/checkpoints/{cfg.bronze_table}"
    )

    print("Starting bronze raw events ingestion")
    print(f"Raw path: {cfg.raw_events_path}")
    print(f"Target table: {cfg.bronze_fqn}")
    print(f"Checkpoint location: {checkpoint_location}")

    df = build_bronze_stream(spark, cfg)
    df = with_bronze_columns(df)

    writer = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .partitionBy("event_date")
    )

    if trigger_once:
        print("Using availableNow trigger")
        writer = writer.trigger(availableNow=True)
    else:
        print("Using processingTime trigger")
        writer = writer.trigger(processingTime="1 minute")

    query = writer.toTable(cfg.bronze_fqn)

    print("Waiting for stream termination")

    query.awaitTermination()

    print("Bronze ingestion completed")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    cfg = LakehouseConfig.from_env()

    start_bronze_stream(
        spark=spark,
        config=cfg,
        trigger_once=True,
    )
