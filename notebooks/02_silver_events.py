# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: MERGE into `behavr.silver.events`
# MAGIC
# MAGIC Deduplicates on `event_id` (latest `occurred_at_utc` wins). Pass `since_ingested_at` for incremental reads from bronze.

# COMMAND ----------

from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from pipelines.config import LakehouseConfig
from pipelines.silver.silver_events import run_silver_merge

spark = SparkSession.builder.getOrCreate()
cfg = LakehouseConfig.from_env()

# Example: last 24h of bronze rows (tune per SLA)
since = datetime.now(timezone.utc) - timedelta(hours=24)

# COMMAND ----------

run_silver_merge(spark, cfg, since_ingested_at=since)
