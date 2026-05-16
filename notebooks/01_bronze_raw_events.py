# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Auto Loader → `behavr.bronze.raw_events`
# MAGIC
# MAGIC Incremental JSONL ingestion with schema evolution and checkpointing.
# MAGIC Configure paths via environment variables (`BEHAVR_RAW_EVENTS_PATH`, `BEHAVR_CHECKPOINT_BASE`, etc.) or edit `LakehouseConfig`.

# COMMAND ----------

from pyspark.sql import SparkSession

from pipelines.bronze.bronze_raw_events import start_bronze_stream
from pipelines.config import LakehouseConfig

spark = SparkSession.builder.getOrCreate()
cfg = LakehouseConfig.from_env()

# COMMAND ----------

# For scheduled jobs use trigger_once=True; for continuous ingestion use False.
start_bronze_stream(spark, cfg, trigger_once=True)
