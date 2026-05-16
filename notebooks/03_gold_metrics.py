# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: metrics tables (MERGE)
# MAGIC
# MAGIC Refreshes `search_metrics`, `product_metrics`, `session_metrics`, `page_metrics`, and `funnel_metrics`. Optionally pass `for_event_dates` to limit recomputation to specific partitions.

# COMMAND ----------

from datetime import date, timedelta

from pyspark.sql import SparkSession

from pipelines.config import LakehouseConfig
from pipelines.jobs import task_gold_all

spark = SparkSession.builder.getOrCreate()
cfg = LakehouseConfig.from_env()

yesterday = date.today() - timedelta(days=1)
dates = (yesterday,)

# COMMAND ----------

task_gold_all(spark, for_event_dates=dates)
