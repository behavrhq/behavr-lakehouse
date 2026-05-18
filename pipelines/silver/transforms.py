"""
Silver transforms: UTC timestamps, dedupe keys, URL cleanup, UTM, flattened properties.

Bronze normalizes camelCase SDK fields (see ``with_bronze_columns``); silver maps those
to analytics-friendly names (e.g. ``event_site_id`` → ``site_id``, ``anonymous_id`` → ``user_id``).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _properties_json_expr(df: DataFrame):
    if "properties" not in df.columns:
        return F.lit(None).cast("string")
    dt = next((f.dataType for f in df.schema.fields if f.name == "properties"), None)
    if isinstance(dt, T.StringType):
        return F.col("properties")
    if dt is None:
        return F.lit(None).cast("string")
    return F.to_json(F.col("properties"))


def _get_prop(df: DataFrame, name: str):
    return F.get_json_object(_properties_json_expr(df), f"$.{name}")


def _col_or_null(df: DataFrame, name: str):
    return F.col(name) if name in df.columns else F.lit(None).cast("string")


def normalize_silver_events(df: DataFrame) -> DataFrame:
    fields = set(df.columns)

    site_id = F.coalesce(
        _col_or_null(df, "event_site_id"),
        _col_or_null(df, "site_id"),
    )
    user_id = F.coalesce(
        _col_or_null(df, "user_id"),
        _col_or_null(df, "anonymous_id"),
    )

    occurred = F.coalesce(
        F.to_timestamp(F.col("occurred_at")),
        F.col("occurred_at_ts"),
        F.to_timestamp(F.col("occurred_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(F.col("occurred_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )
    occurred_utc = F.to_utc_timestamp(occurred, "UTC")

    url_candidates = [_col_or_null(df, "url"), _col_or_null(df, "page_url")]
    if "pageUrl" in fields:
        url_candidates.insert(0, F.col("pageUrl"))
    url = F.coalesce(*url_candidates, F.lit(""))
    cleaned_url = F.regexp_replace(F.trim(url), r"\s+", "")
    utm_source = F.coalesce(_get_prop(df, "utm_source"), F.lit(None))
    utm_medium = F.coalesce(_get_prop(df, "utm_medium"), F.lit(None))
    utm_campaign = F.coalesce(_get_prop(df, "utm_campaign"), F.lit(None))
    event_type = F.lower(F.trim(F.coalesce(F.col("event_type"), F.lit(""))))

    search_query = F.coalesce(_get_prop(df, "query"), _get_prop(df, "search_query"), F.lit(None))
    product_id = _get_prop(df, "product_id")
    category_id = _get_prop(df, "category_id")
    zero_results = F.coalesce(
        _get_prop(df, "zero_results"),
        _get_prop(df, "zero_result"),
        F.lit("false"),
    )
    zero_results_bool = F.lower(zero_results.cast("string")).isin("true", "1", "yes")

    referrer = _col_or_null(df, "referrer")

    return (
        df.withColumn("site_id", site_id)
        .withColumn("user_id", user_id)
        .filter(F.col("event_id").isNotNull() & F.col("site_id").isNotNull() & occurred_utc.isNotNull())
        .withColumn("occurred_at_utc", occurred_utc)
        .withColumn("event_date", F.to_date(F.col("occurred_at_utc")))
        .withColumn("event_hour", F.hour(F.col("occurred_at_utc")))
        .withColumn("event_type", event_type)
        .withColumn("page_url", cleaned_url)
        .withColumn("search_query", search_query.cast("string"))
        .withColumn("product_id", product_id.cast("string"))
        .withColumn("category_id", category_id.cast("string"))
        .withColumn("zero_results", zero_results_bool)
        .withColumn("utm_source", utm_source.cast("string"))
        .withColumn("utm_medium", utm_medium.cast("string"))
        .withColumn("utm_campaign", utm_campaign.cast("string"))
        .withColumn("_silver_updated_at", F.current_timestamp())
        .select(
            "event_id",
            "occurred_at_utc",
            "site_id",
            "event_type",
            F.col("session_id").cast("string").alias("session_id"),
            F.col("user_id").cast("string").alias("user_id"),
            "event_date",
            "event_hour",
            "search_query",
            "product_id",
            "category_id",
            "zero_results",
            "page_url",
            referrer.cast("string").alias("referrer"),
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "_silver_updated_at",
        )
    )
